import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import datetime
import os
import time
from typing import Iterable

import requests
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from database.models import Game, Session

STEAM_STORE_APPLIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
DEFAULT_LIMIT = 10_000
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_RETRIES = 3
BATCH_SIZE = 500
STORE_PAGE_SIZE = 50_000
DEFAULT_HOLD_TIER = "ROLLOUT_HOLD"
DEFAULT_HOLD_UNTIL = "2100-01-01T00:00:00+00:00"

# Conservative keyword filter to bias toward real games.
EXCLUDED_NAME_TOKENS = {
    "demo",
    "soundtrack",
    "ost",
    "dlc",
    "dedicated server",
    "server",
    "tool",
    "editor",
    "sdk",
    "test",
    "benchmark",
    "driver",
    "wallpaper",
    "avatar",
    "trailer",
}


def _chunks(items: list[dict], size: int) -> Iterable[list[dict]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _steam_api_key() -> str | None:
    api_key = os.environ.get("STEAM_API_KEY")
    if not api_key:
        raise RuntimeError("STEAM_API_KEY environment variable not set")
    return api_key


def _format_http_error(endpoint: str, response: requests.Response) -> RuntimeError:
    body_snippet = (response.text or "")[:240].replace("\n", " ")
    return RuntimeError(
        f"Steam app list request failed endpoint={endpoint} status={response.status_code} body={body_snippet!r}"
    )


def _fetch_store_applist_page(last_appid: int, api_key: str) -> tuple[list[dict], bool, int]:
    params = {
        "key": api_key,
        "max_results": STORE_PAGE_SIZE,
        "last_appid": int(last_appid),
        "include_games": "true",
        "include_dlc": "false",
        "include_software": "false",
        "include_videos": "false",
        "include_hardware": "false",
    }

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = requests.get(
                STEAM_STORE_APPLIST_URL,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code >= 400:
                raise _format_http_error(STEAM_STORE_APPLIST_URL, response)

            payload = response.json()
            body = payload.get("response") or {}
            apps = body.get("apps") or []
            have_more = bool(body.get("have_more_results", False))
            next_last_appid = int(body.get("last_appid") or 0)

            if not isinstance(apps, list):
                raise RuntimeError(
                    f"Unexpected Steam app list response format endpoint={STEAM_STORE_APPLIST_URL}"
                )
            return apps, have_more, next_last_appid
        except Exception as exc:
            last_error = exc
            if attempt < REQUEST_RETRIES:
                wait_seconds = min(5, attempt * 1.5)
                print(
                    f"Steam app list page fetch failed (attempt {attempt}/{REQUEST_RETRIES}), "
                    f"retrying in {wait_seconds:.1f}s..."
                )
                time.sleep(wait_seconds)
    raise RuntimeError(f"Failed to fetch Steam app list page endpoint={STEAM_STORE_APPLIST_URL}: {last_error}")


def fetch_steam_applist(limit: int | None = None) -> list[dict]:
    api_key = _steam_api_key()
    all_apps: list[dict] = []
    seen: set[int] = set()
    last_appid = 0
    page = 0

    while True:
        page += 1
        apps, have_more, next_last_appid = _fetch_store_applist_page(last_appid=last_appid, api_key=api_key)
        print(
            f"Fetched app list page={page} count={len(apps)} "
            f"have_more={have_more} last_appid={next_last_appid}"
        )

        for app in apps:
            appid = app.get("appid")
            if appid is None:
                continue
            appid_int = int(appid)
            if appid_int in seen:
                continue
            seen.add(appid_int)
            all_apps.append(app)

        if limit is not None and len(all_apps) >= int(limit):
            break
        if not have_more:
            break
        if next_last_appid <= last_appid:
            raise RuntimeError(
                f"Steam app list pagination stalled endpoint={STEAM_STORE_APPLIST_URL} "
                f"last_appid={last_appid} next_last_appid={next_last_appid}"
            )
        last_appid = next_last_appid

    return all_apps


def is_likely_game(name: str) -> bool:
    clean = (name or "").strip()
    if len(clean) <= 2:
        return False
    lower = clean.lower()
    if not any(ch.isalpha() for ch in lower):
        return False
    for token in EXCLUDED_NAME_TOKENS:
        if token in lower:
            return False
    return True


def build_seed_rows(apps: list[dict], limit: int) -> list[dict]:
    seed_rows: list[dict] = []
    seen_appids: set[str] = set()

    for app in apps:
        appid = app.get("appid")
        name = (app.get("name") or "").strip()
        if appid is None or not is_likely_game(name):
            continue

        appid_text = str(appid)
        if appid_text in seen_appids:
            continue
        seen_appids.add(appid_text)

        seed_rows.append(
            {
                "appid": appid_text,
                "name": name,
                "store_url": f"https://store.steampowered.com/app/{appid_text}/",
            }
        )
        if len(seed_rows) >= limit:
            break

    return seed_rows


def _is_hold_tier(priority_tier: str | None, hold_tier: str) -> bool:
    return str(priority_tier or "").strip().upper() == hold_tier


def insert_missing_games_and_queue_dirty(
    seed_rows: list[dict],
    *,
    hold_new_games: bool,
    hold_tier: str,
    hold_until: datetime.datetime,
    queue_held_games: bool,
) -> tuple[int, int]:
    if not seed_rows:
        return 0, 0

    normalized_hold_tier = (hold_tier or DEFAULT_HOLD_TIER).strip().upper() or DEFAULT_HOLD_TIER
    inserted_new_count = 0
    queued_count = 0
    session = Session()
    try:
        if hold_new_games:
            insert_sql = text(
                """
                INSERT INTO games (
                    appid,
                    name,
                    store_url,
                    is_released,
                    priority,
                    priority_tier,
                    next_refresh_at
                )
                VALUES (:appid, :name, :store_url, 1, 0, :priority_tier, :next_refresh_at)
                ON CONFLICT (appid) DO NOTHING
                """
            )
        else:
            insert_sql = text(
                """
                INSERT INTO games (appid, name, store_url, is_released, priority)
                VALUES (:appid, :name, :store_url, 1, 0)
                ON CONFLICT (appid) DO NOTHING
                """
            )

        existing_appids: set[str] = set()
        for batch in _chunks(seed_rows, BATCH_SIZE):
            batch_payload = batch
            if hold_new_games:
                batch_payload = [
                    {
                        **row,
                        "priority_tier": normalized_hold_tier,
                        "next_refresh_at": hold_until,
                    }
                    for row in batch
                ]
            batch_appids = [row["appid"] for row in batch]
            existing_rows = (
                session.query(Game.appid)
                .filter(Game.appid.in_(batch_appids))
                .all()
            )
            existing_appids.update(str(row[0]) for row in existing_rows if row and row[0] is not None)
            session.execute(insert_sql, batch_payload)
            session.commit()

        appids = [row["appid"] for row in seed_rows]
        id_by_appid: dict[str, int] = {}
        priority_by_id: dict[int, str | None] = {}
        for idx in range(0, len(appids), BATCH_SIZE):
            appid_batch = appids[idx : idx + BATCH_SIZE]
            rows = (
                session.query(Game.id, Game.appid, Game.priority_tier)
                .filter(Game.appid.in_(appid_batch))
                .all()
            )
            for game_id, appid, priority_tier in rows:
                id_by_appid[str(appid)] = int(game_id)
                priority_by_id[int(game_id)] = priority_tier

        inserted_new_count = len([appid for appid in id_by_appid.keys() if appid not in existing_appids])
        game_ids = sorted(set(id_by_appid.values()))
        if hold_new_games and not queue_held_games:
            game_ids = [
                game_id
                for game_id in game_ids
                if not _is_hold_tier(priority_by_id.get(game_id), normalized_hold_tier)
            ]

        if session.bind and session.bind.dialect.name == "postgresql":
            dirty_sql = text(
                """
                INSERT INTO dirty_games (
                    game_id,
                    first_seen_at,
                    last_seen_at,
                    updated_at,
                    retry_count
                )
                VALUES (:game_id, now(), now(), now(), 0)
                ON CONFLICT (game_id) DO UPDATE
                SET updated_at = EXCLUDED.updated_at
                """
            )
        else:
            dirty_sql = text(
                """
                INSERT INTO dirty_games (
                    game_id,
                    first_seen_at,
                    last_seen_at,
                    updated_at,
                    retry_count
                )
                VALUES (:game_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
                ON CONFLICT(game_id) DO UPDATE
                SET updated_at = excluded.updated_at
                """
            )

        for game_id in game_ids:
            session.execute(dirty_sql, {"game_id": int(game_id)})
            queued_count += 1
        session.commit()
        return inserted_new_count, queued_count
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run(
    limit: int,
    *,
    hold_new_games: bool,
    hold_tier: str,
    hold_until: datetime.datetime,
    queue_held_games: bool,
) -> None:
    print("Fetching Steam app catalog...")
    apps = fetch_steam_applist(limit=limit)
    print(f"Fetched {len(apps)} Steam apps")

    seed_rows = build_seed_rows(apps, limit=limit)
    print(f"Filtered to {len(seed_rows)} likely games (limit={limit}).")

    inserted_count, queued_count = insert_missing_games_and_queue_dirty(
        seed_rows,
        hold_new_games=hold_new_games,
        hold_tier=hold_tier,
        hold_until=hold_until,
        queue_held_games=queue_held_games,
    )
    print(f"Inserted {inserted_count} new games")
    print(f"Queued {queued_count} dirty jobs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Steam game catalog into games and dirty_games.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of likely games to seed.")
    parser.add_argument(
        "--hold-new-games",
        action="store_true",
        help="Mark newly inserted games as rollout-held (priority_tier + far-future next_refresh_at).",
    )
    parser.add_argument(
        "--hold-tier",
        default=DEFAULT_HOLD_TIER,
        help=f"Priority tier value for held games (default: {DEFAULT_HOLD_TIER}).",
    )
    parser.add_argument(
        "--hold-until",
        default=DEFAULT_HOLD_UNTIL,
        help=f"ISO timestamp used for held next_refresh_at (default: {DEFAULT_HOLD_UNTIL}).",
    )
    parser.add_argument(
        "--queue-held-games",
        action="store_true",
        help="Also enqueue held games in dirty_games (disabled by default for safer staged rollout).",
    )
    args = parser.parse_args()
    hold_until = datetime.datetime.fromisoformat(str(args.hold_until).strip())
    if hold_until.tzinfo is None:
        hold_until = hold_until.replace(tzinfo=datetime.timezone.utc)
    run(
        limit=max(1, int(args.limit)),
        hold_new_games=bool(args.hold_new_games),
        hold_tier=str(args.hold_tier or DEFAULT_HOLD_TIER),
        hold_until=hold_until.astimezone(datetime.timezone.utc),
        queue_held_games=bool(args.queue_held_games),
    )
