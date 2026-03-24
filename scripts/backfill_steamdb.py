"""Single-app player-history backfill using multi-source merge.

Sources:
- SteamCharts monthly historical rows (long-term history)
- SteamDB embed chart rows (recent high-resolution window)

This script is intentionally scoped to one appid per run.
"""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path
import re
import sys
import time
from dataclasses import dataclass
from typing import Iterable
from bs4 import BeautifulSoup
import requests
from sqlalchemy import func

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database.models import Game, GameInterestSignal, GamePlayerHistory, GamePrice, GameSnapshot, Session

UTC = datetime.timezone.utc


@dataclass(frozen=True)
class PriceHistoryPoint:
    recorded_at: datetime.datetime
    price: float | None
    original_price: float | None = None
    discount_percent: int | None = None
    current_players: int | None = None
    source: str = "steamdb"


@dataclass(frozen=True)
class PlayerHistoryPoint:
    recorded_at: datetime.datetime
    players: int | None
    source: str = "steamdb"


@dataclass(frozen=True)
class SteamDBPlayerSeries:
    point_start_ms: int
    point_interval_ms: int
    data_points: int
    rows: list[PlayerHistoryPoint]


@dataclass(frozen=True)
class SteamDBPlayerBackfillResult:
    windows_fetched: int
    earliest_timestamp: datetime.datetime | None
    latest_timestamp: datetime.datetime | None
    total_rows_parsed: int
    rows: list[PlayerHistoryPoint]


@dataclass(frozen=True)
class HistoricalPlayerBackfillResult:
    source_name: str
    rows_parsed: int
    earliest_timestamp: datetime.datetime | None
    latest_timestamp: datetime.datetime | None
    rows: list[PlayerHistoryPoint]


@dataclass(frozen=True)
class AppBackfillResult:
    steam_appid: str
    game_id: int | None
    success: bool
    rows_merged: int
    inserted_rows: int
    skipped_rows: int
    earliest_timestamp: datetime.datetime | None
    latest_timestamp: datetime.datetime | None
    error: str | None = None


STEAMDB_EMBED_URL_TEMPLATE = "https://steamdb.info/embed/?appid={steam_appid}"
STEAMCHARTS_APP_URL_TEMPLATE = "https://steamcharts.com/app/{steam_appid}"
STEAMDB_REQUEST_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "accept-language": "en-US,en;q=0.9",
}
STEAMCHARTS_REQUEST_HEADERS = {
    "user-agent": STEAMDB_REQUEST_HEADERS["user-agent"],
    "accept-language": "en-US,en;q=0.9",
}
STEAMDB_MAX_RETRIES = 5
STEAMDB_RETRY_BACKOFF_SECONDS = 2.0

PLAYERS_SERIES_PATTERN = re.compile(
    r"name:\s*'Players'[\s\S]*?"
    r"pointStart:\s*(?P<point_start>\d+)[\s\S]*?"
    r"pointInterval:\s*(?P<point_interval>\d+)[\s\S]*?"
    r"data:\s*\[(?P<data>[^\]]*)\]",
    re.IGNORECASE,
)


def _normalize_ts(value: datetime.datetime) -> datetime.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def resolve_game_id_for_steam_appid(session, steam_appid: str) -> int:
    normalized_appid = str(int(str(steam_appid).strip()))
    game = session.query(Game).filter(Game.appid == normalized_appid).first()
    if game is None:
        raise ValueError(f"No game mapping found for steam_appid={normalized_appid}")
    return int(game.id)


def _normalize_appid_token(value: str) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        normalized = str(int(raw))
    except Exception:
        return None
    return normalized if int(normalized) > 0 else None


def parse_appid_list_input(raw_input: str) -> list[str]:
    raw_value = str(raw_input or "").strip()
    if not raw_value:
        return []
    input_path = Path(raw_value)
    if input_path.exists() and input_path.is_file():
        content = input_path.read_text(encoding="utf-8")
        tokens = re.split(r"[\s,]+", content)
    else:
        tokens = re.split(r"[\s,]+", raw_value)
    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        normalized = _normalize_appid_token(token)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def select_top_steam_appids(session, top_n: int) -> list[str]:
    capped_top_n = max(1, min(int(top_n), 500))
    # Priority blend:
    # - current/live activity (last_player_count, snapshot current_players)
    # - popularity scores
    # - user interaction intensity from discovery/search flows
    rows = (
        session.query(
            Game.appid,
            func.coalesce(Game.last_player_count, GameSnapshot.current_players, 0).label("activity"),
            func.coalesce(Game.popularity_score, GameSnapshot.popularity_score, 0.0).label("popularity"),
            func.coalesce(GameInterestSignal.click_count, 0).label("interest_clicks"),
            func.coalesce(GameInterestSignal.wishlist_count, 0).label("interest_wishlist"),
        )
        .outerjoin(GameSnapshot, GameSnapshot.game_id == Game.id)
        .outerjoin(GameInterestSignal, GameInterestSignal.game_id == Game.id)
        .filter(
            Game.appid.isnot(None),
            Game.appid != "",
            Game.is_released == 1,
        )
        .order_by(
            func.coalesce(Game.last_player_count, GameSnapshot.current_players, 0).desc(),
            func.coalesce(Game.popularity_score, GameSnapshot.popularity_score, 0.0).desc(),
            func.coalesce(GameInterestSignal.click_count, 0).desc(),
            func.coalesce(GameInterestSignal.wishlist_count, 0).desc(),
            Game.id.asc(),
        )
        .limit(capped_top_n * 4)
        .all()
    )
    prioritized: list[str] = []
    seen: set[str] = set()
    for row in rows:
        normalized = _normalize_appid_token(row.appid)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        prioritized.append(normalized)
        if len(prioritized) >= capped_top_n:
            break
    return prioritized


def _parse_data_token(raw: str) -> int | None:
    value = str(raw or "").strip().lower()
    if not value or value == "null":
        return None
    return int(value)


def _parse_steamdb_player_embed_html(html: str) -> SteamDBPlayerSeries:
    match = PLAYERS_SERIES_PATTERN.search(str(html or ""))
    if not match:
        raise ValueError("Could not locate SteamDB Players series in embed response.")

    point_start_ms = int(match.group("point_start"))
    point_interval_ms = int(match.group("point_interval"))
    raw_data = match.group("data")
    tokens = [token.strip() for token in raw_data.split(",") if token.strip()]
    values = [_parse_data_token(token) for token in tokens]

    rows: list[PlayerHistoryPoint] = []
    for index, players in enumerate(values):
        if players is None:
            continue
        ts_ms = point_start_ms + (index * point_interval_ms)
        recorded_at = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
        rows.append(
            PlayerHistoryPoint(
                recorded_at=recorded_at,
                players=int(players),
                source="steamdb",
            )
        )

    return SteamDBPlayerSeries(
        point_start_ms=point_start_ms,
        point_interval_ms=point_interval_ms,
        data_points=len(values),
        rows=rows,
    )


def _fetch_steamdb_embed_html(url: str, params: dict[str, str] | None = None) -> str:
    return _fetch_text_with_retry(
        source_name="steamdb",
        url=url,
        headers=STEAMDB_REQUEST_HEADERS,
        params=params,
    )


def _fetch_text_with_retry(
    source_name: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, STEAMDB_MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code in {429, 403, 500, 502, 503, 504}:
                response.raise_for_status()
            response.raise_for_status()
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt >= STEAMDB_MAX_RETRIES:
                break
            sleep_seconds = STEAMDB_RETRY_BACKOFF_SECONDS * attempt
            print(
                f"{source_name} fetch retry: "
                f"attempt={attempt}/{STEAMDB_MAX_RETRIES} "
                f"params={params or {}} "
                f"sleep={sleep_seconds:.1f}s"
            )
            time.sleep(sleep_seconds)
    raise RuntimeError(f"{source_name} fetch failed after {STEAMDB_MAX_RETRIES} retries: {last_error}")


def _fetch_steamdb_player_window(steam_appid: str, params: dict[str, str] | None = None) -> SteamDBPlayerSeries:
    normalized_appid = str(int(str(steam_appid).strip()))
    url = STEAMDB_EMBED_URL_TEMPLATE.format(steam_appid=normalized_appid)
    html = _fetch_steamdb_embed_html(url, params=params)
    return _parse_steamdb_player_embed_html(html)


def _parse_month_start(label: str) -> datetime.datetime | None:
    text = str(label or "").strip()
    if not text:
        return None
    try:
        month_dt = datetime.datetime.strptime(text, "%B %Y")
    except ValueError:
        return None
    return month_dt.replace(tzinfo=UTC, day=1, hour=0, minute=0, second=0, microsecond=0)


def _parse_int_token(raw: str) -> int | None:
    text = str(raw or "").strip().replace(",", "")
    if not text or text in {"-", "—"}:
        return None
    return int(round(float(text)))


def fetch_historical_player_history(steam_appid: str) -> HistoricalPlayerBackfillResult:
    normalized_appid = str(int(str(steam_appid).strip()))
    url = STEAMCHARTS_APP_URL_TEMPLATE.format(steam_appid=normalized_appid)
    html = _fetch_text_with_retry(
        source_name="historical_import",
        url=url,
        headers=STEAMCHARTS_REQUEST_HEADERS,
    )

    soup = BeautifulSoup(html, "html.parser")
    rows: list[PlayerHistoryPoint] = []
    for tr in soup.select("table.common-table tbody tr"):
        cells = tr.find_all("td")
        if len(cells) < 5:
            continue
        month_label = cells[0].get_text(" ", strip=True)
        recorded_at = _parse_month_start(month_label)
        if recorded_at is None:
            # Skip non-calendar labels such as "Last 30 Days".
            continue
        peak_players = _parse_int_token(cells[4].get_text(" ", strip=True))
        if peak_players is None:
            continue
        rows.append(
            PlayerHistoryPoint(
                recorded_at=recorded_at,
                players=peak_players,
                source="historical_import",
            )
        )

    deduped_by_ts: dict[datetime.datetime, PlayerHistoryPoint] = {}
    for row in rows:
        deduped_by_ts[_normalize_ts(row.recorded_at)] = row
    normalized_rows = [deduped_by_ts[key] for key in sorted(deduped_by_ts.keys())]
    earliest = normalized_rows[0].recorded_at if normalized_rows else None
    latest = normalized_rows[-1].recorded_at if normalized_rows else None
    return HistoricalPlayerBackfillResult(
        source_name="historical_import",
        rows_parsed=len(normalized_rows),
        earliest_timestamp=earliest,
        latest_timestamp=latest,
        rows=normalized_rows,
    )


def _window_signature(series: SteamDBPlayerSeries) -> tuple[int, int, int]:
    return (series.point_start_ms, series.point_interval_ms, series.data_points)


def _build_window_param_candidates(anchor_ms: int | None, iteration_index: int) -> list[dict[str, str]]:
    # Candidate params discovered by probing SteamDB embed path.
    # Keep this list bounded and deterministic to avoid hammering.
    if anchor_ms is None:
        return [{}]
    anchor_str = str(max(0, int(anchor_ms) - 1))
    return [
        {},
        {"before": anchor_str},
        {"to": anchor_str},
        {"start": anchor_str},
        {"from": anchor_str},
        {"cursor": anchor_str},
        {"offset": str(iteration_index)},
    ]


def _merge_player_windows(windows: list[SteamDBPlayerSeries]) -> list[PlayerHistoryPoint]:
    deduped: dict[datetime.datetime, PlayerHistoryPoint] = {}
    for window in windows:
        for row in window.rows:
            normalized_ts = _normalize_ts(row.recorded_at)
            deduped[normalized_ts] = PlayerHistoryPoint(
                recorded_at=normalized_ts,
                players=row.players,
                source="steamdb",
            )
    return [deduped[key] for key in sorted(deduped.keys())]


def merge_player_sources(
    historical_rows: list[PlayerHistoryPoint],
    steamdb_rows: list[PlayerHistoryPoint],
) -> list[PlayerHistoryPoint]:
    source_priority = {
        "historical_import": 1,
        "steamdb": 2,
    }
    merged: dict[datetime.datetime, PlayerHistoryPoint] = {}
    for row in historical_rows + steamdb_rows:
        ts = _normalize_ts(row.recorded_at)
        existing = merged.get(ts)
        if existing is None:
            merged[ts] = PlayerHistoryPoint(recorded_at=ts, players=row.players, source=row.source)
            continue
        existing_priority = source_priority.get(str(existing.source), 0)
        candidate_priority = source_priority.get(str(row.source), 0)
        if candidate_priority >= existing_priority:
            merged[ts] = PlayerHistoryPoint(recorded_at=ts, players=row.players, source=row.source)
    return [merged[key] for key in sorted(merged.keys())]


def fetch_steamdb_player_history(
    steam_appid: str,
    max_windows: int,
    request_delay_seconds: float,
) -> SteamDBPlayerBackfillResult:
    """
    Fetch and stitch SteamDB player-history windows moving backward in time.
    Stops when windows repeat or no older earliest timestamp appears.
    """
    windows: list[SteamDBPlayerSeries] = []
    seen_signatures: set[tuple[int, int, int]] = set()
    earliest_anchor_ms: int | None = None
    stable_earliest_windows = 0

    for iteration in range(1, max_windows + 1):
        candidates = _build_window_param_candidates(earliest_anchor_ms, iteration)
        chosen_window: SteamDBPlayerSeries | None = None
        chosen_params: dict[str, str] | None = None

        for candidate in candidates:
            series = _fetch_steamdb_player_window(steam_appid, params=candidate or None)
            sig = _window_signature(series)
            if sig in seen_signatures:
                continue
            chosen_window = series
            chosen_params = candidate
            break

        if chosen_window is None:
            print(
                "steamdb window iteration stop: "
                "all candidate windows repeated"
            )
            break

        windows.append(chosen_window)
        seen_signatures.add(_window_signature(chosen_window))

        window_earliest_ms = chosen_window.point_start_ms
        window_latest_ms = (
            chosen_window.point_start_ms
            + max(0, chosen_window.data_points - 1) * chosen_window.point_interval_ms
        )
        print(
            "steamdb window fetched: "
            f"iteration={iteration} "
            f"params={chosen_params or {}} "
            f"pointStart={chosen_window.point_start_ms} "
            f"pointInterval={chosen_window.point_interval_ms} "
            f"points={chosen_window.data_points} "
            f"window={datetime.datetime.fromtimestamp(window_earliest_ms / 1000.0, tz=UTC).isoformat()}.."
            f"{datetime.datetime.fromtimestamp(window_latest_ms / 1000.0, tz=UTC).isoformat()}"
        )

        if earliest_anchor_ms is None or window_earliest_ms < earliest_anchor_ms:
            earliest_anchor_ms = window_earliest_ms
            stable_earliest_windows = 0
        else:
            stable_earliest_windows += 1

        if stable_earliest_windows >= 1:
            print(
                "steamdb window iteration stop: "
                "earliest timestamp no longer moving backward"
            )
            break

        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)

    merged_rows = _merge_player_windows(windows)
    earliest_ts = merged_rows[0].recorded_at if merged_rows else None
    latest_ts = merged_rows[-1].recorded_at if merged_rows else None
    total_rows_parsed = sum(window.data_points for window in windows)
    return SteamDBPlayerBackfillResult(
        windows_fetched=len(windows),
        earliest_timestamp=earliest_ts,
        latest_timestamp=latest_ts,
        total_rows_parsed=total_rows_parsed,
        rows=merged_rows,
    )


def fetch_steamdb_price_history(steam_appid: str) -> list[PriceHistoryPoint]:
    """Price-history backfill is intentionally out of scope for this milestone."""
    _ = steam_appid
    return []


def _price_row_exists(session, game_id: int, recorded_at: datetime.datetime, source: str | None) -> bool:
    query = session.query(GamePrice.id).filter(
        GamePrice.game_id == game_id,
        GamePrice.recorded_at == recorded_at,
    )
    if source is None:
        query = query.filter(GamePrice.source.is_(None))
    else:
        query = query.filter(GamePrice.source == source)
    return query.first() is not None


def _player_row_exists(session, game_id: int, recorded_at: datetime.datetime) -> bool:
    query = session.query(GamePlayerHistory.id).filter(
        GamePlayerHistory.game_id == game_id,
        GamePlayerHistory.recorded_at == recorded_at,
    )
    return query.first() is not None


def insert_price_history_points(
    session,
    game_id: int,
    rows: Iterable[PriceHistoryPoint],
) -> tuple[int, int]:
    inserted = 0
    skipped_duplicate = 0
    seen_batch_keys: set[tuple[datetime.datetime, str | None]] = set()
    for row in rows:
        recorded_at = _normalize_ts(row.recorded_at)
        source = str(row.source).strip() if row.source is not None else None
        key = (recorded_at, source)
        if key in seen_batch_keys:
            skipped_duplicate += 1
            continue
        seen_batch_keys.add(key)
        if _price_row_exists(session, game_id, recorded_at, source):
            skipped_duplicate += 1
            continue
        session.add(
            GamePrice(
                game_id=game_id,
                game_name=None,
                price=row.price,
                original_price=row.original_price,
                discount_percent=row.discount_percent,
                current_players=row.current_players,
                store_url=None,
                source=source,
                is_backfill=True,
                recorded_at=recorded_at,
            )
        )
        inserted += 1
    return inserted, skipped_duplicate


def insert_player_history_points(
    session,
    game_id: int,
    rows: Iterable[PlayerHistoryPoint],
) -> tuple[int, int]:
    inserted = 0
    skipped_duplicate = 0
    seen_batch_keys: set[tuple[datetime.datetime, str | None]] = set()
    for row in rows:
        recorded_at = _normalize_ts(row.recorded_at)
        source = str(row.source).strip() if row.source is not None else None
        key = (recorded_at, source)
        if key in seen_batch_keys:
            skipped_duplicate += 1
            continue
        seen_batch_keys.add(key)
        if _player_row_exists(session, game_id, recorded_at):
            skipped_duplicate += 1
            continue
        session.add(
            GamePlayerHistory(
                game_id=game_id,
                current_players=row.players,
                source=source,
                is_backfill=True,
                recorded_at=recorded_at,
            )
        )
        inserted += 1
    return inserted, skipped_duplicate


def backfill_single_appid(
    session,
    steam_appid: str,
    max_windows: int,
    request_delay_seconds: float,
) -> AppBackfillResult:
    normalized_appid = _normalize_appid_token(steam_appid)
    if not normalized_appid:
        raise ValueError(f"Invalid steam_appid: {steam_appid!r}")

    game_id = resolve_game_id_for_steam_appid(session, normalized_appid)
    print(f"resolved steam_appid={normalized_appid} -> game_id={game_id}")

    historical_backfill = fetch_historical_player_history(normalized_appid)
    print(
        "historical player payload: "
        f"source={historical_backfill.source_name} "
        f"rows_parsed={historical_backfill.rows_parsed} "
        f"window="
        f"{historical_backfill.earliest_timestamp.isoformat() if historical_backfill.earliest_timestamp else 'n/a'}.."
        f"{historical_backfill.latest_timestamp.isoformat() if historical_backfill.latest_timestamp else 'n/a'}"
    )

    player_backfill = fetch_steamdb_player_history(
        normalized_appid,
        max_windows=max(1, int(max_windows)),
        request_delay_seconds=max(0.0, float(request_delay_seconds)),
    )
    player_rows = merge_player_sources(historical_backfill.rows, player_backfill.rows)
    first_ts = player_rows[0].recorded_at.isoformat() if player_rows else "n/a"
    last_ts = player_rows[-1].recorded_at.isoformat() if player_rows else "n/a"
    print(
        "steamdb player payload: "
        f"windows_fetched={player_backfill.windows_fetched} "
        f"rows_parsed_raw={player_backfill.total_rows_parsed} "
        f"rows_recent_merged={len(player_backfill.rows)} "
        f"rows_all_sources_merged={len(player_rows)} "
        f"window={first_ts}..{last_ts}"
    )

    inserted_players, skipped_players = insert_player_history_points(session, game_id, player_rows)
    print(
        "insert summary: "
        f"players inserted={inserted_players} skipped_duplicate={skipped_players}; "
        "prices inserted=0 skipped_duplicate=0 (price backfill not enabled in this milestone)"
    )

    return AppBackfillResult(
        steam_appid=normalized_appid,
        game_id=game_id,
        success=True,
        rows_merged=len(player_rows),
        inserted_rows=inserted_players,
        skipped_rows=skipped_players,
        earliest_timestamp=player_rows[0].recorded_at if player_rows else None,
        latest_timestamp=player_rows[-1].recorded_at if player_rows else None,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Player-history backfill (single app or safe top-game batch).")
    parser.add_argument("--steam-appid", help="Steam appid to resolve and backfill (single app mode).")
    parser.add_argument("--top-n", type=int, help="Process top-N high-priority released games by activity/popularity.")
    parser.add_argument(
        "--appid-list",
        help="Comma-separated appids OR a file path containing appids (whitespace/comma separated).",
    )
    parser.add_argument("--max-windows", type=int, default=16, help="Max backward windows to fetch for one appid.")
    parser.add_argument("--request-delay-seconds", type=float, default=1.0, help="Delay between window requests.")
    parser.add_argument("--game-delay-seconds", type=float, default=1.5, help="Delay between games in batch mode.")
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Persist inserts. Default is dry-run rollback.",
    )
    args = parser.parse_args()

    selected_modes = sum(
        1 for value in (args.steam_appid, args.top_n, args.appid_list) if value not in (None, "")
    )
    if selected_modes != 1:
        print("error: specify exactly one mode: --steam-appid OR --top-n OR --appid-list")
        return 1

    target_appids: list[str]
    bootstrap_session = Session()
    try:
        if args.steam_appid:
            normalized = _normalize_appid_token(args.steam_appid)
            if not normalized:
                print(f"error: invalid --steam-appid value {args.steam_appid!r}")
                return 1
            target_appids = [normalized]
        elif args.top_n is not None:
            if int(args.top_n) <= 0:
                print("error: --top-n must be > 0")
                return 1
            target_appids = select_top_steam_appids(bootstrap_session, int(args.top_n))
        else:
            target_appids = parse_appid_list_input(str(args.appid_list))
        if not target_appids:
            print("error: no valid appids selected for processing")
            return 1
    finally:
        bootstrap_session.close()

    print(
        "batch targets: "
        f"count={len(target_appids)} "
        f"mode={'single' if args.steam_appid else ('top_n' if args.top_n else 'appid_list')}"
    )

    success_count = 0
    failure_count = 0
    total_inserted = 0
    total_skipped = 0
    results: list[AppBackfillResult] = []
    run_started = time.time()

    for index, appid in enumerate(target_appids, start=1):
        print(f"--- processing {index}/{len(target_appids)} appid={appid} ---")
        session = Session()
        try:
            result = backfill_single_appid(
                session,
                steam_appid=appid,
                max_windows=max(1, int(args.max_windows)),
                request_delay_seconds=max(0.0, float(args.request_delay_seconds)),
            )
            if args.commit:
                session.commit()
                print("commit: applied")
            else:
                session.rollback()
                print("dry-run: rolled back (use --commit to persist)")
            success_count += 1
            total_inserted += result.inserted_rows
            total_skipped += result.skipped_rows
            results.append(result)
        except Exception as exc:
            session.rollback()
            failure_count += 1
            error_text = str(exc)
            print(f"error: appid={appid} failed with {error_text}")
            results.append(
                AppBackfillResult(
                    steam_appid=appid,
                    game_id=None,
                    success=False,
                    rows_merged=0,
                    inserted_rows=0,
                    skipped_rows=0,
                    earliest_timestamp=None,
                    latest_timestamp=None,
                    error=error_text,
                )
            )
        finally:
            session.close()

        if index < len(target_appids) and args.game_delay_seconds and float(args.game_delay_seconds) > 0:
            delay_seconds = float(args.game_delay_seconds)
            print(f"batch delay: sleeping {delay_seconds:.2f}s before next appid")
            time.sleep(delay_seconds)

    elapsed = round(time.time() - run_started, 2)
    print(
        "run summary: "
        f"processed={len(target_appids)} "
        f"success={success_count} "
        f"failed={failure_count} "
        f"inserted={total_inserted} "
        f"skipped_duplicate={total_skipped} "
        f"elapsed_seconds={elapsed}"
    )

    failures = [result for result in results if not result.success]
    if failures:
        print("failed appids:")
        for failure in failures:
            print(f"  appid={failure.steam_appid} error={failure.error}")

    return 0 if failure_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
