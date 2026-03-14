import time
from typing import Iterable

import requests

from database.models import Game, Session
from logger_config import setup_logger

logger = setup_logger("bootstrap_full_catalog")

STEAM_FULL_APPLIST_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
REQUEST_TIMEOUT_SECONDS = 45
REQUEST_RETRIES = 4
BATCH_SIZE = 1000


def _chunked(items: list[dict], size: int) -> Iterable[list[dict]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def fetch_full_applist() -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            logger.info(
                "Fetching Steam full app list url=%s attempt=%s/%s",
                STEAM_FULL_APPLIST_URL,
                attempt,
                REQUEST_RETRIES,
            )
            response = requests.get(STEAM_FULL_APPLIST_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
            applist = payload.get("applist") or {}
            apps = applist.get("apps") or []
            if not isinstance(apps, list):
                raise RuntimeError("Steam full app list response format is invalid.")
            return apps
        except Exception as exc:
            last_error = exc
            if attempt >= REQUEST_RETRIES:
                break
            wait_seconds = min(10.0, attempt * 1.5)
            logger.warning(
                "Steam full app list fetch failed attempt=%s/%s retry_in=%.1fs error=%s",
                attempt,
                REQUEST_RETRIES,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Failed to fetch Steam full app list: {last_error}")


def upsert_catalog_rows(apps: list[dict]) -> tuple[int, int, int]:
    inserted_count = 0
    updated_count = 0
    skipped_count = 0
    seen_appids: set[str] = set()

    session = Session()
    try:
        for batch in _chunked(apps, BATCH_SIZE):
            normalized_rows: list[dict[str, str]] = []
            for app in batch:
                appid_raw = app.get("appid")
                name_raw = app.get("name")
                if appid_raw is None:
                    skipped_count += 1
                    continue

                appid = str(appid_raw).strip()
                name = str(name_raw or "").strip()
                if not appid or not name:
                    skipped_count += 1
                    continue
                if appid in seen_appids:
                    skipped_count += 1
                    continue

                seen_appids.add(appid)
                normalized_rows.append(
                    {
                        "appid": appid,
                        "name": name,
                        "store_url": f"https://store.steampowered.com/app/{appid}/",
                    }
                )

            if not normalized_rows:
                continue

            appids = [row["appid"] for row in normalized_rows]
            existing_rows = session.query(Game).filter(Game.appid.in_(appids)).all()
            existing_by_appid = {str(row.appid): row for row in existing_rows if row.appid is not None}

            for row in normalized_rows:
                existing = existing_by_appid.get(row["appid"])
                if existing is None:
                    session.add(
                        Game(
                            appid=row["appid"],
                            name=row["name"],
                            store_url=row["store_url"],
                            is_released=1,
                            priority=0,
                        )
                    )
                    inserted_count += 1
                    continue

                changed = False
                if existing.name != row["name"]:
                    existing.name = row["name"]
                    changed = True
                if existing.store_url != row["store_url"]:
                    existing.store_url = row["store_url"]
                    changed = True
                if existing.is_released is None:
                    existing.is_released = 1
                    changed = True
                if existing.priority is None:
                    existing.priority = 0
                    changed = True

                if changed:
                    updated_count += 1
                else:
                    skipped_count += 1

            session.commit()

        return inserted_count, updated_count, skipped_count
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> None:
    logger.info("Starting full catalog bootstrap.")
    apps = fetch_full_applist()
    logger.info("Steam full app list received total_apps=%s", len(apps))

    inserted_count, updated_count, skipped_count = upsert_catalog_rows(apps)
    logger.info(
        "Bootstrap complete total_apps=%s inserted=%s updated=%s skipped=%s",
        len(apps),
        inserted_count,
        updated_count,
        skipped_count,
    )


if __name__ == "__main__":
    main()
