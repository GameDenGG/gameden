import re
import time
import random
from typing import List, Dict, Set, Optional

import requests
from bs4 import BeautifulSoup

from database.dirty_games import mark_game_dirty
from database.models import Session, Game
from logger_config import setup_logger
from config import STEAM_USER_AGENT

logger = setup_logger("ingest_top_games")

HEADERS = {
    "User-Agent": STEAM_USER_AGENT
}

RELEASED_TARGET = 50000
UPCOMING_TARGET = 5000


def extract_apps_from_search_html(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    results = []

    for link in soup.select("a.search_result_row"):
        href = link.get("href", "").strip()
        match = re.search(r"/app/(\d+)/", href)
        if not match:
            continue

        appid = match.group(1)

        title_node = link.select_one(".title")
        if not title_node:
            continue

        name = title_node.get_text(strip=True)
        store_url = f"https://store.steampowered.com/app/{appid}/"

        results.append(
            {
                "appid": appid,
                "name": name,
                "store_url": store_url,
            }
        )

    return results


def fetch_with_retry(url: str, page: int, max_attempts: int = 5) -> str:
    params = {
        "page": page,
        "ndl": "1",
    }

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info("Fetching %s page=%s attempt=%s", url, page, attempt)
            response = requests.get(url, headers=HEADERS, params=params, timeout=20)

            if response.status_code == 429:
                wait_time = min(60, attempt * 10) + random.uniform(1, 3)
                logger.warning(
                    "Rate limited on %s page=%s. Waiting %.1f seconds.",
                    url,
                    page,
                    wait_time,
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.text

        except requests.RequestException:
            if attempt == max_attempts:
                raise

            wait_time = min(30, attempt * 5) + random.uniform(1, 2)
            logger.warning(
                "Request failed for %s page=%s. Retrying in %.1f seconds.",
                url,
                page,
                wait_time,
            )
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch {url} page={page} after retries.")


def fetch_search_page(url: str, page: int) -> List[Dict[str, str]]:
    html = fetch_with_retry(url, page)
    return extract_apps_from_search_html(html)


def normalize_descriptions(items: List[dict]) -> str:
    names = []
    for item in items or []:
        description = (item or {}).get("description")
        if description:
            names.append(description.strip())
    return ", ".join(names)


def normalize_platforms(platforms: dict) -> str:
    if not platforms:
        return ""

    names = []
    if platforms.get("windows"):
        names.append("Windows")
    if platforms.get("mac"):
        names.append("Mac")
    if platforms.get("linux"):
        names.append("Linux")

    return ", ".join(names)


def fetch_review_summary(appid: str) -> tuple[Optional[int], Optional[str], Optional[int]]:
    url = f"https://store.steampowered.com/appreviews/{appid}"
    params = {
        "json": 1,
        "language": "english",
        "purchase_type": "all",
        "filter": "summary",
        "num_per_page": 0,
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=20)

        if response.status_code == 429:
            logger.warning("Rate limited while fetching reviews for %s", appid)
            return None, None, None

        response.raise_for_status()

        data = response.json()
        summary = data.get("query_summary", {})

        review_score = summary.get("review_score")
        review_label = summary.get("review_score_desc")

        total_positive = summary.get("total_positive", 0) or 0
        total_negative = summary.get("total_negative", 0) or 0
        review_count = total_positive + total_negative

        if not isinstance(review_score, int):
            review_score = None

        return review_score, review_label, review_count

    except requests.RequestException as e:
        logger.warning("Failed review summary for %s: %s", appid, e)
        return None, None, None


def fetch_app_details(appid: str) -> Optional[dict]:
    api_url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": appid,
        "cc": "us",
        "l": "english",
    }

    try:
        response = requests.get(api_url, headers=HEADERS, params=params, timeout=20)

        if response.status_code == 429:
            logger.warning("Rate limited while fetching appdetails for %s", appid)
            return None

        response.raise_for_status()

        data = response.json()
        app_entry = data.get(appid)

        if not app_entry or not app_entry.get("success"):
            return None

        inner = app_entry.get("data", {})
        if not inner:
            return None

        release_info = inner.get("release_date", {})
        coming_soon = release_info.get("coming_soon", False)
        release_date_text = release_info.get("date", "")
        genres = normalize_descriptions(inner.get("genres", []))
        tags = normalize_descriptions(inner.get("categories", []))
        platforms = normalize_platforms(inner.get("platforms", {}))

        review_score, review_score_label, review_total_count = fetch_review_summary(appid)
        time.sleep(random.uniform(0.25, 0.5))

        return {
            "appid": appid,
            "name": inner.get("name", f"App {appid}"),
            "store_url": f"https://store.steampowered.com/app/{appid}/",
            "is_released": 0 if coming_soon else 1,
            "release_date_text": release_date_text,
            "genres": genres,
            "tags": tags,
            "platforms": platforms,
            "review_score": review_score,
            "review_score_label": review_score_label,
            "review_total_count": review_total_count,
        }

    except requests.RequestException as e:
        logger.warning("Failed appdetails for %s: %s", appid, e)
        return None


def collect_released_games(target: int = RELEASED_TARGET) -> List[Dict[str, str]]:
    seen: Set[str] = set()
    released_games: List[Dict[str, str]] = []

    sources = [
        "https://store.steampowered.com/search/?filter=topsellers",
    ]

    for base_url in sources:
        page = 1
        while True:
            if len(released_games) >= target:
                break

            try:
                candidates = fetch_search_page(base_url, page)
            except Exception:
                logger.exception("Failed fetching released source=%s page=%s", base_url, page)
                break

            if not candidates:
                logger.info("No more released candidates for %s page=%s", base_url, page)
                break

            for candidate in candidates:
                if candidate["appid"] in seen:
                    continue

                seen.add(candidate["appid"])

                details = fetch_app_details(candidate["appid"])
                time.sleep(random.uniform(0.8, 1.5))

                if not details:
                    continue

                if details["is_released"] == 1:
                    released_games.append(details)
                    logger.info(
                        "Added released game %s (%s). Total released=%s",
                        details["name"],
                        details["appid"],
                        len(released_games),
                    )

                    if len(released_games) >= target:
                        break

            time.sleep(random.uniform(3, 6))
            page += 1

    return released_games[:target]


def collect_upcoming_games(target: int = UPCOMING_TARGET) -> List[Dict[str, str]]:
    seen: Set[str] = set()
    upcoming_games: List[Dict[str, str]] = []

    sources = [
        "https://store.steampowered.com/search/?filter=popularwishlist",
    ]

    for base_url in sources:
        page = 1
        while True:
            if len(upcoming_games) >= target:
                break

            try:
                candidates = fetch_search_page(base_url, page)
            except Exception:
                logger.exception("Failed fetching upcoming source=%s page=%s", base_url, page)
                break

            if not candidates:
                logger.info("No more upcoming candidates for %s page=%s", base_url, page)
                break

            for candidate in candidates:
                if candidate["appid"] in seen:
                    continue

                seen.add(candidate["appid"])

                details = fetch_app_details(candidate["appid"])
                time.sleep(random.uniform(0.8, 1.5))

                if not details:
                    continue

                if details["is_released"] == 0:
                    upcoming_games.append(details)
                    logger.info(
                        "Added upcoming game %s (%s). Total upcoming=%s",
                        details["name"],
                        details["appid"],
                        len(upcoming_games),
                    )

                    if len(upcoming_games) >= target:
                        break

            time.sleep(random.uniform(3, 6))
            page += 1

    return upcoming_games[:target]


def save_games(released_games: List[Dict[str, str]], upcoming_games: List[Dict[str, str]]) -> None:
    session = Session()

    try:
        added = 0
        updated = 0
        dirty_ids: Set[int] = set()

        all_games = []

        for index, game in enumerate(released_games):
            game["priority"] = max(0, 5000 - index)
            all_games.append(game)

        for index, game in enumerate(upcoming_games):
            game["priority"] = max(0, 1000 - index)
            all_games.append(game)

        for g in all_games:
            existing = session.query(Game).filter_by(appid=g["appid"]).first()

            if existing:
                existing.name = g["name"]
                existing.store_url = g["store_url"]
                existing.is_released = g["is_released"]
                existing.release_date_text = g["release_date_text"]
                existing.genres = g.get("genres", "")
                existing.tags = g.get("tags", "")
                existing.platforms = g.get("platforms", "")
                existing.review_score = g.get("review_score")
                existing.review_score_label = g.get("review_score_label")
                existing.review_total_count = g.get("review_total_count")
                existing.priority = g["priority"]
                updated += 1
                if existing.id is not None:
                    dirty_ids.add(int(existing.id))
            else:
                new_game = Game(
                    appid=g["appid"],
                    name=g["name"],
                    store_url=g["store_url"],
                    is_released=g["is_released"],
                    release_date_text=g["release_date_text"],
                    genres=g.get("genres", ""),
                    tags=g.get("tags", ""),
                    platforms=g.get("platforms", ""),
                    review_score=g.get("review_score"),
                    review_score_label=g.get("review_score_label"),
                    review_total_count=g.get("review_total_count"),
                    priority=g["priority"],
                )
                session.add(new_game)
                session.flush()
                if new_game.id is not None:
                    dirty_ids.add(int(new_game.id))
                added += 1

        for game_id in dirty_ids:
            mark_game_dirty(session, game_id)

        session.commit()
        logger.info("Saved games. Added=%s Updated=%s DirtyMarked=%s", added, updated, len(dirty_ids))

    except Exception:
        session.rollback()
        logger.exception("Failed saving ingested games.")
        raise
    finally:
        session.close()


def main():
    logger.info("Starting released/upcoming Steam ingestion.")

    released_games = collect_released_games(RELEASED_TARGET)
    logger.info("Collected %s released games.", len(released_games))

    upcoming_games = collect_upcoming_games(UPCOMING_TARGET)
    logger.info("Collected %s upcoming games.", len(upcoming_games))

    save_games(released_games, upcoming_games)

    logger.info(
        "Finished ingestion. Released=%s Upcoming=%s",
        len(released_games),
        len(upcoming_games),
    )


if __name__ == "__main__":
    main()
