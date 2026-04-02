import re
import os
import time
import random
from typing import Optional, Dict, Any, List

import requests

from config import STEAM_USER_AGENT
from logger_config import setup_logger

logger = setup_logger("steam_scraper")

HEADERS = {
    "User-Agent": STEAM_USER_AGENT
}

APP_ID_PATTERN = re.compile(r"/app/(\d+)(?:/|$)", re.IGNORECASE)
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRIES = max(0, int(os.getenv("STEAM_REQUEST_RETRIES", "2")))
APPDETAILS_REQUEST_DELAY_SECONDS = max(0.0, float(os.getenv("STEAM_APPDETAILS_REQUEST_DELAY_SECONDS", "0.25")))
APPDETAILS_429_COOLDOWN_SECONDS = max(0.0, float(os.getenv("STEAM_APPDETAILS_429_COOLDOWN_SECONDS", "45")))
APPDETAILS_429_BACKOFF_BASE_SECONDS = max(0.1, float(os.getenv("STEAM_APPDETAILS_429_BACKOFF_BASE_SECONDS", "2.0")))
APPDETAILS_429_BACKOFF_MAX_SECONDS = max(
    APPDETAILS_429_BACKOFF_BASE_SECONDS,
    float(os.getenv("STEAM_APPDETAILS_429_BACKOFF_MAX_SECONDS", "90")),
)

_APPDETAILS_COOLDOWN_UNTIL_MONOTONIC = 0.0
_LAST_APPDETAILS_REQUEST_MONOTONIC = 0.0
_APPDETAILS_DELAY_LOGGED = False


def extract_app_id(url: str) -> Optional[str]:
    if not url:
        return None

    match = APP_ID_PATTERN.search(url)
    if match:
        return match.group(1)

    return None


def cents_to_dollars(value_in_cents: Optional[int]) -> Optional[float]:
    if value_in_cents is None:
        return None

    try:
        return round(float(value_in_cents) / 100, 2)
    except (TypeError, ValueError):
        return None


def normalize_text_list(items: Optional[List[Dict[str, Any]]], key: str = "description") -> str:
    if not items:
        return ""

    values = []
    for item in items:
        if not isinstance(item, dict):
            continue

        raw_value = item.get(key)
        if not raw_value:
            continue

        cleaned = str(raw_value).strip()
        if cleaned:
            values.append(cleaned)

    seen = set()
    unique_values = []
    for value in values:
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique_values.append(value)

    return ", ".join(unique_values)


def normalize_platforms(platforms: Optional[Dict[str, Any]]) -> str:
    if not isinstance(platforms, dict):
        return ""

    ordered = [
        ("windows", "Windows"),
        ("mac", "Mac"),
        ("linux", "Linux"),
    ]

    enabled = [label for key, label in ordered if platforms.get(key)]
    return ", ".join(enabled)


def normalize_string_list(items: Optional[List[Any]]) -> str:
    if not items:
        return ""

    values = []
    for item in items:
        cleaned = str(item or "").strip()
        if cleaned:
            values.append(cleaned)

    seen = set()
    unique_values = []
    for value in values:
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        unique_values.append(value)
    return ", ".join(unique_values)


def extract_review_summary(inner: Dict[str, Any]) -> Dict[str, Any]:
    # appdetails can expose recommendation / score-adjacent fields inconsistently.
    # Keep this conservative and only use values when they exist in a clean form.
    review_score = None
    review_score_label = None
    review_total_count = None

    metacritic = inner.get("metacritic")
    if isinstance(metacritic, dict):
        score = metacritic.get("score")
        if isinstance(score, int):
            review_score = score

    recommendations = inner.get("recommendations")
    if isinstance(recommendations, dict):
        total = recommendations.get("total")
        if isinstance(total, int):
            review_total_count = total

    return {
        "review_score": review_score,
        "review_score_label": review_score_label,
        "review_total_count": review_total_count,
    }


def build_default_result() -> Dict[str, Any]:
    return {
        "price": None,
        "original_price": None,
        "discount_percent": None,
        "is_free": False,
        "is_released": 1,
        "release_date_text": "",
        "genres": "",
        "tags": "",
        "platforms": "",
        "review_score": None,
        "review_score_label": None,
        "review_total_count": None,
        "developer": "",
        "publisher": "",
        "featured_media": None,
    }


def _primary_movie_url(movie: Dict[str, Any]) -> str:
    for source_key in ("dash_h264", "hls_h264", "mp4", "webm"):
        source = movie.get(source_key)
        if isinstance(source, dict):
            for quality_key in ("max", "480", "720"):
                url = str(source.get(quality_key) or "").strip()
                if url:
                    return url
        else:
            url = str(source or "").strip()
            if url:
                return url
    return ""


def _normalize_featured_media(movie: Dict[str, Any] | None, app_name: str) -> Dict[str, Any] | None:
    if not isinstance(movie, dict):
        return None

    embed_url = _primary_movie_url(movie)
    if not embed_url:
        return None

    poster_url = str(movie.get("thumbnail") or "").strip() or None
    title = str(movie.get("name") or "").strip() or None
    if not title and app_name:
        title = f"{app_name} trailer"

    return {
        "kind": "video",
        "provider": "steam",
        "embed_url": embed_url,
        "poster_url": poster_url,
        "title": title,
    }


def _select_primary_movie(movies: Any) -> Dict[str, Any] | None:
    if not isinstance(movies, list):
        return None

    highlighted = next((movie for movie in movies if isinstance(movie, dict) and movie.get("highlight")), None)
    if highlighted is not None:
        return highlighted

    return next((movie for movie in movies if isinstance(movie, dict)), None)


def _wait_for_appdetails_delay() -> None:
    global _LAST_APPDETAILS_REQUEST_MONOTONIC, _APPDETAILS_DELAY_LOGGED
    if APPDETAILS_REQUEST_DELAY_SECONDS <= 0:
        return

    now = time.monotonic()
    if _LAST_APPDETAILS_REQUEST_MONOTONIC > 0:
        elapsed = now - _LAST_APPDETAILS_REQUEST_MONOTONIC
        remaining = APPDETAILS_REQUEST_DELAY_SECONDS - elapsed
        if remaining > 0:
            if not _APPDETAILS_DELAY_LOGGED:
                logger.info("Steam appdetails request delay enabled: %.2fs", APPDETAILS_REQUEST_DELAY_SECONDS)
                _APPDETAILS_DELAY_LOGGED = True
            logger.info("Steam appdetails delay in effect, sleeping %.2fs", remaining)
            time.sleep(remaining)
    _LAST_APPDETAILS_REQUEST_MONOTONIC = time.monotonic()


def _wait_for_429_cooldown() -> None:
    global _APPDETAILS_COOLDOWN_UNTIL_MONOTONIC
    now = time.monotonic()
    remaining = _APPDETAILS_COOLDOWN_UNTIL_MONOTONIC - now
    if remaining > 0:
        logger.warning("Steam appdetails cooldown active. sleeping %.2fs", remaining)
        time.sleep(remaining)


def _register_429_cooldown(app_id: str, attempt: int, retry_after_header: str | None) -> float:
    global _APPDETAILS_COOLDOWN_UNTIL_MONOTONIC

    retry_after_seconds = 0.0
    if retry_after_header:
        try:
            retry_after_seconds = max(0.0, float(retry_after_header))
        except ValueError:
            retry_after_seconds = 0.0

    cooldown_seconds = max(APPDETAILS_429_COOLDOWN_SECONDS, retry_after_seconds)
    now = time.monotonic()
    cooldown_until = now + cooldown_seconds
    if cooldown_until > _APPDETAILS_COOLDOWN_UNTIL_MONOTONIC:
        _APPDETAILS_COOLDOWN_UNTIL_MONOTONIC = cooldown_until
        logger.warning(
            "Steam appdetails 429 cooldown started app_id=%s cooldown_seconds=%.2f",
            app_id,
            cooldown_seconds,
        )

    backoff_seconds = min(
        APPDETAILS_429_BACKOFF_MAX_SECONDS,
        APPDETAILS_429_BACKOFF_BASE_SECONDS * (2 ** max(0, attempt - 1)),
    )
    jitter = random.uniform(0.0, max(0.25, backoff_seconds * 0.35))
    sleep_seconds = backoff_seconds + jitter
    logger.warning(
        "Steam appdetails 429 backoff app_id=%s attempt=%s sleep=%.2fs",
        app_id,
        attempt,
        sleep_seconds,
    )
    return sleep_seconds


def get_game_price_data(url: str) -> Optional[Dict[str, Any]]:
    logger.info("Requesting Steam appdetails data for URL: %s", url)

    app_id = extract_app_id(url)
    if not app_id:
        logger.warning("Could not extract app id from url: %s", url)
        return None

    api_url = "https://store.steampowered.com/api/appdetails"
    params = {
        "appids": app_id,
        "cc": "us",
        "l": "english",
    }

    data = None
    last_error = None
    for attempt in range(1, REQUEST_RETRIES + 2):
        try:
            _wait_for_429_cooldown()
            _wait_for_appdetails_delay()
            response = requests.get(
                api_url,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code == 429:
                last_error = requests.HTTPError("429 Too Many Requests", response=response)
                if attempt > REQUEST_RETRIES:
                    logger.error("Steam appdetails rate limited and retries exhausted app_id=%s", app_id)
                    return None
                sleep_seconds = _register_429_cooldown(
                    app_id=app_id,
                    attempt=attempt,
                    retry_after_header=response.headers.get("Retry-After"),
                )
                time.sleep(sleep_seconds)
                continue
            response.raise_for_status()
            data = response.json()
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt > REQUEST_RETRIES:
                logger.exception("Steam appdetails request failed for app id %s", app_id)
                return None
            delay = 0.4 * attempt + random.uniform(0.0, 0.25)
            logger.warning("Steam appdetails retry app_id=%s attempt=%s delay=%.2fs", app_id, attempt, delay)
            time.sleep(delay)
        except ValueError:
            logger.exception("Steam appdetails returned invalid JSON for app id %s", app_id)
            return None
    if data is None:
        if last_error:
            logger.exception("Steam appdetails request failed for app id %s", app_id)
        return None

    if app_id not in data:
        logger.warning("App id %s not found in Steam API response.", app_id)
        return None

    app_data = data[app_id]
    if not isinstance(app_data, dict):
        logger.warning("Unexpected Steam API payload for app id %s: %s", app_id, type(app_data).__name__)
        return None

    if not app_data.get("success"):
        logger.warning("Steam API returned success=false for app id %s", app_id)
        return None

    inner = app_data.get("data", {})
    if not isinstance(inner, dict) or not inner:
        logger.warning("Steam API returned empty data for app id %s", app_id)
        return None

    result = build_default_result()

    release_info = inner.get("release_date", {})
    coming_soon = False
    release_date_text = ""

    if isinstance(release_info, dict):
        coming_soon = bool(release_info.get("coming_soon", False))
        release_date_text = str(release_info.get("date", "") or "").strip()

    result["is_released"] = 0 if coming_soon else 1
    result["release_date_text"] = release_date_text

    result["genres"] = normalize_text_list(inner.get("genres"))
    result["platforms"] = normalize_platforms(inner.get("platforms"))
    result["developer"] = normalize_string_list(inner.get("developers"))
    result["publisher"] = normalize_string_list(inner.get("publishers"))
    result["featured_media"] = _normalize_featured_media(
        _select_primary_movie(inner.get("movies")),
        str(inner.get("name") or "").strip(),
    )

    # Tags are not reliably included in appdetails.
    # Preserve schema compatibility by returning an empty string when unavailable.
    result["tags"] = ""

    review_summary = extract_review_summary(inner)
    result.update(review_summary)

    is_free = bool(inner.get("is_free", False))
    result["is_free"] = is_free

    if is_free:
        result["price"] = 0.0
        result["original_price"] = None
        result["discount_percent"] = 0

        logger.info("Detected free game for app id %s: %s", app_id, result)
        return result

    price_overview = inner.get("price_overview")
    if not isinstance(price_overview, dict):
        logger.warning("No price_overview found for app id %s", app_id)
        logger.info("Returning metadata-only result for app id %s: %s", app_id, result)
        return result

    final_price = cents_to_dollars(price_overview.get("final"))
    initial_price = cents_to_dollars(price_overview.get("initial"))

    discount_percent = price_overview.get("discount_percent", 0)
    try:
        discount_percent = int(discount_percent) if discount_percent is not None else None
    except (TypeError, ValueError):
        discount_percent = None

    result["price"] = final_price
    result["original_price"] = initial_price
    result["discount_percent"] = discount_percent

    logger.info("Parsed Steam API data for app id %s: %s", app_id, result)
    return result
