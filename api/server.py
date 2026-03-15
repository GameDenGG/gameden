import datetime
import json
import math
import re
import time
from pathlib import Path
from statistics import mean
from types import SimpleNamespace
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, EmailStr
from sqlalchemy import and_, case, func, or_, text
from sqlalchemy.exc import IntegrityError

from api.cache import json_etag, rate_limit, ttl_cache
from api.metrics import get_cache_stats, get_latency_stats, record_latency
from config import (
    API_DASHBOARD_CACHE_STALE_MINUTES,
    API_DEFAULT_HISTORY_POINTS,
    API_DEFAULT_LIST_LIMIT,
    API_DEFAULT_PAGE_SIZE,
    API_DEFAULT_USER_ID,
    API_MAX_HISTORY_POINTS,
    API_MAX_LIST_LIMIT,
    API_MAX_PAGE_SIZE,
    API_SEARCH_SIMILARITY_THRESHOLD,
    CANONICAL_HOST_REDIRECT,
    CANONICAL_REDIRECT_HOSTS,
    CORS_ALLOW_ALL_ORIGINS,
    CORS_ALLOW_ORIGINS,
    SITE_DESCRIPTION,
    SITE_HOST,
    SITE_NAME,
    SITE_URL,
    validate_settings,
)
from database import ReadSessionLocal, direct_engine
from database.dirty_games import mark_game_dirty
from database.schema_guard import assert_scale_schema_ready
from database.models import (
    DealWatchlist,
    Alert,
    Session,
    DealEvent,
    DirtyGame,
    GamePrice,
    GamePlayerHistory,
    Game,
    GameInterestSignal,
    JobStatus,
    PriceAlert,
    PushSubscription,
    UserAlert,
    Watchlist,
    WishlistItem,
    GameSnapshot,
    DashboardCache,
    LatestGamePrice,
)
from logger_config import setup_logger

logger = setup_logger("api")

validate_settings()

app = FastAPI(title=f"{SITE_NAME} API", description=SITE_DESCRIPTION)

ALLOW_ALL_CORS = CORS_ALLOW_ALL_ORIGINS or "*" in CORS_ALLOW_ORIGINS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ALLOW_ALL_CORS else CORS_ALLOW_ORIGINS,
    allow_credentials=not ALLOW_ALL_CORS,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500)

app.mount("/web", StaticFiles(directory="web"), name="web")
if Path("public").exists():
    app.mount("/public", StaticFiles(directory="public"), name="public")


@app.on_event("startup")
async def startup_guardrails() -> None:
    assert_scale_schema_ready(direct_engine, component_name="api server")
    logger.info(
        "api startup ready "
        "cache_stale_minutes=%s default_user_id=%s page_size_default=%s page_size_max=%s "
        "list_limit_default=%s list_limit_max=%s history_points_default=%s history_points_max=%s",
        API_DASHBOARD_CACHE_STALE_MINUTES,
        API_DEFAULT_USER_ID,
        API_DEFAULT_PAGE_SIZE,
        API_MAX_PAGE_SIZE,
        API_DEFAULT_LIST_LIMIT,
        API_MAX_LIST_LIMIT,
        API_DEFAULT_HISTORY_POINTS,
        API_MAX_HISTORY_POINTS,
    )

PRIMARY_DASHBOARD_CACHE_KEY = "home_v1"
LEGACY_DASHBOARD_CACHE_KEYS = ("home",)
DASHBOARD_CACHE_STALE_AFTER = datetime.timedelta(minutes=API_DASHBOARD_CACHE_STALE_MINUTES)
DEAL_RADAR_CACHE_KEY = "home:deal_radar"
OPPORTUNITY_QUERY_MULTIPLIER = 8
OPPORTUNITY_MIN_CANDIDATES = 96
OPPORTUNITY_MAX_CANDIDATES = 320
DEFAULT_USER_ID = API_DEFAULT_USER_ID
SITEMAP_PATHS = (
    "/",
    "/watchlist",
    "/web/index.html",
    "/web/all-results.html",
    "/web/game-detail.html",
    "/web/game.html",
    "/web/history.html",
    "/worth-buying-now",
    "/trending-deals",
    "/historical-lows",
)
EXTENDED_PLATFORM_FILTER_OPTIONS = ("Steam Deck", "VR Compatibility")
SEARCH_SIMILARITY_THRESHOLD = API_SEARCH_SIMILARITY_THRESHOLD
HISTORY_RANGE_DAYS: dict[str, int] = {
    "30d": 30,
    "90d": 90,
    "1y": 365,
}


def _normalize_host(value: str | None) -> str:
    if not value:
        return ""
    return value.strip().split(",")[0].split(":")[0].lower()


def _request_host(request: Request) -> str:
    forwarded_host = _normalize_host(request.headers.get("x-forwarded-host"))
    if forwarded_host:
        return forwarded_host
    host_header = _normalize_host(request.headers.get("host"))
    if host_header:
        return host_header
    return _normalize_host(request.url.hostname)


def _build_canonical_url(path: str, query: str = "") -> str:
    base = SITE_URL.rstrip("/")
    final_path = path if path.startswith("/") else f"/{path}"
    suffix = f"?{query}" if query else ""
    return f"{base}{final_path}{suffix}"


@app.middleware("http")
async def canonical_host_redirect_middleware(request: Request, call_next):
    if not CANONICAL_HOST_REDIRECT:
        return await call_next(request)

    request_host = _request_host(request)
    if request_host and request_host != SITE_HOST and request_host in CANONICAL_REDIRECT_HOSTS:
        target = _build_canonical_url(request.url.path, request.url.query)
        return RedirectResponse(url=target, status_code=308)

    return await call_next(request)


class AlertCreateRequest(BaseModel):
    game_name: str
    target_price: float
    email: EmailStr


class ListItemCreateRequest(BaseModel):
    game_name: str


class WishlistMutationRequest(BaseModel):
    user_id: str
    game_id: int


class WatchlistMutationRequest(BaseModel):
    user_id: str
    game_id: int


class AlertReadRequest(BaseModel):
    alert_id: int


class PushSubscribeRequest(BaseModel):
    user_id: str
    endpoint: str
    p256dh: str
    auth: str


class PushUnsubscribeRequest(BaseModel):
    user_id: str
    endpoint: str


class DealWatchlistAddRequest(BaseModel):
    user_id: str
    game_id: int
    target_price: float | None = None
    target_discount_percent: int | None = None


class DealWatchlistRemoveRequest(BaseModel):
    user_id: str
    game_id: int


class GameInteractionRequest(BaseModel):
    type: str


def _start_timer() -> float:
    return time.perf_counter()


def _log_timing(endpoint_name: str, started: float) -> None:
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    record_latency(endpoint_name, elapsed_ms)
    logger.info("endpoint=%s elapsed_ms=%.2f", endpoint_name, elapsed_ms)


def parse_csv_field(value):
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _normalize_token(value: str | None) -> str:
    return str(value or "").strip().lower()


def _extend_platform_filter_options(options: list[str]) -> list[str]:
    normalized = {_normalize_token(item) for item in options}
    extended = list(options)
    for option in EXTENDED_PLATFORM_FILTER_OPTIONS:
        if _normalize_token(option) not in normalized:
            extended.append(option)
            normalized.add(_normalize_token(option))
    return extended


def _build_platform_filter_predicate(platform_value: str):
    token = _normalize_token(platform_value)
    if not token:
        return None

    if token in {"steam deck", "steamdeck"}:
        return or_(
            GameSnapshot.platforms.ilike("%steam deck%"),
            GameSnapshot.tags.ilike("%steam deck%"),
            GameSnapshot.tags.ilike("%deck verified%"),
            GameSnapshot.tags.ilike("%deck playable%"),
        )

    if token in {"vr compatibility", "vr"}:
        return or_(
            GameSnapshot.platforms.ilike("%vr%"),
            GameSnapshot.tags.ilike("%vr%"),
            GameSnapshot.tags.ilike("%virtual reality%"),
            GameSnapshot.tags.ilike("%steamvr%"),
        )

    return GameSnapshot.platforms.ilike(f"%{platform_value.strip()}%")


def _normalize_search_text(value: str | None) -> str:
    collapsed = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return collapsed


def _build_catalog_search_predicate(search_text: str, include_similarity: bool):
    trimmed = str(search_text or "").strip()
    if not trimmed:
        return None

    pattern = f"%{trimmed}%"
    filters = [
        Game.name.ilike(pattern),
        Game.developer.ilike(pattern),
        Game.publisher.ilike(pattern),
    ]

    normalized_search = _normalize_search_text(trimmed)
    if include_similarity and normalized_search:
        filters.append(func.similarity(func.lower(Game.name), normalized_search) > SEARCH_SIMILARITY_THRESHOLD)

    return or_(*filters)


def _build_name_relevance_order_columns(search_text: str, include_similarity: bool):
    normalized_search = _normalize_search_text(search_text)
    if not normalized_search:
        return []

    lowered_name = func.lower(Game.name)
    exact_match_rank = case((lowered_name == normalized_search, 0), else_=1)
    prefix_match_rank = case((lowered_name.like(f"{normalized_search}%"), 0), else_=1)
    contains_match_rank = case((lowered_name.like(f"%{normalized_search}%"), 0), else_=1)

    order_columns = [
        exact_match_rank.asc(),
        prefix_match_rank.asc(),
    ]
    if include_similarity:
        order_columns.append(func.similarity(lowered_name, normalized_search).desc())
    order_columns.append(contains_match_rank.asc())
    return order_columns


def safe_num(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            return default
        return numeric
    except Exception:
        return default


def serialize_game_metadata(game: Optional[Game]) -> dict:
    return {
        "appid": game.appid if game else None,
        "genres": parse_csv_field(game.genres if game else ""),
        "tags": parse_csv_field(game.tags if game else ""),
        "platforms": parse_csv_field(game.platforms if game else ""),
        "review_score": game.review_score if game else None,
        "review_score_label": game.review_score_label if game else None,
        "review_total_count": game.review_total_count if game else None,
    }


def extract_appid_from_store_url(store_url: Optional[str]) -> Optional[str]:
    if not store_url:
        return None
    match = re.search(r"/app/(\d+)", store_url)
    return match.group(1) if match else None


def build_steam_banner_url(store_url: Optional[str], appid: Optional[str]) -> Optional[str]:
    resolved_appid = appid or extract_appid_from_store_url(store_url)
    if not resolved_appid:
        return None
    return f"https://cdn.akamai.steamstatic.com/steam/apps/{resolved_appid}/header.jpg"


def compute_deal_score(row, game: Optional[Game], insight: dict) -> int:
    discount = max(0, int(row.discount_percent or 0))
    review_score = max(0, min(int(game.review_score or 0), 100)) if game else 0
    review_total = max(0, int(game.review_total_count or 0)) if game else 0
    current_players = max(0, int(row.current_players or 0))
    price = float(row.price) if row.price is not None else None
    historical_status = insight.get("historical_status")
    historical_low = insight.get("historical_low")
    previous_historical_low = insight.get("previous_historical_low")
    ever_discounted = bool(insight.get("ever_discounted"))
    max_discount = max(0, int(insight.get("max_discount", 0) or 0))

    score = 0

    score += min(discount, 80) * 0.55
    score += review_score * 0.22
    score += min(math.log10(current_players + 1) * 10, 18)
    score += min(math.log10(review_total + 1) * 6, 12)

    if price is not None:
        if price <= 10:
            score += 8
        elif price <= 20:
            score += 5
        elif price <= 35:
            score += 2

    if historical_status == "new_historical_low":
        score += 22
    elif historical_status == "matches_historical_low":
        score += 14
    elif historical_status == "near_historical_low":
        score += 8

    if historical_low is not None and previous_historical_low is not None and row.price is not None:
        if previous_historical_low > 0 and row.price < previous_historical_low:
            score += min(((previous_historical_low - row.price) / previous_historical_low) * 25, 10)

    if ever_discounted:
        score += min(max_discount * 0.08, 6)

    return int(round(score))


def compute_historical_insight_map(session):
    rows = (
        session.query(
            GameSnapshot.game_name,
            GameSnapshot.historical_low,
            GameSnapshot.previous_historical_low_price,
            GameSnapshot.historical_status,
            GameSnapshot.history_point_count,
            GameSnapshot.ever_discounted,
            GameSnapshot.max_discount,
            GameSnapshot.last_discounted_at,
        )
        .all()
    )
    insight_map = {}
    for row in rows:
        insight_map[row.game_name] = {
            "historical_low": row.historical_low,
            "previous_historical_low": row.previous_historical_low_price,
            "historical_status": row.historical_status,
            "history_point_count": int(row.history_point_count or 0),
            "ever_discounted": bool(row.ever_discounted),
            "max_discount": int(row.max_discount or 0),
            "last_discounted_at": row.last_discounted_at.isoformat() if row.last_discounted_at else None,
        }
    return insight_map


def serialize_price_row(row, game_map=None, historical_insight_map=None):
    game = game_map.get(row.game_name) if game_map else None

    insight = historical_insight_map.get(row.game_name, {}) if historical_insight_map else {}

    appid = game.appid if game and game.appid else extract_appid_from_store_url(row.store_url)
    historical_low = insight.get("historical_low")
    previous_historical_low = insight.get("previous_historical_low")
    historical_status = insight.get("historical_status")
    history_point_count = insight.get("history_point_count", 0)
    ever_discounted = bool(insight.get("ever_discounted"))
    max_discount = int(insight.get("max_discount", 0) or 0)

    return {
        "game_name": row.game_name,
        "price": row.price,
        "original_price": row.original_price,
        "discount_percent": row.discount_percent,
        "current_players": row.current_players,
        "store_url": row.store_url,
        "timestamp": row.timestamp.isoformat() if row.timestamp else None,
        "historical_low": historical_low,
        "previous_historical_low": previous_historical_low,
        "historical_status": historical_status,
        "history_point_count": history_point_count,
        "ever_discounted": ever_discounted,
        "max_discount": max_discount,
        "last_discounted_at": insight.get("last_discounted_at"),
        "on_sale": bool((row.discount_percent or 0) > 0),
        "banner_url": build_steam_banner_url(row.store_url, appid),
        **serialize_game_metadata(game),
        "deal_score": compute_deal_score(row, game, insight),
    }


def serialize_upcoming_row(game: Game) -> dict:
    return {
        "game_name": game.name,
        "release_date_text": game.release_date_text,
        "store_url": game.store_url,
        "banner_url": build_steam_banner_url(game.store_url, game.appid),
        **serialize_game_metadata(game),
    }


def get_latest_price_rows(session):
    rows = session.query(GameSnapshot).all()
    latest_prices = []
    for row in rows:
        latest_prices.append(
            SimpleNamespace(
                game_name=row.game_name,
                price=row.latest_price,
                original_price=row.latest_original_price,
                discount_percent=row.latest_discount_percent,
                current_players=row.current_players,
                store_url=row.store_url,
                timestamp=row.updated_at,
            )
        )
    return latest_prices


def build_game_map(session):
    games = session.query(Game).all()
    return {game.name: game for game in games}


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def is_paid_discount_row(row) -> bool:
    return (
        row.discount_percent is not None
        and row.discount_percent >= 50
        and row.price is not None
        and row.price > 0
    )


def parse_release_date_sort_key(release_date_text: Optional[str]):
    if not release_date_text:
        return (5, datetime.date.max, "zzz")

    text = release_date_text.strip()
    lowered = text.lower()

    vague_values = {
        "coming soon",
        "coming soon!",
        "to be announced",
        "tba",
        "announced",
    }
    if lowered in vague_values:
        return (5, datetime.date.max, lowered)

    exact_formats = [
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b, %Y",
        "%d %B, %Y",
        "%b %Y",
        "%B %Y",
    ]

    for fmt in exact_formats:
        try:
            parsed = datetime.datetime.strptime(text, fmt).date()
            return (0, parsed, lowered)
        except ValueError:
            continue

    quarter_match = re.fullmatch(r"Q([1-4])\s+(\d{4})", text, flags=re.IGNORECASE)
    if quarter_match:
        quarter = int(quarter_match.group(1))
        year = int(quarter_match.group(2))
        month_by_quarter = {1: 1, 2: 4, 3: 7, 4: 10}
        parsed = datetime.date(year, month_by_quarter[quarter], 1)
        return (1, parsed, lowered)

    year_match = re.fullmatch(r"(\d{4})", text)
    if year_match:
        year = int(year_match.group(1))
        parsed = datetime.date(year, 1, 1)
        return (2, parsed, lowered)

    month_year_match = re.fullmatch(
        r"([A-Za-z]+)\s+(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if month_year_match:
        for fmt in ("%b %Y", "%B %Y"):
            try:
                parsed = datetime.datetime.strptime(text, fmt).date()
                return (1, parsed, lowered)
            except ValueError:
                continue

    return (4, datetime.date.max, lowered)


def get_seasonal_sale_window(now_date: datetime.date):
    year = now_date.year
    windows = [
        {
            "name": "Steam Spring Sale",
            "slug": "spring_sale",
            "start": datetime.date(year, 3, 10),
            "end": datetime.date(year, 3, 24),
        },
        {
            "name": "Steam Summer Sale",
            "slug": "summer_sale",
            "start": datetime.date(year, 6, 20),
            "end": datetime.date(year, 7, 4),
        },
        {
            "name": "Steam Autumn Sale",
            "slug": "autumn_sale",
            "start": datetime.date(year, 11, 20),
            "end": datetime.date(year, 12, 3),
        },
        {
            "name": "Steam Winter Sale",
            "slug": "winter_sale",
            "start": datetime.date(year, 12, 18),
            "end": datetime.date(year + 1, 1, 5),
        },
    ]

    for window in windows:
        if window["start"] <= now_date <= window["end"]:
            return {
                **window,
                "status": "live",
                "days_until_start": 0,
            }

    upcoming_windows = [window for window in windows if window["start"] > now_date]
    if not upcoming_windows:
        next_window = {
            "name": "Steam Spring Sale",
            "slug": "spring_sale",
            "start": datetime.date(year + 1, 3, 10),
            "end": datetime.date(year + 1, 3, 24),
        }
    else:
        next_window = upcoming_windows[0]

    return {
        **next_window,
        "status": "upcoming",
        "days_until_start": (next_window["start"] - now_date).days,
    }


def build_expected_sale_rows(latest_prices, game_map, insight_map):
    candidates = []

    for row in latest_prices:
        if row.price is None or row.price <= 0:
            continue

        if row.discount_percent is not None and row.discount_percent > 0:
            continue

        insight = insight_map.get(row.game_name, {})
        if not insight.get("ever_discounted"):
            continue

        serialized = serialize_price_row(row, game_map, insight_map)

        prior_discount_signal = min((serialized["max_discount"] or 0) * 0.4, 24)
        review_signal = min((serialized["review_score"] or 0) * 0.15, 15)
        player_signal = min(math.log10((serialized["current_players"] or 0) + 1) * 7, 12)
        low_signal = 0

        if serialized["historical_low"] and row.price:
            if row.price > 0:
                low_signal = min(((row.price - serialized["historical_low"]) / row.price) * 18, 12)

        predicted_sale_score = int(round(
            prior_discount_signal + review_signal + player_signal + low_signal
        ))

        serialized["predicted_sale_score"] = predicted_sale_score
        serialized["seasonal_relevance_score"] = predicted_sale_score
        candidates.append(serialized)

    high_signal_candidates = [
        row
        for row in candidates
        if row["predicted_sale_score"] >= 35
        or (
            (row.get("max_discount") or 0) >= 50
            and (row.get("review_score") or 0) >= 70
        )
        or (
            (row.get("current_players") or 0) >= 1200
            and row["predicted_sale_score"] >= 25
        )
    ]
    ranked_candidates = high_signal_candidates if high_signal_candidates else candidates

    ranked_candidates.sort(
        key=lambda item: (
            item.get("seasonal_relevance_score") if item.get("seasonal_relevance_score") is not None else -1,
            item["predicted_sale_score"],
            item["deal_score"] if item["deal_score"] is not None else -1,
            item["review_score"] if item["review_score"] is not None else -1,
            item["current_players"] if item["current_players"] is not None else -1,
            item["game_name"].lower(),
        ),
        reverse=True,
    )

    return ranked_candidates


def _dedupe_serialized_rows(rows):
    seen = set()
    deduped = []
    for row in rows:
        key = str(row.get("game_name") or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def build_active_sale_rows(latest_prices, game_map, insight_map):
    active_rows = []
    high_signal_rows = []

    for row in latest_prices:
        if row.price is None or row.price <= 0:
            continue
        if row.discount_percent is None or row.discount_percent <= 0:
            continue
        serialized = serialize_price_row(row, game_map, insight_map)
        discount = int(serialized.get("discount_percent") or 0)
        deal_score = float(serialized.get("deal_score") or 0.0)
        current_players = int(serialized.get("current_players") or 0)
        review_score = int(serialized.get("review_score") or 0)
        historical_status = str(serialized.get("historical_status") or "")
        historical_bonus = (
            10 if historical_status == "new_historical_low"
            else 6 if historical_status == "matches_historical_low"
            else 3 if historical_status == "near_historical_low"
            else 0
        )
        player_signal = min(math.log10(current_players + 1) * 10.0, 16.0)
        review_signal = min(review_score * 0.12, 12.0)
        relevance_score = round(
            discount * 0.42 + min(deal_score, 100.0) * 0.38 + player_signal + review_signal + historical_bonus,
            2,
        )
        serialized["seasonal_relevance_score"] = relevance_score
        active_rows.append(serialized)
        if (
            discount >= 20
            or deal_score >= 45
            or current_players >= 1500
            or review_score >= 80
            or historical_bonus >= 6
        ):
            high_signal_rows.append(serialized)

    ranked_rows = high_signal_rows if high_signal_rows else active_rows

    ranked_rows.sort(
        key=lambda item: (
            item.get("seasonal_relevance_score") if item.get("seasonal_relevance_score") is not None else -1,
            item["discount_percent"] if item["discount_percent"] is not None else -1,
            item["deal_score"] if item["deal_score"] is not None else -1,
            item["current_players"] if item["current_players"] is not None else -1,
            item["review_score"] if item["review_score"] is not None else -1,
            item["game_name"].lower(),
        ),
        reverse=True,
    )

    return _dedupe_serialized_rows(ranked_rows)


def serialize_list_item(row):
    return {
        "id": row.id,
        "game_name": row.game_name,
        "user_id": getattr(row, "user_id", None),
        "game_id": getattr(row, "game_id", None),
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def normalize_user_id(value: str | None) -> str:
    text_value = (value or "").strip()
    return text_value or DEFAULT_USER_ID


def _alert_label(alert_type: str | None) -> str:
    label_map = {
        "PRICE_DROP": "Price dropped",
        "NEW_HISTORICAL_LOW": "New historical low",
        "SALE_STARTED": "Sale started",
        "PLAYER_SURGE": "Major player increase",
        "PRICE_TARGET_HIT": "Price target hit",
        "DISCOUNT_TARGET_HIT": "Discount target hit",
    }
    return label_map.get(str(alert_type or "").upper(), "Market signal")


def _format_user_alert_label(
    alert_type: str | None,
    *,
    price: float | None = None,
    discount_percent: int | None = None,
) -> str:
    normalized_type = str(alert_type or "").upper()
    if normalized_type == "PRICE_TARGET_HIT":
        if price is not None:
            return f"Price target hit (${safe_num(price, 0.0):.2f})"
        return "Price target hit"
    if normalized_type == "DISCOUNT_TARGET_HIT":
        if discount_percent is not None:
            return f"Discount target hit ({int(safe_num(discount_percent, 0.0))}% off)"
        return "Discount target hit"
    return _alert_label(normalized_type)


def build_watchlist_signals(snapshot: GameSnapshot | None, latest_row: LatestGamePrice | None, alerts: list[Alert]) -> list[dict]:
    signals: list[dict] = []
    for alert in alerts[:3]:
        alert_type = str(alert.alert_type or "").upper()
        created_at = alert.created_at.isoformat() if alert.created_at else None
        metadata = alert.metadata_json if isinstance(alert.metadata_json, dict) else {}
        label = _alert_label(alert_type)
        if alert_type == "SALE_STARTED":
            discount_percent = (
                int(safe_num(snapshot.latest_discount_percent, 0.0))
                if snapshot and snapshot.latest_discount_percent is not None
                else int(safe_num(latest_row.latest_discount_percent, 0.0))
                if latest_row and latest_row.latest_discount_percent is not None
                else None
            )
            if discount_percent and discount_percent > 0:
                label = f"Sale started ({discount_percent}% off)"
        if alert_type == "PRICE_DROP" and metadata.get("new_price") is not None:
            label = f"Price dropped to ${safe_num(metadata.get('new_price'), 0.0):.2f}"
        signals.append(
            {
                "type": alert_type,
                "label": label,
                "created_at": created_at,
                "metadata": metadata,
            }
        )

    if not signals:
        discount_percent = (
            int(safe_num(snapshot.latest_discount_percent, 0.0))
            if snapshot and snapshot.latest_discount_percent is not None
            else int(safe_num(latest_row.latest_discount_percent, 0.0))
            if latest_row and latest_row.latest_discount_percent is not None
            else None
        )
        if discount_percent and discount_percent > 0:
            signals.append(
                {
                    "type": "SALE_STARTED",
                    "label": f"On sale now ({discount_percent}% off)",
                }
            )
        if snapshot and bool(snapshot.historical_low_hit):
            signals.append(
                {
                    "type": "NEW_HISTORICAL_LOW",
                    "label": "New historical low",
                }
            )

    buy_score = None
    if snapshot and snapshot.buy_score is not None:
        buy_score = float(snapshot.buy_score)
    elif snapshot and snapshot.worth_buying_score is not None:
        buy_score = float(snapshot.worth_buying_score)
    if buy_score is not None and buy_score >= 70:
        signals.append(
            {
                "type": "BUY_SIGNAL",
                "label": f"Worth buying signal ({round(buy_score, 1)})",
            }
        )

    deduped: list[dict] = []
    seen = set()
    for signal in signals:
        key = f"{signal.get('type')}:{signal.get('label')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(signal)
    return deduped


def build_watchlist_entries_payload(session: Session, user_id: str) -> list[dict]:
    rows = (
        session.query(Watchlist, Game, GameSnapshot, LatestGamePrice)
        .outerjoin(Game, Game.id == Watchlist.game_id)
        .outerjoin(GameSnapshot, GameSnapshot.game_id == Watchlist.game_id)
        .outerjoin(LatestGamePrice, LatestGamePrice.game_id == Watchlist.game_id)
        .filter(Watchlist.user_id == user_id)
        .order_by(Watchlist.created_at.desc(), Watchlist.id.desc())
        .all()
    )
    if not rows:
        return []

    game_ids = [int(row.game_id) for row, _, _, _ in rows]
    recent_alerts = (
        session.query(Alert)
        .filter(
            Alert.game_id.in_(game_ids),
            Alert.created_at >= utc_now() - datetime.timedelta(days=14),
        )
        .order_by(Alert.created_at.desc(), Alert.id.desc())
        .all()
    )
    alerts_by_game: dict[int, list[Alert]] = {}
    for alert in recent_alerts:
        alerts_by_game.setdefault(int(alert.game_id), []).append(alert)

    payload: list[dict] = []
    for row, game, snapshot, latest_row in rows:
        game_name = (
            snapshot.game_name
            if snapshot and snapshot.game_name
            else game.name
            if game and game.name
            else f"Game {row.game_id}"
        )
        buy_score = (
            float(snapshot.buy_score)
            if snapshot and snapshot.buy_score is not None
            else float(snapshot.worth_buying_score)
            if snapshot and snapshot.worth_buying_score is not None
            else None
        )
        latest_price = (
            snapshot.latest_price
            if snapshot and snapshot.latest_price is not None
            else latest_row.latest_price
            if latest_row
            else None
        )
        latest_discount = (
            snapshot.latest_discount_percent
            if snapshot and snapshot.latest_discount_percent is not None
            else latest_row.latest_discount_percent
            if latest_row
            else None
        )
        payload.append(
            {
                "id": int(row.id),
                "user_id": row.user_id,
                "game_id": int(row.game_id),
                "game_name": game_name,
                "steam_appid": snapshot.steam_appid if snapshot else (game.appid if game else None),
                "banner_url": snapshot.banner_url if snapshot else None,
                "latest_price": latest_price,
                "latest_discount_percent": latest_discount,
                "buy_score": buy_score,
                "worth_buying_reason_summary": snapshot.worth_buying_reason_summary if snapshot else None,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "signals": build_watchlist_signals(
                    snapshot,
                    latest_row,
                    alerts_by_game.get(int(row.game_id), []),
                ),
            }
        )
    return payload


def build_user_watchlist_alert_feed(session: Session, user_id: str, limit: int = 50) -> list[dict]:
    normalized_user_id = normalize_user_id(user_id)
    normalized_limit = max(1, min(int(limit), 200))
    watchlist_game_ids = [
        int(game_id)
        for (game_id,) in (
            session.query(Watchlist.game_id)
            .filter(Watchlist.user_id == normalized_user_id)
            .distinct()
            .all()
        )
        if game_id is not None
    ]

    global_rows: list[tuple[Alert, Game | None, GameSnapshot | None]] = []
    if watchlist_game_ids:
        global_rows = (
            session.query(Alert, Game, GameSnapshot)
            .outerjoin(Game, Game.id == Alert.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == Alert.game_id)
            .filter(Alert.game_id.in_(watchlist_game_ids))
            .order_by(Alert.created_at.desc(), Alert.id.desc())
            .limit(min(400, normalized_limit * 4))
            .all()
        )

    user_target_rows = (
        session.query(UserAlert, Game, GameSnapshot)
        .outerjoin(Game, Game.id == UserAlert.game_id)
        .outerjoin(GameSnapshot, GameSnapshot.game_id == UserAlert.game_id)
        .filter(UserAlert.user_id == normalized_user_id)
        .order_by(UserAlert.created_at.desc(), UserAlert.id.desc())
        .limit(min(400, normalized_limit * 4))
        .all()
    )

    ranked_feed: list[tuple[datetime.datetime, dict]] = []
    seen: set[tuple[int, str, str | None]] = set()
    for alert, game, snapshot in global_rows:
        alert_type = str(alert.alert_type or "").upper()
        created_at = alert.created_at.isoformat() if alert.created_at else None
        dedupe_key = (int(alert.game_id), alert_type, created_at)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        metadata = alert.metadata_json if isinstance(alert.metadata_json, dict) else {}
        game_name = (
            snapshot.game_name
            if snapshot and snapshot.game_name
            else game.name
            if game and game.name
            else f"Game {int(alert.game_id)}"
        )
        created_dt = alert.created_at if isinstance(alert.created_at, datetime.datetime) else utc_now()
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=datetime.timezone.utc)
        ranked_feed.append(
            (
                created_dt,
                {
                    "id": int(alert.id),
                    "game_id": int(alert.game_id),
                    "game_name": game_name,
                    "steam_appid": snapshot.steam_appid if snapshot else (game.appid if game else None),
                    "banner_url": snapshot.banner_url if snapshot else None,
                    "alert_type": alert_type,
                    "alert_label": _alert_label(alert_type),
                    "created_at": created_at,
                    "alert_created_at": created_at,
                    "metadata": metadata,
                    "alert_metadata": metadata,
                    "latest_price": snapshot.latest_price if snapshot else None,
                    "latest_discount_percent": snapshot.latest_discount_percent if snapshot else None,
                    "current_players": snapshot.current_players if snapshot else None,
                    "buy_score": (
                        snapshot.buy_score
                        if snapshot and snapshot.buy_score is not None
                        else snapshot.worth_buying_score
                        if snapshot
                        else None
                    ),
                },
            )
        )

    for alert, game, snapshot in user_target_rows:
        alert_type = str(alert.alert_type or "").upper()
        created_at = alert.created_at.isoformat() if alert.created_at else None
        dedupe_key = (int(alert.game_id), alert_type, created_at)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        metadata = {
            "price": alert.price,
            "discount_percent": alert.discount_percent,
            "read": bool(alert.read),
        }
        game_name = (
            snapshot.game_name
            if snapshot and snapshot.game_name
            else game.name
            if game and game.name
            else f"Game {int(alert.game_id)}"
        )
        created_dt = alert.created_at if isinstance(alert.created_at, datetime.datetime) else utc_now()
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=datetime.timezone.utc)
        ranked_feed.append(
            (
                created_dt,
                {
                    "id": int(alert.id),
                    "game_id": int(alert.game_id),
                    "game_name": game_name,
                    "steam_appid": snapshot.steam_appid if snapshot else (game.appid if game else None),
                    "banner_url": snapshot.banner_url if snapshot else None,
                    "alert_type": alert_type,
                    "alert_label": _format_user_alert_label(
                        alert_type,
                        price=alert.price,
                        discount_percent=alert.discount_percent,
                    ),
                    "created_at": created_at,
                    "alert_created_at": created_at,
                    "price": alert.price,
                    "discount_percent": alert.discount_percent,
                    "metadata": metadata,
                    "alert_metadata": metadata,
                    "latest_price": snapshot.latest_price if snapshot and snapshot.latest_price is not None else alert.price,
                    "latest_discount_percent": (
                        snapshot.latest_discount_percent
                        if snapshot and snapshot.latest_discount_percent is not None
                        else alert.discount_percent
                    ),
                    "current_players": snapshot.current_players if snapshot else None,
                    "buy_score": (
                        snapshot.buy_score
                        if snapshot and snapshot.buy_score is not None
                        else snapshot.worth_buying_score
                        if snapshot
                        else None
                    ),
                },
            )
        )

    ranked_feed.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in ranked_feed[:normalized_limit]]


def _normalize_deal_radar_item(item: dict) -> dict | None:
    if not isinstance(item, dict):
        return None
    try:
        game_id = int(item.get("game_id") or 0)
    except Exception:
        game_id = 0
    if game_id <= 0:
        return None

    game_name = str(item.get("game_name") or f"Game {game_id}").strip() or f"Game {game_id}"
    signal_type = str(item.get("signal_type") or "MARKET_SIGNAL").strip().upper() or "MARKET_SIGNAL"
    timestamp = item.get("timestamp")
    timestamp_text = str(timestamp).strip() if timestamp is not None else None

    return {
        "game_id": game_id,
        "game_name": game_name,
        "image": item.get("image") or item.get("banner_url"),
        "price": item.get("price"),
        "discount": item.get("discount") if item.get("discount") is not None else item.get("discount_percent"),
        "signal_type": signal_type,
        "timestamp": timestamp_text,
        "signal_text": item.get("signal_text"),
        "current_players": item.get("current_players"),
        "buy_score": item.get("buy_score"),
        "metadata": item.get("metadata") if isinstance(item.get("metadata"), dict) else {},
    }


def _append_unique_reason(reasons: list[str], reason: str | None) -> None:
    normalized = str(reason or "").strip()
    if not normalized:
        return
    normalized_lower = normalized.lower()
    if any(existing.lower() == normalized_lower for existing in reasons):
        return
    reasons.append(normalized)


def _normalize_opportunity_reason(raw_reason: str | None) -> str | None:
    raw = re.sub(r"\s+", " ", str(raw_reason or "").strip())
    if not raw:
        return None
    lower = raw.lower()

    if any(token in lower for token in ("historical low", "all-time low", "near low", "price floor")):
        return "Near historical low"
    if (
        any(token in lower for token in ("player", "momentum", "activity", "engagement"))
        and any(token in lower for token in ("up", "rising", "growth", "surge", "climb"))
    ):
        return "Players rising"
    if any(token in lower for token in ("discount", "sale", "price drop", "markdown")):
        return "Strong discount"
    if any(token in lower for token in ("popular", "trending", "interest", "heat")):
        return "Popular game currently trending"
    if any(token in lower for token in ("buy now", "worth buying", "good buy", "good time to buy")):
        return "Buy-now recommendation"
    if any(token in lower for token in ("unlikely soon", "wait", "next sale", "weeks")):
        return "Next sale likely not soon"

    first_sentence = re.split(r"[.!?]", raw, maxsplit=1)[0].strip() or raw
    compact = first_sentence if len(first_sentence) <= 72 else f"{first_sentence[:69].rstrip()}..."
    if not compact:
        return None
    return compact[0].upper() + compact[1:]


def _build_deal_opportunity_item(snapshot: GameSnapshot) -> dict | None:
    price = snapshot.latest_price
    if price is None:
        return None

    discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    deal_score = safe_num(snapshot.deal_score, 0.0)
    popularity_score = safe_num(snapshot.popularity_score, 0.0)
    momentum_score = safe_num(snapshot.momentum_score, 0.0)
    player_growth_ratio = safe_num(snapshot.player_growth_ratio, 0.0)
    short_term_player_trend = safe_num(snapshot.short_term_player_trend, 0.0)
    max_discount = max(0, int(round(safe_num(snapshot.max_discount, 0.0))))
    price_vs_low_ratio = safe_num(snapshot.price_vs_low_ratio, 0.0)
    recommendation = str(snapshot.buy_recommendation or "").strip().upper()
    historical_status = str(snapshot.historical_status or "").strip().lower()
    predicted_window_days_min = int(round(safe_num(snapshot.predicted_next_sale_window_days_min, 0.0)))

    reasons: list[str] = []
    score = 0.0

    near_historical_low = (
        historical_status in {"new_historical_low", "matches_historical_low", "near_historical_low"}
        or (price_vs_low_ratio > 0 and price_vs_low_ratio <= 1.08)
    )
    if near_historical_low:
        score += 24.0
        _append_unique_reason(reasons, "Near historical low")

    if recommendation == "BUY_NOW":
        score += 26.0
        _append_unique_reason(
            reasons,
            _normalize_opportunity_reason(snapshot.buy_reason) or "Buy-now recommendation",
        )

    if discount >= 60:
        score += 20.0
        _append_unique_reason(reasons, "Strong discount")
    elif discount >= 40:
        score += 13.0
        _append_unique_reason(reasons, "Meaningful discount")

    players_rising = (
        short_term_player_trend >= 0.06
        or player_growth_ratio >= 1.08
        or (momentum_score >= 60 and safe_num(snapshot.current_players, 0.0) >= 300)
    )
    if players_rising:
        score += 12.0
        _append_unique_reason(reasons, "Players rising")

    if deal_score >= 86:
        score += 14.0
        _append_unique_reason(reasons, "Strong deal score")
    elif deal_score >= 74:
        score += 8.0

    if popularity_score >= 70 and discount >= 25 and max_discount <= 55:
        score += 9.0
        _append_unique_reason(reasons, "Rare sale for a popular game")

    if predicted_window_days_min >= 45 and discount >= 25:
        score += 8.0
        _append_unique_reason(reasons, "Next sale likely not soon")

    for summary in (
        snapshot.worth_buying_reason_summary,
        snapshot.trend_reason_summary,
        snapshot.deal_heat_reason,
        snapshot.predicted_sale_reason,
    ):
        if len(reasons) >= 2:
            break
        _append_unique_reason(reasons, _normalize_opportunity_reason(summary))

    if not reasons or score < 22.0:
        return None

    reason_lines = reasons[:2]
    updated_at = snapshot.updated_at if isinstance(snapshot.updated_at, datetime.datetime) else utc_now()
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    buy_score = snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score

    return {
        "game_id": int(snapshot.game_id),
        "id": int(snapshot.game_id),
        "game_name": snapshot.game_name,
        "steam_appid": snapshot.steam_appid,
        "banner_url": snapshot.banner_url,
        "image_url": snapshot.banner_url,
        "store_url": snapshot.store_url,
        "price": snapshot.latest_price,
        "original_price": snapshot.latest_original_price,
        "discount_percent": snapshot.latest_discount_percent,
        "historical_low": snapshot.historical_low,
        "historical_status": snapshot.historical_status,
        "price_vs_low_ratio": snapshot.price_vs_low_ratio,
        "current_players": snapshot.current_players,
        "player_growth_ratio": snapshot.player_growth_ratio,
        "short_term_player_trend": snapshot.short_term_player_trend,
        "momentum_score": snapshot.momentum_score,
        "popularity_score": snapshot.popularity_score,
        "deal_score": snapshot.deal_score,
        "buy_score": buy_score,
        "buy_recommendation": snapshot.buy_recommendation,
        "buy_reason": snapshot.buy_reason,
        "predicted_next_sale_window_days_min": snapshot.predicted_next_sale_window_days_min,
        "predicted_next_sale_window_days_max": snapshot.predicted_next_sale_window_days_max,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "worth_buying_reason_summary": snapshot.worth_buying_reason_summary,
        "trend_reason_summary": snapshot.trend_reason_summary,
        "deal_heat_reason": snapshot.deal_heat_reason,
        "historical_low_info": {
            "historical_low": snapshot.historical_low,
            "status": snapshot.historical_status,
            "price_vs_low_ratio": snapshot.price_vs_low_ratio,
        },
        "player_trend_info": {
            "current_players": snapshot.current_players,
            "growth_ratio": snapshot.player_growth_ratio,
            "short_term_player_trend": snapshot.short_term_player_trend,
            "momentum_score": snapshot.momentum_score,
        },
        "opportunity_score": round(score, 2),
        "opportunity_reasons": reason_lines,
        "opportunity_reason": " and ".join(reason_lines),
        "updated_at": updated_at.isoformat(),
    }


def get_history_range_start(range_key: str) -> datetime.datetime | None:
    days = HISTORY_RANGE_DAYS.get(str(range_key or "").strip())
    if days is None:
        return None
    return utc_now() - datetime.timedelta(days=days)


def downsample_history_points(
    rows: list[tuple[float, datetime.datetime]],
    max_points: int,
) -> list[tuple[float, datetime.datetime]]:
    if len(rows) <= max_points:
        return rows

    if max_points <= 1:
        return [rows[-1]]

    step = (len(rows) - 1) / (max_points - 1)
    sampled: list[tuple[float, datetime.datetime]] = []
    for i in range(max_points):
        idx = int(round(i * step))
        if idx >= len(rows):
            idx = len(rows) - 1
        sampled.append(rows[idx])
    return sampled


def downsample_price_rows(rows, range_key: str):
    if range_key in {"30d", "90d"}:
        return rows

    sampled = []
    last_bucket = None
    for row in rows:
        bucket = row.timestamp.strftime("%Y-%W")
        if bucket != last_bucket:
            sampled.append(row)
            last_bucket = bucket
        else:
            sampled[-1] = row
    return sampled


def find_historical_low_row(rows):
    priced = [row for row in rows if row.price is not None]
    if not priced:
        return None
    return min(priced, key=lambda row: (row.price, row.timestamp, row.id))


def count_distinct_sale_events(sale_rows) -> int:
    if not sale_rows:
        return 0
    event_count = 1
    prev = sale_rows[0]
    for row in sale_rows[1:]:
        if (row.timestamp - prev.timestamp).days > 7:
            event_count += 1
        prev = row
    return event_count


def normalize(value: float, min_value: float, max_value: float) -> float:
    if max_value <= min_value:
        return 0.0
    return max(0.0, min((value - min_value) / (max_value - min_value), 1.0))


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def parse_release_date_to_datetime(release_date_text: Optional[str]) -> Optional[datetime.datetime]:
    if not release_date_text:
        return None

    text = release_date_text.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b, %Y", "%d %B, %Y", "%b %Y", "%B %Y", "%Y"):
        try:
            parsed = datetime.datetime.strptime(text, fmt)
            if fmt == "%Y":
                parsed = parsed.replace(month=1, day=1)
            if fmt in {"%b %Y", "%B %Y"}:
                parsed = parsed.replace(day=1)
            return parsed.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
    return None


def calculate_deal_explanation(game: Game, latest_price, historical_low_row, market: dict) -> dict:
    if not latest_price:
        return {
            "deal_score": 0.0,
            "label": "Unknown Deal",
            "summary": "Not enough price data is available yet.",
            "factors": [],
        }

    latest_discount = float(latest_price.discount_percent or 0)
    discount_strength = normalize(latest_discount, 0, 80) * 30

    historical_value = 0.0
    if historical_low_row and historical_low_row.price and latest_price.price:
        if latest_price.price <= historical_low_row.price:
            historical_value = 25
        elif latest_price.price > 0:
            ratio = historical_low_row.price / latest_price.price
            historical_value = clamp(ratio * 25, 0, 25)

    sale_event_count = market.get("sale_event_count", 0)
    if sale_event_count <= 2:
        sale_rarity = 15
    elif sale_event_count <= 5:
        sale_rarity = 11
    elif sale_event_count <= 10:
        sale_rarity = 7
    else:
        sale_rarity = 4

    review_quality = normalize(float(game.review_score or 0), 0, 100) * 10
    player_interest = normalize(float(latest_price.current_players or 0), 0, 100000) * 10

    trend_momentum = 0.0
    if latest_discount >= 50:
        trend_momentum = 10
    elif latest_discount >= 30:
        trend_momentum = 7
    elif latest_discount > 0:
        trend_momentum = 4

    total = round(
        discount_strength
        + historical_value
        + sale_rarity
        + review_quality
        + player_interest
        + trend_momentum,
        2,
    )
    total = clamp(total, 0, 100)

    if total >= 85:
        label = "Excellent Deal"
    elif total >= 70:
        label = "Great Deal"
    elif total >= 55:
        label = "Good Deal"
    elif total >= 40:
        label = "Fair Deal"
    else:
        label = "Weak Deal"

    if latest_discount >= 50:
        summary = "This is one of the stronger buying opportunities based on discount depth and historical value."
    elif latest_discount >= 30:
        summary = "The current offer is solid and close to typical strong sale territory."
    elif latest_discount > 0:
        summary = "There is a discount now, but the value may improve during bigger seasonal events."
    else:
        summary = "No active discount right now, so waiting may improve value."

    factors = [
        {
            "name": "Discount Strength",
            "score": round(discount_strength, 2),
            "max_score": 30,
            "explanation": "How strong the current discount is versus a typical full-price baseline.",
        },
        {
            "name": "Historical Value",
            "score": round(historical_value, 2),
            "max_score": 25,
            "explanation": "How close the current price is to the best price ever seen.",
        },
        {
            "name": "Sale Rarity",
            "score": round(sale_rarity, 2),
            "max_score": 15,
            "explanation": "Games that do not discount often earn more value when they finally do.",
        },
        {
            "name": "Review Quality",
            "score": round(review_quality, 2),
            "max_score": 10,
            "explanation": "Higher review scores increase confidence that the deal is worth attention.",
        },
        {
            "name": "Player Interest",
            "score": round(player_interest, 2),
            "max_score": 10,
            "explanation": "Higher player activity can indicate stronger relevance and current interest.",
        },
        {
            "name": "Trend Momentum",
            "score": round(trend_momentum, 2),
            "max_score": 10,
            "explanation": "A rough signal based on whether the current offer is meaningfully compelling now.",
        },
    ]

    return {
        "deal_score": round(total, 2),
        "label": label,
        "summary": summary,
        "factors": factors,
    }


def calculate_prediction_v1(game: Game, latest_price, sale_rows):
    sale_dates = [row.timestamp for row in sale_rows]
    sale_discounts = [int(row.discount_percent) for row in sale_rows if row.discount_percent is not None]

    reasoning = []
    score = 0.0
    avg_gap = None

    if len(sale_dates) >= 2:
        intervals = []
        prev = sale_dates[0]
        for current in sale_dates[1:]:
            gap = (current - prev).days
            if gap > 7:
                intervals.append(gap)
            prev = current

        if intervals:
            avg_gap = mean(intervals)
            days_since_last_sale = (utc_now() - sale_dates[-1]).days
            readiness = min(days_since_last_sale / max(avg_gap, 1), 1.5)
            score += min(readiness * 0.35, 0.35)
            reasoning.append(f"Average gap between sale events is about {int(avg_gap)} days.")
            reasoning.append(f"It has been about {days_since_last_sale} days since the last sale snapshot.")

    release_dt = parse_release_date_to_datetime(game.release_date_text)
    if release_dt:
        release_age_days = (utc_now() - release_dt).days
        if release_age_days > 365:
            score += 0.15
            reasoning.append("Older games tend to discount more predictably than newly released titles.")
        elif release_age_days > 180:
            score += 0.08

    if game.review_score:
        if game.review_score >= 85:
            score += 0.10
            reasoning.append("Strong review score supports recurring promotional visibility.")
        elif game.review_score >= 70:
            score += 0.05

    latest_discount = int(latest_price.discount_percent or 0) if latest_price else 0
    if latest_discount == 0:
        score += 0.20
        reasoning.append("The game is not currently discounted, increasing the chance of a future sale event.")
    else:
        score -= 0.10
        reasoning.append("The game is already discounted, so an immediate follow-up sale is less likely.")

    avg_discount = int(mean(sale_discounts)) if sale_discounts else None
    if avg_discount:
        if avg_discount >= 50:
            score += 0.10
        elif avg_discount >= 30:
            score += 0.06
        reasoning.append(f"Typical sale depth appears to be around {avg_discount}%.")

    score = clamp(score, 0.0, 0.95)
    sale_probability_30d = round(score, 2)
    sale_probability_7d = round(min(score * 0.45, 0.80), 2)

    if score >= 0.7:
        confidence = "high"
    elif score >= 0.4:
        confidence = "medium"
    else:
        confidence = "low"

    if avg_gap:
        predicted_start = utc_now() + datetime.timedelta(days=max(int(avg_gap * 0.7), 7))
        predicted_end = predicted_start + datetime.timedelta(days=14)
    else:
        predicted_start = utc_now() + datetime.timedelta(days=21)
        predicted_end = predicted_start + datetime.timedelta(days=14)

    return {
        "sale_probability_7d": sale_probability_7d,
        "sale_probability_30d": sale_probability_30d,
        "predicted_discount_percent": avg_discount,
        "predicted_sale_window_start": predicted_start.isoformat(),
        "predicted_sale_window_end": predicted_end.isoformat(),
        "confidence": confidence,
        "reasoning": reasoning or ["Prediction generated from limited platform signals."],
    }


def build_game_detail_payload(session, game: Game):
    rows = (
        session.query(GamePrice)
        .filter(GamePrice.game_name == game.name)
        .order_by(GamePrice.timestamp.asc(), GamePrice.id.asc())
        .all()
    )
    latest_price = rows[-1] if rows else None
    historical_low_row = find_historical_low_row(rows)
    sale_rows = [row for row in rows if (row.discount_percent or 0) > 0]
    sale_event_count = count_distinct_sale_events(sale_rows)
    latest_sale = sale_rows[-1] if sale_rows else None

    discount_values = [int(row.discount_percent or 0) for row in rows if row.discount_percent is not None]
    avg_discount_percent = round(float(mean(discount_values)), 2) if discount_values else None
    max_discount_percent = max(discount_values) if discount_values else None
    latest_player_count = latest_price.current_players if latest_price else None
    days_since_last_sale = (utc_now() - latest_sale.timestamp).days if latest_sale else None

    market = {
        "sale_event_count": sale_event_count,
    }
    deal = calculate_deal_explanation(game, latest_price, historical_low_row, market)
    prediction = calculate_prediction_v1(game, latest_price, sale_rows)

    appid = None
    try:
        appid = int(game.appid) if game.appid is not None else None
    except (TypeError, ValueError):
        appid = None

    banner_url = build_steam_banner_url(game.store_url, game.appid)
    release_dt = parse_release_date_to_datetime(game.release_date_text)
    watchlisted = (
        session.query(Watchlist.id)
        .filter(Watchlist.user_id == DEFAULT_USER_ID, Watchlist.game_id == game.id)
        .first()
        is not None
    )
    wishlist_count = session.query(func.count(WishlistItem.id)).filter(WishlistItem.game_id == game.id).scalar() or 0

    return {
        "id": game.id,
        "steam_app_id": appid or 0,
        "name": game.name,
        "slug": None,
        "header_image": banner_url,
        "banner_image": banner_url,
        "short_description": None,
        "developer": game.developer,
        "publisher": game.publisher,
        "release_date": release_dt.isoformat() if release_dt else None,
        "review_score": game.review_score,
        "review_score_label": game.review_score_label,
        "review_count": game.review_total_count,
        "tags": parse_csv_field(game.tags),
        "current_price": latest_price.price if latest_price else None,
        "original_price": latest_price.original_price if latest_price else None,
        "discount_percent": latest_price.discount_percent if latest_price else None,
        "current_players": latest_price.current_players if latest_price else None,
        "historical_low_price": historical_low_row.price if historical_low_row else None,
        "historical_low_date": historical_low_row.timestamp.isoformat() if historical_low_row else None,
        "deal_score": deal["deal_score"],
        "deal_label": deal["label"],
        "deal_summary": deal["summary"],
        "wishlist_count": int(wishlist_count),
        "watchlisted": watchlisted,
        "market_insights": {
            "historical_low_price": historical_low_row.price if historical_low_row else None,
            "historical_low_date": historical_low_row.timestamp.isoformat() if historical_low_row else None,
            "avg_discount_percent": avg_discount_percent,
            "max_discount_percent": max_discount_percent,
            "sale_event_count": sale_event_count,
            "days_since_last_sale": days_since_last_sale,
            "latest_player_count": latest_player_count,
        },
        "prediction": prediction,
    }


@app.get("/robots.txt", include_in_schema=False)
def robots_txt():
    body = (
        "User-agent: *\n"
        "Allow: /\n\n"
        f"Sitemap: {SITE_URL.rstrip('/')}/sitemap.xml\n"
    )
    return PlainTextResponse(body)


@app.get("/site-config.js", include_in_schema=False)
def site_config_js():
    payload = {
        "site_name": SITE_NAME,
        "site_url": SITE_URL.rstrip("/"),
        "site_description": SITE_DESCRIPTION,
    }
    content = (
        "window.__GAMEDEN_SITE__ = Object.freeze("
        f"{json.dumps(payload, ensure_ascii=True)}"
        ");\n"
    )
    return Response(content=content, media_type="application/javascript")


@app.get("/site.webmanifest", include_in_schema=False)
def site_manifest():
    site_url = SITE_URL.rstrip("/")
    manifest_payload = {
        "name": SITE_NAME,
        "short_name": SITE_NAME,
        "description": SITE_DESCRIPTION,
        "id": f"{site_url}/",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#050913",
        "theme_color": "#050913",
        "icons": [
            {
                "src": "/web/favicon.ico",
                "sizes": "any",
                "type": "image/x-icon",
            }
        ],
    }
    return Response(
        content=json.dumps(manifest_payload, ensure_ascii=False),
        media_type="application/manifest+json",
    )


@app.get("/sitemap.xml", include_in_schema=False)
def sitemap_xml():
    today = utc_now().date().isoformat()
    urls = []
    for path in SITEMAP_PATHS:
        location = _build_canonical_url(path)
        urls.append(
            "  <url>\n"
            f"    <loc>{location}</loc>\n"
            f"    <lastmod>{today}</lastmod>\n"
            "  </url>"
        )

    xml = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        f"{chr(10).join(urls)}\n"
        "</urlset>\n"
    )
    return Response(content=xml, media_type="application/xml")


@app.get("/")
def home():
    return FileResponse("web/index.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse("web/favicon.ico")


@app.get("/worth-buying-now")
def worth_buying_now_page():
    return RedirectResponse(url="/web/all-results.html?view=worth-buying-now&title=Worth%20Buying%20Now")


@app.get("/trending-deals")
def trending_deals_page():
    return RedirectResponse(url="/web/all-results.html?view=trending-deals&title=Trending%20Deals")


@app.get("/historical-lows")
def historical_lows_page():
    return RedirectResponse(url="/web/all-results.html?view=historical-lows&title=Historical%20Lows")


@app.get("/health")
def health():
    now = utc_now()
    database_status = "ok"
    snapshot_status = "ok"
    ingestion_status = "ok"
    cache_status = "ok"
    dirty_queue_status = "ok"

    session = ReadSessionLocal()
    try:
        try:
            session.execute(text("SELECT 1"))
        except Exception:
            database_status = "error"

        job = session.query(JobStatus).filter(JobStatus.job_name == "refresh_snapshots").first()
        if job is None or job.last_success_at is None:
            snapshot_status = "stale"
        else:
            last_success = job.last_success_at
            if last_success.tzinfo is None:
                last_success = last_success.replace(tzinfo=datetime.timezone.utc)
            if now - last_success > datetime.timedelta(minutes=10):
                snapshot_status = "stale"

        ingestion_job = session.query(JobStatus).filter(JobStatus.job_name == "price_ingestion").first()
        if ingestion_job is None or ingestion_job.last_success_at is None:
            ingestion_status = "stale"
        else:
            ing_success = ingestion_job.last_success_at
            if ing_success.tzinfo is None:
                ing_success = ing_success.replace(tzinfo=datetime.timezone.utc)
            if now - ing_success > datetime.timedelta(minutes=20):
                ingestion_status = "stale"

        dirty_count = int(session.query(func.count(DirtyGame.game_id)).scalar() or 0)
        if dirty_count >= 5000:
            dirty_queue_status = "backlogged"
        oldest_dirty = session.query(func.min(DirtyGame.updated_at)).scalar()
        if oldest_dirty is not None:
            if oldest_dirty.tzinfo is None:
                oldest_dirty = oldest_dirty.replace(tzinfo=datetime.timezone.utc)
            if now - oldest_dirty > datetime.timedelta(minutes=60):
                dirty_queue_status = "stale"

        cache_row = None
        for cache_key in (PRIMARY_DASHBOARD_CACHE_KEY, *LEGACY_DASHBOARD_CACHE_KEYS):
            candidate = session.query(DashboardCache).filter(DashboardCache.cache_key == cache_key).first()
            if candidate is not None:
                cache_row = candidate
                break
        if cache_row is None:
            cache_status = "missing"
        elif cache_row.updated_at:
            updated_at = cache_row.updated_at
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
            if now - updated_at > datetime.timedelta(minutes=15):
                cache_status = "stale"
    finally:
        session.close()

    status = "ok"
    if (
        database_status != "ok"
        or snapshot_status != "ok"
        or ingestion_status != "ok"
        or cache_status != "ok"
        or dirty_queue_status != "ok"
    ):
        status = "degraded"

    return {
        "status": status,
        "database": database_status,
        "ingestion_worker": ingestion_status,
        "snapshot_worker": snapshot_status,
        "dirty_queue": dirty_queue_status,
        "cache": cache_status,
        "timestamp": now.isoformat(),
    }


@app.get("/metrics")
def metrics():
    now = utc_now()
    latency = get_latency_stats()
    cache = get_cache_stats()

    snapshot_last_success_iso = None
    snapshot_minutes_since_success = None
    ingestion_last_success_iso = None
    ingestion_minutes_since_success = None
    dirty_games_count = 0
    dirty_games_oldest_minutes = None
    dirty_games_retry_total = 0
    dirty_games_retrying = 0
    game_prices_written_15m = 0
    cache_freshness = {}
    snapshot_last_duration_ms = None
    ingestion_last_duration_ms = None
    snapshot_last_items_total = None
    snapshot_last_items_success = None
    snapshot_last_items_failed = None
    ingestion_last_items_total = None
    ingestion_last_items_success = None
    ingestion_last_items_failed = None

    session = ReadSessionLocal()
    try:
        snapshot_job = session.query(JobStatus).filter(JobStatus.job_name == "refresh_snapshots").first()
        if snapshot_job and snapshot_job.last_success_at:
            last_success = snapshot_job.last_success_at
            if last_success.tzinfo is None:
                last_success = last_success.replace(tzinfo=datetime.timezone.utc)
            snapshot_minutes_since_success = round((now - last_success).total_seconds() / 60.0, 2)
            snapshot_last_success_iso = last_success.isoformat()
        snapshot_last_duration_ms = snapshot_job.last_duration_ms if snapshot_job else None
        snapshot_last_items_total = snapshot_job.last_items_total if snapshot_job else None
        snapshot_last_items_success = snapshot_job.last_items_success if snapshot_job else None
        snapshot_last_items_failed = snapshot_job.last_items_failed if snapshot_job else None

        ingestion_job = session.query(JobStatus).filter(JobStatus.job_name == "price_ingestion").first()
        if ingestion_job and ingestion_job.last_success_at:
            ingest_success = ingestion_job.last_success_at
            if ingest_success.tzinfo is None:
                ingest_success = ingest_success.replace(tzinfo=datetime.timezone.utc)
            ingestion_minutes_since_success = round((now - ingest_success).total_seconds() / 60.0, 2)
            ingestion_last_success_iso = ingest_success.isoformat()
        ingestion_last_duration_ms = ingestion_job.last_duration_ms if ingestion_job else None
        ingestion_last_items_total = ingestion_job.last_items_total if ingestion_job else None
        ingestion_last_items_success = ingestion_job.last_items_success if ingestion_job else None
        ingestion_last_items_failed = ingestion_job.last_items_failed if ingestion_job else None

        dirty_games_count = int(session.query(func.count(DirtyGame.game_id)).scalar() or 0)
        dirty_games_retry_total = int(session.query(func.coalesce(func.sum(DirtyGame.retry_count), 0)).scalar() or 0)
        dirty_games_retrying = int(session.query(func.count(DirtyGame.game_id)).filter(DirtyGame.retry_count > 0).scalar() or 0)
        oldest_dirty = session.query(func.min(DirtyGame.updated_at)).scalar()
        if oldest_dirty:
            if oldest_dirty.tzinfo is None:
                oldest_dirty = oldest_dirty.replace(tzinfo=datetime.timezone.utc)
            dirty_games_oldest_minutes = round((now - oldest_dirty).total_seconds() / 60.0, 2)

        recent_cutoff = now - datetime.timedelta(minutes=15)
        game_prices_written_15m = int(
            session.query(func.count(GamePrice.id))
            .filter(GamePrice.recorded_at >= recent_cutoff)
            .scalar()
            or 0
        )

        cache_keys = [
            PRIMARY_DASHBOARD_CACHE_KEY,
            *LEGACY_DASHBOARD_CACHE_KEYS,
            "home:worth_buying",
            "home:trending",
            "home:historical_lows",
            "home:biggest_price_drops",
            "home:deal_radar",
            "home:top_played",
            "home:upcoming",
        ]
        rows = session.query(DashboardCache).filter(DashboardCache.cache_key.in_(cache_keys)).all()
        for row in rows:
            updated_at = row.updated_at
            age_minutes = None
            if updated_at:
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
                age_minutes = round((now - updated_at).total_seconds() / 60.0, 2)
            cache_freshness[row.cache_key] = {
                "updated_at": updated_at.isoformat() if updated_at else None,
                "age_minutes": age_minutes,
            }
    finally:
        session.close()

    status = "ok"
    if snapshot_minutes_since_success is None or snapshot_minutes_since_success > 10:
        status = "warning"
    if ingestion_minutes_since_success is None or ingestion_minutes_since_success > 20:
        status = "warning"
    if dirty_games_count >= 5000:
        status = "warning"
    if dirty_games_oldest_minutes is not None and dirty_games_oldest_minutes > 60:
        status = "warning"

    dashboard_latency = latency.get("/dashboard/home")
    if dashboard_latency and int(dashboard_latency.get("count", 0)) >= 20:
        if float(dashboard_latency.get("p95_ms", 0.0)) > 100.0:
            status = "warning"

    search_latency = latency.get("/search")
    if search_latency and int(search_latency.get("count", 0)) >= 20:
        if float(search_latency.get("p95_ms", 0.0)) > 150.0:
            status = "warning"

    return {
        "status": status,
        "timestamp": now.isoformat(),
        "latency": latency,
        "cache": cache,
        "cache_freshness": cache_freshness,
        "pipeline": {
            "game_prices_written_15m": game_prices_written_15m,
            "dirty_games_oldest_minutes": dirty_games_oldest_minutes,
            "dirty_games_retry_total": dirty_games_retry_total,
            "dirty_games_retrying": dirty_games_retrying,
        },
        "worker": {
            "snapshot_last_success_at": snapshot_last_success_iso,
            "snapshot_minutes_since_success": snapshot_minutes_since_success,
            "snapshot_last_duration_ms": snapshot_last_duration_ms,
            "snapshot_last_items_total": snapshot_last_items_total,
            "snapshot_last_items_success": snapshot_last_items_success,
            "snapshot_last_items_failed": snapshot_last_items_failed,
            "ingestion_last_success_at": ingestion_last_success_iso,
            "ingestion_minutes_since_success": ingestion_minutes_since_success,
            "ingestion_last_duration_ms": ingestion_last_duration_ms,
            "ingestion_last_items_total": ingestion_last_items_total,
            "ingestion_last_items_success": ingestion_last_items_success,
            "ingestion_last_items_failed": ingestion_last_items_failed,
            "dirty_games_count": dirty_games_count,
        },
    }


@app.get("/games/latest-prices")
def get_latest_prices():
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        historical_insight_map = compute_historical_insight_map(session)
        return [serialize_price_row(row, game_map, historical_insight_map) for row in latest_prices]
    finally:
        session.close()


@app.get("/games/deal-ranked")
def get_deal_ranked_games(
    limit: int = Query(default=API_DEFAULT_PAGE_SIZE, ge=1, le=API_MAX_PAGE_SIZE),
    include_free: bool = Query(default=False),
):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)

        rows = [serialize_price_row(row, game_map, insight_map) for row in latest_prices]

        if not include_free:
            rows = [row for row in rows if row["price"] is not None and row["price"] > 0]

        rows.sort(
            key=lambda row: (
                row["deal_score"],
                row["discount_percent"] if row["discount_percent"] is not None else -1,
                row["review_score"] if row["review_score"] is not None else -1,
                row["current_players"] if row["current_players"] is not None else -1,
                row["game_name"].lower(),
            ),
            reverse=True,
        )

        return rows[:limit]
    finally:
        session.close()


@app.get("/games/historical-lows")
def get_historical_lows(limit: int = Query(default=API_DEFAULT_LIST_LIMIT, ge=1, le=API_MAX_LIST_LIMIT)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)

        rows = []
        for row in latest_prices:
            serialized = serialize_price_row(row, game_map, insight_map)

            if serialized["history_point_count"] < 2:
                continue

            if serialized["price"] is None or serialized["price"] <= 0:
                continue

            if serialized["discount_percent"] is None or serialized["discount_percent"] <= 0:
                continue

            if not serialized["historical_status"]:
                continue

            rows.append(serialized)

        status_priority = {
            "new_historical_low": 3,
            "matches_historical_low": 2,
            "near_historical_low": 1,
        }

        rows.sort(
            key=lambda r: (
                status_priority.get(r["historical_status"], 0),
                r["discount_percent"] if r["discount_percent"] is not None else 0,
                r["deal_score"],
                r["history_point_count"],
                r["game_name"].lower(),
            ),
            reverse=True,
        )

        return rows[:limit]

    finally:
        session.close()


@app.get("/sales/seasonal-summary")
@json_etag()
@ttl_cache(ttl_seconds=90, endpoint_key="/sales/seasonal-summary")
def get_seasonal_summary(limit: int = Query(default=12, ge=1, le=30)):
    session = ReadSessionLocal()

    try:
        today = utc_now().date()
        sale_window = get_seasonal_sale_window(today)
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)

        is_live = sale_window.get("status") == "live"
        if is_live:
            seasonal_items = build_active_sale_rows(latest_prices, game_map, insight_map)[:limit]
            seasonal_mode = "active_sale"
        else:
            seasonal_items = build_expected_sale_rows(latest_prices, game_map, insight_map)[:limit]
            seasonal_mode = "potential_sale"

        return {
            "sale_event": {
                "name": sale_window["name"],
                "slug": sale_window["slug"],
                "status": sale_window["status"],
                "start_date": sale_window["start"].isoformat(),
                "end_date": sale_window["end"].isoformat(),
                "days_until_start": sale_window["days_until_start"],
            },
            "mode": seasonal_mode,
            "items": seasonal_items,
            # Backward-compatible field retained for existing clients.
            "expected_games": seasonal_items,
        }

    finally:
        session.close()


@app.get("/games/biggest-discounts")
def get_biggest_discounts(limit: int = Query(default=20, ge=1, le=100)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)

        discounted_rows = [row for row in latest_prices if is_paid_discount_row(row)]

        discounted_rows.sort(
            key=lambda row: (
                row.discount_percent if row.discount_percent is not None else 0,
                row.original_price if row.original_price is not None else 0,
                row.price if row.price is not None else 0,
                row.game_name.lower(),
            ),
            reverse=True,
        )

        return [serialize_price_row(row, game_map, insight_map) for row in discounted_rows[:limit]]

    finally:
        session.close()


@app.get("/games/top-reviewed")
def get_top_reviewed_games(limit: int = Query(default=20, ge=1, le=100)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)

        results = []
        for row in latest_prices:
            game = game_map.get(row.game_name)
            if not game or game.review_score is None:
                continue
            results.append(serialize_price_row(row, game_map, insight_map))

        results.sort(
            key=lambda row: (
                row["review_score"] if row["review_score"] is not None else -1,
                row["review_total_count"] if row["review_total_count"] is not None else -1,
                row["game_name"].lower(),
            ),
            reverse=True,
        )

        return results[:limit]

    finally:
        session.close()


@app.get("/games/top-played")
def get_top_played_games(limit: int = Query(default=50, ge=1, le=100)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)
        results = [serialize_price_row(row, game_map, insight_map) for row in latest_prices]

        results.sort(
            key=lambda row: (
                row["current_players"] if row["current_players"] is not None else -1,
                row["game_name"].lower(),
            ),
            reverse=True,
        )

        return results[:limit]

    finally:
        session.close()


@app.get("/games/player-leaderboard")
def get_player_leaderboard(limit: int = Query(default=100, ge=1, le=250)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        snapshot_map = {
            row.game_name: row
            for row in session.query(GameSnapshot.game_name, GameSnapshot.daily_peak, GameSnapshot.avg_player_count).all()
        }

        leaderboard = []

        for row in latest_prices:
            if row.current_players is None:
                continue
            snapshot = snapshot_map.get(row.game_name)
            daily_peak = snapshot.daily_peak if snapshot else None
            avg_30d = snapshot.avg_player_count if snapshot else None

            leaderboard.append(
                {
                    "game_name": row.game_name,
                    "current_players": row.current_players,
                    "daily_peak": int(daily_peak) if daily_peak is not None else None,
                    "avg_30d": round(float(avg_30d), 1) if avg_30d is not None else None,
                    "price": row.price,
                    "discount_percent": row.discount_percent,
                    "store_url": row.store_url,
                }
            )

        leaderboard.sort(
            key=lambda r: (
                r["current_players"] if r["current_players"] is not None else -1,
                r["game_name"].lower(),
            ),
            reverse=True,
        )

        return leaderboard[:limit]

    finally:
        session.close()


@app.get("/games/trending")
def get_trending_games(limit: int = Query(default=20, ge=1, le=100)):
    session = Session()

    try:
        rows = (
            session.query(GameSnapshot)
            .filter(GameSnapshot.current_players.isnot(None))
            .order_by(GameSnapshot.momentum_score.desc().nullslast(), GameSnapshot.game_name.asc())
            .limit(limit)
            .all()
        )
        trending_results = [
            {
                "game_name": row.game_name,
                "current_players": row.current_players,
                "previous_players": None,
                "player_change": int((row.short_term_player_trend or 0.0) * (row.current_players or 0)),
                "price": row.latest_price,
                "discount_percent": row.latest_discount_percent,
                "store_url": row.store_url,
                "timestamp": row.updated_at.isoformat() if row.updated_at else None,
                "trend_reason_summary": row.trend_reason_summary,
                "momentum_score": row.momentum_score,
            }
            for row in rows
        ]

        trending_results.sort(
            key=lambda row: (row["player_change"], row["current_players"]),
            reverse=True,
        )
        return trending_results[:limit]

    finally:
        session.close()


@app.get("/search")
@json_etag()
@rate_limit(max_requests=60, window_seconds=60)
@ttl_cache(ttl_seconds=30, endpoint_key="/search")
def search_games_fast(
    request: Request,
    q: str = Query(default="", min_length=1),
    limit: int = Query(default=10, ge=1, le=20),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        query_text = q.strip()
        if not query_text:
            return []

        try:
            normalized_query = _normalize_search_text(query_text)
            rows = session.execute(
                text(
                    """
                    SELECT
                        g.id,
                        g.name AS game_name,
                        g.developer,
                        g.publisher,
                        s.steam_appid,
                        COALESCE(s.banner_url, 'https://cdn.cloudflare.steamstatic.com/steam/apps/' || g.appid || '/header.jpg') AS image_url,
                        s.latest_price,
                        s.latest_discount_percent,
                        s.deal_score,
                        COALESCE(s.buy_score, s.worth_buying_score) AS buy_score,
                        s.worth_buying_score,
                        COALESCE(s.review_score_label, g.review_score_label) AS review_score_label,
                        COALESCE(s.review_score, g.review_score) AS review_score,
                        COALESCE(s.review_count, g.review_total_count) AS review_total_count,
                        s.deal_heat_reason,
                        s.release_date,
                        s.is_upcoming,
                        similarity(lower(g.name), :normalized_q) AS sim
                    FROM games g
                    LEFT JOIN game_snapshots s ON s.game_id = g.id
                    WHERE
                        g.name ILIKE ('%' || :q || '%')
                        OR COALESCE(g.developer, '') ILIKE ('%' || :q || '%')
                        OR COALESCE(g.publisher, '') ILIKE ('%' || :q || '%')
                        OR similarity(lower(g.name), :normalized_q) > :sim_threshold
                    ORDER BY
                        CASE WHEN lower(g.name) = :normalized_q THEN 0 ELSE 1 END,
                        CASE WHEN lower(g.name) LIKE (:normalized_q || '%') THEN 0 ELSE 1 END,
                        sim DESC,
                        CASE WHEN lower(g.name) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                        COALESCE(s.deal_score, 0) DESC,
                        g.name ASC
                    LIMIT :limit
                    """
                ),
                {
                    "q": query_text,
                    "normalized_q": normalized_query,
                    "sim_threshold": SEARCH_SIMILARITY_THRESHOLD,
                    "limit": int(limit),
                },
            ).mappings().all()
        except Exception:
            rows = session.execute(
                text(
                    """
                    SELECT
                        g.id,
                        g.name AS game_name,
                        g.developer,
                        g.publisher,
                        s.steam_appid,
                        COALESCE(s.banner_url, 'https://cdn.cloudflare.steamstatic.com/steam/apps/' || g.appid || '/header.jpg') AS image_url,
                        s.latest_price,
                        s.latest_discount_percent,
                        s.deal_score,
                        COALESCE(s.buy_score, s.worth_buying_score) AS buy_score,
                        s.worth_buying_score,
                        COALESCE(s.review_score_label, g.review_score_label) AS review_score_label,
                        COALESCE(s.review_score, g.review_score) AS review_score,
                        COALESCE(s.review_count, g.review_total_count) AS review_total_count,
                        s.deal_heat_reason,
                        s.release_date,
                        s.is_upcoming
                    FROM games g
                    LEFT JOIN game_snapshots s ON s.game_id = g.id
                    WHERE
                        g.name ILIKE ('%' || :q || '%')
                        OR COALESCE(g.developer, '') ILIKE ('%' || :q || '%')
                        OR COALESCE(g.publisher, '') ILIKE ('%' || :q || '%')
                    ORDER BY
                        CASE WHEN lower(g.name) = lower(:q) THEN 0 ELSE 1 END,
                        CASE WHEN lower(g.name) LIKE (lower(:q) || '%') THEN 0 ELSE 1 END,
                        CASE WHEN lower(g.name) LIKE ('%' || lower(:q) || '%') THEN 0 ELSE 1 END,
                        COALESCE(s.deal_score, 0) DESC,
                        g.name ASC
                    LIMIT :limit
                    """
                ),
                {"q": query_text, "limit": int(limit)},
            ).mappings().all()

        return [
            {
                "game_id": row["id"],
                "game_name": row["game_name"],
                "developer": row.get("developer"),
                "publisher": row.get("publisher"),
                "steam_appid": row["steam_appid"],
                "banner_url": row["image_url"],
                "image_url": row["image_url"],
                "latest_price": row["latest_price"],
                "latest_discount_percent": row["latest_discount_percent"],
                "deal_score": row["deal_score"],
                "buy_score": row.get("buy_score") if row.get("buy_score") is not None else row["worth_buying_score"],
                "worth_buying_score": row["worth_buying_score"],
                "review_score": row.get("review_score"),
                "review_total_count": row.get("review_total_count"),
                "review_score_label": row.get("review_score_label"),
                "deal_heat_reason": row["deal_heat_reason"],
                "release_date": row["release_date"].isoformat() if row["release_date"] else None,
                "is_upcoming": bool(row["is_upcoming"]) if row["is_upcoming"] is not None else False,
            }
            for row in rows
        ]
    finally:
        session.close()
        _log_timing("/search", started)


@app.get("/games/search")
def search_games(q: str = Query(default="", min_length=0)):
    session = Session()

    try:
        latest_prices = get_latest_price_rows(session)
        game_map = build_game_map(session)
        insight_map = compute_historical_insight_map(session)
        results = [serialize_price_row(row, game_map, insight_map) for row in latest_prices]

        query = q.strip().lower()

        if query:
            results = [row for row in results if query in row["game_name"].lower()]

        results.sort(key=lambda row: row["game_name"].lower())
        return results

    finally:
        session.close()


@app.get("/games/upcoming")
def get_upcoming_games():
    session = Session()

    try:
        rows = session.query(Game).filter(Game.is_released == 0).all()

        rows.sort(
            key=lambda row: (
                parse_release_date_sort_key(row.release_date_text),
                row.name.lower(),
            )
        )

        return [serialize_upcoming_row(row) for row in rows]

    finally:
        session.close()


@app.get("/games/released")
@json_etag()
@rate_limit(max_requests=120, window_seconds=60)
@ttl_cache(ttl_seconds=60, endpoint_key="/games/released")
def get_released_games(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=API_DEFAULT_PAGE_SIZE, ge=1, le=API_MAX_PAGE_SIZE),
    sort: str = Query(default="deal-score"),
    q: str = Query(default=""),
    genre: str = Query(default=""),
    tag: str = Query(default=""),
    platform: str = Query(default=""),
    review_label: str = Query(default=""),
    min_discount: int | None = Query(default=None, ge=0, le=100),
    max_price: float | None = Query(default=None, ge=0),
    min_players: int | None = Query(default=None, ge=0),
    deals_only: bool = Query(default=False),
    include_free: bool = Query(default=True),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        search_text = q.strip()
        sort_mapping = {
            "best-deals": [
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                Game.name.asc(),
            ],
            "best-reviews": [
                GameSnapshot.review_score.desc().nullslast(),
                GameSnapshot.review_count.desc().nullslast(),
                Game.name.asc(),
            ],
            "player-count": [
                GameSnapshot.avg_player_count.desc().nullslast(),
                Game.name.asc(),
            ],
            "price-asc": [
                GameSnapshot.latest_price.asc().nullslast(),
                Game.name.asc(),
            ],
            "price-desc": [
                GameSnapshot.latest_price.desc().nullslast(),
                Game.name.asc(),
            ],
            "alpha-asc": [Game.name.asc()],
            "alpha-desc": [Game.name.desc()],
            "deal-score": [
                GameSnapshot.deal_score.desc().nullslast(),
                Game.name.asc(),
            ],
        }

        order_by_columns = sort_mapping.get(sort, sort_mapping["deal-score"])
        can_use_similarity = (
            bool(search_text)
            and bool(session.bind)
            and session.bind.dialect.name == "postgresql"
        )

        def build_released_query(include_similarity: bool):
            released_query = (
                session.query(Game, GameSnapshot)
                .outerjoin(GameSnapshot, GameSnapshot.game_id == Game.id)
                .filter(Game.is_released == 1)
                .filter(
                    or_(
                        GameSnapshot.is_upcoming.is_(False),
                        GameSnapshot.is_upcoming.is_(None),
                        GameSnapshot.game_id.is_(None),
                    )
                )
            )

            if search_text:
                search_predicate = _build_catalog_search_predicate(search_text, include_similarity=include_similarity)
                if search_predicate is not None:
                    released_query = released_query.filter(search_predicate)
            if genre.strip():
                released_query = released_query.filter(GameSnapshot.genres.ilike(f"%{genre.strip()}%"))
            if tag.strip():
                released_query = released_query.filter(GameSnapshot.tags.ilike(f"%{tag.strip()}%"))
            if platform.strip():
                platform_predicate = _build_platform_filter_predicate(platform)
                if platform_predicate is not None:
                    released_query = released_query.filter(platform_predicate)
            if review_label.strip() and hasattr(GameSnapshot, "review_score_label"):
                released_query = released_query.filter(GameSnapshot.review_score_label.ilike(f"%{review_label.strip()}%"))
            if min_discount is not None:
                released_query = released_query.filter(GameSnapshot.latest_discount_percent >= min_discount)
            if max_price is not None:
                released_query = released_query.filter(GameSnapshot.latest_price <= max_price)
            if min_players is not None:
                released_query = released_query.filter(GameSnapshot.avg_player_count >= min_players)
            if deals_only:
                released_query = released_query.filter(GameSnapshot.latest_discount_percent > 0)
            if not include_free:
                released_query = released_query.filter(
                    (GameSnapshot.latest_price.is_(None)) | (GameSnapshot.latest_price > 0)
                )

            if search_text:
                relevance_order_columns = _build_name_relevance_order_columns(search_text, include_similarity=include_similarity)
                released_query = released_query.order_by(*relevance_order_columns, *order_by_columns)
            else:
                released_query = released_query.order_by(*order_by_columns)
            return released_query

        try:
            query_with_order = build_released_query(include_similarity=can_use_similarity)
            total = query_with_order.order_by(None).count()
            total_pages = max(1, (total + page_size - 1) // page_size) if total else 0
            offset = (page - 1) * page_size
            rows = query_with_order.limit(page_size).offset(offset).all()
        except Exception:
            if not can_use_similarity:
                raise
            logger.warning(
                "Falling back to non-similarity released search ranking for q=%r",
                search_text,
                exc_info=True,
            )
            query_with_order = build_released_query(include_similarity=False)
            total = query_with_order.order_by(None).count()
            total_pages = max(1, (total + page_size - 1) // page_size) if total else 0
            offset = (page - 1) * page_size
            rows = query_with_order.limit(page_size).offset(offset).all()

        items = []
        for game, snapshot in rows:
            image_url = (
                (snapshot.banner_url if snapshot else None)
                or build_steam_banner_url(game.store_url, game.appid)
            )
            items.append(
                {
                    "id": game.id,
                    "steam_appid": (snapshot.steam_appid if snapshot else None) or game.appid,
                    "game_name": game.name,
                    "banner_url": image_url,
                    "image_url": image_url,
                    "price": snapshot.latest_price if snapshot else None,
                    "original_price": snapshot.latest_original_price if snapshot else None,
                    "discount_percent": snapshot.latest_discount_percent if snapshot else None,
                    "historical_low": snapshot.historical_low if snapshot else None,
                    "historical_status": snapshot.historical_status if snapshot else None,
                    "deal_score": snapshot.deal_score if snapshot else None,
                    "buy_score": (snapshot.buy_score if snapshot and snapshot.buy_score is not None else (snapshot.worth_buying_score if snapshot else None)),
                    "buy_recommendation": snapshot.buy_recommendation if snapshot else None,
                    "buy_reason": snapshot.buy_reason if snapshot else None,
                    "price_vs_low_ratio": snapshot.price_vs_low_ratio if snapshot else None,
                    "predicted_next_sale_price": snapshot.predicted_next_sale_price if snapshot else None,
                    "predicted_next_discount_percent": snapshot.predicted_next_discount_percent if snapshot else None,
                    "predicted_sale_reason": snapshot.predicted_sale_reason if snapshot else None,
                    "worth_buying_score": snapshot.worth_buying_score if snapshot else None,
                    "worth_buying_reason_summary": snapshot.worth_buying_reason_summary if snapshot else None,
                    "momentum_score": snapshot.momentum_score if snapshot else None,
                    "popularity_score": snapshot.popularity_score if snapshot else None,
                    "trend_reason_summary": snapshot.trend_reason_summary if snapshot else None,
                    "deal_heat_level": snapshot.deal_heat_level if snapshot else None,
                    "deal_heat_reason": snapshot.deal_heat_reason if snapshot else None,
                    "deal_heat_tags": (snapshot.deal_heat_tags if snapshot else None) or [],
                    "review_score": (
                        snapshot.review_score
                        if snapshot and snapshot.review_score is not None
                        else game.review_score
                    ),
                    "review_score_label": (
                        snapshot.review_score_label
                        if snapshot and snapshot.review_score_label
                        else game.review_score_label
                    ),
                    "review_total_count": (
                        snapshot.review_count
                        if snapshot and snapshot.review_count is not None
                        else game.review_total_count
                    ),
                    "current_players": snapshot.current_players if snapshot else None,
                    "daily_peak": snapshot.daily_peak if snapshot else None,
                    "release_date": snapshot.release_date.isoformat() if snapshot and snapshot.release_date else None,
                    "genres": parse_csv_field(snapshot.genres) if snapshot else [],
                    "tags": parse_csv_field(snapshot.tags) if snapshot else [],
                    "platforms": parse_csv_field(snapshot.platforms) if snapshot else [],
                    "developer": game.developer,
                    "publisher": game.publisher,
                }
            )

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "items": items,
        }
    finally:
        session.close()
        _log_timing("/games/released", started)


@app.get("/deals/search")
@json_etag()
@ttl_cache(ttl_seconds=30, endpoint_key="/deals/search")
def search_deals(
    request: Request,
    discount_min: int | None = Query(default=None, ge=0, le=100),
    discount_max: int | None = Query(default=None, ge=0, le=100),
    price_min: float | None = Query(default=None, ge=0),
    price_max: float | None = Query(default=None, ge=0),
    review_score_min: int | None = Query(default=None, ge=0, le=100),
    players_min: int | None = Query(default=None, ge=0),
    genre: str = Query(default=""),
    release_year_min: int | None = Query(default=None, ge=1970),
    release_year_max: int | None = Query(default=None, ge=1970),
    sort: str = Query(default="trending"),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=API_DEFAULT_PAGE_SIZE, ge=1, le=API_MAX_PAGE_SIZE),
    q: str = Query(default=""),
    preset: str = Query(default=""),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        preset_value = preset if isinstance(preset, str) else ""
        sort_value = sort if isinstance(sort, str) else "trending"
        genre_value = genre if isinstance(genre, str) else ""
        q_value = q if isinstance(q, str) else ""
        discount_min_value = discount_min if isinstance(discount_min, int) else None
        discount_max_value = discount_max if isinstance(discount_max, int) else None
        price_min_value = float(price_min) if isinstance(price_min, (int, float)) else None
        price_max_value = float(price_max) if isinstance(price_max, (int, float)) else None
        review_score_min_value = review_score_min if isinstance(review_score_min, int) else None
        players_min_value = players_min if isinstance(players_min, int) else None
        release_year_min_value = release_year_min if isinstance(release_year_min, int) else None
        release_year_max_value = release_year_max if isinstance(release_year_max, int) else None

        preset_key = (preset_value or "").strip().lower()
        if preset_key == "top-deals":
            if discount_min_value is None:
                discount_min_value = 70
            sort_value = "biggest_discount"
        elif preset_key == "trending":
            sort_value = "trending"
        elif preset_key == "most-played-deals":
            if players_min_value is None:
                players_min_value = 5000
            sort_value = "most_players"
        elif preset_key == "hidden-gems":
            if review_score_min_value is None:
                review_score_min_value = 85
            if players_min_value is None:
                players_min_value = 100
            sort_value = "highest_review"

        query = (
            session.query(GameSnapshot)
            .filter(
                GameSnapshot.is_upcoming.is_(False),
                GameSnapshot.is_released == 1,
            )
        )

        if discount_min_value is not None:
            query = query.filter(GameSnapshot.latest_discount_percent >= discount_min_value)
        if discount_max_value is not None:
            query = query.filter(GameSnapshot.latest_discount_percent <= discount_max_value)
        if price_min_value is not None:
            query = query.filter(GameSnapshot.latest_price >= price_min_value)
        if price_max_value is not None:
            query = query.filter(GameSnapshot.latest_price <= price_max_value)
        if review_score_min_value is not None:
            query = query.filter(GameSnapshot.review_score >= review_score_min_value)
        if players_min_value is not None:
            query = query.filter(GameSnapshot.current_players >= players_min_value)
        if genre_value.strip():
            query = query.filter(GameSnapshot.genres.ilike(f"%{genre_value.strip()}%"))
        if q_value.strip():
            query = query.filter(GameSnapshot.game_name.ilike(f"%{q_value.strip()}%"))
        if release_year_min_value is not None:
            query = query.filter(GameSnapshot.release_date >= datetime.date(release_year_min_value, 1, 1))
        if release_year_max_value is not None:
            query = query.filter(GameSnapshot.release_date < datetime.date(release_year_max_value + 1, 1, 1))

        sort_mapping = {
            "trending": [
                GameSnapshot.trending_score.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
            "biggest_discount": [
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
            "most_players": [
                GameSnapshot.current_players.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
            "lowest_price": [
                GameSnapshot.latest_price.asc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
            "highest_review": [
                GameSnapshot.review_score.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
        }
        order_by_columns = sort_mapping.get(sort_value, sort_mapping["trending"])
        query = query.order_by(*order_by_columns)

        total_results = query.order_by(None).count()
        offset = (page - 1) * limit
        rows = query.limit(limit).offset(offset).all()

        results = [
            {
                "game_id": int(row.game_id),
                "game_name": row.game_name,
                "latest_price": row.latest_price,
                "original_price": row.latest_original_price,
                "latest_discount_percent": row.latest_discount_percent,
                "current_players": row.current_players,
                "review_score": row.review_score,
                "release_date": row.release_date.isoformat() if row.release_date else None,
                "genres": row.genres,
                "trending_score": row.trending_score,
                "steam_appid": row.steam_appid,
                "banner_url": row.banner_url,
            }
            for row in rows
        ]

        return {
            "page": page,
            "limit": limit,
            "total_results": total_results,
            "results": results,
        }
    finally:
        session.close()
        _log_timing("/deals/search", started)


@app.get("/leaderboards/{board_type}")
@json_etag()
@ttl_cache(ttl_seconds=300, endpoint_key="/leaderboards/{board_type}")
def get_leaderboard(
    request: Request,
    board_type: str,
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=100),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        board = (board_type or "").strip().lower()
        offset = (page - 1) * limit

        def _item(snapshot: GameSnapshot, event: DealEvent | None = None) -> dict:
            return {
                "game_id": int(snapshot.game_id),
                "game_name": snapshot.game_name,
                "steam_appid": snapshot.steam_appid,
                "banner_url": snapshot.banner_url,
                "latest_price": snapshot.latest_price,
                "original_price": snapshot.latest_original_price,
                "latest_discount_percent": snapshot.latest_discount_percent,
                "historical_low": snapshot.historical_low,
                "historical_low_price": snapshot.historical_low_price,
                "previous_historical_low_price": snapshot.previous_historical_low_price,
                "historical_low_hit": bool(snapshot.historical_low_hit),
                "historical_low_timestamp": snapshot.historical_low_timestamp.isoformat() if snapshot.historical_low_timestamp else None,
                "historical_low_reason_summary": snapshot.historical_low_reason_summary,
                "deal_score": snapshot.deal_score,
                "trending_score": snapshot.trending_score,
                "momentum_score": snapshot.momentum_score,
                "player_growth_ratio": snapshot.player_growth_ratio,
                "short_term_player_trend": snapshot.short_term_player_trend,
                "trend_reason_summary": snapshot.trend_reason_summary,
                "buy_score": snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score,
                "worth_buying_score": snapshot.worth_buying_score,
                "worth_buying_score_version": snapshot.worth_buying_score_version,
                "worth_buying_reason_summary": snapshot.worth_buying_reason_summary,
                "worth_buying_components": snapshot.worth_buying_components or {},
                "deal_heat_level": snapshot.deal_heat_level,
                "deal_heat_reason": snapshot.deal_heat_reason,
                "deal_heat_tags": snapshot.deal_heat_tags or [],
                "current_players": snapshot.current_players,
                "review_score": snapshot.review_score,
                "release_date": snapshot.release_date.isoformat() if snapshot.release_date else None,
                "event_type": event.event_type if event else None,
                "event_created_at": event.created_at.isoformat() if event and event.created_at else None,
                "event_reason_summary": event.event_reason_summary if event else None,
            }

        if board == "top-deals-today":
            query = (
                session.query(GameSnapshot)
                .filter(
                    GameSnapshot.latest_discount_percent.isnot(None),
                    GameSnapshot.latest_discount_percent > 0,
                    GameSnapshot.is_upcoming.is_(False),
                )
                .order_by(GameSnapshot.deal_score.desc().nullslast(), GameSnapshot.game_id.asc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(row) for row in rows]
        elif board == "historical-lows":
            week_ago = utc_now() - datetime.timedelta(days=7)
            query = (
                session.query(DealEvent, GameSnapshot)
                .join(GameSnapshot, GameSnapshot.game_id == DealEvent.game_id)
                .filter(
                    DealEvent.event_type == "HISTORICAL_LOW",
                    DealEvent.created_at >= week_ago,
                )
                .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(snapshot, event) for event, snapshot in rows]
        elif board == "biggest-price-drops":
            drop_expr = func.coalesce(DealEvent.old_price - DealEvent.new_price, 0.0)
            query = (
                session.query(DealEvent, GameSnapshot)
                .join(GameSnapshot, GameSnapshot.game_id == DealEvent.game_id)
                .filter(DealEvent.event_type == "PRICE_DROP")
                .order_by(drop_expr.desc(), DealEvent.created_at.desc(), DealEvent.id.desc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(snapshot, event) for event, snapshot in rows]
        elif board == "most-played-deals":
            query = (
                session.query(GameSnapshot)
                .filter(
                    GameSnapshot.latest_discount_percent.isnot(None),
                    GameSnapshot.latest_discount_percent > 0,
                )
                .order_by(GameSnapshot.current_players.desc().nullslast(), GameSnapshot.game_id.asc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(row) for row in rows]
        elif board == "trending-deals":
            query = (
                session.query(GameSnapshot)
                .filter(
                    GameSnapshot.latest_discount_percent.isnot(None),
                    GameSnapshot.latest_discount_percent > 0,
                )
                .order_by(GameSnapshot.momentum_score.desc().nullslast(), GameSnapshot.game_id.asc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(row) for row in rows]
        elif board == "worth-buying-now":
            query = (
                session.query(GameSnapshot)
                .filter(
                    GameSnapshot.latest_discount_percent.isnot(None),
                    GameSnapshot.latest_discount_percent > 0,
                    GameSnapshot.is_upcoming.is_(False),
                )
                .order_by(GameSnapshot.worth_buying_score.desc().nullslast(), GameSnapshot.game_id.asc())
            )
            total_results = query.order_by(None).count()
            rows = query.limit(limit).offset(offset).all()
            items = [_item(row) for row in rows]
        else:
            raise HTTPException(status_code=400, detail="Unsupported board_type")

        return {
            "board_type": board,
            "page": page,
            "limit": limit,
            "total_results": total_results,
            "items": items,
        }
    finally:
        session.close()
        _log_timing("/leaderboards/{board_type}", started)


@app.get("/games/filters")
def get_filters():
    session = Session()

    try:
        games = session.query(Game).all()

        genre_counts = {}
        tag_counts = {}
        platform_counts = {}
        found_review_labels = set()

        for game in games:
            for genre in parse_csv_field(game.genres):
                genre_counts[genre] = genre_counts.get(genre, 0) + 1

            for tag in parse_csv_field(game.tags):
                tag_counts[tag] = tag_counts.get(tag, 0) + 1

            for platform in parse_csv_field(game.platforms):
                platform_counts[platform] = platform_counts.get(platform, 0) + 1

            if game.review_score_label:
                found_review_labels.add(game.review_score_label)

        genres = sorted(genre_counts.keys(), key=lambda g: (-genre_counts[g], g.lower()))
        tags = sorted(tag_counts.keys(), key=lambda t: (-tag_counts[t], t.lower()))
        platforms = _extend_platform_filter_options(
            sorted(platform_counts.keys(), key=lambda p: (-platform_counts[p], p.lower()))
        )
        review_labels = sorted(found_review_labels)

        return {
            "genres": genres,
            "tags": tags,
            "platforms": platforms,
            "review_labels": review_labels,
        }

    finally:
        session.close()


def _read_cache_payload(session: Session, cache_key: str):
    row = session.query(DashboardCache).filter(DashboardCache.cache_key == cache_key).first()
    if not row:
        return None, None
    try:
        return row, json.loads(row.payload)
    except json.JSONDecodeError:
        logger.exception("Invalid dashboard cache JSON for key=%s", cache_key)
        return row, None


def _dashboard_cache_keys() -> tuple[str, ...]:
    return (PRIMARY_DASHBOARD_CACHE_KEY, *LEGACY_DASHBOARD_CACHE_KEYS)


def _dashboard_payload_is_empty(payload) -> bool:
    if payload is None:
        return True
    if isinstance(payload, dict):
        return len(payload) == 0
    if isinstance(payload, list):
        return len(payload) == 0
    return False


def _dashboard_cache_is_stale(cache_row: DashboardCache, now: datetime.datetime) -> bool:
    if cache_row is None or cache_row.updated_at is None:
        return True
    updated_at = cache_row.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
    return now - updated_at > DASHBOARD_CACHE_STALE_AFTER


def _read_dashboard_cache(session):
    for cache_key in _dashboard_cache_keys():
        row, payload = _read_cache_payload(session, cache_key)
        if row is not None:
            return row, payload
    return None, None


def _rebuild_dashboard_cache_on_demand():
    session = Session()
    try:
        from jobs.refresh_snapshots import rebuild_dashboard_cache

        rebuild_dashboard_cache(session)
        session.commit()
        return _read_dashboard_cache(session)
    except Exception:
        session.rollback()
        logger.exception("On-demand /dashboard/home cache rebuild failed")
        return None, None
    finally:
        session.close()


def _dashboard_rows(payload: dict, *keys: str) -> list[dict]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _dashboard_identity_key(row: dict, index: int = 0) -> str:
    game_id = row.get("game_id") or row.get("id")
    try:
        parsed_id = int(game_id)
    except Exception:
        parsed_id = 0
    if parsed_id > 0:
        return f"id:{parsed_id}"
    name = str(row.get("game_name") or row.get("name") or "").strip().lower()
    if name:
        return f"name:{name}"
    return f"idx:{index}"


def _dedupe_dashboard_rows(rows: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows):
        key = _dashboard_identity_key(row, idx)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _normalize_buy_recommendation(value) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"BUY_NOW", "WAIT"} else ""


def _decision_rows_by_recommendation(rows: list[dict], recommendation: str, limit: int = 24) -> list[dict]:
    target = _normalize_buy_recommendation(recommendation)
    if not target:
        return []
    picked: list[dict] = []
    for row in rows:
        if _normalize_buy_recommendation(row.get("buy_recommendation")) != target:
            continue
        picked.append(row)
        if len(picked) >= limit:
            break
    return picked


def _build_player_surges(alert_rows: list[dict], trending_rows: list[dict], limit: int = 24) -> list[dict]:
    surge_rows: list[dict] = []
    for row in alert_rows:
        alert_type = str(row.get("alert_type") or row.get("signal_type") or "").strip().upper()
        if alert_type == "PLAYER_SURGE":
            surge_rows.append(row)
    for row in trending_rows:
        if safe_num(row.get("player_change"), 0.0) > 0 or safe_num(row.get("short_term_player_trend"), 0.0) > 0:
            surge_rows.append(row)
    return _dedupe_dashboard_rows(surge_rows)[:limit]


def _augment_dashboard_home_payload(raw_payload: dict) -> dict:
    payload = dict(raw_payload)

    worth_buying_now = _dedupe_dashboard_rows(_dashboard_rows(payload, "worth_buying_now", "worthBuyingNow"))
    biggest_discounts = _dedupe_dashboard_rows(_dashboard_rows(payload, "biggest_discounts", "biggestDeals"))
    trending_now = _dedupe_dashboard_rows(_dashboard_rows(payload, "trending_now", "trending", "trendingDeals"))
    new_historical_lows = _dedupe_dashboard_rows(_dashboard_rows(payload, "new_historical_lows", "newHistoricalLows"))
    deal_radar = _dedupe_dashboard_rows(_dashboard_rows(payload, "deal_radar", "marketRadar", "dealRadar"))
    alert_signals = _dedupe_dashboard_rows(_dashboard_rows(payload, "alertSignals"))

    decision_pool = _dedupe_dashboard_rows(
        [
            *worth_buying_now,
            *_dashboard_rows(payload, "recommendedDeals"),
            *_dashboard_rows(payload, "dealRanked", "topDealsToday"),
            *biggest_discounts,
            *_dashboard_rows(payload, "trendingDeals"),
            *trending_now,
            *new_historical_lows,
        ]
    )
    buy_now_picks = _dedupe_dashboard_rows(_dashboard_rows(payload, "buy_now_picks"))
    wait_picks = _dedupe_dashboard_rows(_dashboard_rows(payload, "wait_picks"))
    if not buy_now_picks:
        buy_now_picks = _decision_rows_by_recommendation(decision_pool, "BUY_NOW")
    if not wait_picks:
        wait_picks = _decision_rows_by_recommendation(decision_pool, "WAIT")
    if not buy_now_picks:
        buy_now_picks = worth_buying_now[:24]
    if not wait_picks:
        wait_picks = [
            row
            for row in decision_pool
            if safe_num(row.get("price_vs_low_ratio"), 0.0) >= 1.08
            or safe_num(row.get("predicted_next_discount_percent"), 0.0) >= 35
        ][:24]

    player_surges = _dedupe_dashboard_rows(_dashboard_rows(payload, "player_surges"))
    if not player_surges:
        player_surges = _build_player_surges(alert_signals, trending_now)

    seasonal_summary = payload.get("seasonal_summary")
    if not isinstance(seasonal_summary, dict):
        seasonal_summary = payload.get("seasonalSale") if isinstance(payload.get("seasonalSale"), dict) else {}

    payload["worth_buying_now"] = worth_buying_now
    payload["biggest_discounts"] = biggest_discounts
    payload["buy_now_picks"] = buy_now_picks
    payload["wait_picks"] = wait_picks
    payload["new_historical_lows"] = new_historical_lows
    payload["trending_now"] = trending_now
    payload["deal_radar"] = deal_radar
    payload["player_surges"] = player_surges
    payload["seasonal_summary"] = seasonal_summary
    payload["decision_dashboard"] = {
        "worth_buying_now": worth_buying_now,
        "biggest_discounts": biggest_discounts,
        "buy_now_picks": buy_now_picks,
        "wait_picks": wait_picks,
        "new_historical_lows": new_historical_lows,
        "trending_now": trending_now,
        "deal_radar": deal_radar,
        "player_surges": player_surges,
        "seasonal_summary": seasonal_summary,
    }

    if not _dashboard_rows(payload, "worthBuyingNow"):
        payload["worthBuyingNow"] = worth_buying_now
    if not _dashboard_rows(payload, "biggestDeals"):
        payload["biggestDeals"] = biggest_discounts
    if not _dashboard_rows(payload, "newHistoricalLows"):
        payload["newHistoricalLows"] = new_historical_lows
    if not _dashboard_rows(payload, "trending"):
        payload["trending"] = trending_now
    if not _dashboard_rows(payload, "dealRadar"):
        payload["dealRadar"] = deal_radar
    if not _dashboard_rows(payload, "marketRadar"):
        payload["marketRadar"] = deal_radar

    return payload


@app.get("/dashboard/home")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/dashboard/home")
def get_dashboard_home(request: Request):
    started = _start_timer()
    try:
        read_session = ReadSessionLocal()
        try:
            cache_row, cached_payload = _read_dashboard_cache(read_session)
        finally:
            read_session.close()

        should_refresh = (
            cache_row is None
            or _dashboard_payload_is_empty(cached_payload)
            or _dashboard_cache_is_stale(cache_row, utc_now())
        )
        if should_refresh:
            rebuilt_row, rebuilt_payload = _rebuild_dashboard_cache_on_demand()
            if rebuilt_row is not None and not _dashboard_payload_is_empty(rebuilt_payload):
                cache_row = rebuilt_row
                cached_payload = rebuilt_payload
            elif cache_row is not None and isinstance(cached_payload, dict) and cached_payload:
                logger.warning(
                    "Serving stale /dashboard/home payload after on-demand rebuild miss for cache_key=%s",
                    cache_row.cache_key,
                )
            else:
                raise HTTPException(
                    status_code=503,
                    detail="Dashboard cache missing. Run jobs/refresh_snapshots.py or wait for worker refresh.",
                )

        if cached_payload is None:
            raise HTTPException(status_code=503, detail="Dashboard cache is invalid JSON")
        if _dashboard_payload_is_empty(cached_payload):
            raise HTTPException(status_code=503, detail="Dashboard cache is empty")
        if not isinstance(cached_payload, dict):
            raise HTTPException(status_code=503, detail="Dashboard cache payload has unexpected shape")

        payload = _augment_dashboard_home_payload(cached_payload)
        payload["_meta"] = {
            "cache_key": cache_row.cache_key,
            "generated_at": cache_row.updated_at.isoformat() if cache_row.updated_at else None,
        }
        return payload
    finally:
        _log_timing("/dashboard/home", started)


@app.get("/games/detail")
def game_detail(game_name: str):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            return {"error": "Game not found"}

        prices = (
            session.query(GamePrice)
            .filter(GamePrice.game_name == game_name)
            .order_by(GamePrice.timestamp.asc())
            .all()
        )

        if not prices:
            return {"error": "No price history found"}

        latest = prices[-1]
        historical_low_row = find_historical_low_row(prices)
        sale_rows = [row for row in prices if (row.discount_percent or 0) > 0]
        deal = calculate_deal_explanation(
            game,
            latest,
            historical_low_row,
            {"sale_event_count": count_distinct_sale_events(sale_rows)},
        )
        factor_map = {factor.get("name"): factor.get("score") for factor in deal.get("factors", [])}

        payload = build_game_detail_payload(session, game)
        payload["game_name"] = payload.get("name")
        payload["price"] = payload.get("current_price")
        payload["historical_low"] = payload.get("historical_low_price")
        payload["deal_explanation"] = {
            "discount_strength": factor_map.get("Discount Strength"),
            "historical_value": factor_map.get("Historical Value"),
            "review_quality": factor_map.get("Review Quality"),
            "player_interest": factor_map.get("Player Interest"),
            "sale_rarity": factor_map.get("Sale Rarity"),
            "summary": payload.get("deal_summary"),
        }
        return payload
    finally:
        session.close()


@app.get("/games/price-history")
def get_game_price_history_windowed(
    game_name: str,
    range: str = Query("90d", pattern="^(30d|90d|1y|all)$"),
):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            return {"error": "Game not found"}

        start_dt = get_history_range_start(range)

        query = (
            session.query(GamePrice)
            .filter(GamePrice.game_name == game_name)
            .order_by(GamePrice.timestamp.asc())
        )

        if start_dt is not None:
            query = query.filter(GamePrice.timestamp >= start_dt)

        rows = query.all()

        rows = downsample_price_rows(rows, range)

        historical_low_row = None
        if rows:
            historical_low_row = min(
                rows,
                key=lambda r: (r.price if r.price is not None else 10**9, r.timestamp),
            )

        sale_markers = []
        in_sale = False
        current_sale_start = None

        for row in rows:
            on_sale = (row.discount_percent or 0) > 0

            if on_sale and not in_sale:
                in_sale = True
                current_sale_start = row.timestamp

            if not on_sale and in_sale:
                sale_markers.append(
                    {
                        "start": current_sale_start.isoformat(),
                        "end": row.timestamp.isoformat(),
                    }
                )
                in_sale = False
                current_sale_start = None

        if in_sale and current_sale_start:
            sale_markers.append(
                {
                    "start": current_sale_start.isoformat(),
                    "end": rows[-1].timestamp.isoformat(),
                }
            )

        return {
            "game_name": game.name,
            "range": range,
            "historical_low": {
                "price": historical_low_row.price if historical_low_row else None,
                "timestamp": (
                    historical_low_row.timestamp.isoformat()
                    if historical_low_row
                    else None
                ),
            },
            "points": [
                {
                    "timestamp": row.timestamp.isoformat(),
                    "price": row.price,
                    "original_price": row.original_price,
                    "discount_percent": row.discount_percent,
                    "current_players": row.current_players,
                }
                for row in rows
            ],
            "sale_markers": sale_markers,
        }
    finally:
        session.close()


@app.get("/games/{game_id}")
def get_game_detail(game_id: int):
    session = Session()
    try:
        game = session.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        payload = build_game_detail_payload(session, game)
        payload["buy_recommendation"] = None
        payload["buy_reason"] = None
        payload["price_vs_low_ratio"] = None
        payload["predicted_next_sale_price"] = None
        payload["predicted_next_discount_percent"] = None
        payload["predicted_next_sale_window_days_min"] = None
        payload["predicted_next_sale_window_days_max"] = None
        payload["predicted_sale_confidence"] = None
        payload["predicted_sale_reason"] = None
        payload["next_sale_prediction"] = {
            "expected_next_price": None,
            "expected_next_discount_percent": None,
            "estimated_window_days_min": None,
            "estimated_window_days_max": None,
            "confidence": None,
            "reason": None,
        }
        snapshot = session.query(GameSnapshot).filter(GameSnapshot.game_id == game_id).first()
        if snapshot:
            payload["buy_recommendation"] = snapshot.buy_recommendation
            payload["buy_reason"] = snapshot.buy_reason
            payload["price_vs_low_ratio"] = snapshot.price_vs_low_ratio
            payload["predicted_next_sale_price"] = snapshot.predicted_next_sale_price
            payload["predicted_next_discount_percent"] = snapshot.predicted_next_discount_percent
            payload["predicted_next_sale_window_days_min"] = snapshot.predicted_next_sale_window_days_min
            payload["predicted_next_sale_window_days_max"] = snapshot.predicted_next_sale_window_days_max
            payload["predicted_sale_confidence"] = snapshot.predicted_sale_confidence
            payload["predicted_sale_reason"] = snapshot.predicted_sale_reason
            payload["next_sale_prediction"] = {
                "expected_next_price": snapshot.predicted_next_sale_price,
                "expected_next_discount_percent": snapshot.predicted_next_discount_percent,
                "estimated_window_days_min": snapshot.predicted_next_sale_window_days_min,
                "estimated_window_days_max": snapshot.predicted_next_sale_window_days_max,
                "confidence": snapshot.predicted_sale_confidence,
                "reason": snapshot.predicted_sale_reason,
            }
            payload["worth_buying"] = {
                "score": snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score,
                "version": snapshot.worth_buying_score_version,
                "reason": snapshot.worth_buying_reason_summary,
                "components": snapshot.worth_buying_components or {},
            }
            payload["buy_score"] = snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score
            payload["momentum"] = {
                "score": snapshot.momentum_score,
                "version": snapshot.momentum_score_version,
                "player_growth_ratio": snapshot.player_growth_ratio,
                "short_term_player_trend": snapshot.short_term_player_trend,
                "reason": snapshot.trend_reason_summary,
            }
            payload["historical_low_radar"] = {
                "hit": bool(snapshot.historical_low_hit),
                "historical_low_price": snapshot.historical_low_price,
                "previous_historical_low_price": snapshot.previous_historical_low_price,
                "historical_low_timestamp": snapshot.historical_low_timestamp.isoformat() if snapshot.historical_low_timestamp else None,
                "reason": snapshot.historical_low_reason_summary,
            }
            payload["deal_heat"] = {
                "level": snapshot.deal_heat_level,
                "reason": snapshot.deal_heat_reason,
                "tags": snapshot.deal_heat_tags or [],
            }
            payload["share_card"] = {
                "title": snapshot.game_name,
                "cover": snapshot.banner_url,
                "current_price": snapshot.latest_price,
                "original_price": snapshot.latest_original_price,
                "discount_percent": snapshot.latest_discount_percent,
                "heat_reason": snapshot.deal_heat_reason,
                "heat_level": snapshot.deal_heat_level,
                "historical_low_hit": bool(snapshot.historical_low_hit),
                "momentum_score": snapshot.momentum_score,
            }
            payload["ranking_explanations"] = snapshot.ranking_explanations or {}
        return payload
    finally:
        session.close()


@app.get("/games/by-name")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/games/by-name")
def get_game_by_name(request: Request, game_name: str):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        name_value = (game_name or "").strip()
        if not name_value:
            raise HTTPException(status_code=400, detail="game_name is required")

        game = session.query(Game).filter(Game.name == name_value).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        snapshot = session.query(GameSnapshot).filter(GameSnapshot.game_id == game.id).first()
        latest = session.query(LatestGamePrice).filter(LatestGamePrice.game_id == game.id).first()

        banner_url = (
            (snapshot.banner_url if snapshot else None)
            or build_steam_banner_url(game.store_url, game.appid)
        )
        buy_score = (
            snapshot.buy_score
            if snapshot and snapshot.buy_score is not None
            else snapshot.worth_buying_score
            if snapshot
            else None
        )
        return {
            "id": int(game.id),
            "game_id": int(game.id),
            "game_name": game.name,
            "steam_appid": (snapshot.steam_appid if snapshot else None) or game.appid,
            "banner_url": banner_url,
            "store_url": game.store_url,
            "developer": game.developer,
            "publisher": game.publisher,
            "release_date": (
                snapshot.release_date.isoformat()
                if snapshot and snapshot.release_date
                else game.release_date.isoformat()
                if game.release_date
                else None
            ),
            "release_date_text": (
                snapshot.release_date_text if snapshot and snapshot.release_date_text else game.release_date_text
            ),
            "price": (
                snapshot.latest_price
                if snapshot and snapshot.latest_price is not None
                else latest.latest_price if latest else None
            ),
            "original_price": (
                snapshot.latest_original_price
                if snapshot and snapshot.latest_original_price is not None
                else latest.original_price if latest else None
            ),
            "discount_percent": (
                snapshot.latest_discount_percent
                if snapshot and snapshot.latest_discount_percent is not None
                else latest.latest_discount_percent if latest else None
            ),
            "current_players": (
                snapshot.current_players
                if snapshot and snapshot.current_players is not None
                else latest.current_players if latest else None
            ),
            "historical_low": (
                snapshot.historical_low
                if snapshot and snapshot.historical_low is not None
                else snapshot.historical_low_price if snapshot else None
            ),
            "historical_status": snapshot.historical_status if snapshot else None,
            "deal_score": snapshot.deal_score if snapshot else None,
            "buy_score": buy_score,
            "buy_recommendation": snapshot.buy_recommendation if snapshot else None,
            "buy_reason": snapshot.buy_reason if snapshot else None,
            "price_vs_low_ratio": snapshot.price_vs_low_ratio if snapshot else None,
            "predicted_next_sale_price": snapshot.predicted_next_sale_price if snapshot else None,
            "predicted_next_discount_percent": snapshot.predicted_next_discount_percent if snapshot else None,
            "predicted_next_sale_window_days_min": snapshot.predicted_next_sale_window_days_min if snapshot else None,
            "predicted_next_sale_window_days_max": snapshot.predicted_next_sale_window_days_max if snapshot else None,
            "predicted_sale_confidence": snapshot.predicted_sale_confidence if snapshot else None,
            "predicted_sale_reason": snapshot.predicted_sale_reason if snapshot else None,
            "next_sale_prediction": {
                "expected_next_price": snapshot.predicted_next_sale_price if snapshot else None,
                "expected_next_discount_percent": snapshot.predicted_next_discount_percent if snapshot else None,
                "estimated_window_days_min": snapshot.predicted_next_sale_window_days_min if snapshot else None,
                "estimated_window_days_max": snapshot.predicted_next_sale_window_days_max if snapshot else None,
                "confidence": snapshot.predicted_sale_confidence if snapshot else None,
                "reason": snapshot.predicted_sale_reason if snapshot else None,
            },
            "worth_buying_reason_summary": snapshot.worth_buying_reason_summary if snapshot else None,
            "review_score": (
                snapshot.review_score
                if snapshot and snapshot.review_score is not None
                else game.review_score
            ),
            "review_score_label": (
                snapshot.review_score_label if snapshot and snapshot.review_score_label else game.review_score_label
            ),
            "review_total_count": (
                snapshot.review_count
                if snapshot and snapshot.review_count is not None
                else game.review_total_count
            ),
            "genres": parse_csv_field(snapshot.genres) if snapshot else parse_csv_field(game.genres),
            "tags": parse_csv_field(snapshot.tags) if snapshot else parse_csv_field(game.tags),
            "platforms": parse_csv_field(snapshot.platforms) if snapshot else parse_csv_field(game.platforms),
            "momentum_score": snapshot.momentum_score if snapshot else None,
            "trend_reason_summary": snapshot.trend_reason_summary if snapshot else None,
            "deal_heat_reason": snapshot.deal_heat_reason if snapshot else None,
            "deal_label": f"Deal score {int(round(snapshot.deal_score))}" if snapshot and snapshot.deal_score is not None else None,
            "prediction": {},
            "deal_explanation": {
                "summary": (
                    snapshot.worth_buying_reason_summary
                    if snapshot and snapshot.worth_buying_reason_summary
                    else snapshot.deal_heat_reason if snapshot and snapshot.deal_heat_reason else "Snapshot-derived market context."
                )
            },
        }
    finally:
        session.close()
        _log_timing("/games/by-name", started)


@app.get("/games/{game_id}/price-history")
@json_etag()
@ttl_cache(ttl_seconds=3600, endpoint_key="/games/{game_id}/price-history")
def get_game_price_history_by_id(
    request: Request,
    game_id: int,
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        game_exists = session.query(Game.id).filter(Game.id == game_id).first()
        if not game_exists:
            raise HTTPException(status_code=404, detail="Game not found")

        row_count = (
            session.query(func.count(GamePrice.id))
            .filter(GamePrice.game_id == game_id, GamePrice.price.isnot(None))
            .scalar()
            or 0
        )

        if row_count > 5000:
            if session.bind and session.bind.dialect.name == "postgresql":
                history_rows = session.execute(
                    text(
                        """
                        SELECT
                            date_trunc('hour', recorded_at) AS bucket,
                            MIN(price) AS price,
                            MAX(original_price) AS original_price,
                            MAX(discount_percent) AS discount_percent,
                            MAX(current_players) AS current_players
                        FROM game_prices
                        WHERE game_id = :game_id
                          AND price IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """
                    ),
                    {"game_id": game_id},
                ).fetchall()
            else:
                history_rows = session.execute(
                    text(
                        """
                        SELECT
                            strftime('%Y-%m-%d %H:00:00', recorded_at) AS bucket,
                            MIN(price) AS price,
                            MAX(original_price) AS original_price,
                            MAX(discount_percent) AS discount_percent,
                            MAX(current_players) AS current_players
                        FROM game_prices
                        WHERE game_id = :game_id
                          AND price IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """
                    ),
                    {"game_id": game_id},
                ).fetchall()
            history = [
                {
                    "timestamp": (
                        row[0].replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                        if row[0] and isinstance(row[0], datetime.datetime) and row[0].tzinfo is None
                        else row[0].isoformat().replace("+00:00", "Z")
                        if row[0] and isinstance(row[0], datetime.datetime)
                        else str(row[0])
                    ),
                    "price": float(row[1]) if row[1] is not None else None,
                    "original_price": float(row[2]) if row[2] is not None else None,
                    "discount_percent": int(row[3]) if row[3] is not None else None,
                    "players": int(row[4]) if row[4] is not None else None,
                }
                for row in history_rows
            ]
        else:
            rows = (
                session.query(
                    GamePrice.recorded_at,
                    GamePrice.price,
                    GamePrice.original_price,
                    GamePrice.discount_percent,
                    GamePrice.current_players,
                )
                .filter(GamePrice.game_id == game_id, GamePrice.price.isnot(None))
                .order_by(GamePrice.recorded_at.asc(), GamePrice.id.asc())
                .all()
            )
            history = [
                {
                    "timestamp": (
                        ts.replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                        if ts and ts.tzinfo is None
                        else ts.isoformat().replace("+00:00", "Z")
                        if ts
                        else None
                    ),
                    "price": float(price) if price is not None else None,
                    "original_price": float(original_price) if original_price is not None else None,
                    "discount_percent": int(discount_percent) if discount_percent is not None else None,
                    "players": int(current_players) if current_players is not None else None,
                }
                for ts, price, original_price, discount_percent, current_players in rows
            ]

        stats_row = (
            session.query(
                func.min(GamePrice.price),
                func.max(GamePrice.price),
                func.avg(GamePrice.price),
            )
            .filter(GamePrice.game_id == game_id, GamePrice.price.isnot(None))
            .first()
        )
        historical_low = float(stats_row[0]) if stats_row and stats_row[0] is not None else None
        historical_high = float(stats_row[1]) if stats_row and stats_row[1] is not None else None
        average_price = float(stats_row[2]) if stats_row and stats_row[2] is not None else None

        event_rows = (
            session.query(DealEvent.event_type, DealEvent.new_price, DealEvent.created_at)
            .filter(DealEvent.game_id == game_id)
            .order_by(DealEvent.created_at.asc(), DealEvent.id.asc())
            .all()
        )
        events = [
            {
                "type": event_type,
                "price": float(new_price) if new_price is not None else None,
                "timestamp": (
                    created_at.replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                    if created_at and created_at.tzinfo is None
                    else created_at.isoformat().replace("+00:00", "Z")
                    if created_at
                    else None
                ),
            }
            for event_type, new_price, created_at in event_rows
        ]

        return {
            "game_id": game_id,
            "stats": {
                "historical_low": historical_low,
                "historical_high": historical_high,
                "average_price": round(average_price, 2) if average_price is not None else None,
            },
            "history": history,
            "events": events,
        }
    finally:
        session.close()
        _log_timing("/games/{game_id}/price-history", started)


@app.get("/games/{game_id}/history")
@json_etag()
@rate_limit(max_requests=60, window_seconds=60)
@ttl_cache(ttl_seconds=120, endpoint_key="/games/{game_id}/history")
def get_game_history_by_id(
    request: Request,
    game_id: int,
    range: str = Query(default="90d", pattern="^(30d|90d|1y|all)$"),
    points: int = Query(default=API_DEFAULT_HISTORY_POINTS, ge=1, le=API_MAX_HISTORY_POINTS),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        game_exists = session.query(Game.id).filter(Game.id == game_id).first()
        if not game_exists:
            raise HTTPException(status_code=404, detail="Game not found")

        range_start = get_history_range_start(range)

        query = (
            session.query(GamePrice.price, GamePrice.recorded_at)
            .filter(
                GamePrice.game_id == game_id,
                GamePrice.price.isnot(None),
                GamePrice.price > 0,
            )
            .order_by(GamePrice.recorded_at.asc(), GamePrice.id.asc())
        )

        if range_start is not None:
            query = query.filter(GamePrice.recorded_at >= range_start)

        rows = query.all()
        sampled_rows = downsample_history_points(rows, points)

        payload_points = [
            {
                "price": float(price),
                "timestamp": (
                    ts.replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                    if ts and ts.tzinfo is None
                    else ts.isoformat().replace("+00:00", "Z") if ts else None
                ),
            }
            for price, ts in sampled_rows
        ]

        return {
            "game_id": game_id,
            "range": range,
            "point_count": len(payload_points),
            "points": payload_points,
        }
    finally:
        session.close()
        _log_timing("/games/{game_id}/history", started)


@app.get("/games/{game_id}/player-history")
@json_etag()
@ttl_cache(ttl_seconds=600, endpoint_key="/games/{game_id}/player-history")
def get_game_player_history(request: Request, game_id: int):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        game_exists = session.query(Game.id).filter(Game.id == game_id).first()
        if not game_exists:
            raise HTTPException(status_code=404, detail="Game not found")

        row_count = (
            session.query(func.count(GamePlayerHistory.id))
            .filter(GamePlayerHistory.game_id == game_id, GamePlayerHistory.current_players.isnot(None))
            .scalar()
            or 0
        )

        if row_count > 5000:
            if session.bind and session.bind.dialect.name == "postgresql":
                rows = session.execute(
                    text(
                        """
                        SELECT
                            date_trunc('hour', recorded_at) AS bucket,
                            AVG(current_players) AS players
                        FROM game_player_history
                        WHERE game_id = :game_id
                          AND current_players IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """
                    ),
                    {"game_id": game_id},
                ).fetchall()
            else:
                rows = session.execute(
                    text(
                        """
                        SELECT
                            strftime('%Y-%m-%d %H:00:00', recorded_at) AS bucket,
                            AVG(current_players) AS players
                        FROM game_player_history
                        WHERE game_id = :game_id
                          AND current_players IS NOT NULL
                        GROUP BY bucket
                        ORDER BY bucket ASC
                        """
                    ),
                    {"game_id": game_id},
                ).fetchall()
            players = [
                {
                    "timestamp": (
                        row[0].replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                        if row[0] and isinstance(row[0], datetime.datetime) and row[0].tzinfo is None
                        else row[0].isoformat().replace("+00:00", "Z")
                        if row[0] and isinstance(row[0], datetime.datetime)
                        else str(row[0])
                    ),
                    "players": int(round(float(row[1]))) if row[1] is not None else None,
                }
                for row in rows
            ]
        else:
            rows = (
                session.query(GamePlayerHistory.recorded_at, GamePlayerHistory.current_players)
                .filter(
                    GamePlayerHistory.game_id == game_id,
                    GamePlayerHistory.current_players.isnot(None),
                )
                .order_by(GamePlayerHistory.recorded_at.asc(), GamePlayerHistory.id.asc())
                .all()
            )
            players = [
                {
                    "timestamp": (
                        ts.replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
                        if ts and ts.tzinfo is None
                        else ts.isoformat().replace("+00:00", "Z")
                        if ts
                        else None
                    ),
                    "players": int(value) if value is not None else None,
                }
                for ts, value in rows
            ]

        seven_days_ago = utc_now() - datetime.timedelta(days=7)
        stats_row = (
            session.query(
                func.max(GamePlayerHistory.current_players),
                func.avg(GamePlayerHistory.current_players),
                func.min(GamePlayerHistory.current_players),
            )
            .filter(
                GamePlayerHistory.game_id == game_id,
                GamePlayerHistory.current_players.isnot(None),
                GamePlayerHistory.recorded_at >= seven_days_ago,
            )
            .first()
        )

        return {
            "game_id": game_id,
            "stats": {
                "peak_players": int(stats_row[0]) if stats_row and stats_row[0] is not None else None,
                "avg_players": int(round(float(stats_row[1]))) if stats_row and stats_row[1] is not None else None,
                "min_players": int(stats_row[2]) if stats_row and stats_row[2] is not None else None,
            },
            "players": players,
        }
    finally:
        session.close()
        _log_timing("/games/{game_id}/player-history", started)


@app.get("/games/{game_id}/related")
@json_etag()
@ttl_cache(ttl_seconds=180, endpoint_key="/games/{game_id}/related")
def get_related_games(
    request: Request,
    game_id: int,
    limit: int = Query(default=8, ge=1, le=24),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        seed = session.query(GameSnapshot).filter(GameSnapshot.game_id == game_id).first()
        if not seed:
            raise HTTPException(status_code=404, detail="Game snapshot not found")

        seed_tags = {tag.lower() for tag in parse_csv_field(seed.tags)}
        seed_genres = {genre.lower() for genre in parse_csv_field(seed.genres)}
        target_trend = safe_num(seed.short_term_player_trend, 0.0)

        def _shared_taxonomy_score(row: GameSnapshot) -> int:
            row_tags = {tag.lower() for tag in parse_csv_field(row.tags)}
            row_genres = {genre.lower() for genre in parse_csv_field(row.genres)}
            return (len(seed_tags & row_tags) * 2) + len(seed_genres & row_genres)

        def _serialize_related_row(row: GameSnapshot) -> dict:
            return {
                "game_id": int(row.game_id),
                "game_name": row.game_name,
                "steam_appid": row.steam_appid,
                "banner_url": row.banner_url,
                "price": row.latest_price,
                "original_price": row.latest_original_price,
                "discount_percent": row.latest_discount_percent,
                "deal_score": row.deal_score,
                "buy_score": row.buy_score if row.buy_score is not None else row.worth_buying_score,
                "current_players": row.current_players,
                "trending_score": row.trending_score,
                "short_term_player_trend": row.short_term_player_trend,
                "genres": parse_csv_field(row.genres),
                "tags": parse_csv_field(row.tags),
            }

        def _take_unique(rows: list[GameSnapshot], *, require_taxonomy: bool = False) -> list[dict]:
            selected: list[dict] = []
            seen_ids: set[int] = set()
            for row in rows:
                row_game_id = int(row.game_id)
                if row_game_id == int(game_id) or row_game_id in seen_ids:
                    continue
                if require_taxonomy and _shared_taxonomy_score(row) <= 0:
                    continue
                selected.append(_serialize_related_row(row))
                seen_ids.add(row_game_id)
                if len(selected) >= limit:
                    break
            return selected

        taxonomy_filters = []
        for genre in seed_genres:
            taxonomy_filters.append(GameSnapshot.genres.ilike(f"%{genre}%"))
        for tag in seed_tags:
            taxonomy_filters.append(GameSnapshot.tags.ilike(f"%{tag}%"))

        base_taxonomy_query = session.query(GameSnapshot).filter(
            GameSnapshot.game_id != game_id,
            GameSnapshot.is_upcoming.is_(False),
        )
        if taxonomy_filters:
            base_taxonomy_query = base_taxonomy_query.filter(or_(*taxonomy_filters))

        trending_candidates = (
            base_taxonomy_query.order_by(
                GameSnapshot.trending_score.desc().nullslast(),
                GameSnapshot.current_players.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(max(limit * 8, 64))
            .all()
        )
        trending_similar = _take_unique(trending_candidates, require_taxonomy=bool(taxonomy_filters))

        discounted_candidates = (
            base_taxonomy_query
            .filter(GameSnapshot.latest_discount_percent.isnot(None), GameSnapshot.latest_discount_percent > 0)
            .order_by(
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                GameSnapshot.deal_score.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(max(limit * 8, 64))
            .all()
        )
        also_discounted = _take_unique(discounted_candidates, require_taxonomy=bool(taxonomy_filters))

        trend_candidates = (
            session.query(GameSnapshot)
            .filter(
                GameSnapshot.game_id != game_id,
                GameSnapshot.is_upcoming.is_(False),
                GameSnapshot.short_term_player_trend.isnot(None),
            )
            .order_by(
                func.abs(GameSnapshot.short_term_player_trend - target_trend).asc(),
                GameSnapshot.momentum_score.desc().nullslast(),
                GameSnapshot.current_players.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(max(limit * 12, 96))
            .all()
        )
        similar_player_trends = _take_unique(trend_candidates, require_taxonomy=False)

        return {
            "game_id": int(game_id),
            "trending_similar_games": trending_similar,
            "also_discounted_games": also_discounted,
            "similar_player_trends": similar_player_trends,
        }
    finally:
        session.close()
        _log_timing("/games/{game_id}/related", started)


@app.get("/games/{game_id}/deal-explanation")
def get_game_deal_explanation(game_id: int):
    session = Session()
    try:
        game = session.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        rows = (
            session.query(GamePrice)
            .filter(GamePrice.game_name == game.name)
            .order_by(GamePrice.timestamp.asc(), GamePrice.id.asc())
            .all()
        )
        latest = rows[-1] if rows else None
        low = find_historical_low_row(rows)
        sale_rows = [row for row in rows if (row.discount_percent or 0) > 0]

        market = {
            "sale_event_count": count_distinct_sale_events(sale_rows),
        }

        return calculate_deal_explanation(game, latest, low, market)
    finally:
        session.close()


@app.get("/games/{game_id}/prediction")
def get_game_prediction(game_id: int):
    session = Session()
    try:
        game = session.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        rows = (
            session.query(GamePrice)
            .filter(GamePrice.game_name == game.name)
            .order_by(GamePrice.timestamp.asc(), GamePrice.id.asc())
            .all()
        )
        latest = rows[-1] if rows else None
        sale_rows = [row for row in rows if (row.discount_percent or 0) > 0]
        return calculate_prediction_v1(game, latest, sale_rows)
    finally:
        session.close()


@app.get("/games/{game_name}/history")
def get_game_price_history(game_name: str):
    started = _start_timer()
    session = ReadSessionLocal()

    try:
        rows = (
            session.query(GamePrice)
            .filter(GamePrice.game_name == game_name)
            .order_by(GamePrice.timestamp.asc(), GamePrice.id.asc())
            .all()
        )

        game = session.query(Game).filter(Game.name == game_name).first()

        if not rows and not game:
            raise HTTPException(status_code=404, detail="Game not found")

        game_metadata = serialize_game_metadata(game)
        historical_insight_map = compute_historical_insight_map(session)
        insight = historical_insight_map.get(game_name, {})
        historical_low = insight.get("historical_low")
        previous_historical_low = insight.get("previous_historical_low")
        latest_status = insight.get("historical_status")
        history_point_count = insight.get("history_point_count", 0)

        latest_row_id = rows[-1].id if rows else None

        return [
            {
                "game_name": row.game_name,
                "price": row.price,
                "original_price": row.original_price,
                "discount_percent": row.discount_percent,
                "current_players": row.current_players,
                "store_url": row.store_url,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "historical_low": historical_low,
                "previous_historical_low": previous_historical_low,
                "historical_status": latest_status if row.id == latest_row_id else None,
                "history_point_count": history_point_count,
                "banner_url": build_steam_banner_url(row.store_url, game.appid if game else None),
                **game_metadata,
            }
            for row in rows
        ]

    finally:
        session.close()
        _log_timing("/games/{game_name}/history", started)


@app.post("/alerts")
def create_alert(payload: AlertCreateRequest):
    session = Session()

    try:
        game_exists = session.query(Game).filter(Game.name == payload.game_name).first()
        if not game_exists:
            raise HTTPException(status_code=404, detail="Game not found")

        if payload.target_price < 0:
            raise HTTPException(status_code=400, detail="Target price must be 0 or greater")

        alert = PriceAlert(
            game_name=payload.game_name,
            target_price=payload.target_price,
            email=str(payload.email),
        )

        session.add(alert)
        session.commit()
        session.refresh(alert)

        logger.info("Created alert for %s at target price %s", alert.game_name, alert.target_price)

        return {
            "id": alert.id,
            "game_name": alert.game_name,
            "target_price": alert.target_price,
            "email": alert.email,
            "created_at": alert.created_at.isoformat() if alert.created_at else None,
        }

    finally:
        session.close()


@app.post("/games/{game_id}/interact")
def interact_with_game(game_id: int, payload: GameInteractionRequest):
    session = Session()
    try:
        game = session.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        interaction_type = (payload.type or "").strip().lower()
        if interaction_type != "click":
            raise HTTPException(status_code=400, detail="Unsupported interaction type")

        now = utc_now()
        signal = session.query(GameInterestSignal).filter(GameInterestSignal.game_id == game_id).first()
        if signal is None:
            signal = GameInterestSignal(game_id=game_id, click_count=0, wishlist_count=0, watchlist_count=0)
            session.add(signal)

        signal.click_count = int(signal.click_count or 0) + 1
        signal.last_clicked_at = now
        signal.updated_at = now

        mark_game_dirty(session, game_id)
        session.commit()

        return {
            "success": True,
            "game_id": game_id,
            "type": "click",
            "click_count": signal.click_count,
        }
    finally:
        session.close()


@app.get("/alerts")
def list_alerts():
    session = Session()

    try:
        rows = session.query(PriceAlert).order_by(PriceAlert.created_at.desc()).all()

        return [
            {
                "id": row.id,
                "game_name": row.game_name,
                "target_price": row.target_price,
                "email": row.email,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ]

    finally:
        session.close()


@app.get("/wishlist")
def list_wishlist():
    session = Session()
    try:
        rows = (
            session.query(WishlistItem, Game.name)
            .outerjoin(Game, Game.id == WishlistItem.game_id)
            .order_by(WishlistItem.created_at.desc())
            .all()
        )
        return [
            {
                "id": item.id,
                "user_id": item.user_id,
                "game_id": int(item.game_id) if item.game_id is not None else None,
                "game_name": item.game_name or game_name,
                "created_at": item.created_at.isoformat() if item.created_at else None,
            }
            for item, game_name in rows
        ]
    finally:
        session.close()


@app.get("/alerts/{user_id}")
def list_user_alerts(user_id: str):
    session = Session()
    try:
        rows = (
            session.query(UserAlert, Game, GameSnapshot)
            .outerjoin(Game, Game.id == UserAlert.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == UserAlert.game_id)
            .filter(UserAlert.user_id == user_id)
            .order_by(UserAlert.created_at.desc(), UserAlert.id.desc())
            .limit(50)
            .all()
        )
        return [
            {
                "id": int(alert.id),
                "user_id": alert.user_id,
                "game_id": int(alert.game_id),
                "game_name": snapshot.game_name if snapshot and snapshot.game_name else (game.name if game else None),
                "alert_type": str(alert.alert_type or "").upper(),
                "alert_label": _format_user_alert_label(
                    alert.alert_type,
                    price=alert.price,
                    discount_percent=alert.discount_percent,
                ),
                "price": alert.price,
                "discount_percent": alert.discount_percent,
                "read": bool(alert.read),
                "created_at": alert.created_at.isoformat() if alert.created_at else None,
                "alert_created_at": alert.created_at.isoformat() if alert.created_at else None,
                "metadata": {
                    "price": alert.price,
                    "discount_percent": alert.discount_percent,
                    "read": bool(alert.read),
                },
                "alert_metadata": {
                    "price": alert.price,
                    "discount_percent": alert.discount_percent,
                    "read": bool(alert.read),
                },
                "latest_price": snapshot.latest_price if snapshot and snapshot.latest_price is not None else alert.price,
                "latest_discount_percent": (
                    snapshot.latest_discount_percent
                    if snapshot and snapshot.latest_discount_percent is not None
                    else alert.discount_percent
                ),
                "banner_url": snapshot.banner_url if snapshot else None,
            }
            for alert, game, snapshot in rows
        ]
    finally:
        session.close()


@app.post("/alerts/read")
def mark_alert_read(payload: AlertReadRequest):
    session = Session()
    try:
        row = session.query(UserAlert).filter(UserAlert.id == payload.alert_id).first()
        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")
        row.read = True
        session.commit()
        return {"ok": True, "alert_id": int(row.id), "read": True}
    finally:
        session.close()


@app.get("/alerts/unread/{user_id}")
def count_unread_alerts(user_id: str):
    session = Session()
    try:
        unread = (
            session.query(func.count(UserAlert.id))
            .filter(UserAlert.user_id == user_id, UserAlert.read.is_(False))
            .scalar()
            or 0
        )
        return {"user_id": user_id, "unread": int(unread)}
    finally:
        session.close()


@app.get("/api/alerts")
def list_watchlist_alert_feed(
    user_id: str = Query(default=DEFAULT_USER_ID),
    limit: int = Query(default=API_DEFAULT_LIST_LIMIT, ge=1, le=API_MAX_LIST_LIMIT),
):
    normalized_user_id = normalize_user_id(user_id)
    session = Session()
    try:
        items = build_user_watchlist_alert_feed(session, normalized_user_id, limit=limit)
        return {
            "user_id": normalized_user_id,
            "count": len(items),
            "items": items,
        }
    finally:
        session.close()


@app.get("/api/deal-radar")
@json_etag()
@ttl_cache(ttl_seconds=45, endpoint_key="/api/deal-radar")
def list_deal_radar_feed(
    request: Request,
    limit: int = Query(default=API_DEFAULT_LIST_LIMIT, ge=1, le=API_MAX_LIST_LIMIT),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        row, payload = _read_cache_payload(session, DEAL_RADAR_CACHE_KEY)
        generated_at = row.updated_at.isoformat() if row and row.updated_at else None
        items: list[dict] = []

        if payload is not None:
            if isinstance(payload, dict):
                payload_items = payload.get("items", [])
                generated_at = payload.get("generated_at") or generated_at
                if isinstance(payload_items, list):
                    items = payload_items
            elif isinstance(payload, list):
                items = payload

        if not items:
            home_row, home_payload = _read_dashboard_cache(session)
            if home_payload and isinstance(home_payload, dict):
                home_items = home_payload.get("dealRadar", [])
                if isinstance(home_items, list):
                    items = home_items
                if generated_at is None and home_row and home_row.updated_at:
                    generated_at = home_row.updated_at.isoformat()

        normalized: list[dict] = []
        for item in items:
            parsed = _normalize_deal_radar_item(item)
            if parsed is not None:
                normalized.append(parsed)
            if len(normalized) >= int(limit):
                break

        return {
            "count": len(normalized),
            "items": normalized,
            "generated_at": generated_at,
        }
    finally:
        session.close()
        _log_timing("/api/deal-radar", started)


@app.get("/api/deal-opportunities")
@json_etag()
@ttl_cache(ttl_seconds=45, endpoint_key="/api/deal-opportunities")
def list_deal_opportunities(
    request: Request,
    limit: int = Query(default=24, ge=1, le=120),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        normalized_limit = max(1, int(limit))
        candidate_limit = max(
            OPPORTUNITY_MIN_CANDIDATES,
            min(OPPORTUNITY_MAX_CANDIDATES, normalized_limit * OPPORTUNITY_QUERY_MULTIPLIER),
        )

        rows = (
            session.query(GameSnapshot)
            .filter(
                GameSnapshot.is_released == 1,
                or_(GameSnapshot.is_upcoming.is_(False), GameSnapshot.is_upcoming.is_(None)),
                GameSnapshot.latest_price.isnot(None),
            )
            .order_by(
                GameSnapshot.buy_score.desc().nullslast(),
                GameSnapshot.worth_buying_score.desc().nullslast(),
                GameSnapshot.deal_score.desc().nullslast(),
                GameSnapshot.momentum_score.desc().nullslast(),
                GameSnapshot.popularity_score.desc().nullslast(),
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                GameSnapshot.updated_at.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(candidate_limit)
            .all()
        )

        scored_items: list[tuple[float, datetime.datetime, dict]] = []
        for snapshot in rows:
            parsed = _build_deal_opportunity_item(snapshot)
            if parsed is None:
                continue
            updated_at = snapshot.updated_at if isinstance(snapshot.updated_at, datetime.datetime) else utc_now()
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
            scored_items.append((safe_num(parsed.get("opportunity_score"), 0.0), updated_at, parsed))

        scored_items.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
        items = [item for _, _, item in scored_items[:normalized_limit]]

        return {
            "count": len(items),
            "items": items,
            "generated_at": utc_now().isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/deal-opportunities", started)


@app.get("/api/market-radar")
@json_etag()
@ttl_cache(ttl_seconds=45, endpoint_key="/api/market-radar")
def list_market_radar_feed(
    request: Request,
    limit: int = Query(default=API_DEFAULT_LIST_LIMIT, ge=1, le=API_MAX_LIST_LIMIT),
):
    return list_deal_radar_feed(request=request, limit=limit)


@app.post("/notifications/subscribe")
def subscribe_notifications(payload: PushSubscribeRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        endpoint = (payload.endpoint or "").strip()
        if not user_id or not endpoint:
            raise HTTPException(status_code=400, detail="user_id and endpoint are required")

        existing = (
            session.query(PushSubscription)
            .filter(
                PushSubscription.user_id == user_id,
                PushSubscription.endpoint == endpoint,
            )
            .first()
        )
        if existing:
            existing.p256dh = payload.p256dh
            existing.auth = payload.auth
            session.commit()
            return {"ok": True, "id": int(existing.id), "updated": True}

        row = PushSubscription(
            user_id=user_id,
            endpoint=endpoint,
            p256dh=payload.p256dh,
            auth=payload.auth,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return {"ok": True, "id": int(row.id), "updated": False}
    finally:
        session.close()


@app.post("/notifications/unsubscribe")
def unsubscribe_notifications(payload: PushUnsubscribeRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        endpoint = (payload.endpoint or "").strip()
        if not user_id or not endpoint:
            raise HTTPException(status_code=400, detail="user_id and endpoint are required")

        rows = (
            session.query(PushSubscription)
            .filter(
                PushSubscription.user_id == user_id,
                PushSubscription.endpoint == endpoint,
            )
            .all()
        )
        for row in rows:
            session.delete(row)
        session.commit()
        return {"ok": True, "deleted": len(rows)}
    finally:
        session.close()


@app.post("/wishlist")
def create_wishlist_item(payload: ListItemCreateRequest):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == payload.game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        existing = (
            session.query(WishlistItem)
            .filter(
                WishlistItem.user_id == "legacy-user",
                WishlistItem.game_id == game.id,
            )
            .first()
        )
        if existing:
            return serialize_list_item(existing)

        item = WishlistItem(user_id="legacy-user", game_id=game.id, game_name=game.name)
        session.add(item)
        session.commit()
        session.refresh(item)
        return serialize_list_item(item)
    finally:
        session.close()


@app.delete("/wishlist/{game_name}")
def delete_wishlist_item(game_name: str):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        row = (
            session.query(WishlistItem)
            .filter(WishlistItem.user_id == "legacy-user", WishlistItem.game_id == game.id)
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="Wishlist item not found")
        session.delete(row)
        session.commit()
        return {"deleted": True, "game_name": game_name}
    finally:
        session.close()


@app.post("/wishlist/add")
def wishlist_add(payload: WishlistMutationRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")

        game = session.query(Game).filter(Game.id == payload.game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        existing = (
            session.query(WishlistItem)
            .filter(
                WishlistItem.user_id == user_id,
                WishlistItem.game_id == payload.game_id,
            )
            .first()
        )
        if existing:
            return {
                "ok": True,
                "id": int(existing.id),
                "user_id": existing.user_id,
                "game_id": int(existing.game_id),
                "game_name": existing.game_name or game.name,
            }

        item = WishlistItem(
            user_id=user_id,
            game_id=payload.game_id,
            game_name=game.name,
        )
        session.add(item)
        session.commit()
        session.refresh(item)
        return {
            "ok": True,
            "id": int(item.id),
            "user_id": item.user_id,
            "game_id": int(item.game_id),
            "game_name": item.game_name,
        }
    finally:
        session.close()


@app.post("/wishlist/remove")
def wishlist_remove(payload: WishlistMutationRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        row = (
            session.query(WishlistItem)
            .filter(
                WishlistItem.user_id == user_id,
                WishlistItem.game_id == payload.game_id,
            )
            .first()
        )
        if not row:
            return {"ok": True, "deleted": False}
        session.delete(row)
        session.commit()
        return {"ok": True, "deleted": True}
    finally:
        session.close()


@app.get("/wishlist/{user_id}")
def list_user_wishlist(user_id: str):
    session = Session()
    try:
        rows = (
            session.query(WishlistItem, Game, GameSnapshot)
            .outerjoin(Game, Game.id == WishlistItem.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == WishlistItem.game_id)
            .filter(WishlistItem.user_id == user_id)
            .order_by(WishlistItem.created_at.desc())
            .all()
        )
        return [
            {
                "id": int(item.id),
                "user_id": item.user_id,
                "game_id": int(item.game_id),
                "game_name": item.game_name or (game.name if game else None),
                "steam_appid": snapshot.steam_appid if snapshot else (game.appid if game else None),
                "banner_url": snapshot.banner_url if snapshot else None,
                "latest_price": snapshot.latest_price if snapshot else None,
                "latest_discount_percent": snapshot.latest_discount_percent if snapshot else None,
                "created_at": item.created_at.isoformat() if item.created_at else None,
            }
            for item, game, snapshot in rows
        ]
    finally:
        session.close()


@app.post("/deal-watchlists/add")
def add_deal_watchlist(payload: DealWatchlistAddRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        if payload.target_price is None and payload.target_discount_percent is None:
            raise HTTPException(status_code=400, detail="target_price or target_discount_percent is required")
        if payload.target_price is not None and payload.target_price < 0:
            raise HTTPException(status_code=400, detail="target_price must be >= 0")
        if payload.target_discount_percent is not None and not (0 <= payload.target_discount_percent <= 100):
            raise HTTPException(status_code=400, detail="target_discount_percent must be between 0 and 100")

        game = session.query(Game).filter(Game.id == payload.game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        now = utc_now()
        row = (
            session.query(DealWatchlist)
            .filter(
                DealWatchlist.user_id == user_id,
                DealWatchlist.game_id == payload.game_id,
            )
            .first()
        )
        if row is None:
            row = DealWatchlist(
                user_id=user_id,
                game_id=payload.game_id,
                target_price=payload.target_price,
                target_discount_percent=payload.target_discount_percent,
                active=True,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
        else:
            row.target_price = payload.target_price
            row.target_discount_percent = payload.target_discount_percent
            row.active = True
            row.updated_at = now

        session.commit()
        session.refresh(row)
        return {
            "ok": True,
            "id": int(row.id),
            "user_id": row.user_id,
            "game_id": int(row.game_id),
            "target_price": row.target_price,
            "target_discount_percent": row.target_discount_percent,
            "active": bool(row.active),
        }
    finally:
        session.close()


@app.post("/deal-watchlists/remove")
def remove_deal_watchlist(payload: DealWatchlistRemoveRequest):
    session = Session()
    try:
        user_id = (payload.user_id or "").strip()
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        row = (
            session.query(DealWatchlist)
            .filter(
                DealWatchlist.user_id == user_id,
                DealWatchlist.game_id == payload.game_id,
            )
            .first()
        )
        if not row:
            return {"ok": True, "updated": False}
        row.active = False
        row.updated_at = utc_now()
        session.commit()
        return {"ok": True, "updated": True}
    finally:
        session.close()


@app.get("/deal-watchlists/{user_id}")
def list_deal_watchlists(user_id: str):
    session = Session()
    try:
        rows = (
            session.query(DealWatchlist, Game, GameSnapshot)
            .outerjoin(Game, Game.id == DealWatchlist.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == DealWatchlist.game_id)
            .filter(DealWatchlist.user_id == user_id, DealWatchlist.active.is_(True))
            .order_by(DealWatchlist.updated_at.desc(), DealWatchlist.id.desc())
            .all()
        )
        return [
            {
                "id": int(row.id),
                "user_id": row.user_id,
                "game_id": int(row.game_id),
                "game_name": snapshot.game_name if snapshot else (game.name if game else None),
                "target_price": row.target_price,
                "target_discount_percent": row.target_discount_percent,
                "active": bool(row.active),
                "latest_price": snapshot.latest_price if snapshot else None,
                "latest_discount_percent": snapshot.latest_discount_percent if snapshot else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row, game, snapshot in rows
        ]
    finally:
        session.close()


@app.get("/api/watchlist")
def list_watchlist_api(user_id: str = Query(default=DEFAULT_USER_ID)):
    normalized_user_id = normalize_user_id(user_id)
    session = Session()
    try:
        items = build_watchlist_entries_payload(session, normalized_user_id)
        return {
            "user_id": normalized_user_id,
            "count": len(items),
            "items": items,
        }
    finally:
        session.close()


@app.post("/api/watchlist")
def create_watchlist_api(payload: WatchlistMutationRequest):
    session = Session()
    try:
        user_id = normalize_user_id(payload.user_id)
        game = session.query(Game).filter(Game.id == payload.game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        existing = (
            session.query(Watchlist)
            .filter(Watchlist.user_id == user_id, Watchlist.game_id == payload.game_id)
            .first()
        )
        created = False
        if existing is None:
            row = Watchlist(user_id=user_id, game_id=payload.game_id)
            session.add(row)
            try:
                session.commit()
                created = True
            except IntegrityError:
                session.rollback()
                row = (
                    session.query(Watchlist)
                    .filter(Watchlist.user_id == user_id, Watchlist.game_id == payload.game_id)
                    .first()
                )
                created = False
        else:
            row = existing

        items = build_watchlist_entries_payload(session, user_id)
        item = next((entry for entry in items if int(entry.get("game_id") or 0) == int(payload.game_id)), None)
        return {
            "ok": True,
            "created": created,
            "user_id": user_id,
            "game_id": int(payload.game_id),
            "item": item,
            "count": len(items),
        }
    finally:
        session.close()


@app.delete("/api/watchlist/{game_id}")
def delete_watchlist_api(game_id: int, user_id: str = Query(default=DEFAULT_USER_ID)):
    normalized_user_id = normalize_user_id(user_id)
    session = Session()
    try:
        row = (
            session.query(Watchlist)
            .filter(Watchlist.user_id == normalized_user_id, Watchlist.game_id == game_id)
            .first()
        )
        if not row:
            return {"ok": True, "deleted": False, "user_id": normalized_user_id, "game_id": int(game_id)}
        session.delete(row)
        session.commit()
        return {"ok": True, "deleted": True, "user_id": normalized_user_id, "game_id": int(game_id)}
    finally:
        session.close()


@app.get("/watchlist")
def watchlist_page():
    return FileResponse("web/watchlist.html")


@app.get("/watchlist/items")
def list_watchlist():
    response = list_watchlist_api(DEFAULT_USER_ID)
    return response["items"]


@app.post("/watchlist/items")
def create_watchlist_item(payload: ListItemCreateRequest):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == payload.game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
    finally:
        session.close()
    response = create_watchlist_api(WatchlistMutationRequest(user_id=DEFAULT_USER_ID, game_id=int(game.id)))
    return response.get("item") or {"game_name": payload.game_name, "game_id": int(game.id)}


@app.delete("/watchlist/items/{game_name}")
def delete_watchlist_item(game_name: str):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
    finally:
        session.close()
    response = delete_watchlist_api(int(game.id), DEFAULT_USER_ID)
    return {"deleted": bool(response.get("deleted")), "game_name": game_name, "game_id": int(game.id)}
