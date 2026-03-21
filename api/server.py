import datetime
import json
import math
import mimetypes
import re
import time
import uuid
from pathlib import Path
from statistics import mean
from types import SimpleNamespace
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
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
    IS_DEPLOYED_RUNTIME,
    SITE_DESCRIPTION,
    SITE_HOST,
    SITE_NAME,
    SITE_URL,
    validate_settings,
)
from database import ReadSessionLocal, direct_engine
from database.dirty_games import mark_game_dirty
from database.migration_guard import assert_database_revision_current, warn_if_model_schema_drift
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
    GameDiscoveryFeed,
    GameSnapshot,
    DashboardCache,
    LatestGamePrice,
)
from logger_config import setup_logger

logger = setup_logger("api")

validate_settings()

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")
mimetypes.add_type("image/webp", ".webp")
mimetypes.add_type("image/avif", ".avif")


class CacheControlStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)

        if response.status_code >= 400:
            return response

        normalized_path = (path or "").lower()
        is_html = normalized_path.endswith(".html") or normalized_path == ""

        if is_html:
            response.headers["Cache-Control"] = "public, max-age=300"
        else:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"

        vary_value = response.headers.get("Vary")
        if vary_value:
            if "Accept-Encoding" not in vary_value:
                response.headers["Vary"] = f"{vary_value}, Accept-Encoding"
        else:
            response.headers["Vary"] = "Accept-Encoding"

        return response


app = FastAPI(title=f"{SITE_NAME} API", description=SITE_DESCRIPTION)

ALLOW_ALL_CORS = CORS_ALLOW_ALL_ORIGINS or "*" in CORS_ALLOW_ORIGINS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ALLOW_ALL_CORS else CORS_ALLOW_ORIGINS,
    allow_credentials=not ALLOW_ALL_CORS,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
    compresslevel=5,
)


@app.middleware("http")
async def security_and_cache_headers_middleware(request: Request, call_next):
    response = await call_next(request)

    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")

    vary_value = response.headers.get("Vary")
    if vary_value:
        if "Accept-Encoding" not in vary_value:
            response.headers["Vary"] = f"{vary_value}, Accept-Encoding"
    else:
        response.headers["Vary"] = "Accept-Encoding"

    if request.url.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")

    return response


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


app.mount("/web", CacheControlStaticFiles(directory="web"), name="web")
if Path("public").exists():
    app.mount("/public", CacheControlStaticFiles(directory="public"), name="public")


@app.on_event("startup")
async def startup_guardrails() -> None:
    assert_database_revision_current(component_name="api server", logger=logger)
    assert_scale_schema_ready(direct_engine, component_name="api server")
    if not IS_DEPLOYED_RUNTIME:
        warn_if_model_schema_drift(component_name="api server", logger=logger)
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
CRITICAL_DASHBOARD_CACHE_KEY = "home_critical_v1"
LEGACY_DASHBOARD_CACHE_KEYS = ("home",)
DASHBOARD_CACHE_STALE_AFTER = datetime.timedelta(minutes=API_DASHBOARD_CACHE_STALE_MINUTES)
DEAL_RADAR_CACHE_KEY = "home:deal_radar"
SEASONAL_SUMMARY_CACHE_KEY = "home:seasonal_summary"
TOP_REVIEWED_CACHE_KEY = "home:top_reviewed"
TOP_PLAYED_CACHE_KEY = "home:top_played"
LEADERBOARD_CACHE_KEY = "home:leaderboard"
CATALOG_SEED_CACHE_KEY = "home:catalog_seed"
HOMEPAGE_CRITICAL_LIMIT = 8
OPPORTUNITY_QUERY_MULTIPLIER = 8
OPPORTUNITY_MIN_CANDIDATES = 96
OPPORTUNITY_MAX_CANDIDATES = 320
PERSONALIZED_QUERY_MULTIPLIER = 4
PERSONALIZED_MIN_CANDIDATES = 48
PERSONALIZED_MAX_CANDIDATES = 120
DAILY_DIGEST_WINDOW_HOURS = 24
DAILY_DIGEST_EVENT_SCAN_LIMIT = 360
DAILY_DIGEST_ALERT_SCAN_LIMIT = 320
DAILY_DIGEST_SNAPSHOT_SCAN_LIMIT = 220
DEFAULT_USER_ID = API_DEFAULT_USER_ID
VIEWER_ID_COOKIE_NAME = "gameden_viewer_id"
VIEWER_ID_HEADER_NAME = "x-gameden-viewer"
VIEWER_ID_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
ANONYMOUS_USER_ID_RE = re.compile(r"^anon_[0-9a-f]{32}$")
SITEMAP_STATIC_PATHS = (
    "/",
    "/all-results",
    "/game",
    "/history",
    "/game-detail",
    "/watchlist",
    "/historical-lows",
    "/best-deals",
    "/trending",
    "/buy-now",
    "/wait-for-sale",
    "/under-10",
    "/under-20",
    "/popular-discounts",
)
SITEMAP_GAME_DETAIL_LIMIT = 1200
EXTENDED_PLATFORM_FILTER_OPTIONS = ("Steam Deck", "VR Compatibility")
SEARCH_SIMILARITY_THRESHOLD = API_SEARCH_SIMILARITY_THRESHOLD
HISTORY_RANGE_DAYS: dict[str, int] = {
    "30d": 30,
    "90d": 90,
    "1y": 365,
}
SEO_DISCOVERY_PAGE_DEFINITIONS: dict[str, dict[str, str]] = {
    "best-deals": {
        "slug": "best-deals",
        "path": "/best-deals",
        "title": "Best Steam Deals Right Now | GameDen.gg",
        "heading": "Best Deals Right Now",
        "intro": "High-conviction Steam deals ranked by discount depth, deal score, and buy-timing signals.",
        "description": "Snapshot-ranked Steam deals blending discount depth, deal score, and timing signals.",
        "empty_message": "No strong live deals are available right now.",
    },
    "historical-lows": {
        "slug": "historical-lows",
        "path": "/historical-lows",
        "title": "Steam Games Near Historical Lows | GameDen.gg",
        "heading": "Steam Games Near Historical Lows",
        "intro": "Games at, matching, or close to tracked historical lows from snapshot-backed price intelligence.",
        "description": "Steam games currently near historical lows using snapshot-backed price signals.",
        "empty_message": "No near-low opportunities are available right now.",
    },
    "trending": {
        "slug": "trending",
        "path": "/trending",
        "title": "Trending Steam Games Today | GameDen.gg",
        "heading": "Trending Steam Games",
        "intro": "Momentum-led games with strong player activity and active deal context right now.",
        "description": "Trending Steam games with rising player momentum and current deal signals.",
        "empty_message": "No strong trending deal candidates are available right now.",
    },
    "buy-now": {
        "slug": "buy-now",
        "path": "/buy-now",
        "title": "Buy Now Picks on Steam | GameDen.gg",
        "heading": "Buy Now Picks",
        "intro": "Games currently flagged BUY NOW by GameDen snapshot signals and pricing context.",
        "description": "Snapshot-backed BUY NOW Steam picks with current pricing and momentum context.",
        "empty_message": "No buy-now picks are available right now.",
    },
    "wait-for-sale": {
        "slug": "wait-for-sale",
        "path": "/wait-for-sale",
        "title": "Steam Games to Wait For Sale | GameDen.gg",
        "heading": "Wait for Next Sale",
        "intro": "Games currently flagged WAIT where a stronger future discount is likely.",
        "description": "Steam games where snapshot signals suggest waiting for a better sale.",
        "empty_message": "No wait-for-sale picks are available right now.",
    },
    "under-10": {
        "slug": "under-10",
        "path": "/under-10",
        "title": "Best Steam Games Under $10 | GameDen.gg",
        "heading": "Best Steam Games Under $10",
        "intro": "Released Steam games currently priced at $10 or less with live quality and deal context.",
        "description": "Snapshot-backed Steam deals under $10 with quality and momentum signals.",
        "empty_message": "No qualifying under-$10 deals are available right now.",
    },
    "under-20": {
        "slug": "under-20",
        "path": "/under-20",
        "title": "Best Steam Games Under $20 | GameDen.gg",
        "heading": "Best Steam Games Under $20",
        "intro": "Released Steam games currently priced above $10 and up to $20 with strong deal context.",
        "description": "Snapshot-backed Steam deals under $20 with quality and momentum context.",
        "empty_message": "No qualifying under-$20 deals are available right now.",
    },
    "popular-discounts": {
        "slug": "popular-discounts",
        "path": "/popular-discounts",
        "title": "Popular Steam Games on Discount | GameDen.gg",
        "heading": "Popular Games on Discount",
        "intro": "Popular Steam titles with meaningful live discounts and current momentum context.",
        "description": "Popular Steam games currently discounted, ranked by discount and momentum signals.",
        "empty_message": "No popular discounted games are available right now.",
    },
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


def _new_anonymous_user_id() -> str:
    return f"anon_{uuid.uuid4().hex}"


def _normalize_anonymous_user_id(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if ANONYMOUS_USER_ID_RE.fullmatch(normalized):
        return normalized
    return None


def _resolve_viewer_id(request: Request) -> tuple[str, bool]:
    cookie_viewer_id = _normalize_anonymous_user_id(request.cookies.get(VIEWER_ID_COOKIE_NAME))
    if cookie_viewer_id:
        return cookie_viewer_id, False

    header_viewer_id = _normalize_anonymous_user_id(request.headers.get(VIEWER_ID_HEADER_NAME))
    if header_viewer_id:
        return header_viewer_id, True

    query_viewer_id = _normalize_anonymous_user_id(request.query_params.get("user_id"))
    if query_viewer_id:
        return query_viewer_id, True

    return _new_anonymous_user_id(), True


def _viewer_cookie_samesite() -> str:
    return "none" if IS_DEPLOYED_RUNTIME else "lax"


def _set_viewer_cookie(response: Response, viewer_id: str) -> None:
    response.set_cookie(
        key=VIEWER_ID_COOKIE_NAME,
        value=viewer_id,
        max_age=VIEWER_ID_MAX_AGE_SECONDS,
        httponly=True,
        secure=IS_DEPLOYED_RUNTIME,
        samesite=_viewer_cookie_samesite(),
        path="/",
    )


def resolve_request_user_id(request: Request, candidate: str | None = None) -> str:
    authenticated_user_id = str(getattr(request.state, "authenticated_user_id", "") or "").strip()
    if authenticated_user_id and not _is_anonymous_user_id(authenticated_user_id):
        return authenticated_user_id

    viewer_id = _normalize_anonymous_user_id(getattr(request.state, "viewer_id", None))
    if not viewer_id:
        viewer_id = _new_anonymous_user_id()

    normalized_candidate = normalize_user_id(candidate)
    if normalized_candidate == viewer_id:
        return viewer_id

    candidate_anonymous = _normalize_anonymous_user_id(normalized_candidate)
    if candidate_anonymous:
        return viewer_id

    return viewer_id


@app.middleware("http")
async def viewer_identity_middleware(request: Request, call_next):
    viewer_id, should_set_cookie = _resolve_viewer_id(request)
    request.state.viewer_id = viewer_id

    response = await call_next(request)
    if should_set_cookie or request.cookies.get(VIEWER_ID_COOKIE_NAME) != viewer_id:
        _set_viewer_cookie(response, viewer_id)
    response.headers.setdefault("X-GameDen-Viewer", viewer_id)
    return response


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
    lowered = str(value or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered)
    collapsed = re.sub(r"\s+", " ", normalized).strip()
    return collapsed


def _search_tokens(value: str | None, max_tokens: int = 6) -> list[str]:
    normalized = _normalize_search_text(value)
    if not normalized:
        return []
    tokens = [token for token in normalized.split(" ") if token]
    if not tokens:
        return []
    return tokens[: max(1, int(max_tokens))]


def _build_catalog_search_predicate(search_text: str, include_similarity: bool):
    trimmed = str(search_text or "").strip()
    if not trimmed:
        return None

    pattern = f"%{trimmed}%"
    filters = [
        Game.name.ilike(pattern),
        func.coalesce(Game.developer, "").ilike(pattern),
        func.coalesce(Game.publisher, "").ilike(pattern),
        func.coalesce(Game.genres, "").ilike(pattern),
        func.coalesce(Game.tags, "").ilike(pattern),
        func.coalesce(GameSnapshot.genres, "").ilike(pattern),
        func.coalesce(GameSnapshot.tags, "").ilike(pattern),
    ]
    for token in _search_tokens(trimmed):
        if len(token) < 2:
            continue
        filters.append(func.lower(Game.name).like(f"%{token}%"))

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


def _compact_search_text(value: str | None) -> str:
    return _normalize_search_text(value).replace(" ", "")


def _search_token_hit_count(text: str, tokens: list[str]) -> int:
    if not text or not tokens:
        return 0
    return sum(1 for token in tokens if token in text)


def _score_search_candidate_row(row: dict, normalized_query: str, query_tokens: list[str]) -> tuple[float, int, int, float, float, str]:
    name = _normalize_search_text(row.get("game_name"))
    developer = _normalize_search_text(row.get("developer"))
    publisher = _normalize_search_text(row.get("publisher"))
    genres = _normalize_search_text(row.get("genres_csv"))
    tags = _normalize_search_text(row.get("tags_csv"))

    compact_query = _compact_search_text(normalized_query)
    compact_name = _compact_search_text(name)

    score = 0.0
    exact_rank = 3
    if normalized_query:
        if name == normalized_query:
            score += 1900.0
            exact_rank = 0
        elif name.startswith(normalized_query):
            score += 1400.0
            exact_rank = 1
        elif normalized_query in name:
            score += 980.0
            exact_rank = 2

    if compact_query:
        if compact_name == compact_query:
            score += 1200.0
            exact_rank = min(exact_rank, 1)
        elif compact_query in compact_name:
            score += 480.0
        elif compact_name and compact_name in compact_query:
            score += 360.0

    name_hits = _search_token_hit_count(name, query_tokens)
    name_hits = max(name_hits, _search_token_hit_count(compact_name, query_tokens))
    developer_hits = _search_token_hit_count(developer, query_tokens)
    publisher_hits = _search_token_hit_count(publisher, query_tokens)
    genre_hits = _search_token_hit_count(genres, query_tokens)
    tag_hits = _search_token_hit_count(tags, query_tokens)
    metadata_hits = developer_hits + publisher_hits + genre_hits + tag_hits

    if name_hits > 0:
        score += float(name_hits) * 220.0
        if name_hits >= len(query_tokens):
            score += 520.0
    score += float(developer_hits) * 70.0
    score += float(publisher_hits) * 60.0
    score += float(genre_hits) * 30.0
    score += float(tag_hits) * 26.0

    if name_hits == 0 and metadata_hits > 0:
        score -= 280.0

    similarity_score = max(0.0, min(safe_num(row.get("sim"), 0.0), 1.0))
    popularity_score = max(0.0, min(safe_num(row.get("popularity_score"), 0.0), 100.0))
    deal_score = max(0.0, min(safe_num(row.get("deal_score"), 0.0), 100.0))

    score += similarity_score * 260.0
    score += popularity_score * 0.85
    score += deal_score * 0.35

    return (
        score,
        exact_rank,
        name_hits,
        popularity_score,
        deal_score,
        name,
    )


def _rank_search_rows(rows: list[dict], normalized_query: str, limit: int) -> list[dict]:
    normalized_limit = max(1, int(limit))
    query_tokens = _search_tokens(normalized_query)
    if not rows:
        return []

    scored_rows = []
    for row in rows:
        score_tuple = _score_search_candidate_row(row, normalized_query, query_tokens)
        scored_rows.append((score_tuple, row))

    scored_rows.sort(
        key=lambda entry: (
            -entry[0][0],
            entry[0][1],
            -entry[0][2],
            -entry[0][3],
            -entry[0][4],
            entry[0][5],
        )
    )

    ranked = []
    seen_ids: set[int] = set()
    for _, row in scored_rows:
        game_id = int(safe_num(row.get("id"), 0.0))
        if game_id <= 0 or game_id in seen_ids:
            continue
        seen_ids.add(game_id)
        ranked.append(row)
        if len(ranked) >= normalized_limit:
            break
    return ranked


def serialize_game_metadata(game: Optional[Game]) -> dict:
    game_slug = _canonical_game_slug(game.name if game else None, game.id if game else None)
    return {
        "appid": game.appid if game else None,
        "slug": game_slug,
        "game_slug": game_slug,
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

    normalized_game_id = int(safe_num(getattr(row, "game_id", None), 0.0))
    if normalized_game_id <= 0 and game is not None and getattr(game, "id", None) is not None:
        normalized_game_id = int(safe_num(game.id, 0.0))
    normalized_game_id = normalized_game_id if normalized_game_id > 0 else None
    appid = game.appid if game and game.appid else extract_appid_from_store_url(row.store_url)
    historical_low = insight.get("historical_low")
    previous_historical_low = insight.get("previous_historical_low")
    historical_status = insight.get("historical_status")
    history_point_count = insight.get("history_point_count", 0)
    ever_discounted = bool(insight.get("ever_discounted"))
    max_discount = int(insight.get("max_discount", 0) or 0)

    return {
        "id": normalized_game_id,
        "game_id": normalized_game_id,
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


def serialize_upcoming_snapshot_row(snapshot: GameSnapshot) -> dict:
    game_slug = _canonical_game_slug(snapshot.game_name, snapshot.game_id)
    return {
        "game_id": int(snapshot.game_id),
        "game_name": snapshot.game_name,
        "slug": game_slug,
        "game_slug": game_slug,
        "steam_appid": snapshot.steam_appid,
        "release_date": snapshot.release_date.isoformat() if snapshot.release_date else None,
        "release_date_text": snapshot.release_date_text,
        "store_url": snapshot.store_url,
        "banner_url": snapshot.banner_url or build_steam_banner_url(snapshot.store_url, snapshot.steam_appid),
        "genres": parse_csv_field(snapshot.genres or ""),
        "tags": parse_csv_field(snapshot.tags or ""),
        "platforms": parse_csv_field(snapshot.platforms or ""),
        "review_score": snapshot.review_score,
        "review_score_label": snapshot.review_score_label,
        "review_total_count": snapshot.review_count,
        "popularity_score": snapshot.popularity_score,
        "upcoming_hot_score": snapshot.upcoming_hot_score,
        "is_upcoming": bool(snapshot.is_upcoming),
        "is_released": int(snapshot.is_released or 0),
    }


def get_latest_price_rows(session):
    rows = session.query(GameSnapshot).all()
    latest_prices = []
    for row in rows:
        latest_prices.append(
            SimpleNamespace(
                game_id=row.game_id,
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


def _is_game_watchlisted_for_user(session: Session, game_id: int, user_id: str | None) -> bool:
    normalized_user_id = normalize_user_id(user_id)
    if _is_guest_user_id(normalized_user_id):
        return False
    return (
        session.query(Watchlist.id)
        .filter(Watchlist.user_id == normalized_user_id, Watchlist.game_id == int(game_id))
        .first()
        is not None
    )


def _is_anonymous_user_id(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return True
    return normalized in {
        str(DEFAULT_USER_ID or "").strip().lower(),
        "anonymous",
        "guest",
    }


def _is_guest_user_id(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return True
    return normalized in {"anonymous", "guest"}


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
                "slug": _canonical_game_slug(game_name, row.game_id),
                "game_slug": _canonical_game_slug(game_name, row.game_id),
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


FeedProjectionRow = GameDiscoveryFeed | GameSnapshot


def _query_release_feed_rows(
    session,
    *,
    limit: int,
    projection_order_by: list,
    snapshot_order_by: list,
    projection_filters: list | None = None,
    snapshot_filters: list | None = None,
) -> list[FeedProjectionRow]:
    base_projection_filters = [
        GameDiscoveryFeed.is_released == 1,
        or_(GameDiscoveryFeed.is_upcoming.is_(False), GameDiscoveryFeed.is_upcoming.is_(None)),
        GameDiscoveryFeed.latest_price.isnot(None),
    ]
    if projection_filters:
        base_projection_filters.extend(projection_filters)
    projection_rows = (
        session.query(GameDiscoveryFeed)
        .filter(*base_projection_filters)
        .order_by(*projection_order_by)
        .limit(limit)
        .all()
    )
    if len(projection_rows) >= int(limit):
        return projection_rows

    base_snapshot_filters = [
        GameSnapshot.is_released == 1,
        or_(GameSnapshot.is_upcoming.is_(False), GameSnapshot.is_upcoming.is_(None)),
        GameSnapshot.latest_price.isnot(None),
    ]
    if snapshot_filters:
        base_snapshot_filters.extend(snapshot_filters)
    snapshot_rows = (
        session.query(GameSnapshot)
        .filter(*base_snapshot_filters)
        .order_by(*snapshot_order_by)
        .limit(limit)
        .all()
    )
    if not projection_rows:
        return snapshot_rows

    seen_game_ids = {int(safe_num(row.game_id, 0.0)) for row in projection_rows}
    merged_rows: list[FeedProjectionRow] = list(projection_rows)
    for row in snapshot_rows:
        game_id = int(safe_num(row.game_id, 0.0))
        if game_id <= 0 or game_id in seen_game_ids:
            continue
        merged_rows.append(row)
        seen_game_ids.add(game_id)
        if len(merged_rows) >= int(limit):
            break
    return merged_rows


def _build_deal_opportunity_item(snapshot: FeedProjectionRow) -> dict | None:
    price = snapshot.latest_price
    discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    if price is None or safe_num(price, 0.0) <= 0 or discount <= 0:
        return None
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
    stored_opportunity_score = safe_num(snapshot.deal_opportunity_score, 0.0)
    stored_opportunity_reason = _normalize_opportunity_reason(snapshot.deal_opportunity_reason)

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

    if stored_opportunity_score > 0:
        score = max(score, stored_opportunity_score)
        _append_unique_reason(reasons, stored_opportunity_reason)

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
    computed_score = round(score, 2)
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
        "is_released": getattr(snapshot, "is_released", 1),
        "is_upcoming": getattr(snapshot, "is_upcoming", False),
        "release_date": getattr(snapshot, "release_date", None),
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
        "deal_opportunity_score": round(stored_opportunity_score, 2) if stored_opportunity_score > 0 else computed_score,
        "deal_opportunity_reason": snapshot.deal_opportunity_reason or " and ".join(reason_lines),
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
        "opportunity_score": computed_score,
        "opportunity_reasons": reason_lines,
        "opportunity_reason": " and ".join(reason_lines),
        "updated_at": updated_at.isoformat(),
    }


def _build_opportunity_radar_item(snapshot: FeedProjectionRow) -> dict | None:
    if snapshot.latest_price is None or safe_num(snapshot.latest_price, 0.0) <= 0:
        return None
    if safe_num(snapshot.latest_discount_percent, 0.0) <= 0:
        return None

    score = round(safe_num(snapshot.deal_opportunity_score, 0.0), 2)
    if score <= 0:
        return None

    reasons: list[str] = []
    for candidate in (
        snapshot.deal_opportunity_reason,
        snapshot.buy_reason,
        snapshot.predicted_sale_reason,
        snapshot.worth_buying_reason_summary,
        snapshot.trend_reason_summary,
        snapshot.deal_heat_reason,
    ):
        _append_unique_reason(reasons, _normalize_opportunity_reason(candidate))
        if len(reasons) >= 2:
            break
    if not reasons:
        reasons = ["Snapshot timing and value signals align"]

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
        "is_released": getattr(snapshot, "is_released", 1),
        "is_upcoming": getattr(snapshot, "is_upcoming", False),
        "release_date": getattr(snapshot, "release_date", None),
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
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "predicted_next_sale_window_days_min": snapshot.predicted_next_sale_window_days_min,
        "predicted_next_sale_window_days_max": snapshot.predicted_next_sale_window_days_max,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "deal_opportunity_score": score,
        "deal_opportunity_reason": snapshot.deal_opportunity_reason or " and ".join(reasons[:2]),
        "opportunity_score": score,
        "opportunity_reasons": reasons[:2],
        "opportunity_reason": " and ".join(reasons[:2]),
        "updated_at": updated_at.isoformat(),
    }


def _collect_deal_opportunity_items(session, limit: int) -> list[dict]:
    opportunities, _ = _collect_opportunity_item_pair(session, limit)
    return opportunities


def _collect_opportunity_item_pair(session, limit: int) -> tuple[list[dict], list[dict]]:
    normalized_limit = max(1, int(limit))
    candidate_limit = max(
        OPPORTUNITY_MIN_CANDIDATES,
        min(OPPORTUNITY_MAX_CANDIDATES, normalized_limit * OPPORTUNITY_QUERY_MULTIPLIER),
    )
    rows = _query_release_feed_rows(
        session,
        limit=candidate_limit,
        projection_order_by=[
            GameDiscoveryFeed.buy_score.desc().nullslast(),
            GameDiscoveryFeed.worth_buying_score.desc().nullslast(),
            GameDiscoveryFeed.deal_score.desc().nullslast(),
            GameDiscoveryFeed.momentum_score.desc().nullslast(),
            GameDiscoveryFeed.popularity_score.desc().nullslast(),
            GameDiscoveryFeed.latest_discount_percent.desc().nullslast(),
            GameDiscoveryFeed.updated_at.desc().nullslast(),
            GameDiscoveryFeed.game_id.asc(),
        ],
        snapshot_order_by=[
            GameSnapshot.buy_score.desc().nullslast(),
            GameSnapshot.worth_buying_score.desc().nullslast(),
            GameSnapshot.deal_score.desc().nullslast(),
            GameSnapshot.momentum_score.desc().nullslast(),
            GameSnapshot.popularity_score.desc().nullslast(),
            GameSnapshot.latest_discount_percent.desc().nullslast(),
            GameSnapshot.updated_at.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        ],
    )

    scored_opportunities: list[tuple[float, datetime.datetime, dict]] = []
    scored_radar: list[tuple[float, datetime.datetime, dict]] = []
    for snapshot in rows:
        opportunity_item = _build_deal_opportunity_item(snapshot)
        if opportunity_item is not None:
            updated_at = snapshot.updated_at if isinstance(snapshot.updated_at, datetime.datetime) else utc_now()
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
            scored_opportunities.append((safe_num(opportunity_item.get("opportunity_score"), 0.0), updated_at, opportunity_item))

        radar_item = _build_opportunity_radar_item(snapshot)
        if radar_item is not None:
            updated_at = snapshot.updated_at if isinstance(snapshot.updated_at, datetime.datetime) else utc_now()
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=datetime.timezone.utc)
            scored_radar.append((safe_num(radar_item.get("deal_opportunity_score"), 0.0), updated_at, radar_item))

    scored_opportunities.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
    opportunity_items = [item for _, _, item in scored_opportunities[:normalized_limit]]
    excluded_game_ids = {
        int(safe_num(item.get("game_id"), 0.0))
        for item in opportunity_items
        if int(safe_num(item.get("game_id"), 0.0)) > 0
    }

    scored_radar.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
    radar_items: list[dict] = []
    seen_radar_ids: set[int] = set()
    for _, _, item in scored_radar:
        game_id = int(safe_num(item.get("game_id"), 0.0))
        if game_id <= 0 or game_id in excluded_game_ids or game_id in seen_radar_ids:
            continue
        seen_radar_ids.add(game_id)
        radar_items.append(item)
        if len(radar_items) >= normalized_limit:
            break

    return opportunity_items, radar_items


def _collect_opportunity_radar_items(session, limit: int, exclude_game_ids: set[int] | None = None) -> list[dict]:
    normalized_limit = max(1, int(limit))
    excluded: set[int] = set()
    for game_id in (exclude_game_ids or set()):
        try:
            parsed_id = int(game_id)
        except Exception:
            continue
        if parsed_id > 0:
            excluded.add(parsed_id)
    _, radar_items = _collect_opportunity_item_pair(session, normalized_limit)
    if not excluded:
        return radar_items[:normalized_limit]
    filtered_items = [
        item
        for item in radar_items
        if int(safe_num(item.get("game_id"), 0.0)) not in excluded
    ]
    return filtered_items[:normalized_limit]


def _coerce_utc_datetime(value: datetime.datetime | None) -> datetime.datetime | None:
    if not isinstance(value, datetime.datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value


def _build_personalization_token_weights(
    seed_rows: list[tuple[int, str | None, str | None]],
    *,
    wishlist_game_ids: set[int],
    watchlist_game_ids: set[int],
    target_game_ids: set[int],
    recent_game_ids: set[int],
) -> dict[str, float]:
    token_weights: dict[str, float] = {}
    for game_id, tags, genres in seed_rows:
        base_weight = 1.0
        if game_id in wishlist_game_ids:
            base_weight += 1.0
        if game_id in watchlist_game_ids:
            base_weight += 0.8
        if game_id in target_game_ids:
            base_weight += 0.6
        if game_id in recent_game_ids:
            base_weight += 0.4

        tokens = {str(token).strip().lower() for token in [*parse_csv_field(tags), *parse_csv_field(genres)]}
        for token in tokens:
            if len(token) < 2:
                continue
            token_weights[token] = token_weights.get(token, 0.0) + base_weight
    return token_weights


def _compute_personalization_similarity_bonus(
    snapshot: GameSnapshot,
    token_weights: dict[str, float],
) -> tuple[float, int]:
    if not token_weights:
        return 0.0, 0

    snapshot_tokens = {
        str(token).strip().lower()
        for token in [*parse_csv_field(snapshot.tags), *parse_csv_field(snapshot.genres)]
        if token
    }
    if not snapshot_tokens:
        return 0.0, 0

    overlap_weights = [token_weights[token] for token in snapshot_tokens if token in token_weights]
    if not overlap_weights:
        return 0.0, 0

    overlap_count = len(overlap_weights)
    bonus = min(22.0, sum(overlap_weights) * 1.85)
    return round(bonus, 2), overlap_count


def _normalize_personalization_score(raw_score: float, *, personalized_context: bool) -> float:
    denominator = 185.0 if personalized_context else 120.0
    normalized = (safe_num(raw_score, 0.0) / denominator) * 100.0
    return round(max(0.0, min(100.0, normalized)), 2)


def _build_deal_confidence_badge(score_value: float | None) -> dict | None:
    parsed_score = safe_num(score_value, -1.0)
    if parsed_score < 0:
        return None

    score = round(max(0.0, min(100.0, parsed_score)), 1)
    if score >= 85:
        return {
            "score": score,
            "confidence_label": "Strong Buy",
            "confidence_icon": "SB",
            "confidence_color": "#5ce4a9",
            "confidence_class": "strong-buy",
        }
    if score >= 70:
        return {
            "score": score,
            "confidence_label": "Good Deal",
            "confidence_icon": "GD",
            "confidence_color": "#6fe8ff",
            "confidence_class": "good-deal",
        }
    if score >= 50:
        return {
            "score": score,
            "confidence_label": "Fair Price",
            "confidence_icon": "FP",
            "confidence_color": "#ffc77a",
            "confidence_class": "fair-price",
        }
    return {
        "score": score,
        "confidence_label": "Wait",
        "confidence_icon": "WT",
        "confidence_color": "#9eb8e7",
        "confidence_class": "wait",
    }


def _escape_svg_text(value: str | None) -> str:
    text_value = str(value or "")
    return (
        text_value
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _truncate_share_text(value: str | None, max_chars: int = 96) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ""
    if len(cleaned) <= max_chars:
        return cleaned
    return f"{cleaned[: max(0, max_chars - 3)].rstrip()}..."


def _wrap_share_title_lines(value: str | None, max_chars: int = 28, max_lines: int = 2) -> list[str]:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        return ["Unknown game"]

    words = cleaned.split(" ")
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= max_chars or not current:
            current = candidate
            continue

        lines.append(current)
        current = word
        if len(lines) >= max_lines - 1:
            break

    if len(lines) < max_lines and current:
        lines.append(current)

    if len(lines) > max_lines:
        lines = lines[:max_lines]
    if len(lines) == max_lines:
        lines[-1] = _truncate_share_text(lines[-1], max_chars)

    return lines


def _format_share_price(value: float | None) -> str:
    if value is None:
        return "--"
    numeric = safe_num(value, 0.0)
    if numeric <= 0:
        return "Free"
    return f"${numeric:,.2f}"


def _build_share_explanation(snapshot: GameSnapshot) -> str:
    for candidate in (
        snapshot.buy_reason,
        snapshot.deal_heat_reason,
        snapshot.predicted_sale_reason,
    ):
        normalized = _normalize_opportunity_reason(candidate)
        if normalized:
            return _truncate_share_text(normalized, 92)

    discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    recommendation = _normalize_buy_recommendation(snapshot.buy_recommendation)
    if recommendation == "BUY_NOW":
        return "Buy-now recommendation"
    if _is_near_historical_low(snapshot):
        return "Near historical low"
    if discount >= 40:
        return "Strong discount right now"
    return "Snapshot-backed deal signal"


def _build_share_deal_svg(snapshot: GameSnapshot, game_id: int) -> str:
    title_lines = _wrap_share_title_lines(snapshot.game_name, max_chars=30, max_lines=2)
    if len(title_lines) == 1:
        title_lines.append("")

    confidence = _build_deal_confidence_badge(
        snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score
        if snapshot.worth_buying_score is not None
        else snapshot.deal_score
    ) or _build_deal_confidence_badge(snapshot.deal_score)
    if confidence is None:
        confidence = {
            "confidence_label": "Wait",
            "confidence_icon": "WT",
            "confidence_color": "#9eb8e7",
        }

    confidence_label = str(confidence.get("confidence_label") or "Wait").strip()
    confidence_icon = str(confidence.get("confidence_icon") or "WT").strip()
    confidence_color = str(confidence.get("confidence_color") or "#9eb8e7").strip()
    confidence_chip_width = max(190, min(390, 116 + len(confidence_label) * 12))

    explanation = _build_share_explanation(snapshot)
    price_text = _format_share_price(snapshot.latest_price)
    discount_value = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    discount_text = f"{discount_value}% off" if discount_value > 0 else "No active discount"

    title_line_one = _escape_svg_text(title_lines[0])
    title_line_two = _escape_svg_text(title_lines[1])
    escaped_price = _escape_svg_text(price_text)
    escaped_discount = _escape_svg_text(discount_text)
    escaped_confidence = _escape_svg_text(f"{confidence_icon} {confidence_label}")
    escaped_explanation = _escape_svg_text(explanation)
    escaped_brand = _escape_svg_text("GameDen.gg")
    escaped_subtitle = _escape_svg_text("The Game Market Radar")

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-label="{_escape_svg_text(snapshot.game_name)} deal card">
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#071324"/>
      <stop offset="100%" stop-color="#0d1f3d"/>
    </linearGradient>
    <linearGradient id="panel" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#132948" stop-opacity="0.94"/>
      <stop offset="100%" stop-color="#0b1b34" stop-opacity="0.94"/>
    </linearGradient>
  </defs>

  <rect x="0" y="0" width="1200" height="630" fill="url(#bg)"/>
  <circle cx="1080" cy="82" r="220" fill="#3b82f6" fill-opacity="0.11"/>
  <circle cx="80" cy="610" r="240" fill="#60a5fa" fill-opacity="0.08"/>
  <rect x="44" y="40" width="1112" height="550" rx="28" fill="url(#panel)" stroke="#9ab6ff" stroke-opacity="0.26"/>

  <text x="88" y="118" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="18" fill="#9ab6ff">{escaped_brand}</text>
  <text x="88" y="146" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="14" fill="#b7c8eb" fill-opacity="0.92">{escaped_subtitle}</text>

  <text x="88" y="240" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="54" font-weight="800" fill="#edf4ff">{title_line_one}</text>
  <text x="88" y="304" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="54" font-weight="800" fill="#edf4ff">{title_line_two}</text>

  <rect x="88" y="344" width="286" height="118" rx="18" fill="#102643" stroke="#9ab6ff" stroke-opacity="0.22"/>
  <text x="114" y="378" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="16" fill="#9ab6ff">Current price</text>
  <text x="114" y="432" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="46" font-weight="800" fill="#edf4ff">{escaped_price}</text>

  <rect x="394" y="344" width="214" height="118" rx="18" fill="#102643" stroke="#9ab6ff" stroke-opacity="0.22"/>
  <text x="420" y="378" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="16" fill="#9ab6ff">Discount</text>
  <text x="420" y="432" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="34" font-weight="700" fill="#edf4ff">{escaped_discount}</text>

  <rect x="88" y="482" width="{confidence_chip_width}" height="54" rx="27" fill="{confidence_color}" fill-opacity="0.2" stroke="{confidence_color}" stroke-opacity="0.65"/>
  <text x="114" y="516" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="23" font-weight="700" fill="#edf4ff">{escaped_confidence}</text>

  <text x="88" y="565" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="19" fill="#dce8ff">{escaped_explanation}</text>

  <text x="1112" y="564" text-anchor="end" font-family="Inter,Segoe UI,Roboto,sans-serif" font-size="13" fill="#9ab6ff">/share/deal/{int(game_id)}</text>
</svg>
"""


def _build_personalized_deal_item(
    snapshot: FeedProjectionRow,
    *,
    wishlist_game_ids: set[int],
    watchlist_game_ids: set[int],
    target_game_ids: set[int],
    recent_game_ids: set[int],
    token_weights: dict[str, float],
    recent_event_counts: dict[int, int],
    personalization_enabled: bool,
    has_personal_seed_data: bool,
) -> dict | None:
    if snapshot.latest_price is None:
        return None
    if safe_num(snapshot.latest_price, 0.0) <= 0:
        return None
    if safe_num(snapshot.latest_discount_percent, 0.0) <= 0:
        return None
    if bool(getattr(snapshot, "is_upcoming", False)):
        return None

    use_personal_context = personalization_enabled and has_personal_seed_data
    game_id = int(snapshot.game_id)
    in_wishlist = game_id in wishlist_game_ids
    in_watchlist = game_id in watchlist_game_ids
    in_target_watch = game_id in target_game_ids
    recently_tracked = game_id in recent_game_ids

    discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    deal_score = safe_num(snapshot.deal_score, 0.0)
    buy_score = safe_num(
        snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score,
        0.0,
    )
    deal_opportunity_score = safe_num(snapshot.deal_opportunity_score, 0.0)
    trending_score = safe_num(snapshot.trending_score, safe_num(snapshot.momentum_score, 0.0))
    popularity_score = safe_num(snapshot.popularity_score, 0.0)
    momentum_score = safe_num(snapshot.momentum_score, 0.0)
    player_growth_ratio = safe_num(snapshot.player_growth_ratio, 0.0)
    short_term_player_trend = safe_num(snapshot.short_term_player_trend, 0.0)
    price_vs_low_ratio = safe_num(snapshot.price_vs_low_ratio, 0.0)
    historical_status = str(snapshot.historical_status or "").strip().lower()
    current_players = safe_num(snapshot.current_players, 0.0)

    score = 0.0
    reasons: list[str] = []

    if use_personal_context and in_wishlist:
        score += 52.0
        _append_unique_reason(reasons, "In your wishlist")
    if use_personal_context and in_watchlist:
        score += 46.0
        _append_unique_reason(reasons, "In your watchlist")
    if use_personal_context and in_target_watch and not in_wishlist and not in_watchlist:
        score += 32.0
        _append_unique_reason(reasons, "In your price alerts")
    if use_personal_context and recently_tracked and not in_wishlist and not in_watchlist:
        score += 10.0
        _append_unique_reason(reasons, "Recently tracked by you")

    # Core ranking factors for the personalized feed.
    score += min(deal_score, 100.0) * 0.36
    score += min(deal_opportunity_score, 100.0) * 0.24
    score += min(trending_score, 100.0) * 0.17
    score += min(popularity_score, 100.0) * 0.08

    if discount >= 70:
        score += 12.0
        _append_unique_reason(reasons, "Large discount")
    elif discount >= 50:
        score += 8.0
        _append_unique_reason(reasons, "Large discount")
    elif discount >= 30:
        score += 4.0

    near_historical_low = (
        historical_status in {"new_historical_low", "matches_historical_low", "near_historical_low"}
        or (price_vs_low_ratio > 0 and price_vs_low_ratio <= 1.08)
    )
    if near_historical_low:
        score += 10.0
        _append_unique_reason(reasons, "Near historical low")

    players_rising = (
        short_term_player_trend >= 0.06
        or player_growth_ratio >= 1.08
        or (momentum_score >= 58 and current_players >= 250)
    )
    if players_rising:
        score += 8.0
        _append_unique_reason(reasons, "Players rising")

    if discount >= 35 and popularity_score >= 65:
        score += 6.0
        _append_unique_reason(reasons, "Large discount on a popular game")

    similarity_bonus = 0.0
    overlap_count = 0
    if use_personal_context:
        similarity_bonus, overlap_count = _compute_personalization_similarity_bonus(snapshot, token_weights)
    if use_personal_context and similarity_bonus > 0:
        score += min(14.0, similarity_bonus * 0.7)
        if overlap_count >= 2 and deal_score >= 60:
            _append_unique_reason(reasons, "Similar to games you watch")
        elif overlap_count >= 1:
            _append_unique_reason(reasons, "Similar to your tracked games")

    recent_event_count = int(recent_event_counts.get(game_id, 0))
    if recent_event_count > 0:
        score += min(12.0, recent_event_count * 3.0)
        _append_unique_reason(reasons, "Fresh deal event")

    if deal_opportunity_score >= 70:
        _append_unique_reason(reasons, "Likely sale opportunity soon")
    if trending_score >= 62 and discount >= 20:
        _append_unique_reason(reasons, "Trending game with strong discount")
    elif trending_score >= 62:
        _append_unique_reason(reasons, "Trending game with rising activity")
    if deal_score >= 78 and not use_personal_context:
        _append_unique_reason(reasons, "High deal score")

    has_strong_signal = (
        (use_personal_context and (in_wishlist or in_watchlist or in_target_watch or recently_tracked))
        or recent_event_count > 0
        or discount >= 20
        or near_historical_low
        or players_rising
        or deal_score >= 52
        or deal_opportunity_score >= 55
        or trending_score >= 50
        or popularity_score >= 55
        or buy_score >= 58
    )
    if not has_strong_signal:
        return None

    normalized_score = _normalize_personalization_score(
        score,
        personalized_context=use_personal_context,
    )
    minimum_normalized_score = 26.0 if use_personal_context else 38.0
    if normalized_score < minimum_normalized_score:
        return None

    if not reasons:
        if use_personal_context:
            _append_unique_reason(reasons, "Relevant to your tracked games")
        else:
            _append_unique_reason(reasons, "Trending game with strong deal score")
    reason_lines = reasons[:2]
    explanation_text = " and ".join(reason_lines)
    confidence_badge = _build_deal_confidence_badge(buy_score if buy_score > 0 else deal_score)

    updated_at = _coerce_utc_datetime(snapshot.updated_at) or utc_now()
    return {
        "game_id": game_id,
        "id": game_id,
        "game_name": snapshot.game_name,
        "steam_appid": snapshot.steam_appid,
        "is_released": getattr(snapshot, "is_released", 1),
        "is_upcoming": getattr(snapshot, "is_upcoming", False),
        "release_date": getattr(snapshot, "release_date", None),
        "banner_url": snapshot.banner_url,
        "image_url": snapshot.banner_url,
        "store_url": snapshot.store_url,
        "price": snapshot.latest_price,
        "current_price": snapshot.latest_price,
        "original_price": snapshot.latest_original_price,
        "discount_percent": snapshot.latest_discount_percent,
        "historical_low": snapshot.historical_low,
        "historical_status": snapshot.historical_status,
        "price_vs_low_ratio": snapshot.price_vs_low_ratio,
        "deal_score": snapshot.deal_score,
        "deal_opportunity_score": snapshot.deal_opportunity_score,
        "buy_score": snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score,
        "buy_recommendation": snapshot.buy_recommendation,
        "buy_reason": snapshot.buy_reason,
        "popularity_score": snapshot.popularity_score,
        "momentum_score": snapshot.momentum_score,
        "trending_score": snapshot.trending_score,
        "player_growth_ratio": snapshot.player_growth_ratio,
        "short_term_player_trend": snapshot.short_term_player_trend,
        "current_players": snapshot.current_players,
        "confidence_badge": confidence_badge,
        "confidence_label": confidence_badge["confidence_label"] if confidence_badge else None,
        "confidence_icon": confidence_badge["confidence_icon"] if confidence_badge else None,
        "confidence_color": confidence_badge["confidence_color"] if confidence_badge else None,
        "confidence_class": confidence_badge["confidence_class"] if confidence_badge else None,
        "personalization_score": normalized_score,
        "personalization_reasons": reason_lines,
        "personalization_reason": explanation_text,
        "explanation_lines": reason_lines,
        "explanation_text": explanation_text,
        "updated_at": updated_at.isoformat(),
    }


def _normalize_seo_slug(value: str | None) -> str:
    return str(value or "").strip().lower()


def _get_seo_page_definition(slug: str) -> dict[str, str] | None:
    return SEO_DISCOVERY_PAGE_DEFINITIONS.get(_normalize_seo_slug(slug))


def _is_near_historical_low(snapshot: FeedProjectionRow) -> bool:
    historical_status = str(snapshot.historical_status or "").strip().lower()
    if historical_status in {"new_historical_low", "matches_historical_low", "near_historical_low"}:
        return True
    ratio = safe_num(snapshot.price_vs_low_ratio, 0.0)
    return ratio > 0 and ratio <= 1.08


def _has_rising_player_signal(snapshot: FeedProjectionRow) -> bool:
    return (
        safe_num(snapshot.short_term_player_trend, 0.0) >= 0.06
        or safe_num(snapshot.player_growth_ratio, 0.0) >= 1.08
        or (safe_num(snapshot.momentum_score, 0.0) >= 60 and safe_num(snapshot.current_players, 0.0) >= 300)
    )


def _build_seo_reason_lines(snapshot: FeedProjectionRow, slug: str, *, limit: int = 2) -> list[str]:
    reasons: list[str] = []
    normalized_slug = _normalize_seo_slug(slug)
    recommendation = _normalize_buy_recommendation(snapshot.buy_recommendation)
    discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
    popularity_score = safe_num(snapshot.popularity_score, 0.0)

    if _is_near_historical_low(snapshot):
        _append_unique_reason(reasons, "Near historical low")
    if recommendation == "BUY_NOW":
        _append_unique_reason(reasons, "Buy-now recommendation")
    elif recommendation == "WAIT":
        _append_unique_reason(reasons, "Wait recommendation")

    if discount >= 65:
        _append_unique_reason(reasons, "Strong discount")
    elif discount >= 35:
        _append_unique_reason(reasons, "Meaningful discount")

    if _has_rising_player_signal(snapshot):
        _append_unique_reason(reasons, "Players rising")

    if popularity_score >= 70 and discount >= 20:
        _append_unique_reason(reasons, "Popular game on sale")

    if normalized_slug == "under-10":
        _append_unique_reason(reasons, "Under $10 right now")
    elif normalized_slug == "under-20":
        _append_unique_reason(reasons, "Under $20 right now")
    elif normalized_slug == "popular-discounts":
        _append_unique_reason(reasons, "Popular game currently discounted")
    elif normalized_slug == "historical-lows":
        _append_unique_reason(reasons, "Historical low opportunity")
    elif normalized_slug == "wait-for-sale":
        _append_unique_reason(reasons, "Better future sale likely")

    for summary in (
        snapshot.buy_reason,
        snapshot.worth_buying_reason_summary,
        snapshot.trend_reason_summary,
        snapshot.deal_heat_reason,
        snapshot.predicted_sale_reason,
    ):
        if len(reasons) >= limit:
            break
        _append_unique_reason(reasons, _normalize_opportunity_reason(summary))

    if not reasons:
        _append_unique_reason(reasons, "Snapshot-backed deal signal")

    return reasons[: max(1, min(3, int(limit)))]


def _serialize_seo_landing_item(snapshot: FeedProjectionRow, slug: str) -> dict:
    explanation_lines = _build_seo_reason_lines(snapshot, slug, limit=2)
    updated_at = _coerce_utc_datetime(snapshot.updated_at) or utc_now()
    buy_score = snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score
    game_slug = _canonical_game_slug(snapshot.game_name, snapshot.game_id)

    return {
        "game_id": int(snapshot.game_id),
        "id": int(snapshot.game_id),
        "game_name": snapshot.game_name,
        "slug": game_slug,
        "game_slug": game_slug,
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
        "trending_score": snapshot.trending_score,
        "popularity_score": snapshot.popularity_score,
        "deal_score": snapshot.deal_score,
        "buy_score": buy_score,
        "buy_recommendation": snapshot.buy_recommendation,
        "buy_reason": snapshot.buy_reason,
        "predicted_next_sale_price": snapshot.predicted_next_sale_price,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "worth_buying_reason_summary": snapshot.worth_buying_reason_summary,
        "trend_reason_summary": snapshot.trend_reason_summary,
        "deal_heat_reason": snapshot.deal_heat_reason,
        "review_score": snapshot.review_score,
        "review_score_label": snapshot.review_score_label,
        "review_label": snapshot.review_score_label,
        "review_total_count": snapshot.review_count,
        "genres": parse_csv_field(snapshot.genres),
        "tags": parse_csv_field(snapshot.tags),
        "platforms": parse_csv_field(snapshot.platforms),
        "seo_reason_lines": explanation_lines,
        "explanation_lines": explanation_lines,
        "seo_reason": " and ".join(explanation_lines),
        "updated_at": updated_at.isoformat(),
    }


def _build_seo_discovery_query(session, slug: str, model_cls=GameDiscoveryFeed):
    normalized_slug = _normalize_seo_slug(slug)
    recommendation_expr = func.upper(func.coalesce(model_cls.buy_recommendation, ""))
    historical_priority = case(
        (model_cls.historical_status == "new_historical_low", 3),
        (model_cls.historical_status == "matches_historical_low", 2),
        (model_cls.historical_status == "near_historical_low", 1),
        else_=0,
    )
    near_low_predicate = or_(
        model_cls.historical_low_hit.is_(True),
        model_cls.historical_status.in_(["new_historical_low", "matches_historical_low", "near_historical_low"]),
        and_(model_cls.price_vs_low_ratio.isnot(None), model_cls.price_vs_low_ratio <= 1.08),
    )
    rising_players_predicate = or_(
        model_cls.short_term_player_trend >= 0.05,
        model_cls.player_growth_ratio >= 1.05,
        and_(
            model_cls.momentum_score >= 58,
            model_cls.current_players >= 250,
        ),
    )
    base_query = (
        session.query(model_cls)
        .filter(
            model_cls.is_released == 1,
            or_(model_cls.is_upcoming.is_(False), model_cls.is_upcoming.is_(None)),
            model_cls.latest_price.isnot(None),
        )
    )

    if normalized_slug == "best-deals":
        return (
            base_query
            .filter(
                or_(
                    model_cls.latest_discount_percent >= 20,
                    model_cls.deal_score >= 72,
                    recommendation_expr == "BUY_NOW",
                    near_low_predicate,
                )
            )
            .order_by(
                model_cls.deal_score.desc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.buy_score.desc().nullslast(),
                model_cls.worth_buying_score.desc().nullslast(),
                model_cls.popularity_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "historical-lows":
        return (
            base_query
            .filter(near_low_predicate)
            .order_by(
                historical_priority.desc(),
                model_cls.price_vs_low_ratio.asc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.deal_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "trending":
        return (
            base_query
            .filter(
                or_(
                    model_cls.trending_score >= 55,
                    model_cls.momentum_score >= 58,
                    rising_players_predicate,
                )
            )
            .order_by(
                model_cls.trending_score.desc().nullslast(),
                model_cls.momentum_score.desc().nullslast(),
                model_cls.current_players.desc().nullslast(),
                model_cls.deal_score.desc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "buy-now":
        return (
            base_query
            .filter(recommendation_expr == "BUY_NOW")
            .order_by(
                model_cls.buy_score.desc().nullslast(),
                model_cls.worth_buying_score.desc().nullslast(),
                model_cls.deal_score.desc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "wait-for-sale":
        return (
            base_query
            .filter(recommendation_expr == "WAIT")
            .order_by(
                model_cls.predicted_next_discount_percent.desc().nullslast(),
                model_cls.price_vs_low_ratio.desc().nullslast(),
                model_cls.popularity_score.desc().nullslast(),
                model_cls.deal_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "under-10":
        return (
            base_query
            .filter(
                model_cls.latest_price > 0,
                model_cls.latest_price <= 10,
            )
            .order_by(
                model_cls.deal_score.desc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.popularity_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "under-20":
        return (
            base_query
            .filter(
                model_cls.latest_price > 10,
                model_cls.latest_price <= 20,
            )
            .order_by(
                model_cls.deal_score.desc().nullslast(),
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.popularity_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    if normalized_slug == "popular-discounts":
        return (
            base_query
            .filter(
                model_cls.latest_discount_percent >= 20,
                or_(
                    model_cls.popularity_score >= 60,
                    model_cls.current_players >= 500,
                ),
            )
            .order_by(
                model_cls.latest_discount_percent.desc().nullslast(),
                model_cls.popularity_score.desc().nullslast(),
                model_cls.momentum_score.desc().nullslast(),
                model_cls.deal_score.desc().nullslast(),
                model_cls.game_id.asc(),
            )
        )
    return None


def _daily_digest_section_label(section_key: str) -> str:
    label_map = {
        "biggest_price_drops": "Biggest Price Drops",
        "new_historical_lows": "New Historical Lows",
        "buy_now_opportunities": "Buy-Now Opportunities",
        "trending_games": "Trending Games",
        "radar_signals": "Radar Signals",
    }
    return label_map.get(str(section_key or "").strip().lower(), "Daily Signal")


def _daily_digest_alert_priority(alert_type: str | None) -> int:
    normalized = str(alert_type or "").strip().upper()
    priority_map = {
        "NEW_HISTORICAL_LOW": 7,
        "PRICE_DROP": 6,
        "SALE_STARTED": 5,
        "PLAYER_SURGE": 4,
        "PRICE_TARGET_HIT": 3,
        "DISCOUNT_TARGET_HIT": 3,
    }
    return int(priority_map.get(normalized, 1))


def _build_daily_digest_reason_lines(
    snapshot: GameSnapshot,
    section_key: str,
    *,
    reason_hint: str | None = None,
    personalization_reasons: list[str] | None = None,
) -> list[str]:
    reasons: list[str] = []
    normalized_section = str(section_key or "").strip().lower()

    if normalized_section == "biggest_price_drops":
        _append_unique_reason(reasons, "Large price drop")
    elif normalized_section == "new_historical_lows":
        _append_unique_reason(reasons, "New historical low")
    elif normalized_section == "buy_now_opportunities":
        _append_unique_reason(reasons, "Buy-now recommendation")
    elif normalized_section == "trending_games":
        _append_unique_reason(reasons, "Players surging")
    elif normalized_section == "radar_signals":
        _append_unique_reason(reasons, "Fresh radar signal")

    _append_unique_reason(reasons, _normalize_opportunity_reason(reason_hint))
    for personalization_reason in personalization_reasons or []:
        _append_unique_reason(reasons, personalization_reason)
    for fallback_reason in _build_seo_reason_lines(snapshot, "best-deals", limit=3):
        _append_unique_reason(reasons, fallback_reason)

    if not reasons:
        reasons.append("Snapshot-backed daily signal")
    return reasons[:2]


def _build_daily_digest_item(
    snapshot: GameSnapshot,
    *,
    section_key: str,
    occurred_at: datetime.datetime | None = None,
    event_type: str | None = None,
    reason_hint: str | None = None,
    metadata: dict | None = None,
    personalization_reasons: list[str] | None = None,
    personalization_score: float = 0.0,
    priority_score: float = 0.0,
) -> dict:
    reason_lines = _build_daily_digest_reason_lines(
        snapshot,
        section_key,
        reason_hint=reason_hint,
        personalization_reasons=personalization_reasons,
    )
    updated_at = _coerce_utc_datetime(snapshot.updated_at) or utc_now()
    occurred_dt = _coerce_utc_datetime(occurred_at) or updated_at
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
        "trending_score": snapshot.trending_score,
        "popularity_score": snapshot.popularity_score,
        "deal_score": snapshot.deal_score,
        "buy_score": buy_score,
        "buy_recommendation": snapshot.buy_recommendation,
        "buy_reason": snapshot.buy_reason,
        "predicted_next_sale_price": snapshot.predicted_next_sale_price,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "worth_buying_reason_summary": snapshot.worth_buying_reason_summary,
        "trend_reason_summary": snapshot.trend_reason_summary,
        "deal_heat_reason": snapshot.deal_heat_reason,
        "review_score": snapshot.review_score,
        "review_score_label": snapshot.review_score_label,
        "review_label": snapshot.review_score_label,
        "review_total_count": snapshot.review_count,
        "genres": parse_csv_field(snapshot.genres),
        "tags": parse_csv_field(snapshot.tags),
        "platforms": parse_csv_field(snapshot.platforms),
        "section": section_key,
        "section_label": _daily_digest_section_label(section_key),
        "event_type": str(event_type or "").strip().upper() or None,
        "occurred_at": occurred_dt.isoformat(),
        "digest_reason_lines": reason_lines,
        "key_signal_explanation": reason_lines[0],
        "digest_reason": " and ".join(reason_lines),
        "personalization_score": round(safe_num(personalization_score, 0.0), 2),
        "priority_score": round(safe_num(priority_score, 0.0), 2),
        "metadata": metadata if isinstance(metadata, dict) else {},
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


def _first_non_null(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _canonical_game_slug(game_name: str | None, fallback_identifier: int | str | None = None) -> str | None:
    slug = _slugify_game_identifier(game_name)
    if slug:
        return slug
    try:
        fallback_value = int(safe_num(fallback_identifier, 0.0))
    except Exception:
        fallback_value = 0
    if fallback_value > 0:
        return str(fallback_value)
    return None


def _canonical_game_detail_path(game_name: str | None, fallback_identifier: int | str | None = None) -> str:
    slug = _canonical_game_slug(game_name, fallback_identifier=fallback_identifier)
    if slug:
        return f"/game/{slug}"
    try:
        fallback_value = int(safe_num(fallback_identifier, 0.0))
    except Exception:
        fallback_value = 0
    if fallback_value > 0:
        return f"/game/{fallback_value}"
    return "/game"


def _build_snapshot_game_detail_payload(
    game: Game,
    snapshot: Optional[GameSnapshot],
    latest: Optional[LatestGamePrice],
) -> dict:
    current_price = _first_non_null(
        snapshot.latest_price if snapshot else None,
        latest.latest_price if latest else None,
    )
    original_price = _first_non_null(
        snapshot.latest_original_price if snapshot else None,
        latest.original_price if latest else None,
    )
    discount_percent = _first_non_null(
        snapshot.latest_discount_percent if snapshot else None,
        latest.latest_discount_percent if latest else None,
    )
    current_players = _first_non_null(
        snapshot.current_players if snapshot else None,
        latest.current_players if latest else None,
    )
    historical_low_price = _first_non_null(
        snapshot.historical_low if snapshot else None,
        snapshot.historical_low_price if snapshot else None,
    )
    banner_url = (
        (snapshot.banner_url if snapshot else None)
        or build_steam_banner_url(game.store_url, game.appid)
    )
    steam_appid = (snapshot.steam_appid if snapshot else None) or game.appid
    steam_app_id = 0
    try:
        steam_app_id = int(steam_appid) if steam_appid is not None else 0
    except (TypeError, ValueError):
        steam_app_id = 0

    buy_score = (
        snapshot.buy_score
        if snapshot and snapshot.buy_score is not None
        else snapshot.worth_buying_score
        if snapshot
        else None
    )
    deal_score = _first_non_null(snapshot.deal_score if snapshot else None, buy_score)
    deal_label = f"Deal score {int(round(deal_score))}" if deal_score is not None else None

    deal_summary = (
        snapshot.worth_buying_reason_summary
        if snapshot and snapshot.worth_buying_reason_summary
        else snapshot.deal_heat_reason
        if snapshot and snapshot.deal_heat_reason
        else "Snapshot-derived market context."
    )

    worth_components = snapshot.worth_buying_components if snapshot and isinstance(snapshot.worth_buying_components, dict) else {}
    discount_strength = None
    historical_value = None
    review_quality = None
    player_interest = None
    sale_rarity = None
    if worth_components:
        discount_strength = round(min(30.0, (safe_num(worth_components.get("discount_component"), 0.0) / 42.0) * 30.0), 2)
        historical_value = round(min(25.0, (safe_num(worth_components.get("historical_low_component"), 0.0) / 16.0) * 25.0), 2)
        review_quality = round(min(20.0, (safe_num(worth_components.get("review_component"), 0.0) / 24.0) * 20.0), 2)
        player_blend = safe_num(worth_components.get("player_activity_component"), 0.0) + safe_num(
            worth_components.get("player_growth_component"), 0.0
        )
        player_interest = round(min(15.0, (player_blend / 30.0) * 15.0), 2)
        sale_rarity = round(min(10.0, max(0.0, safe_num(snapshot.max_discount if snapshot else 0, 0.0)) / 100.0 * 10.0), 2)

    payload = {
        "id": int(game.id),
        "game_id": int(game.id),
        "name": snapshot.game_name if snapshot and snapshot.game_name else game.name,
        "game_name": snapshot.game_name if snapshot and snapshot.game_name else game.name,
        "steam_appid": steam_appid,
        "steam_app_id": steam_app_id,
        "slug": _canonical_game_slug(
            snapshot.game_name if snapshot and snapshot.game_name else game.name,
            int(game.id),
        ),
        "store_url": game.store_url,
        "share_card_url": _build_canonical_url(f"/share/deal/{int(game.id)}"),
        "header_image": banner_url,
        "banner_image": banner_url,
        "banner_url": banner_url,
        "short_description": None,
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
        "review_score": (
            snapshot.review_score
            if snapshot and snapshot.review_score is not None
            else game.review_score
        ),
        "review_score_label": (
            snapshot.review_score_label if snapshot and snapshot.review_score_label else game.review_score_label
        ),
        "review_count": (
            snapshot.review_count
            if snapshot and snapshot.review_count is not None
            else game.review_total_count
        ),
        "review_total_count": (
            snapshot.review_count
            if snapshot and snapshot.review_count is not None
            else game.review_total_count
        ),
        "tags": parse_csv_field(_first_non_null(snapshot.tags if snapshot else None, game.tags)),
        "genres": parse_csv_field(_first_non_null(snapshot.genres if snapshot else None, game.genres)),
        "platforms": parse_csv_field(_first_non_null(snapshot.platforms if snapshot else None, game.platforms)),
        "price": current_price,
        "current_price": current_price,
        "original_price": original_price,
        "discount_percent": discount_percent,
        "current_players": current_players,
        "historical_low": historical_low_price,
        "historical_low_price": historical_low_price,
        "historical_low_date": (
            snapshot.historical_low_timestamp.isoformat()
            if snapshot and snapshot.historical_low_timestamp
            else None
        ),
        "historical_status": snapshot.historical_status if snapshot else None,
        "deal_score": deal_score,
        "deal_label": deal_label,
        "deal_summary": deal_summary,
        "wishlist_count": None,
        "watchlisted": False,
        "market_insights": {
            "historical_low_price": historical_low_price,
            "historical_low_date": (
                snapshot.historical_low_timestamp.isoformat()
                if snapshot and snapshot.historical_low_timestamp
                else None
            ),
            "avg_discount_percent": None,
            "max_discount_percent": snapshot.max_discount if snapshot else None,
            "sale_event_count": None,
            "days_since_last_sale": None,
            "latest_player_count": current_players,
        },
        "prediction": {},
        "deal_explanation": {
            "discount_strength": discount_strength,
            "historical_value": historical_value,
            "review_quality": review_quality,
            "player_interest": player_interest,
            "sale_rarity": sale_rarity,
            "summary": deal_summary,
        },
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
        "deal_opportunity_score": snapshot.deal_opportunity_score if snapshot else None,
        "deal_opportunity_reason": snapshot.deal_opportunity_reason if snapshot else None,
        "deal_opportunity": {
            "score": snapshot.deal_opportunity_score if snapshot else None,
            "reason": snapshot.deal_opportunity_reason if snapshot else None,
        },
        "next_sale_prediction": {
            "expected_next_price": snapshot.predicted_next_sale_price if snapshot else None,
            "expected_next_discount_percent": snapshot.predicted_next_discount_percent if snapshot else None,
            "estimated_window_days_min": snapshot.predicted_next_sale_window_days_min if snapshot else None,
            "estimated_window_days_max": snapshot.predicted_next_sale_window_days_max if snapshot else None,
            "confidence": snapshot.predicted_sale_confidence if snapshot else None,
            "reason": snapshot.predicted_sale_reason if snapshot else None,
        },
        "worth_buying_reason_summary": snapshot.worth_buying_reason_summary if snapshot else None,
        "worth_buying": {
            "score": buy_score,
            "version": snapshot.worth_buying_score_version if snapshot else None,
            "reason": snapshot.worth_buying_reason_summary if snapshot else None,
            "components": snapshot.worth_buying_components if snapshot and snapshot.worth_buying_components else {},
        },
        "momentum": {
            "score": snapshot.momentum_score if snapshot else None,
            "version": snapshot.momentum_score_version if snapshot else None,
            "player_growth_ratio": snapshot.player_growth_ratio if snapshot else None,
            "short_term_player_trend": snapshot.short_term_player_trend if snapshot else None,
            "reason": snapshot.trend_reason_summary if snapshot else None,
        },
        "historical_low_radar": {
            "hit": bool(snapshot.historical_low_hit) if snapshot else False,
            "historical_low_price": snapshot.historical_low_price if snapshot else None,
            "previous_historical_low_price": snapshot.previous_historical_low_price if snapshot else None,
            "historical_low_timestamp": (
                snapshot.historical_low_timestamp.isoformat()
                if snapshot and snapshot.historical_low_timestamp
                else None
            ),
            "reason": snapshot.historical_low_reason_summary if snapshot else None,
        },
        "deal_heat": {
            "level": snapshot.deal_heat_level if snapshot else None,
            "reason": snapshot.deal_heat_reason if snapshot else None,
            "tags": snapshot.deal_heat_tags if snapshot and snapshot.deal_heat_tags else [],
        },
        "deal_heat_reason": snapshot.deal_heat_reason if snapshot else None,
        "deal_heat_level": snapshot.deal_heat_level if snapshot else None,
        "momentum_score": snapshot.momentum_score if snapshot else None,
        "trend_reason_summary": snapshot.trend_reason_summary if snapshot else None,
        "ranking_explanations": snapshot.ranking_explanations if snapshot and snapshot.ranking_explanations else {},
        "share_card": {
            "title": snapshot.game_name if snapshot and snapshot.game_name else game.name,
            "cover": banner_url,
            "image_url": _build_canonical_url(f"/share/deal/{int(game.id)}"),
            "current_price": current_price,
            "original_price": original_price,
            "discount_percent": discount_percent,
            "heat_reason": snapshot.deal_heat_reason if snapshot else None,
            "heat_level": snapshot.deal_heat_level if snapshot else None,
            "historical_low_hit": bool(snapshot.historical_low_hit) if snapshot else False,
            "momentum_score": snapshot.momentum_score if snapshot else None,
        },
    }
    return payload


def _ensure_game_detail_contract(payload: dict) -> dict:
    normalized = dict(payload or {})
    normalized["id"] = int(normalized.get("id") or normalized.get("game_id") or 0)
    normalized["game_id"] = int(normalized.get("game_id") or normalized.get("id") or 0)
    fallback_name = f"Game {normalized['game_id']}" if normalized["game_id"] > 0 else "Unknown game"
    normalized["game_name"] = normalized.get("game_name") or normalized.get("name") or fallback_name
    normalized["name"] = normalized.get("name") or normalized["game_name"]
    canonical_slug = _canonical_game_slug(normalized.get("slug") or normalized["game_name"], normalized["game_id"])
    normalized["slug"] = canonical_slug
    normalized["game_slug"] = canonical_slug
    canonical_path = _canonical_game_detail_path(normalized.get("game_name"), normalized["game_id"])
    normalized["canonical_path"] = canonical_path
    normalized["canonical_url"] = _build_canonical_url(canonical_path)
    normalized["price"] = _first_non_null(normalized.get("price"), normalized.get("current_price"))
    normalized["current_price"] = _first_non_null(normalized.get("current_price"), normalized.get("price"))
    normalized["historical_low"] = _first_non_null(normalized.get("historical_low"), normalized.get("historical_low_price"))
    normalized["historical_low_price"] = _first_non_null(
        normalized.get("historical_low_price"),
        normalized.get("historical_low"),
    )
    normalized.setdefault("prediction", {})
    if not isinstance(normalized.get("prediction"), dict):
        normalized["prediction"] = {}
    normalized.setdefault("deal_explanation", {})
    if not isinstance(normalized.get("deal_explanation"), dict):
        normalized["deal_explanation"] = {}
    normalized.setdefault("deal_opportunity", {"score": None, "reason": None})
    if not isinstance(normalized.get("deal_opportunity"), dict):
        normalized["deal_opportunity"] = {"score": None, "reason": None}
    normalized.setdefault(
        "next_sale_prediction",
        {
            "expected_next_price": None,
            "expected_next_discount_percent": None,
            "estimated_window_days_min": None,
            "estimated_window_days_max": None,
            "confidence": None,
            "reason": None,
        },
    )
    if not isinstance(normalized.get("next_sale_prediction"), dict):
        normalized["next_sale_prediction"] = {
            "expected_next_price": None,
            "expected_next_discount_percent": None,
            "estimated_window_days_min": None,
            "estimated_window_days_max": None,
            "confidence": None,
            "reason": None,
        }
    normalized.setdefault("market_insights", {})
    if not isinstance(normalized.get("market_insights"), dict):
        normalized["market_insights"] = {}
    if normalized["game_id"] > 0:
        normalized["share_card_url"] = normalized.get("share_card_url") or _build_canonical_url(
            f"/share/deal/{normalized['game_id']}"
        )
    else:
        normalized["share_card_url"] = normalized.get("share_card_url")
    return normalized


def build_game_detail_payload(session, game: Game, user_id: str | None = None):
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
    watchlisted = _is_game_watchlisted_for_user(session, int(game.id), user_id)
    wishlist_count = session.query(func.count(WishlistItem.id)).filter(WishlistItem.game_id == game.id).scalar() or 0

    return {
        "id": game.id,
        "steam_app_id": appid or 0,
        "name": game.name,
        "slug": _canonical_game_slug(game.name, game.id),
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
                "src": "/favicon.ico",
                "sizes": "any",
                "type": "image/x-icon",
            }
        ],
    }
    return Response(
        content=json.dumps(manifest_payload, ensure_ascii=False),
        media_type="application/manifest+json",
    )


def _collect_sitemap_game_paths(limit: int = SITEMAP_GAME_DETAIL_LIMIT) -> list[str]:
    session = ReadSessionLocal()
    try:
        rows = (
            session.query(GameSnapshot.game_id, GameSnapshot.game_name)
            .filter(
                GameSnapshot.game_id.isnot(None),
                GameSnapshot.is_released == 1,
                GameSnapshot.is_upcoming.is_(False),
            )
            .order_by(
                case((GameSnapshot.popularity_score.is_(None), 1), else_=0).asc(),
                GameSnapshot.popularity_score.desc(),
                GameSnapshot.updated_at.desc(),
                GameSnapshot.game_id.asc(),
            )
            .limit(max(1, int(limit)))
            .all()
        )
        paths: list[str] = []
        seen_paths: set[str] = set()
        for row in rows:
            game_id = int(safe_num(getattr(row, "game_id", 0), 0.0))
            if game_id <= 0:
                continue
            game_name = str(getattr(row, "game_name", "") or "").strip()
            path = _canonical_game_detail_path(game_name, fallback_identifier=game_id)
            if path in seen_paths:
                continue
            seen_paths.add(path)
            paths.append(path)
        return paths
    finally:
        session.close()


@app.get("/sitemap.xml", include_in_schema=False)
def sitemap_xml():
    today = utc_now().date().isoformat()
    ordered_paths: list[str] = []
    seen_paths: set[str] = set()
    for path in (*SITEMAP_STATIC_PATHS, *_collect_sitemap_game_paths()):
        normalized = str(path or "").strip()
        if not normalized:
            continue
        if not normalized.startswith("/"):
            normalized = f"/{normalized}"
        if normalized in seen_paths:
            continue
        seen_paths.add(normalized)
        ordered_paths.append(normalized)

    urls = []
    for path in ordered_paths:
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


@app.get("/all-results")
def all_results_page():
    return FileResponse("web/all-results.html")


@app.get("/game")
@app.get("/game/")
def game_page():
    return FileResponse("web/game.html")


@app.get("/game/{identifier}")
@app.get("/game/{identifier}/")
def game_page_with_identifier(identifier: str):
    if not str(identifier or "").strip():
        raise HTTPException(status_code=404, detail="Game page not found")
    return FileResponse("web/game.html")


@app.get("/history")
def history_page():
    return FileResponse("web/history.html")


@app.get("/game-detail")
def game_detail_page():
    return FileResponse("web/game-detail.html")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse("web/favicon.ico")


@app.get("/share/deal/{game_id}", include_in_schema=False)
def share_deal_card(game_id: int):
    started = _start_timer()
    normalized_game_id = int(safe_num(game_id, 0.0))
    if normalized_game_id <= 0:
        raise HTTPException(status_code=404, detail="Game snapshot not found")

    session = ReadSessionLocal()
    try:
        snapshot = (
            session.query(GameSnapshot)
            .filter(GameSnapshot.game_id == normalized_game_id)
            .first()
        )
        if snapshot is None:
            raise HTTPException(status_code=404, detail="Game snapshot not found")

        svg_payload = _build_share_deal_svg(snapshot, normalized_game_id)
        return Response(
            content=svg_payload,
            media_type="image/svg+xml",
            headers={
                "Cache-Control": "public, max-age=300, s-maxage=900",
            },
        )
    finally:
        session.close()
        _log_timing("/share/deal", started)


def _serve_all_results_page():
    return FileResponse("web/all-results.html")


@app.get("/best-deals")
def best_deals_page():
    return _serve_all_results_page()


@app.get("/historical-lows")
def historical_lows_page():
    return _serve_all_results_page()


@app.get("/trending")
def trending_page():
    return _serve_all_results_page()


@app.get("/buy-now")
def buy_now_page():
    return _serve_all_results_page()


@app.get("/wait-for-sale")
def wait_for_sale_page():
    return _serve_all_results_page()


@app.get("/under-10")
def under_ten_page():
    return _serve_all_results_page()


@app.get("/under-20")
def under_twenty_page():
    return _serve_all_results_page()


@app.get("/popular-discounts")
def popular_discounts_page():
    return _serve_all_results_page()


@app.get("/worth-buying-now")
def worth_buying_now_page():
    return RedirectResponse(url="/buy-now", status_code=308)


@app.get("/trending-deals")
def trending_deals_page():
    return RedirectResponse(url="/trending", status_code=308)


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
            CRITICAL_DASHBOARD_CACHE_KEY,
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
    session = ReadSessionLocal()

    try:
        cached_rows = _read_cached_section_items(session, "home:historical_lows", limit=limit)
        if cached_rows:
            return _filter_valid_deal_payload_rows(cached_rows, limit=limit)

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

        return _filter_valid_deal_payload_rows(rows, limit=limit)

    finally:
        session.close()


@app.get("/sales/seasonal-summary")
@json_etag()
@ttl_cache(ttl_seconds=90, endpoint_key="/sales/seasonal-summary")
def get_seasonal_summary(limit: int = Query(default=12, ge=1, le=30)):
    session = ReadSessionLocal()

    try:
        cached_items = _read_cached_section_items(session, SEASONAL_SUMMARY_CACHE_KEY, limit=limit)
        _, cached_section_payload = _read_cache_payload(session, SEASONAL_SUMMARY_CACHE_KEY)
        if isinstance(cached_section_payload, dict):
            cached_summary = _limit_seasonal_summary_payload(cached_section_payload, limit)
            if cached_summary.get("sale_event") or cached_items:
                return cached_summary
        _, home_payload = _read_dashboard_cache(session)
        if isinstance(home_payload, dict) and isinstance(home_payload.get("seasonal_summary"), dict):
            cached_summary = _limit_seasonal_summary_payload(home_payload.get("seasonal_summary") or {}, limit)
            if cached_summary.get("sale_event") or cached_summary.get("expected_games"):
                return cached_summary

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
    session = ReadSessionLocal()

    try:
        cached_rows = _read_cached_section_items(session, "home:biggest_discounts", limit=limit)
        if cached_rows:
            return _filter_valid_deal_payload_rows(cached_rows, limit=limit)

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

        serialized_rows = [serialize_price_row(row, game_map, insight_map) for row in discounted_rows[:limit]]
        return _filter_valid_deal_payload_rows(serialized_rows, limit=limit)

    finally:
        session.close()


@app.get("/games/top-reviewed")
def get_top_reviewed_games(limit: int = Query(default=20, ge=1, le=100)):
    session = ReadSessionLocal()

    try:
        cached_rows = _read_cached_section_items(session, TOP_REVIEWED_CACHE_KEY, limit=limit)
        if cached_rows:
            return cached_rows

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
    session = ReadSessionLocal()

    try:
        cached_rows = _read_cached_section_items(session, TOP_PLAYED_CACHE_KEY, limit=limit)
        if cached_rows:
            return cached_rows

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
    session = ReadSessionLocal()

    try:
        cached_rows = _read_cached_section_items(session, LEADERBOARD_CACHE_KEY, limit=limit)
        if cached_rows:
            return cached_rows

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

        normalized_query = _normalize_search_text(query_text)
        normalized_limit = max(1, min(int(limit), 20))
        query_tokens = _search_tokens(normalized_query)
        tokenized_query = "%".join(query_tokens) if len(query_tokens) > 1 else ""
        candidate_limit = min(30, max(12, normalized_limit * 2))
        if len(normalized_query) <= 2:
            candidate_limit = min(24, max(10, normalized_limit * 2))
        rows = []

        if len(normalized_query) <= 2:
            rows = session.execute(
                text(
                    """
                    SELECT
                        g.id,
                        g.name AS game_name,
                        g.developer,
                        g.publisher,
                        COALESCE(s.genres, g.genres, '') AS genres_csv,
                        COALESCE(s.tags, g.tags, '') AS tags_csv,
                        s.steam_appid,
                        COALESCE(s.banner_url, 'https://cdn.cloudflare.steamstatic.com/steam/apps/' || g.appid || '/header.jpg') AS image_url,
                        s.latest_price,
                        s.latest_discount_percent,
                        s.deal_score,
                        COALESCE(s.popularity_score, 0) AS popularity_score,
                        COALESCE(s.buy_score, s.worth_buying_score) AS buy_score,
                        s.worth_buying_score,
                        COALESCE(s.review_score_label, g.review_score_label) AS review_score_label,
                        COALESCE(s.review_score, g.review_score) AS review_score,
                        COALESCE(s.review_count, g.review_total_count) AS review_total_count,
                        s.deal_heat_reason,
                        s.release_date,
                        s.is_upcoming,
                        0.0 AS sim
                    FROM games g
                    LEFT JOIN game_snapshots s ON s.game_id = g.id
                    WHERE
                        lower(g.name) LIKE (:normalized_q || '%')
                        OR lower(g.name) LIKE ('%' || :normalized_q || '%')
                        OR (:tokenized_q <> '' AND lower(g.name) LIKE ('%' || :tokenized_q || '%'))
                    ORDER BY
                        CASE WHEN lower(g.name) = :normalized_q THEN 0 ELSE 1 END,
                        CASE WHEN lower(g.name) LIKE (:normalized_q || '%') THEN 0 ELSE 1 END,
                        COALESCE(s.popularity_score, 0) DESC,
                        COALESCE(s.upcoming_hot_score, 0) DESC,
                        COALESCE(s.deal_score, 0) DESC,
                        g.name ASC
                    LIMIT :limit
                    """
                ),
                {
                    "normalized_q": normalized_query,
                    "tokenized_q": tokenized_query,
                    "limit": candidate_limit,
                },
            ).mappings().all()

        if not rows:
            try:
                rows = session.execute(
                    text(
                        """
                        SELECT
                            g.id,
                            g.name AS game_name,
                            g.developer,
                            g.publisher,
                            COALESCE(s.genres, g.genres, '') AS genres_csv,
                            COALESCE(s.tags, g.tags, '') AS tags_csv,
                            s.steam_appid,
                            COALESCE(s.banner_url, 'https://cdn.cloudflare.steamstatic.com/steam/apps/' || g.appid || '/header.jpg') AS image_url,
                            s.latest_price,
                            s.latest_discount_percent,
                            s.deal_score,
                            COALESCE(s.popularity_score, 0) AS popularity_score,
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
                            OR (:tokenized_q <> '' AND lower(g.name) LIKE ('%' || :tokenized_q || '%'))
                            OR COALESCE(g.developer, '') ILIKE ('%' || :q || '%')
                            OR COALESCE(g.publisher, '') ILIKE ('%' || :q || '%')
                            OR COALESCE(s.genres, COALESCE(g.genres, '')) ILIKE ('%' || :q || '%')
                            OR COALESCE(s.tags, COALESCE(g.tags, '')) ILIKE ('%' || :q || '%')
                            OR similarity(lower(g.name), :normalized_q) > :sim_threshold
                        ORDER BY
                            CASE WHEN lower(g.name) = :normalized_q THEN 0 ELSE 1 END,
                            CASE WHEN lower(g.name) LIKE (:normalized_q || '%') THEN 0 ELSE 1 END,
                            sim DESC,
                            CASE WHEN lower(COALESCE(s.genres, COALESCE(g.genres, ''))) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            CASE WHEN lower(COALESCE(s.tags, COALESCE(g.tags, ''))) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            CASE WHEN lower(g.name) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            COALESCE(s.deal_score, 0) DESC,
                            g.name ASC
                        LIMIT :limit
                        """
                    ),
                    {
                        "q": query_text,
                        "normalized_q": normalized_query,
                        "tokenized_q": tokenized_query,
                        "sim_threshold": SEARCH_SIMILARITY_THRESHOLD,
                        "limit": candidate_limit,
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
                            COALESCE(s.genres, g.genres, '') AS genres_csv,
                            COALESCE(s.tags, g.tags, '') AS tags_csv,
                            s.steam_appid,
                            COALESCE(s.banner_url, 'https://cdn.cloudflare.steamstatic.com/steam/apps/' || g.appid || '/header.jpg') AS image_url,
                            s.latest_price,
                            s.latest_discount_percent,
                            s.deal_score,
                            COALESCE(s.popularity_score, 0) AS popularity_score,
                            COALESCE(s.buy_score, s.worth_buying_score) AS buy_score,
                            s.worth_buying_score,
                            COALESCE(s.review_score_label, g.review_score_label) AS review_score_label,
                            COALESCE(s.review_score, g.review_score) AS review_score,
                            COALESCE(s.review_count, g.review_total_count) AS review_total_count,
                            s.deal_heat_reason,
                            s.release_date,
                            s.is_upcoming,
                            0.0 AS sim
                        FROM games g
                        LEFT JOIN game_snapshots s ON s.game_id = g.id
                        WHERE
                            g.name ILIKE ('%' || :q || '%')
                            OR (:tokenized_q <> '' AND lower(g.name) LIKE ('%' || :tokenized_q || '%'))
                            OR COALESCE(g.developer, '') ILIKE ('%' || :q || '%')
                            OR COALESCE(g.publisher, '') ILIKE ('%' || :q || '%')
                            OR COALESCE(s.genres, COALESCE(g.genres, '')) ILIKE ('%' || :q || '%')
                            OR COALESCE(s.tags, COALESCE(g.tags, '')) ILIKE ('%' || :q || '%')
                        ORDER BY
                            CASE WHEN lower(g.name) = :normalized_q THEN 0 ELSE 1 END,
                            CASE WHEN lower(g.name) LIKE (:normalized_q || '%') THEN 0 ELSE 1 END,
                            CASE WHEN lower(COALESCE(s.genres, COALESCE(g.genres, ''))) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            CASE WHEN lower(COALESCE(s.tags, COALESCE(g.tags, ''))) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            CASE WHEN lower(g.name) LIKE ('%' || :normalized_q || '%') THEN 0 ELSE 1 END,
                            COALESCE(s.deal_score, 0) DESC,
                            g.name ASC
                        LIMIT :limit
                        """
                    ),
                    {
                        "q": query_text,
                        "normalized_q": normalized_query,
                        "tokenized_q": tokenized_query,
                        "limit": candidate_limit,
                    },
                ).mappings().all()

        ranked_rows = _rank_search_rows(rows, normalized_query, normalized_limit)
        return [
            {
                "id": row["id"],
                "game_id": row["id"],
                "game_name": row["game_name"],
                "slug": _canonical_game_slug(row["game_name"], row["id"]),
                "game_slug": _canonical_game_slug(row["game_name"], row["id"]),
                "developer": row.get("developer"),
                "publisher": row.get("publisher"),
                "genres": parse_csv_field(row.get("genres_csv")),
                "tags": parse_csv_field(row.get("tags_csv")),
                "steam_appid": row["steam_appid"],
                "banner_url": row["image_url"],
                "image_url": row["image_url"],
                "price": row["latest_price"],
                "latest_price": row["latest_price"],
                "discount_percent": row["latest_discount_percent"],
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
            for row in ranked_rows
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
def get_upcoming_games(
    limit: int = Query(default=24, ge=1, le=250),
    full: bool = Query(default=False),
):
    session = ReadSessionLocal()

    try:
        normalized_limit = max(1, min(int(limit), 250))
        if not full:
            cached_rows = _read_cached_section_items(session, "home:upcoming", limit=normalized_limit)
            if cached_rows:
                return cached_rows

        artwork_priority = case((GameSnapshot.banner_url.isnot(None), 1), else_=0)
        release_date_priority = case((GameSnapshot.release_date.is_(None), 1), else_=0)
        snapshot_query = (
            session.query(GameSnapshot)
            .filter(GameSnapshot.is_upcoming.is_(True))
            .order_by(
                artwork_priority.desc(),
                GameSnapshot.upcoming_hot_score.desc(),
                release_date_priority.asc(),
                GameSnapshot.release_date.asc(),
                GameSnapshot.game_name.asc(),
                GameSnapshot.game_id.asc(),
            )
        )
        if not full:
            snapshot_query = snapshot_query.limit(normalized_limit)
        snapshot_rows = snapshot_query.all()
        if snapshot_rows:
            return [serialize_upcoming_snapshot_row(row) for row in snapshot_rows]

        rows = session.query(Game).filter(Game.is_released == 0).all()

        rows.sort(
            key=lambda row: (
                parse_release_date_sort_key(row.release_date_text),
                row.name.lower(),
            )
        )

        serialized_rows = [serialize_upcoming_row(row) for row in rows]
        if full:
            return serialized_rows
        return serialized_rows[:normalized_limit]

    finally:
        session.close()


@app.get("/dashboard/catalog-seed")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/dashboard/catalog-seed")
def get_dashboard_catalog_seed(
    request: Request,
    limit: int = Query(default=24, ge=1, le=60),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        normalized_limit = max(1, min(int(limit), 60))
        row, payload = _read_cache_payload(session, CATALOG_SEED_CACHE_KEY)
        if row is not None and isinstance(payload, dict):
            items = payload.get("items")
            if isinstance(items, list) and items:
                bounded_items = [item for item in items if isinstance(item, dict)][:normalized_limit]
                response_payload = {
                    "items": bounded_items,
                    "total": max(len(bounded_items), int(payload.get("total") or 0)),
                    "total_pages": 1,
                    "generated_at": payload.get("generated_at"),
                }
                _log_timing("/dashboard/catalog-seed", started)
                return response_payload

        _, home_payload = _read_dashboard_cache(session)
        fallback_rows: list[dict] = []
        if isinstance(home_payload, dict):
            home_payload = _augment_dashboard_home_payload(home_payload)
            for key in (
                "dealRanked",
                "topDealsToday",
                "worth_buying_now",
                "worthBuyingNow",
                "biggest_discounts",
                "biggestDeals",
                "trending_now",
                "trendingDeals",
                "trending",
                "topReviewed",
                "topPlayed",
            ):
                candidate_rows = home_payload.get(key)
                if isinstance(candidate_rows, list):
                    fallback_rows.extend(candidate_rows)
        fallback_items = _dedupe_dashboard_rows(fallback_rows)[:normalized_limit]
        response_payload = {
            "items": fallback_items,
            "total": len(fallback_items),
            "total_pages": 1,
            "generated_at": home_payload.get("generated_at") if isinstance(home_payload, dict) else None,
        }
        _log_timing("/dashboard/catalog-seed", started)
        return response_payload
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
@json_etag()
@ttl_cache(ttl_seconds=300, endpoint_key="/games/filters")
def get_filters():
    session = ReadSessionLocal()

    try:
        _, cached_dashboard_payload = _read_dashboard_cache(session)
        if isinstance(cached_dashboard_payload, dict):
            cached_filters = cached_dashboard_payload.get("filters")
            if isinstance(cached_filters, dict):
                cached_result = {
                    "genres": sorted({str(value).strip() for value in cached_filters.get("genres", []) if str(value).strip()}),
                    "tags": sorted({str(value).strip() for value in cached_filters.get("tags", []) if str(value).strip()}),
                    "platforms": _extend_platform_filter_options(
                        sorted({str(value).strip() for value in cached_filters.get("platforms", []) if str(value).strip()})
                    ),
                    "review_labels": sorted({str(value).strip() for value in cached_filters.get("review_labels", []) if str(value).strip()}),
                }
                if (
                    cached_result["genres"]
                    or cached_result["tags"]
                    or cached_result["platforms"]
                    or cached_result["review_labels"]
                ):
                    return cached_result

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


def _read_cached_section_items(session: Session, cache_key: str, *, limit: int | None = None) -> list[dict]:
    _, payload = _read_cache_payload(session, cache_key)
    rows: list[dict] = []
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            rows = [_attach_dashboard_row_route_fields(row) for row in items if isinstance(row, dict)]
    elif isinstance(payload, list):
        rows = [_attach_dashboard_row_route_fields(row) for row in payload if isinstance(row, dict)]
    if limit is not None:
        return rows[: max(1, int(limit))]
    return rows


def _limit_seasonal_summary_payload(payload: dict, limit: int) -> dict:
    if not isinstance(payload, dict):
        return {}
    bounded_limit = max(1, int(limit))
    items = payload.get("items")
    if not isinstance(items, list):
        items = payload.get("expected_games")
    if not isinstance(items, list):
        items = []
    trimmed_items = [row for row in items if isinstance(row, dict)][:bounded_limit]
    normalized = dict(payload)
    normalized["items"] = trimmed_items
    normalized["expected_games"] = trimmed_items
    return normalized


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


def _read_dashboard_cache(session, *, mode: str | None = None):
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode == "critical":
        row, payload = _read_cache_payload(session, CRITICAL_DASHBOARD_CACHE_KEY)
        if row is not None:
            return row, payload
    for cache_key in _dashboard_cache_keys():
        row, payload = _read_cache_payload(session, cache_key)
        if row is not None:
            return row, payload
    return None, None


def _upsert_dashboard_cache_payload(cache_key: str, payload: dict, *, updated_at: datetime.datetime | None = None) -> None:
    write_session = Session()
    try:
        now = updated_at or utc_now()
        payload_json = json.dumps(payload, ensure_ascii=False)
        row = write_session.query(DashboardCache).filter(DashboardCache.cache_key == cache_key).first()
        if row is None:
            write_session.add(DashboardCache(cache_key=cache_key, payload=payload_json, updated_at=now))
        else:
            row.payload = payload_json
            row.updated_at = now
        write_session.commit()
    except Exception:
        write_session.rollback()
        logger.exception("Failed to upsert dashboard cache key=%s", cache_key)
    finally:
        write_session.close()


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


def _attach_dashboard_row_route_fields(row: dict) -> dict:
    normalized = dict(row or {})
    game_id = int(safe_num(normalized.get("game_id") or normalized.get("id"), 0.0))
    game_name = str(normalized.get("game_name") or normalized.get("name") or "").strip()
    raw_slug = normalized.get("slug") or normalized.get("game_slug")
    game_slug = _canonical_game_slug(raw_slug or game_name, fallback_identifier=game_id)
    if game_slug:
        normalized["slug"] = game_slug
        normalized["game_slug"] = game_slug
    if game_id > 0 and "game_id" not in normalized:
        normalized["game_id"] = game_id
    if game_id > 0 and "id" not in normalized:
        normalized["id"] = game_id
    return normalized


def _dashboard_rows(payload: dict, *keys: str) -> list[dict]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [_attach_dashboard_row_route_fields(row) for row in value if isinstance(row, dict)]
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


def _is_released_dashboard_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if bool(row.get("is_upcoming")):
        return False
    released_value = row.get("is_released")
    if released_value is None:
        return True
    try:
        return int(released_value) == 1
    except Exception:
        return bool(released_value)


def _dashboard_row_has_actual_sale(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    price = safe_num(row.get("price"), safe_num(row.get("latest_price"), 0.0))
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return price > 0 and discount > 0


def _released_dashboard_rows(rows: list[dict]) -> list[dict]:
    return [row for row in _dedupe_dashboard_rows(rows) if _is_released_dashboard_row(row)]


def _released_deal_dashboard_rows(rows: list[dict]) -> list[dict]:
    return [
        row
        for row in _dedupe_dashboard_rows(rows)
        if _is_released_dashboard_row(row) and _dashboard_row_has_actual_sale(row)
    ]


def _compose_unique_dashboard_rows(
    primary_rows: list[dict],
    fallback_rows: list[dict],
    blocked_keys: set[str],
    limit: int,
) -> list[dict]:
    bounded_limit = max(1, int(limit))
    selected: list[dict] = []
    seen_keys: set[str] = set()
    for source_rows in (primary_rows, fallback_rows):
        for idx, row in enumerate(_dedupe_dashboard_rows(source_rows)):
            if not isinstance(row, dict):
                continue
            row_key = _dashboard_identity_key(row, idx)
            if not row_key or row_key in seen_keys or row_key in blocked_keys:
                continue
            selected.append(row)
            seen_keys.add(row_key)
            blocked_keys.add(row_key)
            if len(selected) >= bounded_limit:
                return selected
    return selected


def _is_wait_dashboard_candidate(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if _normalize_buy_recommendation(row.get("buy_recommendation")) == "WAIT":
        return True
    return (
        safe_num(row.get("price_vs_low_ratio"), 0.0) >= 1.08
        or safe_num(row.get("predicted_next_discount_percent"), 0.0) >= 35
    )


def _is_valid_deal_payload_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if bool(row.get("is_upcoming")):
        return False
    released_value = row.get("is_released")
    if released_value is not None:
        try:
            if int(released_value) != 1:
                return False
        except Exception:
            if not bool(released_value):
                return False
    price = safe_num(row.get("price"), safe_num(row.get("latest_price"), 0.0))
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return price > 0 and discount > 0


def _filter_valid_deal_payload_rows(rows: list[dict], limit: int | None = None) -> list[dict]:
    filtered = [row for row in _dedupe_dashboard_rows(rows) if _is_valid_deal_payload_row(row)]
    if limit is None:
        return filtered
    return filtered[: max(1, int(limit))]


def _dashboard_sort_game_id(row: dict) -> int:
    return int(safe_num((row or {}).get("game_id") or (row or {}).get("id"), 0.0))


def _score_dashboard_opportunity_row(row: dict) -> float:
    buy_score = safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0))
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return (
        safe_num(row.get("deal_opportunity_score"), 0.0) * 0.9
        + buy_score * 0.45
        + safe_num(row.get("deal_score"), 0.0) * 0.35
        + safe_num(row.get("momentum_score"), 0.0) * 0.2
        + safe_num(row.get("trending_score"), 0.0) * 0.15
        + discount * 0.12
    )


def _allocate_protected_dashboard_deal_rails(
    candidate_pool: list[dict],
    limit: int,
) -> tuple[dict[str, list[dict]], set[str]]:
    eligible_pool = _released_deal_dashboard_rows(candidate_pool)
    if not eligible_pool:
        return {
            "deal_opportunities": [],
            "opportunity_radar": [],
            "wait_picks": [],
        }, set()

    bounded_limit = max(1, int(limit))
    ranked_opportunities = sorted(
        eligible_pool,
        key=lambda row: (
            _score_dashboard_opportunity_row(row),
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
            -_dashboard_sort_game_id(row),
        ),
        reverse=True,
    )
    ranked_radar = sorted(
        eligible_pool,
        key=lambda row: (
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("momentum_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
            -_dashboard_sort_game_id(row),
        ),
        reverse=True,
    )
    ranked_wait = sorted(
        [row for row in eligible_pool if _is_wait_dashboard_candidate(row)],
        key=lambda row: (
            safe_num(row.get("predicted_next_discount_percent"), 0.0),
            safe_num(row.get("price_vs_low_ratio"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("momentum_score"), 0.0),
            -_dashboard_sort_game_id(row),
        ),
        reverse=True,
    )

    used_keys: set[str] = set()
    allocated: dict[str, list[dict]] = {}
    for rail_key, ranked_rows in (
        ("deal_opportunities", ranked_opportunities),
        ("opportunity_radar", ranked_radar),
        ("wait_picks", ranked_wait),
    ):
        allocated[rail_key] = _compose_unique_dashboard_rows(
            _released_deal_dashboard_rows(ranked_rows),
            eligible_pool,
            used_keys,
            bounded_limit,
        )
    return allocated, used_keys


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


def _trim_dashboard_home_payload(payload: dict, mode: str) -> dict:
    normalized_mode = str(mode or "full").strip().lower()
    if normalized_mode not in {"critical", "deferred"}:
        return payload

    if normalized_mode == "critical":
        allowed_keys = {
            "catalogSummary",
            "dealRanked",
            "biggest_discounts",
            "worth_buying_now",
            "trending_now",
            "new_historical_lows",
            "buy_now_picks",
            "wait_picks",
            "deal_radar",
            "deal_opportunities",
            "opportunity_radar",
            "daily_digest",
            "_meta",
        }
    else:
        allowed_keys = {
            "catalogSummary",
            "filters",
            "seasonal_summary",
            "upcoming",
            "dealRanked",
            "topDealsToday",
            "worth_buying_now",
            "worthBuyingNow",
            "biggest_discounts",
            "biggestDeals",
            "trending_now",
            "trendingDeals",
            "trending",
            "new_historical_lows",
            "historicalLows",
            "buy_now_picks",
            "wait_picks",
            "topReviewed",
            "topPlayed",
            "leaderboard",
            "deal_radar",
            "dealRadar",
            "marketRadar",
            "generated_at",
            "_meta",
        }
    return {key: value for key, value in payload.items() if key in allowed_keys}


def _augment_dashboard_home_payload(raw_payload: dict) -> dict:
    payload = dict(raw_payload)

    rail_limit = 24
    worth_buying_now = _released_deal_dashboard_rows(_dashboard_rows(payload, "worth_buying_now", "worthBuyingNow"))
    biggest_discounts = _released_deal_dashboard_rows(_dashboard_rows(payload, "biggest_discounts", "biggestDeals"))
    trending_now = _released_dashboard_rows(_dashboard_rows(payload, "trending_now", "trending", "trendingDeals"))
    new_historical_lows = _released_dashboard_rows(_dashboard_rows(payload, "new_historical_lows", "newHistoricalLows"))
    deal_radar = _dedupe_dashboard_rows(_dashboard_rows(payload, "deal_radar", "marketRadar", "dealRadar"))
    alert_signals = _dedupe_dashboard_rows(_dashboard_rows(payload, "alertSignals"))
    deal_opportunities = _released_deal_dashboard_rows(_dashboard_rows(payload, "deal_opportunities", "dealOpportunities"))
    opportunity_radar = _released_deal_dashboard_rows(_dashboard_rows(payload, "opportunity_radar", "opportunityRadar"))
    deal_ranked = _released_deal_dashboard_rows(_dashboard_rows(payload, "dealRanked", "topDealsToday"))

    canonical_deal_pool = _released_deal_dashboard_rows(
        [
            *worth_buying_now,
            *_dashboard_rows(payload, "recommendedDeals"),
            *deal_ranked,
            *biggest_discounts,
            *_dashboard_rows(payload, "trendingDeals"),
            *trending_now,
            *new_historical_lows,
        ]
    )
    buy_now_picks = _released_deal_dashboard_rows(_dashboard_rows(payload, "buy_now_picks", "buyNowPicks"))
    wait_picks = _released_deal_dashboard_rows(_dashboard_rows(payload, "wait_picks", "waitPicks"))
    if not buy_now_picks:
        buy_now_picks = _released_deal_dashboard_rows(_decision_rows_by_recommendation(canonical_deal_pool, "BUY_NOW"))
    if not buy_now_picks:
        buy_now_picks = worth_buying_now[:rail_limit]
    allocated_rails, protected_visible_keys = _allocate_protected_dashboard_deal_rails(
        [*canonical_deal_pool, *deal_opportunities, *opportunity_radar],
        rail_limit,
    )
    deal_opportunities = allocated_rails.get("deal_opportunities", [])
    opportunity_radar = allocated_rails.get("opportunity_radar", [])
    worth_buying_now = _released_deal_dashboard_rows(worth_buying_now)
    biggest_discounts = _released_deal_dashboard_rows(biggest_discounts)
    if not biggest_discounts:
        biggest_discounts = _released_deal_dashboard_rows(deal_ranked)
    wait_picks = allocated_rails.get("wait_picks", [])

    buy_now_picks = _compose_unique_dashboard_rows(
        buy_now_picks,
        _released_deal_dashboard_rows([*worth_buying_now, *canonical_deal_pool]),
        protected_visible_keys,
        rail_limit,
    )
    deal_ranked = _compose_unique_dashboard_rows(
        _released_deal_dashboard_rows(deal_ranked),
        _released_deal_dashboard_rows([*canonical_deal_pool, *biggest_discounts]),
        set(),
        rail_limit,
    )

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
    payload["deal_opportunities"] = deal_opportunities
    payload["opportunity_radar"] = opportunity_radar
    payload["dealRanked"] = deal_ranked
    payload["new_historical_lows"] = new_historical_lows
    payload["trending_now"] = trending_now
    payload["deal_radar"] = deal_radar
    payload["player_surges"] = player_surges
    payload["seasonal_summary"] = seasonal_summary
    payload["worthBuyingNow"] = worth_buying_now
    payload["biggestDeals"] = biggest_discounts
    payload["buyNowPicks"] = buy_now_picks
    payload["waitPicks"] = wait_picks
    payload["topDealsToday"] = deal_ranked or biggest_discounts
    payload["newHistoricalLows"] = new_historical_lows
    payload["trendingDeals"] = trending_now
    payload["trending"] = trending_now
    payload["dealRadar"] = deal_radar
    payload["marketRadar"] = deal_radar
    payload["dealOpportunities"] = deal_opportunities
    payload["opportunityRadar"] = opportunity_radar
    payload["decision_dashboard"] = {
        "worth_buying_now": worth_buying_now,
        "biggest_discounts": biggest_discounts,
        "buy_now_picks": buy_now_picks,
        "wait_picks": wait_picks,
        "deal_opportunities": deal_opportunities,
        "opportunity_radar": opportunity_radar,
        "dealRanked": deal_ranked,
        "new_historical_lows": new_historical_lows,
        "trending_now": trending_now,
        "deal_radar": deal_radar,
        "player_surges": player_surges,
        "seasonal_summary": seasonal_summary,
    }

    if "dailyDigest" not in payload and isinstance(payload.get("daily_digest"), dict):
        payload["dailyDigest"] = payload["daily_digest"]

    return payload


@app.get("/dashboard/home")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/dashboard/home")
def get_dashboard_home(request: Request, mode: str | None = None):
    started = _start_timer()
    try:
        normalized_mode = str(mode or "").strip().lower()
        read_session = ReadSessionLocal()
        try:
            cache_row, cached_payload = _read_dashboard_cache(read_session, mode=normalized_mode)
            if normalized_mode == "critical" and (
                cache_row is None or _dashboard_payload_is_empty(cached_payload)
            ):
                fallback_row, fallback_payload = _read_dashboard_cache(read_session)
                if fallback_row is not None and not _dashboard_payload_is_empty(fallback_payload):
                    cache_row, cached_payload = fallback_row, fallback_payload
        finally:
            read_session.close()

        should_refresh = cache_row is None or _dashboard_payload_is_empty(cached_payload)
        if normalized_mode != "critical" and cache_row is not None:
            should_refresh = should_refresh or _dashboard_cache_is_stale(cache_row, utc_now())
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

        if normalized_mode == "critical":
            payload = _augment_dashboard_home_payload(dict(cached_payload))
            served_cache_key = cache_row.cache_key
            generated_at = cache_row.updated_at.isoformat() if cache_row.updated_at else None
            # If critical cache is missing and we fell back to full home payload,
            # rebuild the strict critical contract on-demand to avoid shipping
            # oversized first-paint payloads.
            try:
                from jobs.refresh_snapshots import _build_homepage_critical_payload

                payload = _build_homepage_critical_payload(payload)
            except Exception:
                logger.exception("Failed to normalize critical dashboard payload from cache_key=%s", cache_row.cache_key)
            needs_opportunities = not _dashboard_rows(payload, "deal_opportunities", "dealOpportunities")
            needs_opportunity_radar = not _dashboard_rows(payload, "opportunity_radar", "opportunityRadar")
            if needs_opportunities or needs_opportunity_radar:
                critical_session = ReadSessionLocal()
                try:
                    if needs_opportunities and needs_opportunity_radar:
                        opportunity_items, radar_items = _collect_opportunity_item_pair(
                            critical_session,
                            HOMEPAGE_CRITICAL_LIMIT,
                        )
                        payload["deal_opportunities"] = opportunity_items
                        payload["dealOpportunities"] = opportunity_items
                        payload["opportunity_radar"] = radar_items
                        payload["opportunityRadar"] = radar_items
                    elif needs_opportunities:
                        opportunity_items = _collect_deal_opportunity_items(critical_session, HOMEPAGE_CRITICAL_LIMIT)
                        payload["deal_opportunities"] = opportunity_items
                        payload["dealOpportunities"] = opportunity_items
                    elif needs_opportunity_radar:
                        existing_opportunities = _dashboard_rows(payload, "deal_opportunities", "dealOpportunities")
                        exclude_ids: set[int] = set()
                        for row in existing_opportunities:
                            game_id = int(safe_num(row.get("game_id") or row.get("id"), 0.0))
                            if game_id > 0:
                                exclude_ids.add(game_id)
                        radar_items = _collect_opportunity_radar_items(
                            critical_session,
                            HOMEPAGE_CRITICAL_LIMIT,
                            exclude_game_ids=exclude_ids,
                        )
                        payload["opportunity_radar"] = radar_items
                        payload["opportunityRadar"] = radar_items
                finally:
                    critical_session.close()
            if served_cache_key != CRITICAL_DASHBOARD_CACHE_KEY:
                _upsert_dashboard_cache_payload(
                    CRITICAL_DASHBOARD_CACHE_KEY,
                    payload,
                    updated_at=cache_row.updated_at or utc_now(),
                )
                served_cache_key = CRITICAL_DASHBOARD_CACHE_KEY
            payload["_meta"] = {
                "cache_key": served_cache_key,
                "generated_at": generated_at,
            }
            trimmed = _trim_dashboard_home_payload(payload, normalized_mode)
            return JSONResponse(content=trimmed)

        if normalized_mode == "deferred":
            payload = _augment_dashboard_home_payload(dict(cached_payload))
            payload["_meta"] = {
                "cache_key": cache_row.cache_key,
                "generated_at": cache_row.updated_at.isoformat() if cache_row.updated_at else None,
            }
            return JSONResponse(content=_trim_dashboard_home_payload(payload, normalized_mode))

        payload = _augment_dashboard_home_payload(cached_payload)
        payload["_meta"] = {
            "cache_key": cache_row.cache_key,
            "generated_at": cache_row.updated_at.isoformat() if cache_row.updated_at else None,
        }
        return _trim_dashboard_home_payload(payload, mode)
    finally:
        _log_timing("/dashboard/home", started)


@app.get("/games/detail")
def game_detail(request: Request, game_name: str):
    session = Session()
    try:
        viewer_user_id = resolve_request_user_id(request)
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

        payload = build_game_detail_payload(session, game, user_id=viewer_user_id)
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


@app.get("/games/{game_id:int}")
def get_game_detail(request: Request, game_id: int):
    viewer_user_id = resolve_request_user_id(request)
    session = ReadSessionLocal()
    try:
        game = session.query(Game).filter(Game.id == game_id).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        return _build_game_detail_response_payload(session, game, viewer_user_id)
    finally:
        session.close()


def _build_game_detail_response_payload(session: Session, game: Game, viewer_user_id: str) -> dict:
    game_id = int(game.id)
    snapshot = session.query(GameSnapshot).filter(GameSnapshot.game_id == game_id).first()
    latest = session.query(LatestGamePrice).filter(LatestGamePrice.game_id == game_id).first()
    if snapshot is not None or latest is not None:
        payload = _build_snapshot_game_detail_payload(game, snapshot, latest)
    else:
        payload = build_game_detail_payload(session, game, user_id=viewer_user_id)
        payload["share_card_url"] = _build_canonical_url(f"/share/deal/{game_id}")
    payload["watchlisted"] = _is_game_watchlisted_for_user(session, game_id, viewer_user_id)
    return _ensure_game_detail_contract(payload)


def _slugify_game_identifier(value: str | None) -> str:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return ""
    slug = re.sub(r"[^a-z0-9]+", "-", lowered)
    return slug.strip("-")


def _resolve_game_by_identifier(session: Session, identifier: str | None) -> Game | None:
    raw_identifier = str(identifier or "").strip().strip("/")
    if not raw_identifier:
        return None

    if raw_identifier.isdigit():
        numeric_id = int(raw_identifier)
        if numeric_id > 0:
            by_id = session.query(Game).filter(Game.id == numeric_id).first()
            if by_id is not None:
                return by_id
            by_appid = session.query(Game).filter(Game.appid == str(numeric_id)).first()
            if by_appid is not None:
                return by_appid
        return None

    lowered_identifier = raw_identifier.lower()
    exact_name = session.query(Game).filter(func.lower(Game.name) == lowered_identifier).first()
    if exact_name is not None:
        return exact_name

    spaced_identifier = re.sub(r"[-_]+", " ", lowered_identifier).strip()
    if spaced_identifier:
        spaced_name = session.query(Game).filter(func.lower(Game.name) == spaced_identifier).first()
        if spaced_name is not None:
            return spaced_name

    slug_identifier = _slugify_game_identifier(raw_identifier)
    if not slug_identifier:
        return None

    if session.bind and session.bind.dialect.name == "postgresql":
        try:
            slug_name = (
                session.query(Game)
                .filter(
                    func.regexp_replace(
                        func.lower(Game.name),
                        r"[^a-z0-9]+",
                        "-",
                        "g",
                    ) == slug_identifier
                )
                .first()
            )
            if slug_name is not None:
                return slug_name
        except Exception:
            pass

    pivot_token = (spaced_identifier.split(" ")[0] if spaced_identifier else slug_identifier.split("-")[0]).strip()
    if not pivot_token:
        return None
    candidates = session.query(Game).filter(Game.name.ilike(f"%{pivot_token}%")).limit(250).all()
    for candidate in candidates:
        if _slugify_game_identifier(candidate.name) == slug_identifier:
            return candidate
    return None


@app.get("/games/resolve/{identifier}")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/games/resolve/{identifier}")
def resolve_game_detail(request: Request, identifier: str):
    started = _start_timer()
    viewer_user_id = resolve_request_user_id(request)
    session = ReadSessionLocal()
    try:
        game = _resolve_game_by_identifier(session, identifier)
        if game is None:
            raise HTTPException(status_code=404, detail="Game not found")
        return _build_game_detail_response_payload(session, game, viewer_user_id)
    finally:
        session.close()
        _log_timing("/games/resolve/{identifier}", started)


@app.get("/games/by-name")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/games/by-name")
def get_game_by_name(request: Request, game_name: str):
    started = _start_timer()
    viewer_user_id = resolve_request_user_id(request)
    session = ReadSessionLocal()
    try:
        name_value = (game_name or "").strip()
        if not name_value:
            raise HTTPException(status_code=400, detail="game_name is required")

        game = session.query(Game).filter(Game.name == name_value).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        return _build_game_detail_response_payload(session, game, viewer_user_id)
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
            game_slug = _canonical_game_slug(row.game_name, row.game_id)
            return {
                "game_id": int(row.game_id),
                "game_name": row.game_name,
                "slug": game_slug,
                "game_slug": game_slug,
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
def list_wishlist(request: Request):
    normalized_user_id = resolve_request_user_id(request)
    session = Session()
    try:
        rows = (
            session.query(WishlistItem, Game.name)
            .outerjoin(Game, Game.id == WishlistItem.game_id)
            .filter(WishlistItem.user_id == normalized_user_id)
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
    request: Request,
    user_id: str = Query(default=DEFAULT_USER_ID),
    limit: int = Query(default=API_DEFAULT_LIST_LIMIT, ge=1, le=API_MAX_LIST_LIMIT),
):
    normalized_user_id = resolve_request_user_id(request, user_id)
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


@app.get("/api/viewer")
def get_viewer_identity(request: Request):
    viewer_id = resolve_request_user_id(request)
    return {
        "user_id": viewer_id,
        "anonymous": bool(_normalize_anonymous_user_id(viewer_id)),
    }


@app.get("/api/home-personal-summary")
@json_etag()
@ttl_cache(ttl_seconds=30, endpoint_key="/api/home-personal-summary")
def get_home_personal_summary(
    request: Request,
    user_id: str = Query(default=DEFAULT_USER_ID),
    limit: int = Query(default=4, ge=1, le=8),
):
    started = _start_timer()
    normalized_user_id = resolve_request_user_id(request, user_id)
    if _is_guest_user_id(normalized_user_id):
        return {
            "user_id": normalized_user_id,
            "personalized": False,
            "wishlist": [],
            "watchlist": [],
            "alerts": [],
            "generated_at": utc_now().isoformat(),
        }

    session = ReadSessionLocal()
    try:
        normalized_limit = max(1, min(int(limit), 8))
        wishlist_rows = (
            session.query(WishlistItem.game_id, WishlistItem.game_name)
            .filter(WishlistItem.user_id == normalized_user_id)
            .order_by(WishlistItem.created_at.desc(), WishlistItem.id.desc())
            .limit(normalized_limit)
            .all()
        )
        watchlist_rows = (
            session.query(Watchlist.game_id)
            .filter(Watchlist.user_id == normalized_user_id)
            .order_by(Watchlist.created_at.desc(), Watchlist.id.desc())
            .limit(normalized_limit)
            .all()
        )
        alert_rows = (
            session.query(UserAlert, Game)
            .outerjoin(Game, Game.id == UserAlert.game_id)
            .filter(UserAlert.user_id == normalized_user_id)
            .order_by(UserAlert.created_at.desc(), UserAlert.id.desc())
            .limit(min(3, normalized_limit))
            .all()
        )

        game_ids: set[int] = set()
        for game_id, _ in wishlist_rows:
            if game_id is not None:
                game_ids.add(int(game_id))
        for (game_id,) in watchlist_rows:
            if game_id is not None:
                game_ids.add(int(game_id))

        name_map: dict[int, str] = {}
        if game_ids:
            for game_id, name in session.query(Game.id, Game.name).filter(Game.id.in_(game_ids)).all():
                if game_id is None or not name:
                    continue
                name_map[int(game_id)] = name

        wishlist_payload: list[dict] = []
        for game_id, game_name in wishlist_rows:
            if game_id is None:
                continue
            normalized_id = int(game_id)
            resolved_name = game_name or name_map.get(normalized_id) or f"Game {normalized_id}"
            wishlist_payload.append(
                {
                    "game_id": normalized_id,
                    "game_name": resolved_name,
                    "slug": _canonical_game_slug(resolved_name, normalized_id),
                    "game_slug": _canonical_game_slug(resolved_name, normalized_id),
                }
            )

        watchlist_payload: list[dict] = []
        for (game_id,) in watchlist_rows:
            if game_id is None:
                continue
            normalized_id = int(game_id)
            resolved_name = name_map.get(normalized_id) or f"Game {normalized_id}"
            watchlist_payload.append(
                {
                    "game_id": normalized_id,
                    "game_name": resolved_name,
                    "slug": _canonical_game_slug(resolved_name, normalized_id),
                    "game_slug": _canonical_game_slug(resolved_name, normalized_id),
                }
            )

        alerts_payload: list[dict] = []
        for alert, game in alert_rows:
            if alert is None:
                continue
            game_id = int(alert.game_id) if alert.game_id is not None else 0
            game_name = game.name if game and game.name else f"Game {game_id}"
            alerts_payload.append(
                {
                    "game_id": game_id,
                    "game_name": game_name,
                    "slug": _canonical_game_slug(game_name, game_id),
                    "game_slug": _canonical_game_slug(game_name, game_id),
                    "alert_type": str(alert.alert_type or "").upper(),
                    "alert_created_at": alert.created_at.isoformat() if alert.created_at else None,
                }
            )

        return {
            "user_id": normalized_user_id,
            "personalized": True,
            "wishlist": wishlist_payload,
            "watchlist": watchlist_payload,
            "alerts": alerts_payload,
            "generated_at": utc_now().isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/home-personal-summary", started)


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
        cached_items = _read_cached_section_items(session, "home:deal_opportunities", limit=limit)
        if cached_items:
            items = _filter_valid_deal_payload_rows(cached_items, limit=limit)
            return {
                "count": len(items),
                "items": items,
                "generated_at": utc_now().isoformat(),
            }
        items = _collect_deal_opportunity_items(session, limit)
        return {
            "count": len(items),
            "items": items,
            "generated_at": utc_now().isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/deal-opportunities", started)


@app.get("/api/opportunity-radar")
@json_etag()
@ttl_cache(ttl_seconds=45, endpoint_key="/api/opportunity-radar")
def list_opportunity_radar(
    request: Request,
    limit: int = Query(default=24, ge=1, le=120),
):
    started = _start_timer()
    session = ReadSessionLocal()
    try:
        cached_items = _read_cached_section_items(session, "home:opportunity_radar", limit=limit)
        if cached_items:
            items = _filter_valid_deal_payload_rows(cached_items, limit=limit)
            return {
                "count": len(items),
                "items": items,
                "generated_at": utc_now().isoformat(),
            }
        items = _collect_opportunity_radar_items(session, limit)
        return {
            "count": len(items),
            "items": items,
            "generated_at": utc_now().isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/opportunity-radar", started)


@app.get("/api/daily-digest")
@json_etag()
@ttl_cache(ttl_seconds=60, endpoint_key="/api/daily-digest")
def get_daily_deal_digest(
    request: Request,
    user_id: str = Query(default=DEFAULT_USER_ID),
    section_limit: int = Query(default=8, ge=3, le=20),
):
    started = _start_timer()
    now = utc_now()
    window_start = now - datetime.timedelta(hours=DAILY_DIGEST_WINDOW_HOURS)
    normalized_user_id = resolve_request_user_id(request, user_id)
    personalization_enabled = not _is_anonymous_user_id(normalized_user_id)
    normalized_limit = max(3, int(section_limit))

    session = ReadSessionLocal()
    try:
        wishlist_game_ids: set[int] = set()
        watchlist_game_ids: set[int] = set()
        target_game_ids: set[int] = set()
        token_weights: dict[str, float] = {}

        if personalization_enabled:
            wishlist_game_ids = {
                int(game_id)
                for game_id, in (
                    session.query(WishlistItem.game_id)
                    .filter(WishlistItem.user_id == normalized_user_id)
                    .all()
                )
                if game_id is not None
            }
            watchlist_game_ids = {
                int(game_id)
                for game_id, in (
                    session.query(Watchlist.game_id)
                    .filter(Watchlist.user_id == normalized_user_id)
                    .all()
                )
                if game_id is not None
            }
            target_game_ids = {
                int(game_id)
                for game_id, in (
                    session.query(DealWatchlist.game_id)
                    .filter(DealWatchlist.user_id == normalized_user_id, DealWatchlist.active.is_(True))
                    .all()
                )
                if game_id is not None
            }

            seed_game_ids = wishlist_game_ids | watchlist_game_ids | target_game_ids
            if seed_game_ids:
                seed_rows = [
                    (int(game_id), tags, genres)
                    for game_id, tags, genres in (
                        session.query(GameSnapshot.game_id, GameSnapshot.tags, GameSnapshot.genres)
                        .filter(GameSnapshot.game_id.in_(list(seed_game_ids)))
                        .all()
                    )
                    if game_id is not None
                ]
                token_weights = _build_personalization_token_weights(
                    seed_rows,
                    wishlist_game_ids=wishlist_game_ids,
                    watchlist_game_ids=watchlist_game_ids,
                    target_game_ids=target_game_ids,
                    recent_game_ids=set(),
                )

        def personalization_context(snapshot: GameSnapshot) -> tuple[float, list[str]]:
            if not personalization_enabled:
                return 0.0, []

            score = 0.0
            reasons: list[str] = []
            game_id = int(snapshot.game_id)
            if game_id in wishlist_game_ids:
                score += 34.0
                _append_unique_reason(reasons, "In your wishlist")
            if game_id in watchlist_game_ids:
                score += 30.0
                _append_unique_reason(reasons, "In your watchlist")
            if game_id in target_game_ids and game_id not in wishlist_game_ids and game_id not in watchlist_game_ids:
                score += 18.0
                _append_unique_reason(reasons, "In your price alerts")

            similarity_bonus, overlap_count = _compute_personalization_similarity_bonus(snapshot, token_weights)
            if similarity_bonus > 0:
                score += min(18.0, similarity_bonus * 0.42)
                if overlap_count >= 2:
                    _append_unique_reason(reasons, "Similar to games you track")
                elif overlap_count >= 1:
                    _append_unique_reason(reasons, "Similar to your interests")

            return round(score, 2), reasons[:2]

        event_scan_limit = max(DAILY_DIGEST_EVENT_SCAN_LIMIT, normalized_limit * 30)
        alert_scan_limit = max(DAILY_DIGEST_ALERT_SCAN_LIMIT, normalized_limit * 24)
        snapshot_scan_limit = max(DAILY_DIGEST_SNAPSHOT_SCAN_LIMIT, normalized_limit * 20)

        recent_event_rows = (
            session.query(DealEvent, GameSnapshot)
            .join(GameSnapshot, GameSnapshot.game_id == DealEvent.game_id)
            .filter(
                DealEvent.created_at >= window_start,
                GameSnapshot.is_released == 1,
                or_(GameSnapshot.is_upcoming.is_(False), GameSnapshot.is_upcoming.is_(None)),
            )
            .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
            .limit(event_scan_limit)
            .all()
        )

        recent_alert_rows = (
            session.query(Alert, GameSnapshot)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == Alert.game_id)
            .filter(Alert.created_at >= window_start)
            .order_by(Alert.created_at.desc(), Alert.id.desc())
            .limit(alert_scan_limit)
            .all()
        )

        recommendation_expr = func.upper(func.coalesce(GameSnapshot.buy_recommendation, ""))
        buy_now_rows = (
            session.query(GameSnapshot)
            .filter(
                GameSnapshot.is_released == 1,
                or_(GameSnapshot.is_upcoming.is_(False), GameSnapshot.is_upcoming.is_(None)),
                GameSnapshot.latest_price.isnot(None),
                recommendation_expr == "BUY_NOW",
                or_(
                    GameSnapshot.updated_at >= window_start,
                    GameSnapshot.deal_detected_at >= window_start,
                    GameSnapshot.last_discounted_at >= window_start,
                ),
            )
            .order_by(
                GameSnapshot.buy_score.desc().nullslast(),
                GameSnapshot.worth_buying_score.desc().nullslast(),
                GameSnapshot.deal_score.desc().nullslast(),
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                GameSnapshot.updated_at.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(snapshot_scan_limit)
            .all()
        )

        trending_rows = (
            session.query(GameSnapshot)
            .filter(
                GameSnapshot.is_released == 1,
                or_(GameSnapshot.is_upcoming.is_(False), GameSnapshot.is_upcoming.is_(None)),
                GameSnapshot.latest_price.isnot(None),
                GameSnapshot.updated_at >= window_start,
                or_(
                    GameSnapshot.trending_score >= 55,
                    GameSnapshot.momentum_score >= 58,
                    GameSnapshot.short_term_player_trend >= 0.05,
                    GameSnapshot.player_growth_ratio >= 1.05,
                ),
            )
            .order_by(
                GameSnapshot.trending_score.desc().nullslast(),
                GameSnapshot.momentum_score.desc().nullslast(),
                GameSnapshot.short_term_player_trend.desc().nullslast(),
                GameSnapshot.current_players.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            )
            .limit(snapshot_scan_limit)
            .all()
        )

        drops_by_game: dict[int, tuple[float, dict]] = {}
        lows_by_game: dict[int, tuple[float, dict]] = {}
        buy_now_by_game: dict[int, tuple[float, dict]] = {}
        trending_by_game: dict[int, tuple[float, dict]] = {}
        radar_by_game_and_type: dict[tuple[int, str], tuple[float, dict]] = {}

        def keep_best_by_game(bucket: dict[int, tuple[float, dict]], game_id: int, score: float, item: dict) -> None:
            existing = bucket.get(game_id)
            if existing is None or score > existing[0]:
                bucket[game_id] = (score, item)

        for event_row, snapshot in recent_event_rows:
            if snapshot is None:
                continue
            event_type = str(event_row.event_type or "").strip().upper()
            metadata = event_row.metadata_json if isinstance(event_row.metadata_json, dict) else {}
            event_time = _coerce_utc_datetime(event_row.created_at) or now
            age_hours = max(0.0, (now - event_time).total_seconds() / 3600.0)
            personalization_score, personalization_reasons = personalization_context(snapshot)

            if event_type == "PRICE_DROP":
                old_price = safe_num(event_row.old_price, safe_num(metadata.get("old_price"), 0.0))
                new_price = safe_num(event_row.new_price, safe_num(metadata.get("new_price"), safe_num(snapshot.latest_price, 0.0)))
                drop_amount = old_price - new_price if old_price > 0 else 0.0
                drop_percent = (drop_amount / old_price * 100.0) if old_price > 0 and drop_amount > 0 else 0.0
                discount = max(0, int(round(safe_num(
                    event_row.discount_percent if event_row.discount_percent is not None else snapshot.latest_discount_percent,
                    0.0,
                ))))
                if drop_amount <= 0 and drop_percent < 8 and discount < 20:
                    continue

                reason_hint = (
                    event_row.event_reason_summary
                    or metadata.get("event_reason_summary")
                    or ("Large price drop" if drop_percent >= 15 else "Price dropped")
                )
                priority = (
                    drop_amount * 11.0
                    + drop_percent * 1.55
                    + discount * 0.45
                    + safe_num(snapshot.deal_score, 0.0) * 0.35
                    + max(0.0, 16.0 - age_hours)
                    + personalization_score
                )
                item = _build_daily_digest_item(
                    snapshot,
                    section_key="biggest_price_drops",
                    occurred_at=event_time,
                    event_type=event_type,
                    reason_hint=reason_hint,
                    metadata=metadata,
                    personalization_reasons=personalization_reasons,
                    personalization_score=personalization_score,
                    priority_score=priority,
                )
                item["drop_amount"] = round(drop_amount, 2) if drop_amount > 0 else None
                item["drop_percent"] = round(drop_percent, 2) if drop_percent > 0 else None
                keep_best_by_game(drops_by_game, int(snapshot.game_id), priority, item)
                continue

            if event_type == "HISTORICAL_LOW":
                discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
                priority = (
                    safe_num(snapshot.deal_score, 0.0) * 0.42
                    + safe_num(snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score, 0.0) * 0.35
                    + discount * 0.42
                    + max(0.0, 18.0 - age_hours)
                    + 12.0
                    + personalization_score
                )
                item = _build_daily_digest_item(
                    snapshot,
                    section_key="new_historical_lows",
                    occurred_at=event_time,
                    event_type=event_type,
                    reason_hint=event_row.event_reason_summary or "New historical low",
                    metadata=metadata,
                    personalization_reasons=personalization_reasons,
                    personalization_score=personalization_score,
                    priority_score=priority,
                )
                keep_best_by_game(lows_by_game, int(snapshot.game_id), priority, item)
                continue

            if event_type == "PLAYER_SPIKE":
                trend_strength = (
                    safe_num(snapshot.short_term_player_trend, 0.0) * 520.0
                    + safe_num(snapshot.player_growth_ratio, 0.0) * 10.0
                    + safe_num(snapshot.momentum_score, 0.0) * 0.9
                )
                priority = (
                    trend_strength
                    + safe_num(snapshot.current_players, 0.0) * 0.02
                    + max(0.0, 14.0 - age_hours)
                    + personalization_score
                )
                item = _build_daily_digest_item(
                    snapshot,
                    section_key="trending_games",
                    occurred_at=event_time,
                    event_type=event_type,
                    reason_hint=event_row.event_reason_summary or "Players surging",
                    metadata=metadata,
                    personalization_reasons=personalization_reasons,
                    personalization_score=personalization_score,
                    priority_score=priority,
                )
                keep_best_by_game(trending_by_game, int(snapshot.game_id), priority, item)

        for snapshot in buy_now_rows:
            personalization_score, personalization_reasons = personalization_context(snapshot)
            buy_score = safe_num(snapshot.buy_score if snapshot.buy_score is not None else snapshot.worth_buying_score, 0.0)
            deal_score = safe_num(snapshot.deal_score, 0.0)
            discount = max(0, int(round(safe_num(snapshot.latest_discount_percent, 0.0))))
            near_low = _is_near_historical_low(snapshot)
            if not near_low and discount < 20 and buy_score < 60 and deal_score < 65:
                continue

            updated_dt = _coerce_utc_datetime(snapshot.updated_at) or now
            age_hours = max(0.0, (now - updated_dt).total_seconds() / 3600.0)
            priority = (
                buy_score * 0.78
                + deal_score * 0.52
                + discount * 0.42
                + (12.0 if near_low else 0.0)
                + max(0.0, 12.0 - age_hours)
                + personalization_score
            )
            item = _build_daily_digest_item(
                snapshot,
                section_key="buy_now_opportunities",
                occurred_at=updated_dt,
                event_type="BUY_NOW",
                reason_hint=snapshot.buy_reason or snapshot.worth_buying_reason_summary or "Buy-now recommendation",
                metadata={},
                personalization_reasons=personalization_reasons,
                personalization_score=personalization_score,
                priority_score=priority,
            )
            keep_best_by_game(buy_now_by_game, int(snapshot.game_id), priority, item)

        for snapshot in trending_rows:
            personalization_score, personalization_reasons = personalization_context(snapshot)
            trend_strength = (
                safe_num(snapshot.trending_score, 0.0) * 0.9
                + safe_num(snapshot.momentum_score, 0.0) * 0.88
                + safe_num(snapshot.short_term_player_trend, 0.0) * 460.0
                + safe_num(snapshot.player_growth_ratio, 0.0) * 8.0
                + safe_num(snapshot.current_players, 0.0) * 0.012
            )
            if trend_strength < 26.0:
                continue

            updated_dt = _coerce_utc_datetime(snapshot.updated_at) or now
            age_hours = max(0.0, (now - updated_dt).total_seconds() / 3600.0)
            priority = trend_strength + max(0.0, 10.0 - age_hours) + personalization_score
            item = _build_daily_digest_item(
                snapshot,
                section_key="trending_games",
                occurred_at=updated_dt,
                event_type="TRENDING",
                reason_hint=snapshot.trend_reason_summary or "Players surging",
                metadata={},
                personalization_reasons=personalization_reasons,
                personalization_score=personalization_score,
                priority_score=priority,
            )
            keep_best_by_game(trending_by_game, int(snapshot.game_id), priority, item)

        for alert_row, snapshot in recent_alert_rows:
            if snapshot is None:
                continue
            alert_type = str(alert_row.alert_type or "").strip().upper()
            if not alert_type:
                continue

            metadata = alert_row.metadata_json if isinstance(alert_row.metadata_json, dict) else {}
            alert_time = _coerce_utc_datetime(alert_row.created_at) or now
            age_hours = max(0.0, (now - alert_time).total_seconds() / 3600.0)
            personalization_score, personalization_reasons = personalization_context(snapshot)
            reason_hint = _alert_label(alert_type)
            if alert_type == "SALE_STARTED" and safe_num(snapshot.popularity_score, 0.0) >= 65:
                reason_hint = "Rare sale on popular game"
            elif alert_type == "PLAYER_SURGE":
                reason_hint = "Players surging"
            elif alert_type == "NEW_HISTORICAL_LOW":
                reason_hint = "New historical low"
            elif alert_type == "PRICE_DROP":
                reason_hint = "Large price drop"

            priority = (
                _daily_digest_alert_priority(alert_type) * 9.0
                + safe_num(snapshot.deal_score, 0.0) * 0.24
                + safe_num(snapshot.momentum_score, 0.0) * 0.18
                + max(0.0, 12.0 - age_hours)
                + personalization_score
            )
            item = _build_daily_digest_item(
                snapshot,
                section_key="radar_signals",
                occurred_at=alert_time,
                event_type=alert_type,
                reason_hint=reason_hint,
                metadata=metadata,
                personalization_reasons=personalization_reasons,
                personalization_score=personalization_score,
                priority_score=priority,
            )
            radar_key = (int(snapshot.game_id), alert_type)
            existing = radar_by_game_and_type.get(radar_key)
            if existing is None or priority > existing[0]:
                radar_by_game_and_type[radar_key] = (priority, item)

        def _parse_digest_item_timestamp(item: dict) -> float:
            occurred_raw = str(item.get("occurred_at") or item.get("updated_at") or "").strip()
            if not occurred_raw:
                return 0.0
            try:
                parsed = datetime.datetime.fromisoformat(occurred_raw.replace("Z", "+00:00"))
            except Exception:
                return 0.0
            normalized = _coerce_utc_datetime(parsed)
            if normalized is None:
                return 0.0
            return float(normalized.timestamp())

        def _item_sort_key(entry: tuple[float, dict]) -> tuple[float, float]:
            score, item = entry
            return safe_num(score, 0.0), _parse_digest_item_timestamp(item)

        def _limit_bucket(bucket: dict[int, tuple[float, dict]], limit: int) -> list[dict]:
            ranked = sorted(bucket.values(), key=_item_sort_key, reverse=True)
            items: list[dict] = []
            for score, item in ranked[:limit]:
                item["priority_score"] = round(safe_num(score, 0.0), 2)
                items.append(item)
            return items

        def _limit_radar_bucket(bucket: dict[tuple[int, str], tuple[float, dict]], limit: int) -> list[dict]:
            ranked = sorted(bucket.values(), key=_item_sort_key, reverse=True)
            seen_game_ids: set[int] = set()
            items: list[dict] = []
            for score, item in ranked:
                game_id = int(safe_num(item.get("game_id"), 0.0))
                if game_id <= 0 or game_id in seen_game_ids:
                    continue
                seen_game_ids.add(game_id)
                item["priority_score"] = round(safe_num(score, 0.0), 2)
                items.append(item)
                if len(items) >= limit:
                    break
            return items

        sections = {
            "biggest_price_drops": _limit_bucket(drops_by_game, normalized_limit),
            "new_historical_lows": _limit_bucket(lows_by_game, normalized_limit),
            "buy_now_opportunities": _limit_bucket(buy_now_by_game, normalized_limit),
            "trending_games": _limit_bucket(trending_by_game, normalized_limit),
            "radar_signals": _limit_radar_bucket(radar_by_game_and_type, normalized_limit),
        }

        highlight_weights = {
            "biggest_price_drops": 34.0,
            "new_historical_lows": 32.0,
            "buy_now_opportunities": 30.0,
            "trending_games": 26.0,
            "radar_signals": 24.0,
        }
        highlights_by_game: dict[int, tuple[float, dict]] = {}
        for section_key, items in sections.items():
            section_weight = safe_num(highlight_weights.get(section_key), 0.0)
            for index, item in enumerate(items[: max(3, normalized_limit // 2)]):
                game_id = int(safe_num(item.get("game_id"), 0.0))
                if game_id <= 0:
                    continue
                highlight_score = (
                    safe_num(item.get("priority_score"), 0.0)
                    + section_weight
                    - index * 1.6
                )
                existing = highlights_by_game.get(game_id)
                if existing is None or highlight_score > existing[0]:
                    highlights_by_game[game_id] = (highlight_score, item)

        highlights = [
            item
            for _, item in sorted(
                highlights_by_game.values(),
                key=lambda entry: safe_num(entry[0], 0.0),
                reverse=True,
            )[: max(8, min(16, normalized_limit * 2))]
        ]

        return {
            "user_id": normalized_user_id,
            "personalized": personalization_enabled,
            "window_hours": DAILY_DIGEST_WINDOW_HOURS,
            "window_start": window_start.isoformat(),
            "window_end": now.isoformat(),
            "generated_at": now.isoformat(),
            "counts": {key: len(value) for key, value in sections.items()},
            "biggest_price_drops": sections.get("biggest_price_drops", []),
            "new_historical_lows": sections.get("new_historical_lows", []),
            "buy_now_opportunities": sections.get("buy_now_opportunities", []),
            "trending_games": sections.get("trending_games", []),
            "radar_signals": sections.get("radar_signals", []),
            "sections": sections,
            "highlights": highlights,
        }
    finally:
        session.close()
        _log_timing("/api/daily-digest", started)


@app.get("/api/seo/discovery/{slug}")
@json_etag()
@ttl_cache(ttl_seconds=90, endpoint_key="/api/seo/discovery")
def get_seo_discovery_page(
    request: Request,
    slug: str,
    limit: int = Query(default=60, ge=1, le=120),
):
    started = _start_timer()
    normalized_slug = _normalize_seo_slug(slug)
    page_definition = _get_seo_page_definition(normalized_slug)
    if page_definition is None:
        raise HTTPException(status_code=404, detail="SEO discovery page not found")

    session = ReadSessionLocal()
    try:
        query = _build_seo_discovery_query(session, normalized_slug, model_cls=GameDiscoveryFeed)
        if query is None:
            raise HTTPException(status_code=404, detail="SEO discovery page not found")

        normalized_limit = max(1, int(limit))
        rows = query.limit(normalized_limit).all()
        if len(rows) < normalized_limit:
            fallback_query = _build_seo_discovery_query(session, normalized_slug, model_cls=GameSnapshot)
            if fallback_query is not None:
                fallback_rows = fallback_query.limit(normalized_limit).all()
                if not rows:
                    rows = fallback_rows
                else:
                    seen_game_ids = {int(safe_num(getattr(row, "game_id", 0), 0.0)) for row in rows}
                    for fallback_row in fallback_rows:
                        fallback_game_id = int(safe_num(getattr(fallback_row, "game_id", 0), 0.0))
                        if fallback_game_id <= 0 or fallback_game_id in seen_game_ids:
                            continue
                        rows.append(fallback_row)
                        seen_game_ids.add(fallback_game_id)
                        if len(rows) >= normalized_limit:
                            break
        items = [_serialize_seo_landing_item(row, normalized_slug) for row in rows]
        page_payload = dict(page_definition)
        page_payload["canonical_url"] = _build_canonical_url(page_definition["path"])
        page_payload["slug"] = normalized_slug

        return {
            "slug": normalized_slug,
            "page": page_payload,
            "count": len(items),
            "items": items,
            "generated_at": utc_now().isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/seo/discovery", started)


def _score_personalized_fallback_row(row: dict) -> float:
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return (
        safe_num(row.get("deal_opportunity_score"), 0.0) * 0.42
        + safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0)) * 0.38
        + safe_num(row.get("deal_score"), 0.0) * 0.30
        + safe_num(row.get("trending_score"), 0.0) * 0.24
        + safe_num(row.get("momentum_score"), 0.0) * 0.18
        + safe_num(row.get("popularity_score"), 0.0) * 0.10
        + discount * 0.16
    )


def _compact_personalized_feed_item(row: dict) -> dict:
    is_upcoming = row.get("is_upcoming")
    is_released = row.get("is_released")
    if is_released is None:
        is_released = 0 if bool(is_upcoming) else 1
    compact = {
        "game_id": int(safe_num(row.get("game_id") or row.get("id"), 0.0)),
        "game_name": str(row.get("game_name") or row.get("name") or "").strip(),
        "store_url": row.get("store_url"),
        "banner_url": row.get("banner_url") or row.get("header_image"),
        "price": row.get("price", row.get("latest_price")),
        "original_price": row.get("original_price", row.get("latest_original_price")),
        "discount_percent": row.get("discount_percent", row.get("latest_discount_percent")),
        "is_released": is_released,
        "is_upcoming": is_upcoming,
        "release_date": row.get("release_date"),
        "historical_status": row.get("historical_status"),
        "deal_score": row.get("deal_score"),
        "buy_score": row.get("buy_score", row.get("worth_buying_score")),
        "worth_buying_score": row.get("worth_buying_score"),
        "trending_score": row.get("trending_score"),
        "momentum_score": row.get("momentum_score"),
        "deal_opportunity_score": row.get("deal_opportunity_score"),
        "deal_opportunity_reason": row.get("deal_opportunity_reason"),
        "buy_recommendation": row.get("buy_recommendation"),
        "buy_reason": row.get("buy_reason"),
        "price_vs_low_ratio": row.get("price_vs_low_ratio"),
        "predicted_next_sale_price": row.get("predicted_next_sale_price"),
        "predicted_next_discount_percent": row.get("predicted_next_discount_percent"),
        "predicted_sale_confidence": row.get("predicted_sale_confidence"),
        "review_score": row.get("review_score"),
        "review_score_label": row.get("review_score_label"),
        "current_players": row.get("current_players"),
        "deal_detected_at": row.get("deal_detected_at"),
        "personalization_score": safe_num(row.get("personalization_score"), 0.0),
    }
    if compact["game_id"] <= 0:
        compact["game_id"] = int(safe_num(row.get("id"), 0.0))
    compact["id"] = compact["game_id"]
    return compact


def _build_personalized_fallback_items(session: Session, limit: int) -> list[dict]:
    _, payload = _read_dashboard_cache(session)
    if not isinstance(payload, dict):
        return []
    protected_visible_ids = _collect_protected_deal_game_ids(payload)
    candidate_rows = _released_deal_dashboard_rows(
        [
            *_dashboard_rows(payload, "deal_opportunities", "dealOpportunities"),
            *_dashboard_rows(payload, "worth_buying_now", "worthBuyingNow"),
            *_dashboard_rows(payload, "dealRanked", "topDealsToday"),
            *_dashboard_rows(payload, "trending_now", "trendingDeals", "trending"),
            *_dashboard_rows(payload, "biggest_discounts", "biggestDeals"),
            *_dashboard_rows(payload, "new_historical_lows", "newHistoricalLows"),
        ]
    )
    if not candidate_rows:
        return []
    scored: list[tuple[float, int, dict]] = []
    for index, row in enumerate(candidate_rows):
        if not isinstance(row, dict):
            continue
        game_id = int(safe_num(row.get("game_id") or row.get("id"), 0.0))
        if game_id > 0 and game_id in protected_visible_ids:
            continue
        score = _score_personalized_fallback_row(row)
        normalized = dict(row)
        normalized["personalization_score"] = round(score, 2)
        scored.append((score, -index, normalized))
    scored.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
    items: list[dict] = []
    for _, _, raw_item in scored:
        compact = _compact_personalized_feed_item(raw_item)
        if compact.get("game_id", 0) <= 0:
            continue
        if not str(compact.get("game_name") or "").strip():
            continue
        items.append(compact)
        if len(items) >= int(limit):
            break
    return items


def _collect_protected_deal_game_ids(payload: dict) -> set[int]:
    if not isinstance(payload, dict):
        return set()
    protected_visible_ids: set[int] = set()
    for row in _dedupe_dashboard_rows(
        [
            *_dashboard_rows(payload, "deal_opportunities", "dealOpportunities"),
            *_dashboard_rows(payload, "opportunity_radar", "opportunityRadar"),
            *_dashboard_rows(payload, "worth_buying_now", "worthBuyingNow"),
            *_dashboard_rows(payload, "biggest_discounts", "biggestDeals"),
            *_dashboard_rows(payload, "wait_picks", "waitPicks"),
            *_dashboard_rows(payload, "buy_now_picks", "buyNowPicks"),
        ]
    ):
        game_id = int(safe_num(row.get("game_id") or row.get("id"), 0.0))
        if game_id > 0:
            protected_visible_ids.add(game_id)
    return protected_visible_ids


@app.get("/api/personalized-deals")
@json_etag()
@ttl_cache(ttl_seconds=45, endpoint_key="/api/personalized-deals")
def list_personalized_deals(
    request: Request,
    user_id: str = Query(default=DEFAULT_USER_ID),
    limit: int = Query(default=20, ge=1, le=120),
    summary: bool = Query(default=False),
):
    started = _start_timer()
    normalized_user_id = resolve_request_user_id(request, user_id)
    personalization_enabled = not _is_anonymous_user_id(normalized_user_id)

    session = ReadSessionLocal()
    try:
        normalized_limit = max(1, int(limit))
        now = utc_now()
        recent_cutoff = now - datetime.timedelta(days=21)
        event_cutoff = now - datetime.timedelta(days=30)
        _, home_payload = _read_dashboard_cache(session)
        protected_deal_ids = _collect_protected_deal_game_ids(home_payload if isinstance(home_payload, dict) else {})

        wishlist_rows: list[tuple[int | None, datetime.datetime | None]] = []
        watchlist_rows: list[tuple[int | None, datetime.datetime | None]] = []
        target_rows: list[tuple[int | None, datetime.datetime | None]] = []
        if personalization_enabled:
            wishlist_rows = (
                session.query(WishlistItem.game_id, WishlistItem.created_at)
                .filter(WishlistItem.user_id == normalized_user_id)
                .all()
            )
            watchlist_rows = (
                session.query(Watchlist.game_id, Watchlist.created_at)
                .filter(Watchlist.user_id == normalized_user_id)
                .all()
            )
            target_rows = (
                session.query(DealWatchlist.game_id, DealWatchlist.updated_at)
                .filter(DealWatchlist.user_id == normalized_user_id, DealWatchlist.active.is_(True))
                .all()
            )

        wishlist_game_ids = {int(game_id) for game_id, _ in wishlist_rows if game_id is not None}
        watchlist_game_ids = {int(game_id) for game_id, _ in watchlist_rows if game_id is not None}
        target_game_ids = {int(game_id) for game_id, _ in target_rows if game_id is not None}
        has_personal_seed_data = bool(wishlist_game_ids or watchlist_game_ids or target_game_ids)
        if not personalization_enabled or not has_personal_seed_data:
            fallback_items = _build_personalized_fallback_items(session, normalized_limit)
            return {
                "user_id": normalized_user_id,
                "personalized": False,
                "fallback_mode": True,
                "fallback_reason": "Using bounded shared ranking until enough personal signals are available.",
                "count": len(fallback_items),
                "items": fallback_items,
                "generated_at": now.isoformat(),
            }
        if bool(summary):
            seed_game_id_list = list(wishlist_game_ids | watchlist_game_ids | target_game_ids)
            seed_candidate_limit = max(normalized_limit, min(36, max(12, len(seed_game_id_list) * 6)))
            seed_rows = _query_release_feed_rows(
                session,
                limit=seed_candidate_limit,
                projection_filters=[GameDiscoveryFeed.game_id.in_(seed_game_id_list)],
                snapshot_filters=[GameSnapshot.game_id.in_(seed_game_id_list)],
                projection_order_by=[
                    GameDiscoveryFeed.buy_score.desc().nullslast(),
                    GameDiscoveryFeed.worth_buying_score.desc().nullslast(),
                    GameDiscoveryFeed.deal_opportunity_score.desc().nullslast(),
                    GameDiscoveryFeed.deal_score.desc().nullslast(),
                    GameDiscoveryFeed.updated_at.desc().nullslast(),
                    GameDiscoveryFeed.game_id.asc(),
                ],
                snapshot_order_by=[
                    GameSnapshot.buy_score.desc().nullslast(),
                    GameSnapshot.worth_buying_score.desc().nullslast(),
                    GameSnapshot.deal_opportunity_score.desc().nullslast(),
                    GameSnapshot.deal_score.desc().nullslast(),
                    GameSnapshot.updated_at.desc().nullslast(),
                    GameSnapshot.game_id.asc(),
                ],
            )
            personalized_seed_items: list[dict] = []
            for snapshot in seed_rows:
                item = _build_personalized_deal_item(
                    snapshot,
                    wishlist_game_ids=wishlist_game_ids,
                    watchlist_game_ids=watchlist_game_ids,
                    target_game_ids=target_game_ids,
                    recent_game_ids=set(),
                    token_weights={},
                    recent_event_counts={},
                    personalization_enabled=True,
                    has_personal_seed_data=True,
                )
                if item is None:
                    continue
                compact_item = _compact_personalized_feed_item(item)
                game_id = int(safe_num(compact_item.get("game_id") or compact_item.get("id"), 0.0))
                if game_id > 0 and game_id in protected_deal_ids:
                    continue
                personalized_seed_items.append(compact_item)
            fallback_items = _build_personalized_fallback_items(session, max(normalized_limit * 2, normalized_limit + 8))
            merged_items: list[dict] = []
            seen_game_ids: set[int] = set()
            for item in [*personalized_seed_items, *fallback_items]:
                if not isinstance(item, dict):
                    continue
                game_id = int(safe_num(item.get("game_id") or item.get("id"), 0.0))
                if game_id <= 0 or game_id in seen_game_ids or game_id in protected_deal_ids:
                    continue
                merged_items.append(item)
                seen_game_ids.add(game_id)
                if len(merged_items) >= normalized_limit:
                    break
            personalized_feed = len(personalized_seed_items) > 0
            return {
                "user_id": normalized_user_id,
                "personalized": personalized_feed,
                "fallback_mode": not personalized_feed,
                "fallback_reason": (
                    None
                    if personalized_feed
                    else "Using bounded shared ranking until enough personal signals are available."
                ),
                "count": len(merged_items),
                "items": merged_items,
                "generated_at": now.isoformat(),
            }

        recent_touch_by_game: dict[int, datetime.datetime] = {}
        for game_id, touched_at in [*wishlist_rows, *watchlist_rows, *target_rows]:
            if game_id is None:
                continue
            touched_dt = _coerce_utc_datetime(touched_at)
            if touched_dt is None:
                continue
            game_key = int(game_id)
            existing = recent_touch_by_game.get(game_key)
            if existing is None or touched_dt > existing:
                recent_touch_by_game[game_key] = touched_dt
        recent_game_ids = {
            game_id
            for game_id, touched_at in recent_touch_by_game.items()
            if touched_at >= recent_cutoff
        }

        seed_game_ids = wishlist_game_ids | watchlist_game_ids | target_game_ids
        seed_rows: list[tuple[int, str | None, str | None]] = []
        if personalization_enabled and seed_game_ids:
            seed_rows = [
                (int(game_id), tags, genres)
                for game_id, tags, genres in (
                    session.query(GameSnapshot.game_id, GameSnapshot.tags, GameSnapshot.genres)
                    .filter(GameSnapshot.game_id.in_(list(seed_game_ids)))
                    .all()
                )
            ]

        token_weights = _build_personalization_token_weights(
            seed_rows,
            wishlist_game_ids=wishlist_game_ids,
            watchlist_game_ids=watchlist_game_ids,
            target_game_ids=target_game_ids,
            recent_game_ids=recent_game_ids,
        )

        query_multiplier = max(4, PERSONALIZED_QUERY_MULTIPLIER)
        minimum_candidate_pool = max(PERSONALIZED_MIN_CANDIDATES, normalized_limit * 3)
        candidate_limit = max(
            minimum_candidate_pool,
            min(PERSONALIZED_MAX_CANDIDATES, normalized_limit * query_multiplier),
        )
        candidate_rows = _query_release_feed_rows(
            session,
            limit=candidate_limit,
            projection_filters=[
                or_(
                    GameDiscoveryFeed.latest_discount_percent > 0,
                    GameDiscoveryFeed.deal_score >= 45,
                    GameDiscoveryFeed.buy_score >= 45,
                    GameDiscoveryFeed.worth_buying_score >= 45,
                    GameDiscoveryFeed.deal_opportunity_score >= 45,
                    GameDiscoveryFeed.trending_score >= 45,
                    GameDiscoveryFeed.momentum_score >= 45,
                    GameDiscoveryFeed.popularity_score >= 50,
                    GameDiscoveryFeed.historical_status.in_([
                        "new_historical_low",
                        "matches_historical_low",
                        "near_historical_low",
                    ]),
                ),
            ],
            snapshot_filters=[
                or_(
                    GameSnapshot.latest_discount_percent > 0,
                    GameSnapshot.deal_score >= 45,
                    GameSnapshot.buy_score >= 45,
                    GameSnapshot.worth_buying_score >= 45,
                    GameSnapshot.deal_opportunity_score >= 45,
                    GameSnapshot.trending_score >= 45,
                    GameSnapshot.momentum_score >= 45,
                    GameSnapshot.popularity_score >= 50,
                    GameSnapshot.historical_status.in_([
                        "new_historical_low",
                        "matches_historical_low",
                        "near_historical_low",
                    ]),
                ),
            ],
            projection_order_by=[
                GameDiscoveryFeed.buy_score.desc().nullslast(),
                GameDiscoveryFeed.worth_buying_score.desc().nullslast(),
                GameDiscoveryFeed.deal_score.desc().nullslast(),
                GameDiscoveryFeed.deal_opportunity_score.desc().nullslast(),
                GameDiscoveryFeed.trending_score.desc().nullslast(),
                GameDiscoveryFeed.latest_discount_percent.desc().nullslast(),
                GameDiscoveryFeed.momentum_score.desc().nullslast(),
                GameDiscoveryFeed.popularity_score.desc().nullslast(),
                GameDiscoveryFeed.updated_at.desc().nullslast(),
                GameDiscoveryFeed.game_id.asc(),
            ],
            snapshot_order_by=[
                GameSnapshot.buy_score.desc().nullslast(),
                GameSnapshot.worth_buying_score.desc().nullslast(),
                GameSnapshot.deal_score.desc().nullslast(),
                GameSnapshot.deal_opportunity_score.desc().nullslast(),
                GameSnapshot.trending_score.desc().nullslast(),
                GameSnapshot.latest_discount_percent.desc().nullslast(),
                GameSnapshot.momentum_score.desc().nullslast(),
                GameSnapshot.popularity_score.desc().nullslast(),
                GameSnapshot.updated_at.desc().nullslast(),
                GameSnapshot.game_id.asc(),
            ],
        )

        candidate_game_ids = [int(snapshot.game_id) for snapshot in candidate_rows]
        recent_event_counts: dict[int, int] = {}
        event_scan_limit = max(32, min(64, normalized_limit * 3))
        event_game_ids = candidate_game_ids[:event_scan_limit]
        if event_game_ids:
            event_rows = (
                session.query(
                    DealEvent.game_id,
                    func.count(DealEvent.id),
                )
                .filter(
                    DealEvent.game_id.in_(event_game_ids),
                    DealEvent.created_at >= event_cutoff,
                )
                .group_by(DealEvent.game_id)
                .all()
            )
            recent_event_counts = {
                int(game_id): int(event_count or 0)
                for game_id, event_count in event_rows
                if game_id is not None
            }

        scored_rows: list[tuple[float, datetime.datetime, dict]] = []
        for snapshot in candidate_rows:
            game_id = int(snapshot.game_id)
            if game_id in protected_deal_ids:
                continue
            item = _build_personalized_deal_item(
                snapshot,
                wishlist_game_ids=wishlist_game_ids,
                watchlist_game_ids=watchlist_game_ids,
                target_game_ids=target_game_ids,
                recent_game_ids=recent_game_ids,
                token_weights=token_weights,
                recent_event_counts=recent_event_counts,
                personalization_enabled=personalization_enabled,
                has_personal_seed_data=has_personal_seed_data,
            )
            if item is None:
                continue
            updated_at = _coerce_utc_datetime(snapshot.updated_at) or now
            scored_rows.append((safe_num(item.get("personalization_score"), 0.0), updated_at, item))
        personalized_scored_count = len(scored_rows)

        if len(scored_rows) < normalized_limit:
            fallback_rows = _query_release_feed_rows(
                session,
                limit=candidate_limit,
                projection_filters=[
                    or_(
                        GameDiscoveryFeed.trending_score >= 45,
                        GameDiscoveryFeed.deal_score >= 45,
                        GameDiscoveryFeed.deal_opportunity_score >= 45,
                        GameDiscoveryFeed.latest_discount_percent >= 20,
                        GameDiscoveryFeed.historical_status.in_([
                            "new_historical_low",
                            "matches_historical_low",
                            "near_historical_low",
                        ]),
                    ),
                ],
                snapshot_filters=[
                    or_(
                        GameSnapshot.trending_score >= 45,
                        GameSnapshot.deal_score >= 45,
                        GameSnapshot.deal_opportunity_score >= 45,
                        GameSnapshot.latest_discount_percent >= 20,
                        GameSnapshot.historical_status.in_([
                            "new_historical_low",
                            "matches_historical_low",
                            "near_historical_low",
                        ]),
                    ),
                ],
                projection_order_by=[
                    GameDiscoveryFeed.trending_score.desc().nullslast(),
                    GameDiscoveryFeed.deal_score.desc().nullslast(),
                    GameDiscoveryFeed.deal_opportunity_score.desc().nullslast(),
                    GameDiscoveryFeed.latest_discount_percent.desc().nullslast(),
                    GameDiscoveryFeed.popularity_score.desc().nullslast(),
                    GameDiscoveryFeed.updated_at.desc().nullslast(),
                    GameDiscoveryFeed.game_id.asc(),
                ],
                snapshot_order_by=[
                    GameSnapshot.trending_score.desc().nullslast(),
                    GameSnapshot.deal_score.desc().nullslast(),
                    GameSnapshot.deal_opportunity_score.desc().nullslast(),
                    GameSnapshot.latest_discount_percent.desc().nullslast(),
                    GameSnapshot.popularity_score.desc().nullslast(),
                    GameSnapshot.updated_at.desc().nullslast(),
                    GameSnapshot.game_id.asc(),
                ],
            )
            seen_game_ids = {int(safe_num(item.get("game_id"), 0.0)) for _, _, item in scored_rows}
            for snapshot in fallback_rows:
                game_id = int(snapshot.game_id)
                if game_id in seen_game_ids or game_id in protected_deal_ids:
                    continue
                item = _build_personalized_deal_item(
                    snapshot,
                    wishlist_game_ids=wishlist_game_ids,
                    watchlist_game_ids=watchlist_game_ids,
                    target_game_ids=target_game_ids,
                    recent_game_ids=recent_game_ids,
                    token_weights={},
                    recent_event_counts=recent_event_counts,
                    personalization_enabled=False,
                    has_personal_seed_data=False,
                )
                if item is None:
                    continue
                updated_at = _coerce_utc_datetime(snapshot.updated_at) or now
                scored_rows.append((safe_num(item.get("personalization_score"), 0.0), updated_at, item))
                seen_game_ids.add(game_id)
                if len(scored_rows) >= max(normalized_limit * 2, normalized_limit + 16):
                    break

        scored_rows.sort(key=lambda entry: (entry[0], entry[1]), reverse=True)
        items: list[dict] = []
        seen_items: set[int] = set()
        for _, _, item in scored_rows:
            compact_item = _compact_personalized_feed_item(item)
            game_id = int(safe_num(compact_item.get("game_id") or compact_item.get("id"), 0.0))
            if game_id <= 0 or game_id in seen_items or game_id in protected_deal_ids:
                continue
            items.append(compact_item)
            seen_items.add(game_id)
            if len(items) >= normalized_limit:
                break
        personalized_feed = personalization_enabled and has_personal_seed_data and personalized_scored_count > 0

        return {
            "user_id": normalized_user_id,
            "personalized": personalized_feed,
            "fallback_mode": not personalized_feed,
            "fallback_reason": (
                None
                if personalized_feed
                else "Using trending and high deal-score ranking until more personal signals are available."
            ),
            "count": len(items),
            "items": items,
            "generated_at": now.isoformat(),
        }
    finally:
        session.close()
        _log_timing("/api/personalized-deals", started)


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
def create_wishlist_item(payload: ListItemCreateRequest, request: Request):
    normalized_user_id = resolve_request_user_id(request)
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == payload.game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        existing = (
            session.query(WishlistItem)
            .filter(
                WishlistItem.user_id == normalized_user_id,
                WishlistItem.game_id == game.id,
            )
            .first()
        )
        if existing:
            return serialize_list_item(existing)

        item = WishlistItem(user_id=normalized_user_id, game_id=game.id, game_name=game.name)
        session.add(item)
        session.commit()
        session.refresh(item)
        return serialize_list_item(item)
    finally:
        session.close()


@app.delete("/wishlist/{game_name}")
def delete_wishlist_item(game_name: str, request: Request):
    normalized_user_id = resolve_request_user_id(request)
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
        row = (
            session.query(WishlistItem)
            .filter(WishlistItem.user_id == normalized_user_id, WishlistItem.game_id == game.id)
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
def wishlist_add(payload: WishlistMutationRequest, request: Request):
    session = Session()
    try:
        user_id = resolve_request_user_id(request, payload.user_id)

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
def wishlist_remove(payload: WishlistMutationRequest, request: Request):
    session = Session()
    try:
        user_id = resolve_request_user_id(request, payload.user_id)
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
def list_user_wishlist(request: Request, user_id: str):
    normalized_user_id = resolve_request_user_id(request, user_id)
    session = Session()
    try:
        rows = (
            session.query(WishlistItem, Game, GameSnapshot)
            .outerjoin(Game, Game.id == WishlistItem.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == WishlistItem.game_id)
            .filter(WishlistItem.user_id == normalized_user_id)
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
def add_deal_watchlist(payload: DealWatchlistAddRequest, request: Request):
    session = Session()
    try:
        user_id = resolve_request_user_id(request, payload.user_id)
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
def remove_deal_watchlist(payload: DealWatchlistRemoveRequest, request: Request):
    session = Session()
    try:
        user_id = resolve_request_user_id(request, payload.user_id)
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
def list_deal_watchlists(request: Request, user_id: str):
    normalized_user_id = resolve_request_user_id(request, user_id)
    session = Session()
    try:
        rows = (
            session.query(DealWatchlist, Game, GameSnapshot)
            .outerjoin(Game, Game.id == DealWatchlist.game_id)
            .outerjoin(GameSnapshot, GameSnapshot.game_id == DealWatchlist.game_id)
            .filter(DealWatchlist.user_id == normalized_user_id, DealWatchlist.active.is_(True))
            .order_by(DealWatchlist.updated_at.desc(), DealWatchlist.id.desc())
            .all()
        )
        return [
            {
                "id": int(row.id),
                "user_id": row.user_id,
                "game_id": int(row.game_id),
                "game_name": snapshot.game_name if snapshot else (game.name if game else None),
                "slug": _canonical_game_slug(
                    snapshot.game_name if snapshot else (game.name if game else None),
                    row.game_id,
                ),
                "game_slug": _canonical_game_slug(
                    snapshot.game_name if snapshot else (game.name if game else None),
                    row.game_id,
                ),
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
def list_watchlist_api(request: Request, user_id: str = Query(default=DEFAULT_USER_ID)):
    normalized_user_id = resolve_request_user_id(request, user_id)
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
def create_watchlist_api(payload: WatchlistMutationRequest, request: Request):
    session = Session()
    try:
        user_id = resolve_request_user_id(request, payload.user_id)
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
def delete_watchlist_api(
    game_id: int,
    request: Request,
    user_id: str = Query(default=DEFAULT_USER_ID),
):
    normalized_user_id = resolve_request_user_id(request, user_id)
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
def list_watchlist(request: Request):
    response = list_watchlist_api(request, DEFAULT_USER_ID)
    return response["items"]


@app.post("/watchlist/items")
def create_watchlist_item(payload: ListItemCreateRequest, request: Request):
    normalized_user_id = resolve_request_user_id(request)
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == payload.game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
    finally:
        session.close()
    response = create_watchlist_api(
        WatchlistMutationRequest(user_id=normalized_user_id, game_id=int(game.id)),
        request,
    )
    return response.get("item") or {"game_name": payload.game_name, "game_id": int(game.id)}


@app.delete("/watchlist/items/{game_name}")
def delete_watchlist_item(game_name: str, request: Request):
    session = Session()
    try:
        game = session.query(Game).filter(Game.name == game_name).first()
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")
    finally:
        session.close()
    response = delete_watchlist_api(int(game.id), request, DEFAULT_USER_ID)
    return {
        "ok": bool(response.get("ok")),
        "deleted": bool(response.get("deleted")),
        "game_name": game_name,
        "game_id": int(game.id),
    }
