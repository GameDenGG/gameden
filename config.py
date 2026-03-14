import os
from urllib.parse import urlsplit
from dotenv import load_dotenv

load_dotenv()


def get_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except ValueError:
        return default


def get_float(value: str | None, default: float) -> float:
    try:
        return float(value) if value is not None else default
    except ValueError:
        return default


def get_env_with_alias(primary_name: str, *alias_names: str) -> str | None:
    for env_name in (primary_name, *alias_names):
        env_value = os.getenv(env_name)
        if env_value is not None:
            return env_value
    return None


def normalize_origin(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    if value == "*":
        return "*"
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def parse_origin_list(raw_value: str | None, default: list[str]) -> list[str]:
    values = raw_value.split(",") if raw_value is not None else default
    normalized_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_origin(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(normalized)
    return normalized_values


_DEFAULT_DATABASE_URL = (
    "postgresql://postgres:YOURPASSWORD@localhost:5432/newworld"
)

_DEFAULT_DISPLAY_SITE_NAME = "GameDen.gg"
_DEFAULT_SITE_URL = "https://gameden.gg"
_DEFAULT_SITE_DESCRIPTION = (
    "Discover game deals, analytics, player trends, and price history on GameDen.gg."
)

# Canonical database URL source of truth:
# 1) use DATABASE_URL if set
# 2) otherwise use local development Postgres fallback
_ENV_DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_URL = _ENV_DATABASE_URL or _DEFAULT_DATABASE_URL
DATABASE_URL_SOURCE = "environment" if _ENV_DATABASE_URL else "local_fallback"

# Keep compatibility with existing engine/session split.
DATABASE_URL_POOLED = DATABASE_URL
DATABASE_URL_DIRECT = DATABASE_URL

# Optional read replica URL for read-only endpoints.
DATABASE_URL_READ_REPLICA = os.getenv("DATABASE_URL_READ_REPLICA")


def normalize_site_url(raw_value: str | None, default: str = _DEFAULT_SITE_URL) -> str:
    value = (raw_value or default).strip()
    if not value:
        value = default
    if "://" not in value:
        value = f"https://{value}"
    return value.rstrip("/")


DISPLAY_SITE_NAME = (
    (os.getenv("DISPLAY_SITE_NAME") or os.getenv("SITE_NAME") or _DEFAULT_DISPLAY_SITE_NAME).strip()
    or _DEFAULT_DISPLAY_SITE_NAME
)
# Keep `SITE_NAME` as the compatibility name used by existing modules/routes.
SITE_NAME = DISPLAY_SITE_NAME
SITE_URL = normalize_site_url(os.getenv("SITE_URL"))
SITE_DESCRIPTION = (
    (os.getenv("SITE_DESCRIPTION") or _DEFAULT_SITE_DESCRIPTION).strip()
    or _DEFAULT_SITE_DESCRIPTION
)
SITE_HOST = (urlsplit(SITE_URL).hostname or "gameden.gg").lower()

_site_hosts = {SITE_HOST}
if SITE_HOST.startswith("www.") and len(SITE_HOST) > 4:
    _site_hosts.add(SITE_HOST[4:])
else:
    _site_hosts.add(f"www.{SITE_HOST}")
_default_site_origins = [f"https://{host}" for host in sorted(_site_hosts)]
if SITE_URL not in _default_site_origins:
    _default_site_origins.insert(0, SITE_URL)
_DEFAULT_CORS_ALLOW_ORIGINS = _default_site_origins + [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
CORS_ALLOW_ALL_ORIGINS = get_bool(os.getenv("CORS_ALLOW_ALL_ORIGINS"), False)
if CORS_ALLOW_ALL_ORIGINS:
    CORS_ALLOW_ORIGINS = ["*"]
else:
    CORS_ALLOW_ORIGINS = parse_origin_list(os.getenv("CORS_ALLOW_ORIGINS"), _DEFAULT_CORS_ALLOW_ORIGINS)
if not CORS_ALLOW_ORIGINS:
    CORS_ALLOW_ORIGINS = [SITE_URL]

CANONICAL_HOST_REDIRECT = get_bool(os.getenv("CANONICAL_HOST_REDIRECT"), False)
_CANONICAL_REDIRECT_HOSTS_RAW = os.getenv("CANONICAL_REDIRECT_HOSTS", "www.gameden.gg")
CANONICAL_REDIRECT_HOSTS = {
    host.strip().lower()
    for host in _CANONICAL_REDIRECT_HOSTS_RAW.split(",")
    if host.strip()
}
CANONICAL_REDIRECT_HOSTS.discard(SITE_HOST)

PRICE_CHECK_INTERVAL_MINUTES = get_int(
    os.getenv("PRICE_CHECK_INTERVAL_MINUTES"),
    60,
)

EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")

STEAM_USER_AGENT = os.getenv("STEAM_USER_AGENT", "Mozilla/5.0")

DEBUG = get_bool(os.getenv("DEBUG"), True)

SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 1800,
    "pool_size": 5,
    "max_overflow": 10,
    "pool_timeout": 30,
    "future": True,
}

# Shared API runtime defaults.
API_DEFAULT_USER_ID = (
    (
        get_env_with_alias("API_DEFAULT_USER_ID", "DEFAULT_USER_ID")
        or "legacy-user"
    ).strip()
    or "legacy-user"
)
API_DASHBOARD_CACHE_STALE_MINUTES = max(
    1,
    get_int(get_env_with_alias("API_DASHBOARD_CACHE_STALE_MINUTES", "DASHBOARD_CACHE_STALE_MINUTES"), 20),
)
API_SEARCH_SIMILARITY_THRESHOLD = max(
    0.0,
    get_float(get_env_with_alias("API_SEARCH_SIMILARITY_THRESHOLD", "SEARCH_SIMILARITY_THRESHOLD"), 0.18),
)
API_DEFAULT_PAGE_SIZE = max(
    1,
    get_int(get_env_with_alias("API_DEFAULT_PAGE_SIZE", "DEFAULT_PAGE_SIZE"), 24),
)
API_MAX_PAGE_SIZE = max(
    API_DEFAULT_PAGE_SIZE,
    get_int(get_env_with_alias("API_MAX_PAGE_SIZE", "MAX_PAGE_SIZE"), 100),
)
API_DEFAULT_LIST_LIMIT = max(
    1,
    get_int(get_env_with_alias("API_DEFAULT_LIST_LIMIT", "DEFAULT_LIST_LIMIT"), 50),
)
API_MAX_LIST_LIMIT = max(
    API_DEFAULT_LIST_LIMIT,
    get_int(get_env_with_alias("API_MAX_LIST_LIMIT", "MAX_LIST_LIMIT"), 200),
)
API_DEFAULT_HISTORY_POINTS = max(
    1,
    get_int(get_env_with_alias("API_DEFAULT_HISTORY_POINTS", "DEFAULT_HISTORY_POINTS"), 120),
)
API_MAX_HISTORY_POINTS = max(
    API_DEFAULT_HISTORY_POINTS,
    get_int(get_env_with_alias("API_MAX_HISTORY_POINTS", "MAX_HISTORY_POINTS"), 240),
)

# Shared ingestion runtime settings.
INGESTION_GAMES_PER_RUN_DEFAULT = 600
INGESTION_GAMES_PER_RUN_LIMIT_DEFAULT = 1000
INGESTION_MIN_DELAY_SECONDS_DEFAULT = 0.05
INGESTION_MAX_DELAY_SECONDS_DEFAULT = 0.20
INGESTION_REQUEST_RETRIES_DEFAULT = 2
INGESTION_LOOP_INTERVAL_SECONDS_DEFAULT = 300
INGESTION_RAW_GAMES_PER_RUN = max(
    0,
    get_int(
        get_env_with_alias("TRACK_GAMES_PER_RUN", "INGESTION_BATCH_SIZE"),
        INGESTION_GAMES_PER_RUN_DEFAULT,
    ),
)
INGESTION_GAMES_PER_RUN_LIMIT = get_int(
    get_env_with_alias("TRACK_GAMES_PER_RUN_LIMIT", "INGESTION_BATCH_SIZE_LIMIT"),
    INGESTION_GAMES_PER_RUN_LIMIT_DEFAULT,
)
if INGESTION_GAMES_PER_RUN_LIMIT > 0:
    INGESTION_GAMES_PER_RUN = min(INGESTION_RAW_GAMES_PER_RUN, INGESTION_GAMES_PER_RUN_LIMIT)
else:
    INGESTION_GAMES_PER_RUN = INGESTION_RAW_GAMES_PER_RUN
INGESTION_MIN_DELAY_SECONDS = max(
    0.0,
    get_float(os.getenv("TRACK_MIN_DELAY_SECONDS"), INGESTION_MIN_DELAY_SECONDS_DEFAULT),
)
INGESTION_MAX_DELAY_SECONDS = max(
    0.0,
    get_float(os.getenv("TRACK_MAX_DELAY_SECONDS"), INGESTION_MAX_DELAY_SECONDS_DEFAULT),
)
INGESTION_REQUEST_RETRIES = max(
    0,
    get_int(os.getenv("TRACK_REQUEST_RETRIES"), INGESTION_REQUEST_RETRIES_DEFAULT),
)
INGESTION_SHARD_TOTAL = max(
    1,
    get_int(os.getenv("TRACK_SHARD_TOTAL"), 1),
)
INGESTION_SHARD_INDEX = max(
    0,
    get_int(os.getenv("TRACK_SHARD_INDEX"), 0),
)
INGESTION_LOOP_INTERVAL_SECONDS = max(
    5,
    get_int(os.getenv("INGESTION_LOOP_INTERVAL_SECONDS"), INGESTION_LOOP_INTERVAL_SECONDS_DEFAULT),
)
INGESTION_ROLLOUT_HOLD_TIER = (
    (os.getenv("TRACK_ROLLOUT_HOLD_TIER", "ROLLOUT_HOLD").strip().upper()) or "ROLLOUT_HOLD"
)
INGESTION_INCLUDE_ROLLOUT_HOLD = get_bool(os.getenv("TRACK_INCLUDE_ROLLOUT_HOLD"), False)
HOT_PLAYER_THRESHOLD = max(
    1,
    get_int(os.getenv("TRACK_HOT_MIN_PLAYERS"), 7500),
)
MEDIUM_PLAYER_THRESHOLD = max(
    1,
    get_int(os.getenv("TRACK_MEDIUM_MIN_PLAYERS"), 1200),
)
HOT_REFRESH_MINUTES = max(
    5,
    get_int(os.getenv("TRACK_HOT_REFRESH_MINUTES"), 20),
)
MEDIUM_REFRESH_MINUTES = max(
    30,
    get_int(os.getenv("TRACK_MEDIUM_REFRESH_MINUTES"), 180),
)
COLD_REFRESH_MINUTES = max(
    120,
    get_int(os.getenv("TRACK_COLD_REFRESH_MINUTES"), 1440),
)

# Shared snapshot worker runtime settings.
SNAPSHOT_MIN_BATCH_SIZE = 1
SNAPSHOT_MAX_BATCH_SIZE_FLOOR = 100
SNAPSHOT_BATCH_SIZE = max(
    SNAPSHOT_MIN_BATCH_SIZE,
    get_int(os.getenv("SNAPSHOT_BATCH_SIZE"), 1000),
)
SNAPSHOT_MAX_BATCH_SIZE = max(
    SNAPSHOT_MAX_BATCH_SIZE_FLOOR,
    get_int(os.getenv("SNAPSHOT_MAX_BATCH_SIZE"), 5000),
)
if SNAPSHOT_BATCH_SIZE > SNAPSHOT_MAX_BATCH_SIZE:
    SNAPSHOT_BATCH_SIZE = SNAPSHOT_MAX_BATCH_SIZE
DIRTY_QUEUE_FETCH_SIZE = max(
    SNAPSHOT_MIN_BATCH_SIZE,
    min(
        get_int(os.getenv("DIRTY_QUEUE_FETCH_SIZE"), SNAPSHOT_BATCH_SIZE),
        SNAPSHOT_MAX_BATCH_SIZE,
    ),
)
SNAPSHOT_IDLE_SLEEP_SECONDS = max(
    1,
    get_int(os.getenv("SNAPSHOT_IDLE_SLEEP_SECONDS"), 10),
)
SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES = max(
    1,
    get_int(os.getenv("SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES"), 3),
)
SNAPSHOT_RETRY_BACKOFF_BASE_SECONDS = max(
    1.0,
    get_float(os.getenv("SNAPSHOT_RETRY_BACKOFF_BASE_SECONDS"), 30.0),
)
SNAPSHOT_RETRY_BACKOFF_MAX_SECONDS = max(
    SNAPSHOT_RETRY_BACKOFF_BASE_SECONDS,
    get_float(os.getenv("SNAPSHOT_RETRY_BACKOFF_MAX_SECONDS"), 3600.0),
)
SNAPSHOT_RETRY_BACKOFF_EXPONENT_CAP = max(
    1,
    get_int(os.getenv("SNAPSHOT_RETRY_BACKOFF_EXPONENT_CAP"), 10),
)
SNAPSHOT_SPARKLINE_POINTS = max(
    10,
    get_int(os.getenv("SNAPSHOT_SPARKLINE_POINTS"), 60),
)
SNAPSHOT_SALE_EVENTS_MAX = max(
    1,
    get_int(os.getenv("SNAPSHOT_SALE_EVENTS_MAX"), 24),
)
SNAPSHOT_SALE_EVENT_GAP_DAYS = max(
    3,
    get_int(os.getenv("SALE_EVENT_GAP_DAYS"), 5),
)
SNAPSHOT_PREDICTION_SALE_HISTORY_LIMIT = max(
    SNAPSHOT_SALE_EVENTS_MAX * 5,
    get_int(os.getenv("PREDICTION_SALE_HISTORY_LIMIT"), 120),
)
SNAPSHOT_UPCOMING_LIMIT = max(
    1,
    get_int(os.getenv("SNAPSHOT_UPCOMING_LIMIT"), 250),
)
SNAPSHOT_HOMEPAGE_RAIL_LIMIT = max(
    1,
    get_int(os.getenv("SNAPSHOT_HOMEPAGE_RAIL_LIMIT"), 24),
)
SNAPSHOT_HOMEPAGE_DEAL_CANDIDATE_POOL = max(
    SNAPSHOT_HOMEPAGE_RAIL_LIMIT,
    get_int(os.getenv("HOMEPAGE_DEAL_CANDIDATE_POOL"), 384),
)
SNAPSHOT_HOMEPAGE_DIVERSITY_WINDOW = max(
    1,
    get_int(os.getenv("SNAPSHOT_HOMEPAGE_DIVERSITY_WINDOW"), 12),
)
SNAPSHOT_ALERT_DEDUPE_HOURS = max(
    1,
    get_int(os.getenv("ALERT_DEDUPE_HOURS"), 6),
)
SNAPSHOT_DEAL_RADAR_LIMIT = max(
    10,
    get_int(os.getenv("DEAL_RADAR_LIMIT"), 50),
)
SNAPSHOT_DEAL_RADAR_LOOKBACK_DAYS = max(
    1,
    get_int(os.getenv("DEAL_RADAR_LOOKBACK_DAYS"), 3),
)
SNAPSHOT_DEAL_RADAR_ALERT_SCAN_LIMIT = max(
    SNAPSHOT_DEAL_RADAR_LIMIT * 4,
    get_int(os.getenv("DEAL_RADAR_ALERT_SCAN_LIMIT"), 320),
)
SNAPSHOT_DEAL_RADAR_TRENDING_POOL = max(
    SNAPSHOT_DEAL_RADAR_LIMIT * 2,
    get_int(os.getenv("DEAL_RADAR_TRENDING_POOL"), 120),
)
SNAPSHOT_DEAL_RADAR_POPULAR_POOL = max(
    SNAPSHOT_DEAL_RADAR_LIMIT * 2,
    get_int(os.getenv("DEAL_RADAR_POPULAR_POOL"), 120),
)
SNAPSHOT_DEAL_RADAR_DISCOUNT_POOL = max(
    SNAPSHOT_DEAL_RADAR_LIMIT * 3,
    get_int(os.getenv("DEAL_RADAR_DISCOUNT_POOL"), 200),
)
SNAPSHOT_DEAL_RADAR_MAX_PER_SIGNAL = max(
    3,
    get_int(os.getenv("DEAL_RADAR_MAX_PER_SIGNAL"), 16),
)
SNAPSHOT_DEAL_RADAR_MAX_SIGNAL_SHARE = max(
    0.20,
    min(0.60, get_float(os.getenv("DEAL_RADAR_MAX_SIGNAL_SHARE"), 0.30)),
)
SNAPSHOT_DEAL_RADAR_DIVERSITY_WINDOW = max(
    8,
    get_int(os.getenv("DEAL_RADAR_DIVERSITY_WINDOW"), 16),
)
SNAPSHOT_DEAL_RADAR_MIN_SIGNAL_CATEGORIES = max(
    4,
    min(5, get_int(os.getenv("DEAL_RADAR_MIN_SIGNAL_CATEGORIES"), 5)),
)
SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_ABS = max(
    0.5,
    get_float(os.getenv("DEAL_RADAR_BIG_DROP_MIN_ABS"), 2.0),
)
SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_PCT = max(
    5.0,
    get_float(os.getenv("DEAL_RADAR_BIG_DROP_MIN_PCT"), 15.0),
)


def validate_settings() -> None:
    errors: list[str] = []

    if not DATABASE_URL:
        errors.append("DATABASE_URL is missing.")

    parsed_site_url = urlsplit(SITE_URL)
    if not parsed_site_url.scheme or not parsed_site_url.netloc:
        errors.append("SITE_URL must be an absolute URL.")

    if PRICE_CHECK_INTERVAL_MINUTES <= 0:
        errors.append("PRICE_CHECK_INTERVAL_MINUTES must be greater than 0.")

    if not CORS_ALLOW_ALL_ORIGINS and not CORS_ALLOW_ORIGINS:
        errors.append(
            "CORS_ALLOW_ORIGINS must include at least one origin when CORS_ALLOW_ALL_ORIGINS is disabled."
        )

    if not STEAM_USER_AGENT:
        errors.append("STEAM_USER_AGENT is missing.")

    if EMAIL_USER and not EMAIL_PASSWORD:
        errors.append("EMAIL_PASSWORD is missing but EMAIL_USER is set.")

    if EMAIL_PASSWORD and not EMAIL_USER:
        errors.append("EMAIL_USER is missing but EMAIL_PASSWORD is set.")

    if errors:
        joined = "\n".join(f"- {error}" for error in errors)
        raise ValueError(f"Invalid configuration:\n{joined}")
