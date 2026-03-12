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
