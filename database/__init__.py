from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse

from config import (
    DATABASE_URL_SOURCE,
    DATABASE_URL_DIRECT,
    DATABASE_URL_POOLED,
    DATABASE_URL_READ_REPLICA,
    SQLALCHEMY_ENGINE_OPTIONS,
)


def _build_engine(url: str):
    return create_engine(url, **SQLALCHEMY_ENGINE_OPTIONS)


def _log_database_target(url: str) -> None:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    port = parsed.port or 5432
    database = (parsed.path or "").lstrip("/") or "unknown"
    print(f"Database connection: {host}:{port}/{database} (source={DATABASE_URL_SOURCE})")


_log_database_target(DATABASE_URL_POOLED)

# Pooled connection is for high-concurrency app traffic.
runtime_engine = _build_engine(DATABASE_URL_POOLED)

# Direct connection is for migrations / DDL / admin tasks.
direct_engine = _build_engine(DATABASE_URL_DIRECT)

# Optional read replica for read-only endpoints. Falls back to pooled primary.
read_engine = _build_engine(DATABASE_URL_READ_REPLICA) if DATABASE_URL_READ_REPLICA else runtime_engine

RuntimeSessionLocal = sessionmaker(bind=runtime_engine, autocommit=False, autoflush=False)
DirectSessionLocal = sessionmaker(bind=direct_engine, autocommit=False, autoflush=False)
ReadSessionLocal = sessionmaker(bind=read_engine, autocommit=False, autoflush=False)
