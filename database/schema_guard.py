from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine


REQUIRED_SCALE_COLUMNS: dict[str, set[str]] = {
    "games": {
        "id",
        "name",
        "is_released",
        "next_refresh_at",
        "priority_tier",
        "last_player_count",
        "popularity_score",
    },
    "dirty_games": {
        "game_id",
        "reason",
        "first_seen_at",
        "last_seen_at",
        "updated_at",
        "retry_count",
        "locked_at",
        "locked_by",
        "next_attempt_at",
    },
    "game_prices": {
        "id",
        "game_id",
        "price",
        "recorded_at",
    },
    "game_player_history": {
        "id",
        "game_id",
        "current_players",
        "recorded_at",
    },
    "latest_game_prices": {
        "game_id",
        "latest_price",
        "latest_discount_percent",
        "current_players",
        "recorded_at",
    },
    "game_snapshots": {
        "game_id",
        "game_name",
        "latest_price",
        "latest_discount_percent",
        "deal_score",
        "worth_buying_score",
        "momentum_score",
        "updated_at",
    },
    "dashboard_cache": {
        "key",
        "payload",
        "updated_at",
    },
    "alerts": {
        "id",
        "game_id",
        "alert_type",
        "created_at",
    },
}

REQUIRED_INDEXES_COMMON: set[str] = {
    "ix_games_is_released_name",
    "ix_games_next_refresh_at",
    "ix_games_priority_tier_next_refresh",
    "ix_games_popularity_score_desc",
    "ix_dirty_games_next_attempt_at",
    "ix_dirty_games_next_attempt_updated",
    "ix_game_prices_game_id_recorded_id_desc",
    "ix_player_history_game_recorded_desc",
    "ix_game_snapshots_released_discovery",
    "uq_latest_game_prices_game_id",
    "uq_game_snapshots_game_id",
    "idx_alerts_game_type_created",
    "idx_alerts_created_at",
}

REQUIRED_INDEXES_POSTGRES_ONLY: set[str] = {
    "ix_games_name_trgm",
    "ix_games_developer_trgm",
    "ix_games_publisher_trgm",
}


@dataclass
class SchemaReadinessReport:
    dialect: str
    missing_tables: list[str] = field(default_factory=list)
    missing_columns: dict[str, list[str]] = field(default_factory=dict)
    missing_indexes: list[str] = field(default_factory=list)
    dirty_queue_primary_key_ok: bool = True
    warnings: list[str] = field(default_factory=list)

    @property
    def is_ready(self) -> bool:
        return (
            not self.missing_tables
            and not self.missing_columns
            and not self.missing_indexes
            and self.dirty_queue_primary_key_ok
        )

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["is_ready"] = self.is_ready
        return payload


def _as_sorted_lower(values: Iterable[str]) -> list[str]:
    return sorted({str(value).strip().lower() for value in values if str(value).strip()})


def _load_columns_postgres(conn) -> dict[str, set[str]]:
    rows = conn.execute(
        text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
            """
        )
    ).fetchall()
    columns: dict[str, set[str]] = {}
    for table_name, column_name in rows:
        table_key = str(table_name).lower()
        columns.setdefault(table_key, set()).add(str(column_name).lower())
    return columns


def _load_columns_sqlite(conn) -> dict[str, set[str]]:
    tables = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type = 'table'")
    ).fetchall()
    columns: dict[str, set[str]] = {}
    for (table_name,) in tables:
        table_key = str(table_name).lower()
        pragma_rows = conn.execute(
            text(f"PRAGMA table_info('{table_name}')")
        ).fetchall()
        columns[table_key] = {str(row[1]).lower() for row in pragma_rows}
    return columns


def _load_indexes_postgres(conn) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = ANY(current_schemas(false))
            """
        )
    ).fetchall()
    return {str(row[0]).lower() for row in rows}


def _load_indexes_sqlite(conn) -> set[str]:
    tables = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type = 'table'")
    ).fetchall()
    indexes: set[str] = set()
    for (table_name,) in tables:
        rows = conn.execute(text(f"PRAGMA index_list('{table_name}')")).fetchall()
        for row in rows:
            # PRAGMA index_list columns: seq, name, unique, origin, partial
            indexes.add(str(row[1]).lower())
    return indexes


def _dirty_queue_primary_key_ok_postgres(conn) -> bool:
    row = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = current_schema()
              AND tc.table_name = 'dirty_games'
              AND tc.constraint_type = 'PRIMARY KEY'
              AND kcu.column_name = 'game_id'
            """
        )
    ).scalar()
    return bool(row)


def _dirty_queue_primary_key_ok_sqlite(conn) -> bool:
    rows = conn.execute(text("PRAGMA table_info('dirty_games')")).fetchall()
    for row in rows:
        column_name = str(row[1]).lower()
        is_pk = int(row[5] or 0) > 0
        if column_name == "game_id" and is_pk:
            return True
    return False


def validate_scale_schema(engine: Engine) -> SchemaReadinessReport:
    dialect = str(engine.dialect.name).lower()
    with engine.connect() as conn:
        if dialect == "postgresql":
            columns = _load_columns_postgres(conn)
            indexes = _load_indexes_postgres(conn)
            dirty_queue_pk_ok = _dirty_queue_primary_key_ok_postgres(conn)
        else:
            columns = _load_columns_sqlite(conn)
            indexes = _load_indexes_sqlite(conn)
            dirty_queue_pk_ok = _dirty_queue_primary_key_ok_sqlite(conn)

    report = SchemaReadinessReport(dialect=dialect, dirty_queue_primary_key_ok=dirty_queue_pk_ok)

    required_tables = set(REQUIRED_SCALE_COLUMNS.keys())
    present_tables = set(columns.keys())
    report.missing_tables = _as_sorted_lower(required_tables - present_tables)

    for table_name, expected_columns in REQUIRED_SCALE_COLUMNS.items():
        table_columns = columns.get(table_name, set())
        missing = expected_columns - table_columns
        if missing:
            report.missing_columns[table_name] = _as_sorted_lower(missing)

    required_indexes = set(REQUIRED_INDEXES_COMMON)
    if dialect == "postgresql":
        required_indexes |= REQUIRED_INDEXES_POSTGRES_ONLY
    missing_indexes = required_indexes - indexes
    report.missing_indexes = _as_sorted_lower(missing_indexes)

    if not report.dirty_queue_primary_key_ok:
        report.warnings.append("dirty_games.game_id is not enforced as primary key")

    return report


def assert_scale_schema_ready(engine: Engine, *, component_name: str) -> SchemaReadinessReport:
    report = validate_scale_schema(engine)
    if report.is_ready:
        return report

    details: list[str] = []
    if report.missing_tables:
        details.append(f"missing_tables={', '.join(report.missing_tables)}")
    if report.missing_columns:
        formatted = ", ".join(
            f"{table}:[{', '.join(columns)}]"
            for table, columns in sorted(report.missing_columns.items())
        )
        details.append(f"missing_columns={formatted}")
    if report.missing_indexes:
        details.append(f"missing_indexes={', '.join(report.missing_indexes)}")
    if not report.dirty_queue_primary_key_ok:
        details.append("dirty_games primary key on game_id missing")

    detail_text = "; ".join(details) if details else "unknown schema mismatch"
    raise RuntimeError(
        f"{component_name} cannot start because required scale schema is missing ({detail_text}). "
        "Run: python setup_database.py"
    )
