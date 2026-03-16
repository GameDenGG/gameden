from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory

from database import direct_engine
from database.models import Base


@dataclass(frozen=True)
class RevisionStatus:
    current_heads: tuple[str, ...]
    target_heads: tuple[str, ...]

    @property
    def is_current(self) -> bool:
        return set(self.current_heads) == set(self.target_heads)


def _load_alembic_config() -> Config:
    config = Config("alembic.ini")
    env_database_url = os.getenv("DATABASE_URL")
    if env_database_url:
        config.set_main_option("sqlalchemy.url", env_database_url.replace("%", "%%"))
    return config


def get_revision_status() -> RevisionStatus:
    alembic_config = _load_alembic_config()
    script_dir = ScriptDirectory.from_config(alembic_config)
    target_heads = tuple(sorted(script_dir.get_heads()))
    with direct_engine.connect() as connection:
        migration_context = MigrationContext.configure(connection)
        current_heads = tuple(sorted(migration_context.get_current_heads()))
    return RevisionStatus(current_heads=current_heads, target_heads=target_heads)


def assert_database_revision_current(*, component_name: str, logger: Any | None = None) -> RevisionStatus:
    status = get_revision_status()
    if status.is_current:
        if logger:
            logger.info(
                "Database migration status: OK (component=%s current=%s head=%s)",
                component_name,
                list(status.current_heads),
                list(status.target_heads),
            )
        return status

    message = (
        "Database migration status: OUTDATED "
        f"(component={component_name} current={list(status.current_heads)} head={list(status.target_heads)}). "
        "Run alembic upgrade head."
    )
    if logger:
        logger.error(message)
    raise RuntimeError("Database schema is outdated. Run alembic upgrade head.")


def get_model_drift_diffs() -> list[Any]:
    with direct_engine.connect() as connection:
        migration_context = MigrationContext.configure(
            connection,
            opts={
                "target_metadata": Base.metadata,
                "compare_type": True,
                "compare_server_default": False,
            },
        )
        diffs = compare_metadata(migration_context, Base.metadata)
    return [diff for diff in diffs if _is_relevant_drift(diff)]


def _is_relevant_drift(diff: Any) -> bool:
    kind = _extract_diff_kind(diff)
    if kind is None:
        return True

    # Extra compatibility indexes may exist in DB by design; don't fail drift checks on these.
    if kind == "remove_index":
        return False

    # Default-expression drift is noisy across Postgres versions/dialects.
    if kind == "modify_default":
        return False

    # Ignore TEXT <-> VARCHAR normalization noise.
    if kind == "modify_type":
        try:
            existing_type = str(_extract_existing_type(diff)).lower()
            target_type = str(_extract_target_type(diff)).lower()
            if "text" in existing_type and "varchar" in target_type:
                return False
        except Exception:
            return True

    return True


def _extract_diff_kind(diff: Any) -> str | None:
    if isinstance(diff, tuple) and diff and isinstance(diff[0], str):
        return diff[0]
    if isinstance(diff, list) and diff and isinstance(diff[0], tuple) and diff[0] and isinstance(diff[0][0], str):
        return diff[0][0]
    return None


def _extract_existing_type(diff: Any) -> Any:
    # Alembic modify_type tuple layout:
    # (kind, schema, table_name, column_name, existing_kw, existing_type, target_type)
    if isinstance(diff, list):
        diff = diff[0]
    return diff[5]


def _extract_target_type(diff: Any) -> Any:
    if isinstance(diff, list):
        diff = diff[0]
    return diff[6]


def warn_if_model_schema_drift(*, component_name: str, logger: Any | None = None) -> list[Any]:
    diffs = get_model_drift_diffs()
    if diffs:
        message = (
            "Model/schema drift detected without migration "
            f"(component={component_name} diff_count={len(diffs)}). "
            "Create an Alembic migration for structural model changes."
        )
        if logger:
            logger.warning(message)
            for diff in diffs[:10]:
                logger.warning("Migration drift diff: %s", diff)
        return diffs

    if logger:
        logger.info("Model/schema drift check: clean (component=%s)", component_name)
    return diffs
