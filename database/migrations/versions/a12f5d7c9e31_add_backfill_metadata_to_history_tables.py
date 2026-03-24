"""add backfill metadata to history tables

Revision ID: a12f5d7c9e31
Revises: f3e1a6c9b2d7
Create Date: 2026-03-24 01:55:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a12f5d7c9e31"
down_revision: Union[str, Sequence[str], None] = "f3e1a6c9b2d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def _existing_indexes(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {str(index["name"]) for index in inspector.get_indexes(table_name)}


def _add_history_metadata_columns(table_name: str) -> None:
    existing_columns = _existing_columns(table_name)
    if "source" not in existing_columns:
        op.add_column(table_name, sa.Column("source", sa.String(), nullable=True))
    if "is_backfill" not in existing_columns:
        op.add_column(
            table_name,
            sa.Column("is_backfill", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        )


def _drop_history_metadata_columns(table_name: str) -> None:
    existing_columns = _existing_columns(table_name)
    if "is_backfill" in existing_columns:
        op.drop_column(table_name, "is_backfill")
    if "source" in existing_columns:
        op.drop_column(table_name, "source")


def _create_index_if_missing(name: str, table_name: str, columns: list[str]) -> None:
    if name in _existing_indexes(table_name):
        return
    op.create_index(name, table_name, columns, unique=False)


def _drop_index_if_present(name: str, table_name: str) -> None:
    if name not in _existing_indexes(table_name):
        return
    op.drop_index(name, table_name=table_name)


def upgrade() -> None:
    """Upgrade schema."""
    _add_history_metadata_columns("game_prices")
    _add_history_metadata_columns("game_player_history")

    _create_index_if_missing(
        "ix_game_prices_game_recorded_source",
        "game_prices",
        ["game_id", "recorded_at", "source"],
    )
    _create_index_if_missing(
        "ix_player_history_game_recorded_source",
        "game_player_history",
        ["game_id", "recorded_at", "source"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    _drop_index_if_present("ix_player_history_game_recorded_source", "game_player_history")
    _drop_index_if_present("ix_game_prices_game_recorded_source", "game_prices")

    _drop_history_metadata_columns("game_player_history")
    _drop_history_metadata_columns("game_prices")
