"""add missing game_snapshots signal columns

Revision ID: 8c9a7d5c2e11
Revises: d7bef0e0c44a
Create Date: 2026-03-16 10:22:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8c9a7d5c2e11"
down_revision: Union[str, Sequence[str], None] = "d7bef0e0c44a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def _add_columns_if_missing(table_name: str, columns: list[sa.Column]) -> None:
    existing_columns = _existing_columns(table_name)
    for column in columns:
        if column.name not in existing_columns:
            op.add_column(table_name, column)
            existing_columns.add(column.name)


def _drop_columns_if_present(table_name: str, column_names: list[str]) -> None:
    existing_columns = _existing_columns(table_name)
    for column_name in column_names:
        if column_name in existing_columns:
            op.drop_column(table_name, column_name)


SNAPSHOT_SIGNAL_COLUMNS: list[sa.Column] = [
    sa.Column("price_vs_low_ratio", sa.Float(), nullable=True),
    sa.Column("predicted_next_sale_price", sa.Float(), nullable=True),
    sa.Column("predicted_next_discount_percent", sa.Integer(), nullable=True),
    sa.Column("predicted_next_sale_window_days_min", sa.Integer(), nullable=True),
    sa.Column("predicted_next_sale_window_days_max", sa.Integer(), nullable=True),
    sa.Column("predicted_sale_confidence", sa.String(), nullable=True),
    sa.Column("predicted_sale_reason", sa.String(), nullable=True),
    sa.Column("deal_opportunity_score", sa.Float(), nullable=True),
    sa.Column("deal_opportunity_reason", sa.String(), nullable=True),
    sa.Column("worth_buying_score", sa.Float(), nullable=True),
    sa.Column("worth_buying_score_version", sa.String(), nullable=True),
    sa.Column("worth_buying_reason_summary", sa.String(), nullable=True),
    sa.Column("worth_buying_components", sa.JSON(), nullable=True),
    sa.Column("momentum_score", sa.Float(), nullable=True),
    sa.Column("momentum_score_version", sa.String(), nullable=True),
    sa.Column("player_growth_ratio", sa.Float(), nullable=True),
    sa.Column("short_term_player_trend", sa.Float(), nullable=True),
    sa.Column("trend_reason_summary", sa.String(), nullable=True),
    sa.Column("historical_low_hit", sa.Boolean(), nullable=True),
    sa.Column("historical_low_price", sa.Float(), nullable=True),
    sa.Column("previous_historical_low_price", sa.Float(), nullable=True),
    sa.Column("history_point_count", sa.Integer(), nullable=True),
    sa.Column("ever_discounted", sa.Boolean(), nullable=True),
    sa.Column("max_discount", sa.Integer(), nullable=True),
    sa.Column("last_discounted_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("historical_low_timestamp", sa.DateTime(timezone=True), nullable=True),
    sa.Column("historical_low_reason_summary", sa.String(), nullable=True),
    sa.Column("deal_heat_level", sa.String(), nullable=True),
    sa.Column("deal_heat_reason", sa.String(), nullable=True),
    sa.Column("deal_heat_tags", sa.JSON(), nullable=True),
    sa.Column("ranking_explanations", sa.JSON(), nullable=True),
    sa.Column("is_upcoming", sa.Boolean(), nullable=True),
    sa.Column("is_historical_low", sa.Boolean(), nullable=True),
    sa.Column("release_date", sa.Date(), nullable=True),
    sa.Column("upcoming_hot_score", sa.Float(), nullable=True),
    sa.Column("price_sparkline_90d", sa.JSON(), nullable=True),
    sa.Column("sale_events_compact", sa.JSON(), nullable=True),
    sa.Column("deal_detected_at", sa.DateTime(timezone=True), nullable=True),
]


def upgrade() -> None:
    """Upgrade schema."""
    _add_columns_if_missing("game_snapshots", SNAPSHOT_SIGNAL_COLUMNS)


def downgrade() -> None:
    """Downgrade schema."""
    _drop_columns_if_present(
        "game_snapshots",
        [column.name for column in SNAPSHOT_SIGNAL_COLUMNS],
    )
