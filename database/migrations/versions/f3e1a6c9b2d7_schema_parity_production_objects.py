"""schema parity for production-critical setup objects

Revision ID: f3e1a6c9b2d7
Revises: 8c9a7d5c2e11
Create Date: 2026-03-16 14:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f3e1a6c9b2d7"
down_revision: Union[str, Sequence[str], None] = "8c9a7d5c2e11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


EXTENSION_SQL: tuple[str, ...] = (
    "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
    # Optional on some managed Postgres providers.
    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
)

SCHEMA_GUARD_COLUMN_SQL: tuple[str, ...] = (
    "ALTER TABLE game_prices ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ;",
    "UPDATE game_prices SET recorded_at = COALESCE(recorded_at, NOW());",
    "ALTER TABLE game_prices ALTER COLUMN recorded_at SET DEFAULT now();",
    "ALTER TABLE latest_game_prices ADD COLUMN IF NOT EXISTS current_players INTEGER;",
    "ALTER TABLE wishlist_items ADD COLUMN IF NOT EXISTS user_id TEXT;",
    "ALTER TABLE wishlist_items ADD COLUMN IF NOT EXISTS game_id BIGINT;",
    "ALTER TABLE wishlist_items ADD COLUMN IF NOT EXISTS game_name TEXT;",
    "UPDATE wishlist_items SET user_id = COALESCE(user_id, 'legacy-user');",
    """
    UPDATE wishlist_items wi
    SET game_id = g.id
    FROM games g
    WHERE wi.game_id IS NULL
      AND wi.game_name IS NOT NULL
      AND wi.game_name = g.name;
    """,
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS release_date DATE;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS developer TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS publisher TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS next_refresh_at TIMESTAMPTZ;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS priority_tier TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS last_player_count INTEGER;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS popularity_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_duration_ms INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_total INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_success INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_failed INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS latest_price DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS latest_original_price DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS latest_discount_percent INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS historical_low DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS avg_player_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS review_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS player_momentum DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS popularity_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS recommended_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS trending_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS buy_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS buy_recommendation TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS buy_reason TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS price_vs_low_ratio DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_next_sale_price DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_next_discount_percent INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_next_sale_window_days_min INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_next_sale_window_days_max INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_sale_confidence TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS predicted_sale_reason TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_opportunity_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_opportunity_reason TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS worth_buying_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS worth_buying_score_version TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS worth_buying_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS worth_buying_components JSONB;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS momentum_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS momentum_score_version TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS player_growth_ratio DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS short_term_player_trend DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS trend_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS historical_low_hit BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS historical_low_price DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS previous_historical_low_price DOUBLE PRECISION;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS history_point_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS ever_discounted BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS max_discount INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS last_discounted_at TIMESTAMPTZ;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS historical_low_timestamp TIMESTAMPTZ;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS historical_low_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_heat_level TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_heat_reason TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_heat_tags JSONB;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS ranking_explanations JSONB;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS is_upcoming BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS is_historical_low BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS release_date DATE;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS upcoming_hot_score DOUBLE PRECISION DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS price_sparkline_90d JSONB;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS sale_events_compact JSONB;",
    "ALTER TABLE game_snapshots ADD COLUMN IF NOT EXISTS deal_detected_at TIMESTAMPTZ;",
    "CREATE TABLE IF NOT EXISTS dirty_games (game_id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS reason TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS locked_by TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;",
)

INDEX_AND_CONSTRAINT_SQL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_game_prices_game_id ON game_prices (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_prices_recorded ON game_prices (recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_at_desc ON game_prices (game_id, recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_id_desc ON game_prices (game_id, recorded_at DESC, id DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_latest_game_prices_game_id ON latest_game_prices (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_game_price_lows_game_id ON game_price_lows (game_id);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_updated_at ON game_interest_signals (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_click_count_desc ON game_interest_signals (click_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_wishlist_watchlist_desc ON game_interest_signals (wishlist_count DESC, watchlist_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_job_status_updated_at ON job_status (updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created_at ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_game_id ON deal_events (game_id);",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_dedupe_key TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_reason_summary TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS metadata_json JSONB;",
    "CREATE INDEX IF NOT EXISTS ix_deal_events_dedupe_key ON deal_events (event_dedupe_key);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_events_dedupe_key ON deal_events (event_dedupe_key) WHERE event_dedupe_key IS NOT NULL;",
    "ALTER TABLE deal_events DROP CONSTRAINT IF EXISTS deal_events_event_type_check;",
    "ALTER TABLE deal_events ADD CONSTRAINT deal_events_event_type_check CHECK (event_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE'));",
    "ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_alert_type_check;",
    "ALTER TABLE alerts ADD CONSTRAINT alerts_alert_type_check CHECK (alert_type IN ('PRICE_DROP', 'NEW_HISTORICAL_LOW', 'SALE_STARTED', 'PLAYER_SURGE'));",
    "ALTER TABLE user_alerts DROP CONSTRAINT IF EXISTS user_alerts_alert_type_check;",
    "ALTER TABLE user_alerts ADD CONSTRAINT user_alerts_alert_type_check CHECK (alert_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE', 'PRICE_TARGET_HIT', 'DISCOUNT_TARGET_HIT'));",
    "CREATE INDEX IF NOT EXISTS idx_alerts_game_type_created ON alerts (game_id, alert_type, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_player_history_game ON game_player_history (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_player_history_time ON game_player_history (recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_player_history_game_recorded_desc ON game_player_history (game_id, recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_user ON user_alerts (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_created ON user_alerts (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_user ON deal_watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_game ON deal_watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_watchlists_user_game ON deal_watchlists (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_game ON watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_watchlists_user_game ON watchlists (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_user ON wishlist_items (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_game ON wishlist_items (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_wishlist_user_game ON wishlist_items (user_id, game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_game_snapshots_game_id ON game_snapshots (game_id);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_deal_score_desc ON game_snapshots (deal_score DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_latest_discount_percent_desc ON game_snapshots (latest_discount_percent DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_avg_player_count_desc ON game_snapshots (avg_player_count DESC);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_discount ON game_snapshots (latest_discount_percent);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_players ON game_snapshots (current_players);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_trending ON game_snapshots (trending_score);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_buy_score ON game_snapshots (buy_score);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_worth_buying ON game_snapshots (worth_buying_score);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_momentum ON game_snapshots (momentum_score);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_opportunity_score ON game_snapshots (deal_opportunity_score DESC);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_price ON game_snapshots (latest_price);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_review_score_review_count_desc ON game_snapshots (review_score DESC, review_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_upcoming_hot ON game_snapshots (is_upcoming, upcoming_hot_score DESC, release_date ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_released_discovery ON game_snapshots (is_upcoming, is_released, deal_score DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_historical_low_hit_ts ON game_snapshots (historical_low_hit, historical_low_timestamp DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_game_discovery_feed_game_id ON game_discovery_feed (game_id);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_released_deal_score ON game_discovery_feed (is_released, is_upcoming, deal_score DESC, game_id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_released_buy_score ON game_discovery_feed (is_released, is_upcoming, buy_score DESC, game_id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_released_trending_score ON game_discovery_feed (is_released, is_upcoming, trending_score DESC, game_id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_released_opportunity_score ON game_discovery_feed (is_released, is_upcoming, deal_opportunity_score DESC, game_id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_discount_price ON game_discovery_feed (latest_discount_percent DESC, latest_price ASC, game_id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_latest_price ON game_discovery_feed (latest_price);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_release_date ON game_discovery_feed (release_date);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_historical_low_hit_updated ON game_discovery_feed (historical_low_hit, updated_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_discovery_feed_feed_flags ON game_discovery_feed (is_strong_buy, is_wait_pick, is_big_discount, is_trending_now);",
    "CREATE INDEX IF NOT EXISTS ix_games_is_released_name ON games (is_released, name);",
    "CREATE INDEX IF NOT EXISTS ix_games_developer ON games (developer);",
    "CREATE INDEX IF NOT EXISTS ix_games_publisher ON games (publisher);",
    "CREATE INDEX IF NOT EXISTS ix_games_next_refresh_at ON games (next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_priority_tier_next_refresh ON games (priority_tier, next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_popularity_score_desc ON games (popularity_score DESC, id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_games_name_trgm ON games USING gin (name gin_trgm_ops);",
    "CREATE INDEX IF NOT EXISTS ix_games_developer_trgm ON games USING gin (developer gin_trgm_ops);",
    "CREATE INDEX IF NOT EXISTS ix_games_publisher_trgm ON games USING gin (publisher gin_trgm_ops);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_updated_at ON dirty_games (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_last_seen_at ON dirty_games (last_seen_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_at ON dirty_games (next_attempt_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_updated ON dirty_games (next_attempt_at, updated_at);",
)


def _execute_parity_sql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    statements = EXTENSION_SQL + SCHEMA_GUARD_COLUMN_SQL + INDEX_AND_CONSTRAINT_SQL
    for statement in statements:
        if "pg_stat_statements" in statement:
            try:
                op.execute(sa.text(statement))
            except Exception:
                continue
        else:
            op.execute(sa.text(statement))


def upgrade() -> None:
    """Upgrade schema."""
    _execute_parity_sql()


def downgrade() -> None:
    """Downgrade schema.

    Parity migration is intentionally additive/idempotent and does not drop objects.
    """
    pass
