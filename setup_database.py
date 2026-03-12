from sqlalchemy import text

from database import direct_engine
from database.models import Base


POSTGRES_SQL = [
    "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
    "ALTER TABLE game_prices ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ;",
    "UPDATE game_prices SET recorded_at = COALESCE(recorded_at, NOW());",
    "ALTER TABLE game_prices ALTER COLUMN recorded_at SET DEFAULT now();",
    """
    CREATE TABLE IF NOT EXISTS latest_game_prices (
        game_id BIGINT PRIMARY KEY,
        latest_price DOUBLE PRECISION,
        original_price DOUBLE PRECISION,
        latest_discount_percent INTEGER,
        current_players INTEGER,
        recorded_at TIMESTAMPTZ
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_price_lows (
        game_id BIGINT PRIMARY KEY,
        historical_low DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_interest_signals (
        game_id BIGINT PRIMARY KEY,
        click_count INTEGER NOT NULL DEFAULT 0,
        wishlist_count INTEGER NOT NULL DEFAULT 0,
        watchlist_count INTEGER NOT NULL DEFAULT 0,
        last_clicked_at TIMESTAMPTZ NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS job_status (
        job_name TEXT PRIMARY KEY,
        last_started_at TIMESTAMPTZ,
        last_completed_at TIMESTAMPTZ,
        last_success_at TIMESTAMPTZ,
        last_error TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_duration_ms INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_total INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_success INTEGER;",
    "ALTER TABLE job_status ADD COLUMN IF NOT EXISTS last_items_failed INTEGER;",
    """
    CREATE TABLE IF NOT EXISTS deal_events (
        id BIGSERIAL PRIMARY KEY,
        game_id BIGINT NOT NULL,
        event_type TEXT NOT NULL CHECK (event_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE')),
        old_price DOUBLE PRECISION,
        new_price DOUBLE PRECISION,
        discount_percent INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id BIGSERIAL PRIMARY KEY,
        game_id BIGINT NOT NULL,
        alert_type TEXT NOT NULL CHECK (alert_type IN ('PRICE_DROP', 'NEW_HISTORICAL_LOW', 'SALE_STARTED', 'PLAYER_SURGE')),
        metadata_json JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_player_history (
        id BIGSERIAL PRIMARY KEY,
        game_id BIGINT NOT NULL,
        current_players INTEGER,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_alerts (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        game_id BIGINT NOT NULL,
        alert_type TEXT NOT NULL CHECK (alert_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE', 'PRICE_TARGET_HIT', 'DISCOUNT_TARGET_HIT')),
        price DOUBLE PRECISION,
        discount_percent INTEGER,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        read BOOLEAN NOT NULL DEFAULT FALSE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS deal_watchlists (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        game_id BIGINT NOT NULL,
        target_price DOUBLE PRECISION NULL,
        target_discount_percent INTEGER NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS watchlists (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        game_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS push_subscriptions (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        p256dh TEXT NOT NULL,
        auth TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
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
    "ALTER TABLE latest_game_prices ADD COLUMN IF NOT EXISTS current_players INTEGER;",
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
    "ALTER TABLE dashboard_cache ADD COLUMN IF NOT EXISTS cache_key TEXT;",
    "UPDATE dashboard_cache SET cache_key = COALESCE(cache_key, key);",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS release_date DATE;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS developer TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS publisher TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS next_refresh_at TIMESTAMPTZ;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS priority_tier TEXT;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS last_player_count INTEGER;",
    "ALTER TABLE games ADD COLUMN IF NOT EXISTS popularity_score DOUBLE PRECISION DEFAULT 0;",
    "CREATE INDEX IF NOT EXISTS idx_game_prices_game_id ON game_prices (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_prices_recorded ON game_prices (recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_at_desc ON game_prices (game_id, recorded_at DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_latest_game_prices_game_id ON latest_game_prices (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_game_price_lows_game_id ON game_price_lows (game_id);",
    """
    INSERT INTO latest_game_prices (
        game_id,
        latest_price,
        original_price,
        latest_discount_percent,
        current_players,
        recorded_at
    )
    SELECT DISTINCT ON (gp.game_id)
        gp.game_id,
        gp.price AS latest_price,
        gp.original_price,
        gp.discount_percent AS latest_discount_percent,
        gp.current_players,
        gp.recorded_at
    FROM game_prices gp
    WHERE gp.game_id IS NOT NULL
      AND gp.price IS NOT NULL
    ORDER BY gp.game_id, gp.recorded_at DESC, gp.id DESC
    ON CONFLICT (game_id) DO UPDATE SET
        latest_price = EXCLUDED.latest_price,
        original_price = EXCLUDED.original_price,
        latest_discount_percent = EXCLUDED.latest_discount_percent,
        current_players = EXCLUDED.current_players,
        recorded_at = EXCLUDED.recorded_at;
    """,
    """
    INSERT INTO game_price_lows (game_id, historical_low)
    SELECT gp.game_id, MIN(gp.price) AS historical_low
    FROM game_prices gp
    WHERE gp.game_id IS NOT NULL
      AND gp.price IS NOT NULL
      AND gp.price > 0
    GROUP BY gp.game_id
    ON CONFLICT (game_id) DO UPDATE SET
        historical_low = EXCLUDED.historical_low;
    """,
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_updated_at ON game_interest_signals (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_job_status_updated_at ON job_status (updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created_at ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_game_id ON deal_events (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_game_type_created ON alerts (game_id, alert_type, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_dedupe_key TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_reason_summary TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS metadata_json TEXT;",
    "CREATE INDEX IF NOT EXISTS ix_deal_events_dedupe_key ON deal_events (event_dedupe_key);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_events_dedupe_key ON deal_events (event_dedupe_key);",
    "CREATE INDEX IF NOT EXISTS ix_deal_events_dedupe_key ON deal_events (event_dedupe_key);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_events_dedupe_key ON deal_events (event_dedupe_key) WHERE event_dedupe_key IS NOT NULL;",
    "CREATE INDEX IF NOT EXISTS idx_player_history_game ON game_player_history (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_player_history_time ON game_player_history (recorded_at DESC);",
    "ALTER TABLE deal_events DROP CONSTRAINT IF EXISTS deal_events_event_type_check;",
    "ALTER TABLE deal_events ADD CONSTRAINT deal_events_event_type_check CHECK (event_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE'));",
    "ALTER TABLE alerts DROP CONSTRAINT IF EXISTS alerts_alert_type_check;",
    "ALTER TABLE alerts ADD CONSTRAINT alerts_alert_type_check CHECK (alert_type IN ('PRICE_DROP', 'NEW_HISTORICAL_LOW', 'SALE_STARTED', 'PLAYER_SURGE'));",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_dedupe_key TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS event_reason_summary TEXT;",
    "ALTER TABLE deal_events ADD COLUMN IF NOT EXISTS metadata_json JSONB;",
    "ALTER TABLE user_alerts DROP CONSTRAINT IF EXISTS user_alerts_alert_type_check;",
    "ALTER TABLE user_alerts ADD CONSTRAINT user_alerts_alert_type_check CHECK (alert_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE', 'PRICE_TARGET_HIT', 'DISCOUNT_TARGET_HIT'));",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_user ON user_alerts (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_created ON user_alerts (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_user ON deal_watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_game ON deal_watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_watchlists_user_game ON deal_watchlists (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_game ON watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_watchlists_user_game ON watchlists (user_id, game_id);",
    """
    INSERT INTO watchlists (user_id, game_id, created_at)
    SELECT 'legacy-user', g.id, COALESCE(wi.created_at, now())
    FROM watchlist_items wi
    JOIN games g ON lower(g.name) = lower(wi.game_name)
    ON CONFLICT (user_id, game_id) DO NOTHING;
    """,
    "CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_user ON wishlist_items (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_game ON wishlist_items (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_wishlist_user_game ON wishlist_items (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_click_count_desc ON game_interest_signals (click_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_wishlist_watchlist_desc ON game_interest_signals (wishlist_count DESC, watchlist_count DESC);",
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
    "CREATE INDEX IF NOT EXISTS idx_snapshots_price ON game_snapshots (latest_price);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_review_score_review_count_desc ON game_snapshots (review_score DESC, review_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_upcoming_hot ON game_snapshots (is_upcoming, upcoming_hot_score DESC, release_date ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_released_discovery ON game_snapshots (is_upcoming, is_released, deal_score DESC);",
    "CREATE INDEX IF NOT EXISTS ix_games_name_trgm ON games USING gin (name gin_trgm_ops);",
    "CREATE INDEX IF NOT EXISTS ix_games_is_released_name ON games (is_released, name);",
    "CREATE INDEX IF NOT EXISTS ix_games_developer ON games (developer);",
    "CREATE INDEX IF NOT EXISTS ix_games_publisher ON games (publisher);",
    "CREATE INDEX IF NOT EXISTS ix_games_next_refresh_at ON games (next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_priority_tier_next_refresh ON games (priority_tier, next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_popularity_score_desc ON games (popularity_score DESC, id ASC);",
    "CREATE INDEX IF NOT EXISTS ix_games_developer_trgm ON games USING gin (developer gin_trgm_ops);",
    "CREATE INDEX IF NOT EXISTS ix_games_publisher_trgm ON games USING gin (publisher gin_trgm_ops);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_dashboard_cache_cache_key ON dashboard_cache (cache_key);",
    "CREATE TABLE IF NOT EXISTS dirty_games (game_id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS reason TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS locked_by TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_updated_at ON dirty_games (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_last_seen_at ON dirty_games (last_seen_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_at ON dirty_games (next_attempt_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_updated ON dirty_games (next_attempt_at, updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_id_desc ON game_prices (game_id, recorded_at DESC, id DESC);",
    "CREATE INDEX IF NOT EXISTS ix_player_history_game_recorded_desc ON game_player_history (game_id, recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_historical_low_hit_ts ON game_snapshots (historical_low_hit, historical_low_timestamp DESC);",
]


SQLITE_SQL = [
    "ALTER TABLE game_snapshots ADD COLUMN upcoming_hot_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN price_sparkline_90d TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN sale_events_compact TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN deal_detected_at DATETIME;",
    "ALTER TABLE game_snapshots ADD COLUMN latest_price REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN latest_original_price REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN latest_discount_percent INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN historical_low REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN avg_player_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN review_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN player_momentum REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN popularity_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN recommended_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN is_upcoming INTEGER DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN is_historical_low INTEGER DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN release_date DATE;",
    "ALTER TABLE dashboard_cache ADD COLUMN cache_key TEXT;",
    "UPDATE dashboard_cache SET cache_key = COALESCE(cache_key, key);",
    "ALTER TABLE game_prices ADD COLUMN recorded_at DATETIME;",
    "UPDATE game_prices SET recorded_at = COALESCE(recorded_at, timestamp);",
    """
    CREATE TABLE IF NOT EXISTS latest_game_prices (
        game_id INTEGER PRIMARY KEY,
        latest_price REAL,
        original_price REAL,
        latest_discount_percent INTEGER,
        current_players INTEGER,
        recorded_at DATETIME
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_price_lows (
        game_id INTEGER PRIMARY KEY,
        historical_low REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_interest_signals (
        game_id INTEGER PRIMARY KEY,
        click_count INTEGER NOT NULL DEFAULT 0,
        wishlist_count INTEGER NOT NULL DEFAULT 0,
        watchlist_count INTEGER NOT NULL DEFAULT 0,
        last_clicked_at DATETIME NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS job_status (
        job_name TEXT PRIMARY KEY,
        last_started_at DATETIME,
        last_completed_at DATETIME,
        last_success_at DATETIME,
        last_error TEXT,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "ALTER TABLE job_status ADD COLUMN last_duration_ms INTEGER;",
    "ALTER TABLE job_status ADD COLUMN last_items_total INTEGER;",
    "ALTER TABLE job_status ADD COLUMN last_items_success INTEGER;",
    "ALTER TABLE job_status ADD COLUMN last_items_failed INTEGER;",
    """
    CREATE TABLE IF NOT EXISTS deal_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        event_type TEXT NOT NULL CHECK (event_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE')),
        old_price REAL,
        new_price REAL,
        discount_percent INTEGER,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        alert_type TEXT NOT NULL CHECK (alert_type IN ('PRICE_DROP', 'NEW_HISTORICAL_LOW', 'SALE_STARTED', 'PLAYER_SURGE')),
        metadata_json TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS game_player_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        current_players INTEGER,
        recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        game_id INTEGER NOT NULL,
        alert_type TEXT NOT NULL CHECK (alert_type IN ('NEW_SALE', 'PRICE_DROP', 'HISTORICAL_LOW', 'PLAYER_SPIKE', 'PRICE_TARGET_HIT', 'DISCOUNT_TARGET_HIT')),
        price REAL,
        discount_percent INTEGER,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        read INTEGER NOT NULL DEFAULT 0
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS deal_watchlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        game_id INTEGER NOT NULL,
        target_price REAL NULL,
        target_discount_percent INTEGER NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS watchlists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        game_id INTEGER NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS push_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        p256dh TEXT NOT NULL,
        auth TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "ALTER TABLE wishlist_items ADD COLUMN user_id TEXT;",
    "ALTER TABLE wishlist_items ADD COLUMN game_id INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN trending_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN buy_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN worth_buying_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN worth_buying_score_version TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN worth_buying_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN worth_buying_components TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN momentum_score REAL DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN momentum_score_version TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN player_growth_ratio REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN short_term_player_trend REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN trend_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN historical_low_hit INTEGER DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN historical_low_price REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN previous_historical_low_price REAL;",
    "ALTER TABLE game_snapshots ADD COLUMN history_point_count INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN ever_discounted INTEGER DEFAULT 0;",
    "ALTER TABLE game_snapshots ADD COLUMN max_discount INTEGER;",
    "ALTER TABLE game_snapshots ADD COLUMN last_discounted_at DATETIME;",
    "ALTER TABLE game_snapshots ADD COLUMN historical_low_timestamp DATETIME;",
    "ALTER TABLE game_snapshots ADD COLUMN historical_low_reason_summary TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN deal_heat_level TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN deal_heat_reason TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN deal_heat_tags TEXT;",
    "ALTER TABLE game_snapshots ADD COLUMN ranking_explanations TEXT;",
    "ALTER TABLE games ADD COLUMN release_date DATE;",
    "ALTER TABLE games ADD COLUMN developer TEXT;",
    "ALTER TABLE games ADD COLUMN publisher TEXT;",
    "ALTER TABLE games ADD COLUMN next_refresh_at DATETIME;",
    "ALTER TABLE games ADD COLUMN priority_tier TEXT;",
    "ALTER TABLE games ADD COLUMN last_player_count INTEGER;",
    "ALTER TABLE games ADD COLUMN popularity_score REAL DEFAULT 0;",
    "CREATE INDEX IF NOT EXISTS idx_game_prices_game_id ON game_prices (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_game_prices_recorded ON game_prices (recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_at_desc ON game_prices (game_id, recorded_at DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_latest_game_prices_game_id ON latest_game_prices (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_game_price_lows_game_id ON game_price_lows (game_id);",
    """
    INSERT INTO latest_game_prices (
        game_id,
        latest_price,
        original_price,
        latest_discount_percent,
        current_players,
        recorded_at
    )
    SELECT gp.game_id, gp.price, gp.original_price, gp.discount_percent, gp.current_players, gp.recorded_at
    FROM game_prices gp
    JOIN (
        SELECT game_id, MAX(recorded_at) AS max_recorded_at
        FROM game_prices
        WHERE game_id IS NOT NULL AND price IS NOT NULL
        GROUP BY game_id
    ) latest ON latest.game_id = gp.game_id AND latest.max_recorded_at = gp.recorded_at
    WHERE gp.game_id IS NOT NULL AND gp.price IS NOT NULL
    ON CONFLICT(game_id) DO UPDATE SET
        latest_price = excluded.latest_price,
        original_price = excluded.original_price,
        latest_discount_percent = excluded.latest_discount_percent,
        current_players = excluded.current_players,
        recorded_at = excluded.recorded_at;
    """,
    """
    INSERT INTO game_price_lows (game_id, historical_low)
    SELECT game_id, MIN(price) AS historical_low
    FROM game_prices
    WHERE game_id IS NOT NULL AND price IS NOT NULL AND price > 0
    GROUP BY game_id
    ON CONFLICT(game_id) DO UPDATE SET historical_low = excluded.historical_low;
    """,
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_updated_at ON game_interest_signals (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_job_status_updated_at ON job_status (updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created_at ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_created ON deal_events (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_events_game_id ON deal_events (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_game_type_created ON alerts (game_id, alert_type, created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_player_history_game ON game_player_history (game_id);",
    "CREATE INDEX IF NOT EXISTS idx_player_history_time ON game_player_history (recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_user ON user_alerts (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_user_alerts_created ON user_alerts (created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_user ON deal_watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_deal_watchlists_game ON deal_watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_deal_watchlists_user_game ON deal_watchlists (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_watchlists_game ON watchlists (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_watchlists_user_game ON watchlists (user_id, game_id);",
    """
    INSERT OR IGNORE INTO watchlists (user_id, game_id, created_at)
    SELECT 'legacy-user', g.id, COALESCE(wi.created_at, CURRENT_TIMESTAMP)
    FROM watchlist_items wi
    JOIN games g ON lower(g.name) = lower(wi.game_name);
    """,
    "CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_user ON wishlist_items (user_id);",
    "CREATE INDEX IF NOT EXISTS idx_wishlist_game ON wishlist_items (game_id);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_wishlist_user_game ON wishlist_items (user_id, game_id);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_click_count_desc ON game_interest_signals (click_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_interest_signals_wishlist_watchlist_desc ON game_interest_signals (wishlist_count DESC, watchlist_count DESC);",
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
    "CREATE INDEX IF NOT EXISTS idx_snapshots_price ON game_snapshots (latest_price);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_review_score_review_count_desc ON game_snapshots (review_score DESC, review_count DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_upcoming_hot ON game_snapshots (is_upcoming, upcoming_hot_score DESC, release_date ASC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_released_discovery ON game_snapshots (is_upcoming, is_released, deal_score DESC);",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_dashboard_cache_cache_key ON dashboard_cache (cache_key);",
    "CREATE INDEX IF NOT EXISTS ix_games_is_released_name ON games (is_released, name);",
    "CREATE INDEX IF NOT EXISTS ix_games_developer ON games (developer);",
    "CREATE INDEX IF NOT EXISTS ix_games_publisher ON games (publisher);",
    "CREATE INDEX IF NOT EXISTS ix_games_next_refresh_at ON games (next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_priority_tier_next_refresh ON games (priority_tier, next_refresh_at);",
    "CREATE INDEX IF NOT EXISTS ix_games_popularity_score_desc ON games (popularity_score DESC, id ASC);",
    "CREATE TABLE IF NOT EXISTS dirty_games (game_id INTEGER PRIMARY KEY, updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);",
    "ALTER TABLE dirty_games ADD COLUMN reason TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN first_seen_at DATETIME;",
    "ALTER TABLE dirty_games ADD COLUMN last_seen_at DATETIME;",
    "ALTER TABLE dirty_games ADD COLUMN retry_count INTEGER DEFAULT 0;",
    "ALTER TABLE dirty_games ADD COLUMN locked_at DATETIME;",
    "ALTER TABLE dirty_games ADD COLUMN locked_by TEXT;",
    "ALTER TABLE dirty_games ADD COLUMN next_attempt_at DATETIME;",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_updated_at ON dirty_games (updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_last_seen_at ON dirty_games (last_seen_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_at ON dirty_games (next_attempt_at);",
    "CREATE INDEX IF NOT EXISTS ix_dirty_games_next_attempt_updated ON dirty_games (next_attempt_at, updated_at);",
    "CREATE INDEX IF NOT EXISTS ix_game_prices_game_id_recorded_id_desc ON game_prices (game_id, recorded_at DESC, id DESC);",
    "CREATE INDEX IF NOT EXISTS ix_player_history_game_recorded_desc ON game_player_history (game_id, recorded_at DESC);",
    "CREATE INDEX IF NOT EXISTS ix_game_snapshots_historical_low_hit_ts ON game_snapshots (historical_low_hit, historical_low_timestamp DESC);",
]


def _is_tolerable_rerun_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "duplicate column name" in msg or "already exists" in msg


def run_sql_statements(statements: list[str], dialect: str) -> None:
    total = len(statements)
    with direct_engine.connect() as conn:
        for idx, sql in enumerate(statements, start=1):
            cleaned_sql = sql.strip()
            print(f"[{idx}/{total}] RUNNING SQL:\n{cleaned_sql}")
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[{idx}/{total}] OK")
            except Exception as exc:
                conn.rollback()
                if dialect == "sqlite" and _is_tolerable_rerun_error(exc):
                    print(f"[{idx}/{total}] SKIPPED (idempotent sqlite rerun): {exc}")
                    continue
                print(f"[{idx}/{total}] FAILED SQL:\n{cleaned_sql}")
                raise


def setup_database() -> None:
    print("Creating tables...")
    Base.metadata.create_all(direct_engine)

    dialect = direct_engine.dialect.name.lower()
    print(f"Applying indexes and schema extras for {dialect}...")
    if dialect == "postgresql":
        run_sql_statements(POSTGRES_SQL, dialect)
    else:
        run_sql_statements(SQLITE_SQL, dialect)

    print("Database setup complete!")


if __name__ == "__main__":
    setup_database()
