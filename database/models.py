import datetime

from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import declarative_base, synonym

from database import RuntimeSessionLocal, runtime_engine


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


engine = runtime_engine

Base = declarative_base()


class GamePrice(Base):
    __tablename__ = "game_prices"

    id = Column(Integer, primary_key=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=True)
    game_name = Column(String)
    price = Column(Float)
    original_price = Column(Float, nullable=True)
    discount_percent = Column(Integer, nullable=True)
    current_players = Column(Integer, nullable=True)
    store_url = Column(String)
    recorded_at = Column(DateTime(timezone=True), default=_utc_now, nullable=False)

    # Backward-compatible alias for older code paths.
    timestamp = synonym("recorded_at")


class GameLatestPrice(Base):
    __tablename__ = "game_latest_prices"

    id = Column(Integer, primary_key=True)
    game_id = Column(BigInteger, unique=True, nullable=False)
    game_name = Column(String, nullable=False)
    price = Column(Float)
    original_price = Column(Float, nullable=True)
    discount_percent = Column(Integer, nullable=True)
    current_players = Column(Integer, nullable=True)
    store_url = Column(String)
    recorded_at = Column(DateTime(timezone=True), default=_utc_now, nullable=False)

    timestamp = synonym("recorded_at")


class LatestGamePrice(Base):
    __tablename__ = "latest_game_prices"

    game_id = Column(BigInteger, primary_key=True)
    latest_price = Column(Float, nullable=True)
    original_price = Column(Float, nullable=True)
    latest_discount_percent = Column(Integer, nullable=True)
    current_players = Column(Integer, nullable=True)
    recorded_at = Column(DateTime(timezone=True), nullable=True)


class Game(Base):
    __tablename__ = "games"

    id = Column(Integer, primary_key=True)
    appid = Column(String, unique=True)
    name = Column(String)
    store_url = Column(String)
    is_released = Column(Integer, default=1)
    release_date = Column(Date, nullable=True)
    release_date_text = Column(String, nullable=True)
    genres = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    platforms = Column(String, nullable=True)

    review_score = Column(Integer, nullable=True)
    review_score_label = Column(String, nullable=True)
    review_total_count = Column(Integer, nullable=True)
    developer = Column(String, nullable=True)
    publisher = Column(String, nullable=True)

    last_checked_at = Column(DateTime(timezone=True), nullable=True)
    next_refresh_at = Column(DateTime(timezone=True), nullable=True)
    priority = Column(Integer, default=0)
    priority_tier = Column(String, nullable=True)
    last_player_count = Column(Integer, nullable=True)
    popularity_score = Column(Float, default=0.0)


class PriceAlert(Base):
    __tablename__ = "price_alerts"

    id = Column(Integer, primary_key=True)
    game_name = Column(String)
    target_price = Column(Float)
    email = Column(String)
    created_at = Column(DateTime(timezone=True), default=_utc_now)


class WishlistItem(Base):
    __tablename__ = "wishlist_items"

    id = Column(BigInteger, primary_key=True)
    user_id = Column(String, nullable=False, index=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    game_name = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=_utc_now)


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"

    id = Column(Integer, primary_key=True)
    game_name = Column(String, unique=True)
    created_at = Column(DateTime(timezone=True), default=_utc_now)


class Watchlist(Base):
    __tablename__ = "watchlists"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False, index=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class GameSnapshot(Base):
    __tablename__ = "game_snapshots"

    id = Column(Integer, primary_key=True)
    game_id = Column(BigInteger, unique=True, index=True, nullable=False)

    game_name = Column(String, index=True, nullable=False)
    steam_appid = Column(String, index=True, nullable=True)
    store_url = Column(String, nullable=True)
    banner_url = Column(String, nullable=True)

    latest_price = Column(Float, nullable=True)
    latest_original_price = Column(Float, nullable=True)
    latest_discount_percent = Column(Integer, nullable=True)

    current_players = Column(Integer, nullable=True)
    avg_player_count = Column(Integer, nullable=True)
    player_change = Column(Integer, nullable=True)
    player_momentum = Column(Float, nullable=True)

    historical_low = Column(Float, nullable=True)
    historical_status = Column(String, nullable=True)
    is_historical_low = Column(Boolean, default=False)

    review_score = Column(Integer, nullable=True)
    review_score_label = Column(String, nullable=True)
    review_count = Column(Integer, nullable=True)

    genres = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    platforms = Column(String, nullable=True)

    is_upcoming = Column(Boolean, default=False)
    is_released = Column(Integer, default=1)
    release_date = Column(Date, nullable=True)
    release_date_text = Column(String, nullable=True)

    deal_score = Column(Float, nullable=True)
    popularity_score = Column(Float, default=0.0)
    recommended_score = Column(Float, default=0.0)
    trending_score = Column(Float, default=0.0)
    buy_score = Column(Float, default=0.0)
    buy_recommendation = Column(String, nullable=True)
    buy_reason = Column(String, nullable=True)
    price_vs_low_ratio = Column(Float, nullable=True)
    predicted_next_sale_price = Column(Float, nullable=True)
    predicted_next_discount_percent = Column(Integer, nullable=True)
    predicted_next_sale_window_days_min = Column(Integer, nullable=True)
    predicted_next_sale_window_days_max = Column(Integer, nullable=True)
    predicted_sale_confidence = Column(String, nullable=True)
    predicted_sale_reason = Column(String, nullable=True)
    deal_opportunity_score = Column(Float, default=0.0)
    deal_opportunity_reason = Column(String, nullable=True)
    worth_buying_score = Column(Float, default=0.0)
    worth_buying_score_version = Column(String, nullable=True)
    worth_buying_reason_summary = Column(String, nullable=True)
    worth_buying_components = Column(JSON, nullable=True)
    momentum_score = Column(Float, default=0.0)
    momentum_score_version = Column(String, nullable=True)
    player_growth_ratio = Column(Float, nullable=True)
    short_term_player_trend = Column(Float, nullable=True)
    trend_reason_summary = Column(String, nullable=True)
    historical_low_hit = Column(Boolean, default=False)
    historical_low_price = Column(Float, nullable=True)
    previous_historical_low_price = Column(Float, nullable=True)
    history_point_count = Column(Integer, nullable=True)
    ever_discounted = Column(Boolean, default=False)
    max_discount = Column(Integer, nullable=True)
    last_discounted_at = Column(DateTime(timezone=True), nullable=True)
    historical_low_timestamp = Column(DateTime(timezone=True), nullable=True)
    historical_low_reason_summary = Column(String, nullable=True)
    deal_heat_level = Column(String, nullable=True)
    deal_heat_reason = Column(String, nullable=True)
    deal_heat_tags = Column(JSON, nullable=True)
    ranking_explanations = Column(JSON, nullable=True)
    upcoming_hot_score = Column(Float, default=0.0)
    price_sparkline_90d = Column(JSON, nullable=True)
    sale_events_compact = Column(JSON, nullable=True)
    deal_detected_at = Column(DateTime(timezone=True), nullable=True)

    daily_peak = Column(Integer, nullable=True)
    updated_at = Column(DateTime(timezone=True), default=_utc_now, index=True)

    # Backward-compatible aliases for existing API code.
    price = synonym("latest_price")
    original_price = synonym("latest_original_price")
    discount_percent = synonym("latest_discount_percent")
    avg_30d = synonym("avg_player_count")
    review_total_count = synonym("review_count")


class GameDiscoveryFeed(Base):
    __tablename__ = "game_discovery_feed"

    id = Column(Integer, primary_key=True)
    game_id = Column(BigInteger, unique=True, index=True, nullable=False)

    game_name = Column(String, index=True, nullable=False)
    steam_appid = Column(String, index=True, nullable=True)
    store_url = Column(String, nullable=True)
    banner_url = Column(String, nullable=True)

    latest_price = Column(Float, nullable=True)
    latest_original_price = Column(Float, nullable=True)
    latest_discount_percent = Column(Integer, nullable=True)
    historical_low = Column(Float, nullable=True)
    historical_status = Column(String, nullable=True)
    historical_low_hit = Column(Boolean, default=False)

    buy_recommendation = Column(String, nullable=True)
    buy_reason = Column(String, nullable=True)
    deal_score = Column(Float, nullable=True)
    buy_score = Column(Float, default=0.0)
    worth_buying_score = Column(Float, default=0.0)
    momentum_score = Column(Float, default=0.0)
    trending_score = Column(Float, default=0.0)
    deal_opportunity_score = Column(Float, default=0.0)
    deal_opportunity_reason = Column(String, nullable=True)

    predicted_next_sale_price = Column(Float, nullable=True)
    predicted_next_discount_percent = Column(Integer, nullable=True)
    predicted_next_sale_window_days_min = Column(Integer, nullable=True)
    predicted_next_sale_window_days_max = Column(Integer, nullable=True)
    predicted_sale_confidence = Column(String, nullable=True)
    predicted_sale_reason = Column(String, nullable=True)

    popularity_score = Column(Float, default=0.0)
    price_vs_low_ratio = Column(Float, nullable=True)
    max_discount = Column(Integer, nullable=True)
    current_players = Column(Integer, nullable=True)
    player_growth_ratio = Column(Float, nullable=True)
    short_term_player_trend = Column(Float, nullable=True)

    review_score = Column(Integer, nullable=True)
    review_score_label = Column(String, nullable=True)
    review_count = Column(Integer, nullable=True)

    genres = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    platforms = Column(String, nullable=True)

    worth_buying_reason_summary = Column(String, nullable=True)
    trend_reason_summary = Column(String, nullable=True)
    deal_heat_reason = Column(String, nullable=True)

    is_released = Column(Integer, default=1)
    is_upcoming = Column(Boolean, default=False)
    release_date = Column(Date, nullable=True)

    is_strong_buy = Column(Boolean, default=False)
    is_wait_pick = Column(Boolean, default=False)
    is_new_historical_low = Column(Boolean, default=False)
    is_big_discount = Column(Boolean, default=False)
    is_trending_now = Column(Boolean, default=False)

    updated_at = Column(DateTime(timezone=True), default=_utc_now, index=True)

    # Keep field aliases aligned with existing card/render helpers.
    price = synonym("latest_price")
    original_price = synonym("latest_original_price")
    discount_percent = synonym("latest_discount_percent")
    review_total_count = synonym("review_count")


class DashboardCache(Base):
    __tablename__ = "dashboard_cache"

    # Canonical physical PK column in existing databases is `key`.
    # Keep `cache_key` as the public attribute for backward compatibility.
    key = Column("key", String, primary_key=True)
    payload = Column(Text, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)

    # Backward-compatible/public alias used throughout API/worker code.
    cache_key = synonym("key")


class DirtyGame(Base):
    __tablename__ = "dirty_games"

    game_id = Column(BigInteger, primary_key=True)
    reason = Column(String, nullable=True)
    first_seen_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)
    last_seen_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now, index=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now, index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    locked_by = Column(String, nullable=True)
    next_attempt_at = Column(DateTime(timezone=True), nullable=True, index=True)


class GamePriceLow(Base):
    __tablename__ = "game_price_lows"

    game_id = Column(BigInteger, primary_key=True)
    historical_low = Column(Float, nullable=True)


class GameInterestSignal(Base):
    __tablename__ = "game_interest_signals"

    game_id = Column(BigInteger, primary_key=True)
    click_count = Column(Integer, nullable=False, default=0)
    wishlist_count = Column(Integer, nullable=False, default=0)
    watchlist_count = Column(Integer, nullable=False, default=0)
    last_clicked_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class JobStatus(Base):
    __tablename__ = "job_status"

    job_name = Column(String, primary_key=True)
    last_started_at = Column(DateTime(timezone=True), nullable=True)
    last_completed_at = Column(DateTime(timezone=True), nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    last_duration_ms = Column(Integer, nullable=True)
    last_items_total = Column(Integer, nullable=True)
    last_items_success = Column(Integer, nullable=True)
    last_items_failed = Column(Integer, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class DealEvent(Base):
    __tablename__ = "deal_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    event_type = Column(String, nullable=False)
    old_price = Column(Float, nullable=True)
    new_price = Column(Float, nullable=True)
    discount_percent = Column(Integer, nullable=True)
    event_dedupe_key = Column(String, nullable=True, index=True)
    event_reason_summary = Column(String, nullable=True)
    metadata_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    alert_type = Column(String, nullable=False, index=True)
    metadata_json = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now, index=True)


class UserAlert(Base):
    __tablename__ = "user_alerts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False, index=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    alert_type = Column(String, nullable=False)
    price = Column(Float, nullable=True)
    discount_percent = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)
    read = Column(Boolean, nullable=False, default=False)


class GamePlayerHistory(Base):
    __tablename__ = "game_player_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    current_players = Column(Integer, nullable=True)
    recorded_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class PushSubscription(Base):
    __tablename__ = "push_subscriptions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False, index=True)
    endpoint = Column(Text, nullable=False)
    p256dh = Column(Text, nullable=False)
    auth = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


class DealWatchlist(Base):
    __tablename__ = "deal_watchlists"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String, nullable=False, index=True)
    game_id = Column(BigInteger, ForeignKey("games.id"), nullable=False, index=True)
    target_price = Column(Float, nullable=True)
    target_discount_percent = Column(Integer, nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_utc_now)


Index("ix_game_prices_game_name_recorded_at", GamePrice.game_name, GamePrice.recorded_at)
Index("ix_game_prices_game_id_recorded_at", GamePrice.game_id, GamePrice.recorded_at)
Index("ix_game_prices_recorded_at", GamePrice.recorded_at)
Index("ix_game_latest_prices_game_id", GameLatestPrice.game_id)
Index("ix_game_latest_prices_game_name", GameLatestPrice.game_name)
Index("ix_game_latest_prices_recorded_at", GameLatestPrice.recorded_at)
Index("ix_latest_game_prices_recorded_at", LatestGamePrice.recorded_at)
Index("ix_games_name", Game.name)
Index("ix_games_appid", Game.appid)
Index("ix_games_developer", Game.developer)
Index("ix_games_publisher", Game.publisher)
Index("ix_games_next_refresh_at", Game.next_refresh_at)
Index("ix_games_priority_tier_next_refresh", Game.priority_tier, Game.next_refresh_at)
Index("ix_games_popularity_score_desc", Game.popularity_score.desc(), Game.id.asc())
Index("idx_wishlist_user", WishlistItem.user_id)
Index("idx_wishlist_game", WishlistItem.game_id)
Index("uq_wishlist_user_game", WishlistItem.user_id, WishlistItem.game_id, unique=True)
Index("ix_wishlist_items_game_name", WishlistItem.game_name)
Index("ix_watchlist_items_game_name", WatchlistItem.game_name)
Index("idx_watchlists_user", Watchlist.user_id)
Index("idx_watchlists_game", Watchlist.game_id)
Index("uq_watchlists_user_game", Watchlist.user_id, Watchlist.game_id, unique=True)
Index("ix_dashboard_cache_updated_at", DashboardCache.updated_at)
Index("ix_game_interest_signals_updated_at", GameInterestSignal.updated_at)
Index("idx_deal_events_created_at", DealEvent.created_at.desc())
Index("idx_deal_events_game_id", DealEvent.game_id)
Index("idx_alerts_game_type_created", Alert.game_id, Alert.alert_type, Alert.created_at.desc())
Index("idx_alerts_created_at", Alert.created_at.desc())
Index("idx_user_alerts_user", UserAlert.user_id)
Index("idx_user_alerts_created", UserAlert.created_at.desc())
Index("idx_player_history_game", GamePlayerHistory.game_id)
Index("idx_player_history_time", GamePlayerHistory.recorded_at.desc())
Index("ix_player_history_game_recorded_desc", GamePlayerHistory.game_id, GamePlayerHistory.recorded_at.desc())
Index("ix_dirty_games_next_attempt_updated", DirtyGame.next_attempt_at, DirtyGame.updated_at)
Index("ix_game_snapshots_historical_low_hit_ts", GameSnapshot.historical_low_hit, GameSnapshot.historical_low_timestamp.desc())
Index(
    "ix_game_discovery_feed_released_deal_score",
    GameDiscoveryFeed.is_released,
    GameDiscoveryFeed.is_upcoming,
    GameDiscoveryFeed.deal_score.desc(),
    GameDiscoveryFeed.game_id.asc(),
)
Index(
    "ix_game_discovery_feed_released_buy_score",
    GameDiscoveryFeed.is_released,
    GameDiscoveryFeed.is_upcoming,
    GameDiscoveryFeed.buy_score.desc(),
    GameDiscoveryFeed.game_id.asc(),
)
Index(
    "ix_game_discovery_feed_released_trending_score",
    GameDiscoveryFeed.is_released,
    GameDiscoveryFeed.is_upcoming,
    GameDiscoveryFeed.trending_score.desc(),
    GameDiscoveryFeed.game_id.asc(),
)
Index(
    "ix_game_discovery_feed_released_opportunity_score",
    GameDiscoveryFeed.is_released,
    GameDiscoveryFeed.is_upcoming,
    GameDiscoveryFeed.deal_opportunity_score.desc(),
    GameDiscoveryFeed.game_id.asc(),
)
Index(
    "ix_game_discovery_feed_discount_price",
    GameDiscoveryFeed.latest_discount_percent.desc(),
    GameDiscoveryFeed.latest_price.asc(),
    GameDiscoveryFeed.game_id.asc(),
)
Index("ix_game_discovery_feed_release_date", GameDiscoveryFeed.release_date)
Index(
    "ix_game_discovery_feed_historical_low_hit_updated",
    GameDiscoveryFeed.historical_low_hit,
    GameDiscoveryFeed.updated_at.desc(),
)
Index(
    "ix_game_discovery_feed_feed_flags",
    GameDiscoveryFeed.is_strong_buy,
    GameDiscoveryFeed.is_wait_pick,
    GameDiscoveryFeed.is_big_discount,
    GameDiscoveryFeed.is_trending_now,
)
Index("idx_push_user", PushSubscription.user_id)
Index("idx_deal_watchlists_user", DealWatchlist.user_id)
Index("idx_deal_watchlists_game", DealWatchlist.game_id)
Index("uq_deal_watchlists_user_game", DealWatchlist.user_id, DealWatchlist.game_id, unique=True)

Session = RuntimeSessionLocal
