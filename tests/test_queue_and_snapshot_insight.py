import datetime
from types import SimpleNamespace

import main
from api import server
from jobs.refresh_snapshots import (
    DIRTY_CLAIM_PREDICATE_SQL,
    _build_insight_engine_payload,
    _run_insight_engine_subprocess,
    compute_retry_backoff_seconds,
)


def test_dirty_claim_predicate_is_parenthesized():
    assert DIRTY_CLAIM_PREDICATE_SQL.startswith("(")
    assert ") AND (" in DIRTY_CLAIM_PREDICATE_SQL


def test_compute_historical_insight_map_uses_snapshot_rows():
    class FakeQuery:
        def all(self):
            return [
                SimpleNamespace(
                    game_name="Alpha",
                    historical_low=9.99,
                    previous_historical_low_price=12.99,
                    historical_status="new_historical_low",
                    history_point_count=120,
                    ever_discounted=True,
                    max_discount=75,
                    last_discounted_at=None,
                )
            ]

    class FakeSession:
        def query(self, *args):
            return FakeQuery()

    insight_map = server.compute_historical_insight_map(FakeSession())
    assert "Alpha" in insight_map
    assert insight_map["Alpha"]["historical_low"] == 9.99
    assert insight_map["Alpha"]["previous_historical_low"] == 12.99
    assert insight_map["Alpha"]["history_point_count"] == 120
    assert insight_map["Alpha"]["ever_discounted"] is True
    assert insight_map["Alpha"]["max_discount"] == 75


def test_get_latest_price_rows_uses_snapshots_not_history_scans():
    snapshot = SimpleNamespace(
        game_id=1,
        game_name="Beta",
        latest_price=19.99,
        latest_original_price=39.99,
        latest_discount_percent=50,
        current_players=1200,
        store_url="https://store.steampowered.com/app/1/",
        updated_at=None,
    )

    class FakeQuery:
        def __init__(self):
            self.all_called = False

        def all(self):
            self.all_called = True
            return [snapshot]

    class FakeSession:
        def __init__(self):
            self.query_args = None

        def query(self, *args):
            self.query_args = args
            return FakeQuery()

    session = FakeSession()
    rows = server.get_latest_price_rows(session)
    assert len(rows) == 1
    assert rows[0].game_name == "Beta"
    assert rows[0].price == 19.99
    assert session.query_args and session.query_args[0] == server.GameSnapshot


def test_get_games_for_run_applies_shard_filter_when_enabled(monkeypatch):
    class FakeQuery:
        def __init__(self):
            self.filter_calls = 0
            self.limit_value = None

        def filter(self, *args):
            self.filter_calls += 1
            return self

        def order_by(self, *args):
            return self

        def limit(self, n):
            self.limit_value = n
            return self

        def all(self):
            return []

    class FakeSession:
        def __init__(self):
            self.query_obj = FakeQuery()

        def query(self, *args):
            return self.query_obj

    monkeypatch.setattr(main, "TRACK_SHARD_TOTAL", 4)
    monkeypatch.setattr(main, "TRACK_SHARD_INDEX", 1)
    monkeypatch.setattr(main, "GAMES_PER_RUN", 123)

    session = FakeSession()
    rows = main.get_games_for_run(session)
    assert rows == []
    assert session.query_obj.filter_calls >= 2
    assert session.query_obj.limit_value == 123


def test_apply_ingestion_schedule_assigns_hot_tier(monkeypatch):
    monkeypatch.setattr(main, "TRACK_HOT_MIN_PLAYERS", 5000)
    monkeypatch.setattr(main, "TRACK_MEDIUM_MIN_PLAYERS", 500)
    monkeypatch.setattr(main, "TRACK_HOT_REFRESH_MINUTES", 20)

    game = SimpleNamespace(
        is_released=1,
        priority_tier=None,
        last_player_count=None,
        review_total_count=10000,
        priority=1,
        popularity_score=0.0,
        next_refresh_at=None,
        last_checked_at=None,
    )
    main.apply_ingestion_schedule(game, observed_players=12000)
    assert game.priority_tier == main.TIER_HOT
    assert game.last_player_count == 12000
    assert game.popularity_score > 0
    assert game.next_refresh_at is not None


def test_retry_backoff_uses_exponential_delay(monkeypatch):
    monkeypatch.setattr("jobs.refresh_snapshots.RETRY_BACKOFF_BASE_SECONDS", 10.0)
    monkeypatch.setattr("jobs.refresh_snapshots.RETRY_BACKOFF_MAX_SECONDS", 3600.0)
    monkeypatch.setattr("jobs.refresh_snapshots.RETRY_BACKOFF_EXPONENT_CAP", 10)

    assert compute_retry_backoff_seconds(1) == 10
    assert compute_retry_backoff_seconds(2) == 20
    assert compute_retry_backoff_seconds(3) == 40


def test_insight_engine_payload_restores_preserved_trigger_categories():
    now = datetime.datetime(2026, 3, 14, 3, 59, 14, 230367, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    recorded_at = datetime.datetime(2026, 3, 14, 3, 53, 6, 284466, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    payload = _build_insight_engine_payload(
        game_id=64384,
        now=now,
        latest_recorded_at=recorded_at,
        prior_snapshot_state={
            "snapshot_exists": True,
            "price": 29.99,
            "original_price": 29.99,
            "discount_percent": 0,
            "historical_low": 29.99,
            "player_momentum": 0.0,
            "daily_peak": 1000,
            "review_label": "Mixed",
            "is_upcoming": True,
            "popularity_score": 40.0,
        },
        latest_price=14.99,
        latest_original_price=29.99,
        latest_discount_percent=50,
        review_score=85.0,
        review_score_label=None,
        is_upcoming=False,
        popularity_score=75.0,
        is_historical_low=True,
        is_new_historical_low=True,
        wishlist_count=1,
        watchlist_count=1,
        click_count=0,
    )

    trigger_types = {trigger["type"] for trigger in payload["triggers"]}
    assert trigger_types == {"price_change", "review_change", "release_event", "relevance_increase"}


def test_insight_engine_payload_bridges_strong_sale_start_to_non_null_primary_insight():
    now = datetime.datetime(2026, 3, 14, 3, 59, 14, 230367, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    recorded_at = datetime.datetime(2026, 3, 14, 3, 53, 6, 284466, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    payload = _build_insight_engine_payload(
        game_id=64384,
        now=now,
        latest_recorded_at=recorded_at,
        prior_snapshot_state={
            "snapshot_exists": True,
            "price": None,
            "original_price": None,
            "discount_percent": 0,
            "historical_low": None,
            "player_momentum": 0.0,
            "daily_peak": None,
            "review_label": None,
            "is_upcoming": False,
            "popularity_score": 0.0,
        },
        latest_price=6.24,
        latest_original_price=24.99,
        latest_discount_percent=75,
        review_score=89.0,
        review_score_label=None,
        is_upcoming=False,
        popularity_score=0.0,
        is_historical_low=True,
        is_new_historical_low=True,
        wishlist_count=0,
        watchlist_count=0,
        click_count=0,
    )

    assert payload["triggers"] == [
        {
            "type": "price_change",
            "gameId": "64384",
            "timestamp": 1773485586284,
            "previous": 24.99,
            "current": 6.24,
        }
    ]

    result = _run_insight_engine_subprocess(payload["triggers"], payload["context"])
    assert result["output"]["primaryInsight"] is not None
    assert result["output"]["primaryInsight"]["type"] == "price_change"
    assert result["output"]["primaryInsight"]["gameId"] == "64384"


def test_insight_engine_payload_keeps_empty_no_signal_cases_safe():
    now = datetime.datetime(2026, 3, 14, 3, 59, 14, 230367, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    recorded_at = datetime.datetime(2026, 3, 14, 3, 53, 6, 284466, tzinfo=datetime.timezone(datetime.timedelta(hours=-7)))
    payload = _build_insight_engine_payload(
        game_id=70000,
        now=now,
        latest_recorded_at=recorded_at,
        prior_snapshot_state={
            "snapshot_exists": True,
            "price": 19.99,
            "original_price": 19.99,
            "discount_percent": 0,
            "historical_low": 19.99,
            "player_momentum": 0.0,
            "daily_peak": 100,
            "review_label": "Mostly Positive",
            "is_upcoming": False,
            "popularity_score": 0.0,
        },
        latest_price=19.99,
        latest_original_price=19.99,
        latest_discount_percent=0,
        review_score=74.0,
        review_score_label="Mostly Positive",
        is_upcoming=False,
        popularity_score=0.0,
        is_historical_low=False,
        is_new_historical_low=False,
        wishlist_count=0,
        watchlist_count=0,
        click_count=0,
    )

    assert payload["triggers"] == []

    result = _run_insight_engine_subprocess(payload["triggers"], payload["context"])
    assert result["output"]["primaryInsight"] is None
    assert result["output"]["supportingSignals"] == []
    assert result["debug"] == []
