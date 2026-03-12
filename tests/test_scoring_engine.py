from jobs.refresh_snapshots import compute_deal_heat, compute_momentum_score, compute_worth_buying_score


def test_worth_buying_score_prefers_strong_signal_bundle():
    score, components, reason = compute_worth_buying_score(
        discount_percent=70,
        review_score=90,
        review_count=50000,
        avg_player_count=12000,
        player_growth_ratio=1.6,
        latest_price=19.99,
        historical_low_price=18.99,
        historical_low_hit=False,
    )
    assert score > 60
    assert components["discount_component"] > 0
    assert "discount" in reason or "momentum" in reason


def test_momentum_tiny_sample_guard_reduces_false_spikes():
    low_pop_score, *_ = compute_momentum_score(
        discount_percent=10,
        current_players=50,
        avg_players_last_24h=10,
    )
    high_pop_score, *_ = compute_momentum_score(
        discount_percent=10,
        current_players=5000,
        avg_players_last_24h=1000,
    )
    assert low_pop_score < high_pop_score


def test_deal_heat_historical_low_gets_hot_tag():
    level, reason, tags = compute_deal_heat(
        discount_percent=55,
        review_score=88,
        current_players=9000,
        player_growth_ratio=1.7,
        historical_low_hit=True,
        trend_reason_summary="Players up 100%",
    )
    assert level in {"hot", "viral"}
    assert "historical_low" in tags
    assert reason
