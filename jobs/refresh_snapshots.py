from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import argparse
import datetime
import json
import math
import os
import time
import uuid
from statistics import median
from typing import Any

from sqlalchemy import case, func, text
from sqlalchemy.orm import Session

from config import (
    DIRTY_QUEUE_FETCH_SIZE as CONFIG_DIRTY_QUEUE_FETCH_SIZE,
    INGESTION_ROLLOUT_HOLD_TIER,
    SNAPSHOT_ALERT_DEDUPE_HOURS,
    SNAPSHOT_BATCH_SIZE as CONFIG_SNAPSHOT_BATCH_SIZE,
    SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES,
    SNAPSHOT_DEAL_RADAR_ALERT_SCAN_LIMIT,
    SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_ABS,
    SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_PCT,
    SNAPSHOT_DEAL_RADAR_DISCOUNT_POOL,
    SNAPSHOT_DEAL_RADAR_DIVERSITY_WINDOW,
    SNAPSHOT_DEAL_RADAR_LIMIT,
    SNAPSHOT_DEAL_RADAR_LOOKBACK_DAYS,
    SNAPSHOT_DEAL_RADAR_MAX_PER_SIGNAL,
    SNAPSHOT_DEAL_RADAR_MAX_SIGNAL_SHARE,
    SNAPSHOT_DEAL_RADAR_MIN_SIGNAL_CATEGORIES,
    SNAPSHOT_DEAL_RADAR_POPULAR_POOL,
    SNAPSHOT_DEAL_RADAR_TRENDING_POOL,
    SNAPSHOT_HOMEPAGE_DEAL_CANDIDATE_POOL,
    SNAPSHOT_HOMEPAGE_DIVERSITY_WINDOW,
    SNAPSHOT_HOMEPAGE_RAIL_LIMIT,
    SNAPSHOT_IDLE_SLEEP_SECONDS,
    SNAPSHOT_MAX_BATCH_SIZE as CONFIG_SNAPSHOT_MAX_BATCH_SIZE,
    SNAPSHOT_MIN_BATCH_SIZE as CONFIG_SNAPSHOT_MIN_BATCH_SIZE,
    SNAPSHOT_PREDICTION_SALE_HISTORY_LIMIT,
    SNAPSHOT_RETRY_BACKOFF_BASE_SECONDS,
    SNAPSHOT_RETRY_BACKOFF_EXPONENT_CAP,
    SNAPSHOT_RETRY_BACKOFF_MAX_SECONDS,
    SNAPSHOT_SALE_EVENT_GAP_DAYS,
    SNAPSHOT_SALE_EVENTS_MAX,
    SNAPSHOT_SPARKLINE_POINTS,
    SNAPSHOT_UPCOMING_LIMIT,
    validate_settings,
)
from database import direct_engine
from database.job_status import normalize_counter_triplet
from database.models import (
    Alert,
    Session as DBSession,
    DashboardCache,
    DealWatchlist,
    DealEvent,
    DirtyGame,
    Game,
    GameInterestSignal,
    JobStatus,
    GamePrice,
    GamePlayerHistory,
    GamePriceLow,
    GameSnapshot,
    LatestGamePrice,
    PushSubscription,
    UserAlert,
    Watchlist,
    WishlistItem,
    WatchlistItem,
)
from database.schema_guard import assert_scale_schema_ready
from services.push_notifications import send_push_notification

CACHE_KEY = "home_v1"
LEGACY_CACHE_KEYS = ("home",)
# Shared runtime settings (defined once in config.py).
SNAPSHOT_MIN_BATCH_SIZE = CONFIG_SNAPSHOT_MIN_BATCH_SIZE
MAX_BATCH_SIZE = CONFIG_SNAPSHOT_MAX_BATCH_SIZE
BATCH_SIZE = CONFIG_SNAPSHOT_BATCH_SIZE
DIRTY_QUEUE_FETCH_SIZE = CONFIG_DIRTY_QUEUE_FETCH_SIZE
IDLE_SLEEP_SECONDS = SNAPSHOT_IDLE_SLEEP_SECONDS
SPARKLINE_POINTS = SNAPSHOT_SPARKLINE_POINTS
SALE_EVENTS_MAX = SNAPSHOT_SALE_EVENTS_MAX
SALE_EVENT_GAP_DAYS = SNAPSHOT_SALE_EVENT_GAP_DAYS
PREDICTION_SALE_HISTORY_LIMIT = SNAPSHOT_PREDICTION_SALE_HISTORY_LIMIT
UPCOMING_LIMIT = SNAPSHOT_UPCOMING_LIMIT
HOMEPAGE_RAIL_LIMIT = SNAPSHOT_HOMEPAGE_RAIL_LIMIT
HOMEPAGE_DEAL_CANDIDATE_POOL = SNAPSHOT_HOMEPAGE_DEAL_CANDIDATE_POOL
HOMEPAGE_DIVERSITY_WINDOW = SNAPSHOT_HOMEPAGE_DIVERSITY_WINDOW
RETRY_BACKOFF_BASE_SECONDS = SNAPSHOT_RETRY_BACKOFF_BASE_SECONDS
RETRY_BACKOFF_MAX_SECONDS = SNAPSHOT_RETRY_BACKOFF_MAX_SECONDS
RETRY_BACKOFF_EXPONENT_CAP = SNAPSHOT_RETRY_BACKOFF_EXPONENT_CAP
HOMEPAGE_DIVERSITY_RAIL_ORDER = (
    "deal_ranked",
    "worth_buying_now",
    "recommended_deals",
    "biggest_deals",
    "trending_deals",
)
EXTENDED_PLATFORM_FILTER_OPTIONS = ("Steam Deck", "VR Compatibility")
UTC = datetime.timezone.utc
DEAL_EVENT_NEW_SALE = "NEW_SALE"
DEAL_EVENT_PRICE_DROP = "PRICE_DROP"
DEAL_EVENT_HISTORICAL_LOW = "HISTORICAL_LOW"
DEAL_EVENT_PLAYER_SPIKE = "PLAYER_SPIKE"
ALERT_PRICE_DROP = "PRICE_DROP"
ALERT_NEW_HISTORICAL_LOW = "NEW_HISTORICAL_LOW"
ALERT_SALE_STARTED = "SALE_STARTED"
ALERT_PLAYER_SURGE = "PLAYER_SURGE"
ALERT_PRICE_TARGET_HIT = "PRICE_TARGET_HIT"
ALERT_DISCOUNT_TARGET_HIT = "DISCOUNT_TARGET_HIT"
ALERT_DEDUPE_HOURS = SNAPSHOT_ALERT_DEDUPE_HOURS
DEAL_RADAR_LIMIT = SNAPSHOT_DEAL_RADAR_LIMIT
DEAL_RADAR_LOOKBACK_DAYS = SNAPSHOT_DEAL_RADAR_LOOKBACK_DAYS
DEAL_RADAR_ALERT_SCAN_LIMIT = SNAPSHOT_DEAL_RADAR_ALERT_SCAN_LIMIT
DEAL_RADAR_TRENDING_POOL = SNAPSHOT_DEAL_RADAR_TRENDING_POOL
DEAL_RADAR_POPULAR_POOL = SNAPSHOT_DEAL_RADAR_POPULAR_POOL
DEAL_RADAR_DISCOUNT_POOL = SNAPSHOT_DEAL_RADAR_DISCOUNT_POOL
DEAL_RADAR_MAX_PER_SIGNAL = SNAPSHOT_DEAL_RADAR_MAX_PER_SIGNAL
DEAL_RADAR_MAX_SIGNAL_SHARE = SNAPSHOT_DEAL_RADAR_MAX_SIGNAL_SHARE
DEAL_RADAR_DIVERSITY_WINDOW = SNAPSHOT_DEAL_RADAR_DIVERSITY_WINDOW
DEAL_RADAR_MIN_SIGNAL_CATEGORIES = SNAPSHOT_DEAL_RADAR_MIN_SIGNAL_CATEGORIES
DEAL_RADAR_BIG_DROP_MIN_ABS = SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_ABS
DEAL_RADAR_BIG_DROP_MIN_PCT = SNAPSHOT_DEAL_RADAR_BIG_DROP_MIN_PCT
DEAL_RADAR_SIGNAL_NEW_HISTORICAL_LOW = "NEW_HISTORICAL_LOW"
DEAL_RADAR_SIGNAL_BIG_PRICE_DROP = "BIG_PRICE_DROP"
DEAL_RADAR_SIGNAL_PLAYER_SURGE = "PLAYER_SURGE"
DEAL_RADAR_SIGNAL_SALE_STARTED = "SALE_STARTED"
DEAL_RADAR_SIGNAL_BIG_DISCOUNT = "BIG_DISCOUNT"
DEAL_RADAR_SIGNAL_NEAR_HISTORICAL_LOW = "NEAR_HISTORICAL_LOW"
DEAL_RADAR_SIGNAL_TRENDING = "TRENDING"
DEAL_RADAR_SIGNAL_POPULAR_NOW = "POPULAR_NOW"
DEAL_RADAR_SIGNAL_PRIORITY = {
    DEAL_RADAR_SIGNAL_NEW_HISTORICAL_LOW: 7,
    DEAL_RADAR_SIGNAL_SALE_STARTED: 6,
    DEAL_RADAR_SIGNAL_BIG_PRICE_DROP: 5,
    DEAL_RADAR_SIGNAL_BIG_DISCOUNT: 4,
    DEAL_RADAR_SIGNAL_NEAR_HISTORICAL_LOW: 3,
    DEAL_RADAR_SIGNAL_PLAYER_SURGE: 3,
    DEAL_RADAR_SIGNAL_TRENDING: 2,
    DEAL_RADAR_SIGNAL_POPULAR_NOW: 1,
}
WORTH_BUYING_SCORE_VERSION = "v1"
MOMENTUM_SCORE_VERSION = "v1"
WORKER_ID = os.getenv("SNAPSHOT_WORKER_ID") or f"refresh_snapshots:{uuid.uuid4().hex[:8]}"
ROLLOUT_HOLD_TIER = INGESTION_ROLLOUT_HOLD_TIER
DIRTY_CLAIM_PREDICATE_SQL = (
    "(next_attempt_at IS NULL OR next_attempt_at <= now()) "
    "AND (locked_at IS NULL OR locked_at < now() - interval '10 minutes')"
)
EVENT_TO_ALERT_TYPE = {
    DEAL_EVENT_NEW_SALE: ALERT_SALE_STARTED,
    DEAL_EVENT_PRICE_DROP: ALERT_PRICE_DROP,
    DEAL_EVENT_HISTORICAL_LOW: ALERT_NEW_HISTORICAL_LOW,
    DEAL_EVENT_PLAYER_SPIKE: ALERT_PLAYER_SURGE,
}


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(UTC)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        numeric = float(value)
        if math.isnan(numeric) or math.isinf(numeric):
            return default
        return numeric
    except Exception:
        return default


def split_csv_field(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part and part.strip()]


def clamp_batch_size(batch_size: int) -> int:
    return max(SNAPSHOT_MIN_BATCH_SIZE, min(int(batch_size), MAX_BATCH_SIZE))


def compute_retry_backoff_seconds(retry_count: int) -> int:
    attempt = max(1, min(int(retry_count), RETRY_BACKOFF_EXPONENT_CAP))
    delay_seconds = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    return int(min(RETRY_BACKOFF_MAX_SECONDS, delay_seconds))


def compute_deal_score(
    discount_percent: float,
    latest_price: float | None,
    historical_low: float | None,
    review_score: float,
    review_count: float,
    avg_player_count: float,
    player_momentum: float,
) -> float:
    discount_component = clamp(discount_percent, 0.0, 100.0) * 0.45

    historical_component = 0.0
    if latest_price and latest_price > 0 and historical_low and historical_low > 0:
        proximity = clamp(historical_low / latest_price, 0.0, 1.0)
        historical_component = proximity * 25.0

    review_quality = clamp(review_score, 0.0, 100.0)
    review_confidence = clamp(math.log10(max(review_count, 1.0)) / 4.0, 0.0, 1.0)
    review_component = (review_quality / 100.0) * review_confidence * 15.0

    player_component = clamp(math.log10(max(avg_player_count, 0.0) + 1.0) * 4.0, 0.0, 10.0)
    momentum_component = clamp(player_momentum, -5.0, 10.0)

    total = discount_component + historical_component + review_component + player_component + momentum_component
    return round(clamp(total, 0.0, 100.0), 2)


def downsample(points: list[dict], target: int = SPARKLINE_POINTS) -> list[dict]:
    if len(points) <= target:
        return points
    step = (len(points) - 1) / (target - 1)
    sampled = []
    for i in range(target):
        sampled.append(points[int(round(i * step))])
    return sampled


def _parse_release_date(text_value: str | None) -> datetime.date | None:
    if not text_value:
        return None

    text_clean = text_value.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b, %Y", "%d %B, %Y", "%b %Y", "%B %Y", "%Y"):
        try:
            parsed = datetime.datetime.strptime(text_clean, fmt).date()
            if fmt in {"%b %Y", "%B %Y", "%Y"}:
                parsed = parsed.replace(day=1)
            return parsed
        except ValueError:
            continue

    return None


def compute_upcoming_hot_score(
    release_date: datetime.date | None,
    wishlist_count: int,
    watchlist_count: int,
    review_score: float,
    review_count: float,
) -> float:
    now_date = utcnow().date()

    release_proximity = 0.0
    if release_date:
        days_out = (release_date - now_date).days
        if days_out >= 0:
            release_proximity = 35.0 * (1.0 / (1.0 + (days_out / 30.0)))

    demand_component = clamp(wishlist_count * 1.8 + watchlist_count * 1.2, 0.0, 80.0)
    review_component = clamp(review_score, 0.0, 100.0) * clamp(math.log10(max(review_count, 1.0)) / 4.0, 0.0, 1.0) * 0.2
    base = 20.0

    return round(clamp(base + release_proximity + demand_component + review_component, 0.0, 250.0), 2)


def compute_popularity_score(
    click_count: int,
    wishlist_count: int,
    watchlist_count: int,
    last_clicked_at: datetime.datetime | None,
) -> float:
    score = 0.0
    score += min(30.0, click_count * 1.5)
    score += min(40.0, wishlist_count * 2.0)
    score += min(25.0, watchlist_count * 1.5)

    if last_clicked_at:
        clicked_at = last_clicked_at
        if clicked_at.tzinfo is None:
            clicked_at = clicked_at.replace(tzinfo=UTC)
        age = utcnow() - clicked_at
        if age <= datetime.timedelta(days=1):
            score += 15.0
        elif age <= datetime.timedelta(days=7):
            score += 8.0

    return round(clamp(score, 0.0, 100.0), 2)


def compute_recommended_score(
    deal_score: float,
    click_count: int,
    wishlist_count: int,
    watchlist_count: int,
    is_historical_low: bool,
    deal_detected_at: datetime.datetime | None,
) -> float:
    score = float(safe_num(deal_score, 0.0))
    score += min(15.0, wishlist_count * 2.0)
    score += min(10.0, watchlist_count * 1.5)
    score += min(10.0, click_count * 1.0)
    if is_historical_low:
        score += 12.0

    if deal_detected_at:
        detected_at = deal_detected_at
        if detected_at.tzinfo is None:
            detected_at = detected_at.replace(tzinfo=UTC)
        if utcnow() - detected_at <= datetime.timedelta(days=1):
            score += 8.0

    return round(clamp(score, 0.0, 150.0), 2)


def compute_momentum_score(
    discount_percent: int | None,
    current_players: int | None,
    avg_players_last_24h: float | None,
) -> tuple[float, float, float, str]:
    discount = max(0.0, safe_num(discount_percent, 0.0))
    players = max(0.0, safe_num(current_players, 0.0))
    baseline = max(1.0, safe_num(avg_players_last_24h, 1.0))
    growth_ratio = players / baseline if players > 0 else 0.0
    short_term_trend = growth_ratio - 1.0

    # Avoid tiny-sample spikes dominating trend ranking.
    tiny_sample_guard = 0.4 if players < 300 else 1.0
    spike_bonus = 0.0
    if players >= 1000 and growth_ratio >= 1.8:
        spike_bonus = 10.0
    elif players >= 500 and growth_ratio >= 1.5:
        spike_bonus = 6.0

    momentum_score = (
        discount * 0.45
        + math.log1p(players) * 1.7
        + max(0.0, short_term_trend) * 28.0
        + spike_bonus
    ) * tiny_sample_guard
    momentum_score = round(clamp(momentum_score, 0.0, 100.0), 2)
    growth_ratio = round(max(0.0, growth_ratio), 6)
    short_term_trend = round(short_term_trend, 6)

    growth_pct = int(round(max(0.0, (growth_ratio - 1.0) * 100.0)))
    if growth_pct >= 150:
        reason = f"Players up {growth_pct}% while discounted"
    elif growth_pct >= 60:
        reason = f"On sale and climbing fast (+{growth_pct}%)"
    elif discount >= 50:
        reason = "Deep discount with positive player momentum"
    else:
        reason = "Discounted with steady player momentum"
    return momentum_score, growth_ratio, short_term_trend, reason


def compute_worth_buying_score(
    discount_percent: int | None,
    review_score: int | None,
    review_count: int | None,
    avg_player_count: int | None,
    player_growth_ratio: float | None,
    latest_price: float | None,
    historical_low_price: float | None,
    historical_low_hit: bool,
) -> tuple[float, dict[str, float], str]:
    discount_value = clamp(safe_num(discount_percent, 0.0), 0.0, 100.0)
    review_value = clamp(safe_num(review_score, 0.0), 0.0, 100.0)
    review_count_value = max(0.0, safe_num(review_count, 0.0))
    players = max(0.0, safe_num(avg_player_count, 0.0))
    growth_ratio = max(0.0, safe_num(player_growth_ratio, 0.0))

    discount_component = round(discount_value * 0.42, 2)
    review_confidence = clamp(math.log10(max(10.0, review_count_value)) / 4.0, 0.0, 1.0)
    review_component = round((review_value / 100.0) * review_confidence * 24.0, 2)
    player_activity_component = round(clamp(math.log10(players + 1.0) * 5.5, 0.0, 14.0), 2)
    player_growth_component = round(clamp((growth_ratio - 1.0) * 18.0, 0.0, 16.0), 2)
    historical_low_component = 0.0
    if latest_price and latest_price > 0 and historical_low_price and historical_low_price > 0:
        proximity = clamp(historical_low_price / latest_price, 0.0, 1.0)
        historical_low_component = round(proximity * 14.0, 2)
    if historical_low_hit:
        historical_low_component = round(min(16.0, historical_low_component + 6.0), 2)

    components = {
        "discount_component": discount_component,
        "review_component": review_component,
        "player_activity_component": player_activity_component,
        "player_growth_component": player_growth_component,
        "historical_low_component": historical_low_component,
    }
    score = round(clamp(sum(components.values()), 0.0, 100.0), 2)

    reasons: list[str] = []
    if review_value >= 85 and review_confidence > 0.4:
        reasons.append("high reviews")
    if discount_value >= 50:
        reasons.append("meaningful discount")
    if players >= 5000:
        reasons.append("strong player activity")
    if growth_ratio >= 1.4:
        reasons.append("rising momentum")
    if historical_low_hit:
        reasons.append("new historical low")
    elif historical_low_component >= 10.0:
        reasons.append("near historical low")
    reason_summary = " + ".join(reasons[:3]) if reasons else "Balanced value signal"
    return score, components, reason_summary


def compute_buy_recommendation(
    current_price: float | None,
    historical_low: float | None,
    discount_percent: int | None,
    days_since_last_sale: int | None,
) -> tuple[str, str, float | None]:
    ratio = None
    if current_price is not None and current_price > 0 and historical_low is not None and historical_low > 0:
        ratio = round(current_price / historical_low, 6)
        if current_price <= historical_low * 1.05:
            return "BUY_NOW", "Price near historical low", ratio

    normalized_discount = int(safe_num(discount_percent, -1.0)) if discount_percent is not None else None
    if normalized_discount is not None and normalized_discount < 25:
        return "WAIT", "Discount depth historically larger", ratio

    if days_since_last_sale is not None and days_since_last_sale < 30:
        return "WAIT", "Recent sale suggests another upcoming", ratio

    if ratio is None:
        return "WAIT", "Insufficient historical context to confirm favorable timing", ratio

    return "BUY_NOW", "Price favorable relative to history", ratio


def _normalize_price_bucket(value: float | None) -> float | None:
    numeric = safe_num(value, default=0.0)
    if numeric <= 0:
        return None
    return round(float(numeric), 2)


def _build_distinct_sale_events(
    sale_rows: list[tuple[datetime.datetime | None, float | None, int | None]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for recorded_at, price, discount_percent in sale_rows:
        if recorded_at is None:
            continue
        normalized.append(
            {
                "recorded_at": recorded_at,
                "price": _normalize_price_bucket(price),
                "discount_percent": int(clamp(safe_num(discount_percent, 0.0), 0.0, 100.0)),
            }
        )

    if not normalized:
        return []

    normalized.sort(key=lambda row: row["recorded_at"])
    events: list[dict[str, Any]] = []
    current_event: dict[str, Any] | None = None
    previous_timestamp: datetime.datetime | None = None

    for row in normalized:
        recorded_at = row["recorded_at"]
        gap_days = None
        if previous_timestamp is not None:
            gap_days = (recorded_at - previous_timestamp).total_seconds() / 86400.0

        if current_event is None or (gap_days is not None and gap_days > SALE_EVENT_GAP_DAYS):
            if current_event is not None:
                events.append(current_event)
            current_event = {
                "start_at": recorded_at,
                "end_at": recorded_at,
                "best_price": row["price"],
                "max_discount_percent": int(row["discount_percent"]),
                "observations": 1,
            }
        else:
            current_event["end_at"] = recorded_at
            current_event["max_discount_percent"] = max(
                int(current_event["max_discount_percent"]),
                int(row["discount_percent"]),
            )
            current_event["observations"] = int(current_event["observations"]) + 1
            row_price = row["price"]
            if row_price is not None:
                best_price = current_event.get("best_price")
                if best_price is None or float(row_price) < float(best_price):
                    current_event["best_price"] = row_price

        previous_timestamp = recorded_at

    if current_event is not None:
        events.append(current_event)

    return events


def compute_next_sale_prediction(
    current_price: float | None,
    latest_original_price: float | None,
    historical_low_price: float | None,
    sale_rows: list[tuple[datetime.datetime | None, float | None, int | None]],
) -> dict[str, Any]:
    sale_events = _build_distinct_sale_events(sale_rows)
    fallback_window_min = 30
    fallback_window_max = 90

    if not sale_events:
        best_known_low = _normalize_price_bucket(historical_low_price)
        near_best_now = bool(
            safe_num(current_price, 0.0) > 0
            and safe_num(best_known_low, 0.0) > 0
            and safe_num(current_price, 0.0) <= safe_num(best_known_low, 0.0) * 1.05
        )
        fallback_reason = "Discount history is sparse, so prediction confidence is low."
        if near_best_now:
            fallback_reason += " Current price is already near the best historical sale price."

        fallback_discount = None
        if best_known_low is not None:
            base_price = safe_num(latest_original_price, 0.0)
            if base_price <= 0:
                base_price = safe_num(current_price, 0.0)
            if base_price > 0 and best_known_low < base_price:
                fallback_discount = int(
                    clamp(
                        round((1.0 - (float(best_known_low) / float(base_price))) * 100.0),
                        1.0,
                        95.0,
                    )
                )

        return {
            "predicted_next_sale_price": best_known_low if near_best_now else None,
            "predicted_next_discount_percent": fallback_discount if near_best_now else None,
            "predicted_next_sale_window_days_min": fallback_window_min,
            "predicted_next_sale_window_days_max": fallback_window_max,
            "predicted_sale_confidence": "LOW",
            "predicted_sale_reason": fallback_reason,
        }

    intervals: list[int] = []
    for idx in range(1, len(sale_events)):
        prior = sale_events[idx - 1]["start_at"]
        current = sale_events[idx]["start_at"]
        gap_days = (current - prior).days
        if gap_days > 0:
            intervals.append(int(gap_days))

    discount_counts: dict[int, int] = {}
    discount_values: list[int] = []
    for event in sale_events:
        discount_value = int(safe_num(event.get("max_discount_percent"), 0.0))
        if discount_value > 0:
            discount_counts[discount_value] = discount_counts.get(discount_value, 0) + 1
            discount_values.append(discount_value)

    most_frequent_discount = None
    most_frequent_discount_count = 0
    if discount_counts:
        sorted_discounts = sorted(
            discount_counts.items(),
            key=lambda item: (-item[1], -item[0]),
        )
        most_frequent_discount, most_frequent_discount_count = sorted_discounts[0]

    price_counts: dict[float, int] = {}
    for event in sale_events:
        event_price = _normalize_price_bucket(event.get("best_price"))
        if event_price is None:
            continue
        price_counts[event_price] = price_counts.get(event_price, 0) + 1

    repeated_prices = [(price, count) for price, count in price_counts.items() if count >= 2]
    best_repeated_price = min((price for price, _ in repeated_prices), default=None)
    best_repeated_price_count = max((count for _, count in repeated_prices), default=0)

    best_historical_price = min(price_counts.keys()) if price_counts else None
    if best_historical_price is None:
        best_historical_price = _normalize_price_bucket(historical_low_price)

    if len(intervals) >= 2:
        median_spacing = max(7, int(round(float(median(intervals)))))
        window_min = max(14, int(round(median_spacing * 0.8)))
        window_max = max(window_min + 7, int(round(median_spacing * 1.2)))
    elif len(intervals) == 1:
        single_spacing = max(7, int(intervals[0]))
        window_min = max(21, int(round(single_spacing * 0.75)))
        window_max = max(window_min + 7, int(round(single_spacing * 1.35)))
    else:
        window_min = fallback_window_min
        window_max = fallback_window_max

    predicted_next_sale_price = best_repeated_price or best_historical_price
    if predicted_next_sale_price is None and most_frequent_discount is not None:
        base_price = safe_num(latest_original_price, 0.0)
        if base_price <= 0:
            base_price = safe_num(current_price, 0.0)
        if base_price > 0:
            predicted_next_sale_price = round(
                max(0.01, base_price * (1.0 - (float(most_frequent_discount) / 100.0))),
                2,
            )

    predicted_next_discount_percent = most_frequent_discount
    if predicted_next_discount_percent is None and predicted_next_sale_price is not None:
        base_price = safe_num(latest_original_price, 0.0)
        if base_price <= 0:
            base_price = safe_num(current_price, 0.0)
        if base_price > 0 and predicted_next_sale_price < base_price:
            predicted_next_discount_percent = int(
                clamp(
                    round((1.0 - (float(predicted_next_sale_price) / float(base_price))) * 100.0),
                    1.0,
                    95.0,
                )
            )

    interval_spread = (max(intervals) - min(intervals)) if len(intervals) >= 2 else None
    discount_spread = (max(discount_values) - min(discount_values)) if len(discount_values) >= 2 else None

    if (
        len(sale_events) >= 4
        and len(intervals) >= 3
        and most_frequent_discount_count >= 3
        and (interval_spread is None or interval_spread <= 45)
        and (discount_spread is None or discount_spread <= 20)
    ):
        confidence = "HIGH"
    elif len(sale_events) >= 2:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    near_best_now = bool(
        safe_num(current_price, 0.0) > 0
        and safe_num(best_historical_price, 0.0) > 0
        and safe_num(current_price, 0.0) <= safe_num(best_historical_price, 0.0) * 1.05
    )

    reason_parts: list[str] = []
    if most_frequent_discount is not None and most_frequent_discount_count >= 2:
        reason_parts.append(
            f"This game has repeated a {int(most_frequent_discount)}% discount across {int(most_frequent_discount_count)} prior sales."
        )
    elif best_repeated_price is not None and best_repeated_price_count >= 2:
        reason_parts.append(
            f"The ${best_repeated_price:.2f} sale price has repeated across {int(best_repeated_price_count)} prior sales."
        )
    elif len(sale_events) >= 2:
        reason_parts.append(
            f"This game has {len(sale_events)} prior sale events, but discount patterns are less consistent."
        )
    else:
        reason_parts.append("Discount history is sparse, so prediction confidence is low.")

    if intervals:
        median_interval = int(round(float(median(intervals))))
        reason_parts.append(f"Sales tend to recur about every {median_interval} days.")
    if near_best_now:
        reason_parts.append("Current price is already near the best historical sale price.")

    if confidence == "LOW" and len(sale_events) < 2:
        reason = "Discount history is sparse, so prediction confidence is low."
        if near_best_now:
            reason += " Current price is already near the best historical sale price."
    else:
        reason = " ".join(reason_parts[:2]).strip()
        if near_best_now and "Current price is already near the best historical sale price." not in reason:
            reason = f"{reason} Current price is already near the best historical sale price.".strip()

    return {
        "predicted_next_sale_price": _normalize_price_bucket(predicted_next_sale_price),
        "predicted_next_discount_percent": predicted_next_discount_percent,
        "predicted_next_sale_window_days_min": int(window_min),
        "predicted_next_sale_window_days_max": int(window_max),
        "predicted_sale_confidence": confidence,
        "predicted_sale_reason": reason,
    }


def compute_deal_heat(
    discount_percent: int | None,
    review_score: int | None,
    current_players: int | None,
    player_growth_ratio: float | None,
    historical_low_hit: bool,
    trend_reason_summary: str | None,
) -> tuple[str, str, list[str]]:
    discount = clamp(safe_num(discount_percent, 0.0), 0.0, 100.0)
    reviews = clamp(safe_num(review_score, 0.0), 0.0, 100.0)
    players = max(0.0, safe_num(current_players, 0.0))
    growth = max(0.0, safe_num(player_growth_ratio, 0.0))

    tags: list[str] = []
    if historical_low_hit:
        tags.append("historical_low")
    if discount >= 60:
        tags.append("major_discount")
    if reviews >= 85:
        tags.append("high_reviews")
    if players >= 8000:
        tags.append("high_activity")
    if growth >= 1.5:
        tags.append("player_spike")
        tags.append("trending_up")

    if len(tags) >= 4:
        level = "viral"
    elif len(tags) >= 2:
        level = "hot"
    else:
        level = "warm"

    if historical_low_hit and discount >= 40:
        reason = "Now at a new historical low with a strong discount"
    elif growth >= 1.8 and discount > 0:
        reason = "Player counts are surging while the game is discounted"
    elif reviews >= 90 and discount >= 30:
        reason = "Overwhelming player sentiment plus a meaningful discount"
    elif trend_reason_summary:
        reason = trend_reason_summary
    else:
        reason = "Community signals and price action are aligned"
    return level, reason, tags[:8]


def upsert_alert_signal(
    session: Session,
    game_id: int,
    alert_type: str | None,
    metadata_json: dict[str, Any] | None,
    created_at: datetime.datetime | None = None,
) -> None:
    if not alert_type:
        return

    signal_time = created_at or utcnow()
    if signal_time.tzinfo is None:
        signal_time = signal_time.replace(tzinfo=UTC)

    recent_cutoff = signal_time - datetime.timedelta(hours=ALERT_DEDUPE_HOURS)
    recent_alert = (
        session.query(Alert.id)
        .filter(
            Alert.game_id == game_id,
            Alert.alert_type == alert_type,
            Alert.created_at >= recent_cutoff,
        )
        .first()
    )
    if recent_alert:
        return

    session.add(
        Alert(
            game_id=game_id,
            alert_type=alert_type,
            metadata_json=metadata_json,
            created_at=signal_time,
        )
    )


def insert_deal_event_and_user_alerts(
    session: Session,
    game_id: int,
    game_name: str | None,
    steam_appid: str | None,
    event_type: str,
    old_price: float | None,
    new_price: float | None,
    discount_percent: int | None,
    event_reason_summary: str | None = None,
    metadata_json: dict[str, Any] | None = None,
    event_dedupe_key: str | None = None,
) -> None:
    now = utcnow()
    if event_dedupe_key:
        existing = (
            session.query(DealEvent.id)
            .filter(DealEvent.event_dedupe_key == event_dedupe_key)
            .first()
        )
        if existing:
            return

    session.add(
        DealEvent(
            game_id=game_id,
            event_type=event_type,
            old_price=old_price,
            new_price=new_price,
            discount_percent=discount_percent,
            event_dedupe_key=event_dedupe_key,
            event_reason_summary=event_reason_summary,
            metadata_json=metadata_json,
        )
    )

    alert_type = EVENT_TO_ALERT_TYPE.get(event_type)
    alert_metadata = dict(metadata_json or {})
    alert_metadata.setdefault("event_type", event_type)
    alert_metadata.setdefault("event_reason_summary", event_reason_summary)
    alert_metadata.setdefault("old_price", old_price)
    alert_metadata.setdefault("new_price", new_price)
    alert_metadata.setdefault("discount_percent", discount_percent)
    upsert_alert_signal(
        session=session,
        game_id=game_id,
        alert_type=alert_type,
        metadata_json=alert_metadata,
        created_at=now,
    )

    game_label = game_name or "A wishlisted game"
    game_url = f"/games/{steam_appid}" if steam_appid else f"/games/{game_id}"
    if event_type == DEAL_EVENT_NEW_SALE:
        push_payload = {
            "title": "Game on Sale!",
            "body": f"{game_label} is now {int(safe_num(discount_percent, 0.0))}% off",
            "url": game_url,
        }
    elif event_type == DEAL_EVENT_PRICE_DROP:
        push_payload = {
            "title": "Price Drop!",
            "body": f"{game_label} dropped to ${safe_num(new_price, 0.0):.2f}",
            "url": game_url,
        }
    elif event_type == DEAL_EVENT_HISTORICAL_LOW:
        push_payload = {
            "title": "Historical Low!",
            "body": f"{game_label} reached its lowest price ever",
            "url": game_url,
        }
    else:
        push_payload = {
            "title": "Trending Game!",
            "body": f"{game_label} player activity is spiking",
            "url": game_url,
        }

    wishlist_user_rows = (
        session.query(WishlistItem.user_id)
        .filter(WishlistItem.game_id == game_id, WishlistItem.user_id.isnot(None))
        .distinct()
        .all()
    )
    watchlist_user_rows = (
        session.query(Watchlist.user_id)
        .filter(Watchlist.game_id == game_id, Watchlist.user_id.isnot(None))
        .distinct()
        .all()
    )
    user_ids = {
        str(user_id).strip()
        for (user_id,) in [*wishlist_user_rows, *watchlist_user_rows]
        if user_id is not None and str(user_id).strip()
    }
    for user_id_text in sorted(user_ids):
        recent_alert = (
            session.query(UserAlert.id)
            .filter(
                UserAlert.user_id == user_id_text,
                UserAlert.game_id == game_id,
                UserAlert.alert_type == event_type,
                UserAlert.created_at >= now - datetime.timedelta(hours=ALERT_DEDUPE_HOURS),
            )
            .first()
        )
        if recent_alert:
            continue
        session.add(
            UserAlert(
                user_id=user_id_text,
                game_id=game_id,
                alert_type=event_type,
                price=new_price,
                discount_percent=discount_percent,
            )
        )
        subscriptions = session.query(PushSubscription).filter(PushSubscription.user_id == user_id_text).all()
        for sub in subscriptions:
            subscription = {
                "endpoint": sub.endpoint,
                "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
            }
            send_push_notification(subscription, push_payload)


def process_watchlist_target_alerts(
    session: Session,
    game_id: int,
    game_name: str | None,
    steam_appid: str | None,
    latest_price: float | None,
    latest_discount_percent: int | None,
) -> None:
    watchlists = (
        session.query(DealWatchlist)
        .filter(DealWatchlist.game_id == game_id, DealWatchlist.active.is_(True))
        .all()
    )
    if not watchlists:
        return

    now = utcnow()
    cutoff = now - datetime.timedelta(hours=24)
    game_label = game_name or "A wishlisted game"
    game_url = f"/games/{steam_appid}" if steam_appid else f"/games/{game_id}"

    for row in watchlists:
        if row.target_price is None and row.target_discount_percent is None:
            continue

        alert_types: list[str] = []
        if row.target_price is not None and latest_price is not None and latest_price <= float(row.target_price):
            alert_types.append(ALERT_PRICE_TARGET_HIT)
        if (
            row.target_discount_percent is not None
            and latest_discount_percent is not None
            and latest_discount_percent >= int(row.target_discount_percent)
        ):
            alert_types.append(ALERT_DISCOUNT_TARGET_HIT)
        if not alert_types:
            continue

        subscriptions = session.query(PushSubscription).filter(PushSubscription.user_id == row.user_id).all()
        for alert_type in alert_types:
            recent = (
                session.query(UserAlert.id)
                .filter(
                    UserAlert.user_id == row.user_id,
                    UserAlert.game_id == game_id,
                    UserAlert.alert_type == alert_type,
                    UserAlert.created_at >= cutoff,
                )
                .first()
            )
            if recent:
                continue

            session.add(
                UserAlert(
                    user_id=row.user_id,
                    game_id=game_id,
                    alert_type=alert_type,
                    price=latest_price,
                    discount_percent=latest_discount_percent,
                )
            )

            if alert_type == ALERT_PRICE_TARGET_HIT:
                payload = {
                    "title": "Price Target Hit!",
                    "body": f"{game_label} is now ${safe_num(latest_price, 0.0):.2f}",
                    "url": game_url,
                }
            else:
                payload = {
                    "title": "Discount Target Hit!",
                    "body": f"{game_label} is now {int(safe_num(latest_discount_percent, 0.0))}% off",
                    "url": game_url,
                }
            for sub in subscriptions:
                subscription = {
                    "endpoint": sub.endpoint,
                    "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                }
                send_push_notification(subscription, payload)


def claim_dirty_batch(session: Session, batch_size: int) -> list[int]:
    effective_batch_size = clamp_batch_size(min(int(batch_size), int(DIRTY_QUEUE_FETCH_SIZE)))
    if session.bind and session.bind.dialect.name == "postgresql":
        rows = session.execute(
            text(
                f"""
                SELECT game_id
                FROM dirty_games
                WHERE {DIRTY_CLAIM_PREDICATE_SQL}
                ORDER BY COALESCE(next_attempt_at, updated_at) ASC, updated_at ASC
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
                """
            ),
            {"batch_size": int(effective_batch_size)},
        ).fetchall()
        game_ids = [int(row[0]) for row in rows]
        if game_ids:
            session.execute(
                text(
                    """
                    UPDATE dirty_games
                    SET locked_at = now(), locked_by = :locked_by
                    WHERE game_id = ANY(:game_ids)
                    """
                ),
                {"game_ids": game_ids, "locked_by": WORKER_ID},
            )
        return game_ids

    rows = (
        session.query(DirtyGame.game_id)
        .filter((DirtyGame.next_attempt_at.is_(None)) | (DirtyGame.next_attempt_at <= utcnow()))
        .filter((DirtyGame.locked_at.is_(None)) | (DirtyGame.locked_at < (utcnow() - datetime.timedelta(minutes=10))))
        .order_by(DirtyGame.updated_at.asc())
        .limit(effective_batch_size)
        .all()
    )
    game_ids = [int(row[0]) for row in rows]
    if game_ids:
        (
            session.query(DirtyGame)
            .filter(DirtyGame.game_id.in_(game_ids))
            .update(
                {
                    DirtyGame.locked_at: utcnow(),
                    DirtyGame.locked_by: WORKER_ID,
                },
                synchronize_session=False,
            )
        )
    return game_ids


def delete_dirty(session: Session, game_ids: list[int]) -> None:
    if not game_ids:
        return
    session.query(DirtyGame).filter(DirtyGame.game_id.in_(game_ids)).delete(synchronize_session=False)


def mark_dirty_retry(session: Session, game_ids: list[int], error_message: str) -> None:
    if not game_ids:
        return
    now = utcnow()
    if session.bind and session.bind.dialect.name == "postgresql":
        session.execute(
            text(
                """
                UPDATE dirty_games
                SET
                    retry_count = COALESCE(retry_count, 0) + 1,
                    reason = LEFT(:error_message, 255),
                    next_attempt_at = now() + (
                        LEAST(
                            :retry_backoff_max_seconds,
                            :retry_backoff_base_seconds
                            * power(
                                2,
                                GREATEST(0, LEAST(COALESCE(retry_count, 0) + 1, :retry_backoff_exponent_cap) - 1)
                            )
                        ) * interval '1 second'
                    ),
                    locked_at = NULL,
                    locked_by = NULL,
                    updated_at = now()
                WHERE game_id = ANY(:game_ids)
                """
            ),
            {
                "game_ids": game_ids,
                "error_message": error_message,
                "retry_backoff_base_seconds": float(RETRY_BACKOFF_BASE_SECONDS),
                "retry_backoff_max_seconds": float(RETRY_BACKOFF_MAX_SECONDS),
                "retry_backoff_exponent_cap": int(RETRY_BACKOFF_EXPONENT_CAP),
            },
        )
        return

    for row in session.query(DirtyGame).filter(DirtyGame.game_id.in_(game_ids)).all():
        row.retry_count = int(row.retry_count or 0) + 1
        row.reason = str(error_message)[:255]
        row.next_attempt_at = now + datetime.timedelta(seconds=compute_retry_backoff_seconds(row.retry_count))
        row.locked_at = None
        row.locked_by = None
        row.updated_at = now


def update_job_status(
    session: Session,
    job_name: str,
    started: bool = False,
    completed_success: bool = False,
    error_message: str | None = None,
    duration_ms: int | None = None,
    items_total: int | None = None,
    items_success: int | None = None,
    items_failed: int | None = None,
) -> None:
    now = utcnow()
    row = session.get(JobStatus, job_name)
    if row is None:
        row = JobStatus(job_name=job_name)
        session.add(row)
        session.flush()

    if started:
        row.last_started_at = now
        row.last_error = None
    if completed_success:
        row.last_completed_at = now
        row.last_success_at = now
        row.last_error = None
        if duration_ms is not None:
            row.last_duration_ms = int(duration_ms)
        if items_total is not None or items_success is not None or items_failed is not None:
            normalized_total, normalized_success, normalized_failed = normalize_counter_triplet(
                items_total,
                items_success,
                items_failed,
            )
            row.last_items_total = normalized_total
            row.last_items_success = normalized_success
            row.last_items_failed = normalized_failed
    if error_message is not None:
        row.last_completed_at = now
        row.last_error = error_message[:2000]
    if (
        row.last_items_total is not None
        or row.last_items_success is not None
        or row.last_items_failed is not None
    ):
        normalized_total, normalized_success, normalized_failed = normalize_counter_triplet(
            row.last_items_total,
            row.last_items_success,
            row.last_items_failed,
        )
        row.last_items_total = normalized_total
        row.last_items_success = normalized_success
        row.last_items_failed = normalized_failed
    row.updated_at = now


def refresh_price_aggregates_for_games(session: Session, game_ids: list[int]) -> None:
    if not game_ids:
        return

    if session.bind and session.bind.dialect.name == "postgresql":
        session.execute(
            text(
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
                WHERE gp.game_id = ANY(:game_ids)
                  AND gp.game_id IS NOT NULL
                  AND gp.price IS NOT NULL
                ORDER BY gp.game_id, gp.recorded_at DESC, gp.id DESC
                ON CONFLICT (game_id) DO UPDATE SET
                    latest_price = EXCLUDED.latest_price,
                    original_price = EXCLUDED.original_price,
                    latest_discount_percent = EXCLUDED.latest_discount_percent,
                    current_players = EXCLUDED.current_players,
                    recorded_at = EXCLUDED.recorded_at
                """
            ),
            {"game_ids": game_ids},
        )
        session.execute(
            text(
                """
                INSERT INTO game_price_lows (game_id, historical_low)
                SELECT gp.game_id, MIN(gp.price) AS historical_low
                FROM game_prices gp
                WHERE gp.game_id = ANY(:game_ids)
                  AND gp.price IS NOT NULL
                  AND gp.price > 0
                GROUP BY gp.game_id
                ON CONFLICT (game_id) DO UPDATE SET
                    historical_low = EXCLUDED.historical_low
                """
            ),
            {"game_ids": game_ids},
        )
        return

    # Portable fallback for SQLite/dev.
    for game_id in game_ids:
        latest = (
            session.query(GamePrice)
            .filter(GamePrice.game_id == game_id)
            .order_by(GamePrice.recorded_at.desc(), GamePrice.id.desc())
            .limit(1)
            .first()
        )
        if latest:
            row = session.query(LatestGamePrice).filter(LatestGamePrice.game_id == game_id).first()
            if row is None:
                row = LatestGamePrice(game_id=game_id)
                session.add(row)
            row.latest_price = latest.price
            row.original_price = latest.original_price
            row.latest_discount_percent = latest.discount_percent
            row.current_players = latest.current_players
            row.recorded_at = latest.recorded_at

        low = (
            session.query(func.min(GamePrice.price))
            .filter(GamePrice.game_id == game_id, GamePrice.price.isnot(None), GamePrice.price > 0)
            .scalar()
        )
        if low is not None:
            low_row = session.query(GamePriceLow).filter(GamePriceLow.game_id == game_id).first()
            if low_row is None:
                low_row = GamePriceLow(game_id=game_id)
                session.add(low_row)
            low_row.historical_low = float(low)


def _snapshot_row_to_dict(snapshot: GameSnapshot) -> dict:
    return {
        "game_id": int(snapshot.game_id),
        "game_name": snapshot.game_name,
        "steam_appid": snapshot.steam_appid,
        "store_url": snapshot.store_url,
        "banner_url": snapshot.banner_url,
        "latest_price": snapshot.latest_price,
        "latest_original_price": snapshot.latest_original_price,
        "latest_discount_percent": snapshot.latest_discount_percent,
        "price": snapshot.latest_price,
        "original_price": snapshot.latest_original_price,
        "discount_percent": snapshot.latest_discount_percent,
        "historical_low": snapshot.historical_low,
        "is_historical_low": bool(snapshot.is_historical_low),
        "deal_score": snapshot.deal_score,
        "popularity_score": snapshot.popularity_score,
        "recommended_score": snapshot.recommended_score,
        "trending_score": snapshot.trending_score,
        "buy_score": snapshot.buy_score,
        "buy_recommendation": snapshot.buy_recommendation,
        "buy_reason": snapshot.buy_reason,
        "price_vs_low_ratio": snapshot.price_vs_low_ratio,
        "predicted_next_sale_price": snapshot.predicted_next_sale_price,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "predicted_next_sale_window_days_min": snapshot.predicted_next_sale_window_days_min,
        "predicted_next_sale_window_days_max": snapshot.predicted_next_sale_window_days_max,
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "worth_buying_score": snapshot.worth_buying_score,
        "worth_buying_score_version": snapshot.worth_buying_score_version,
        "worth_buying_reason_summary": snapshot.worth_buying_reason_summary,
        "worth_buying_components": snapshot.worth_buying_components or {},
        "momentum_score": snapshot.momentum_score,
        "momentum_score_version": snapshot.momentum_score_version,
        "player_growth_ratio": snapshot.player_growth_ratio,
        "short_term_player_trend": snapshot.short_term_player_trend,
        "trend_reason_summary": snapshot.trend_reason_summary,
        "historical_low_hit": bool(snapshot.historical_low_hit),
        "historical_low_price": snapshot.historical_low_price,
        "previous_historical_low_price": snapshot.previous_historical_low_price,
        "history_point_count": snapshot.history_point_count,
        "ever_discounted": bool(snapshot.ever_discounted),
        "max_discount": snapshot.max_discount,
        "last_discounted_at": snapshot.last_discounted_at.isoformat() if snapshot.last_discounted_at else None,
        "historical_low_timestamp": snapshot.historical_low_timestamp.isoformat() if snapshot.historical_low_timestamp else None,
        "historical_low_reason_summary": snapshot.historical_low_reason_summary,
        "deal_heat_level": snapshot.deal_heat_level,
        "deal_heat_reason": snapshot.deal_heat_reason,
        "deal_heat_tags": snapshot.deal_heat_tags or [],
        "ranking_explanations": snapshot.ranking_explanations or {},
        "review_score": snapshot.review_score,
        "review_score_label": snapshot.review_score_label,
        "review_count": snapshot.review_count,
        "review_total_count": snapshot.review_count,
        "genres": split_csv_field(snapshot.genres),
        "tags": split_csv_field(snapshot.tags),
        "platforms": split_csv_field(snapshot.platforms),
        "avg_player_count": snapshot.avg_player_count,
        "avg_30d": snapshot.avg_player_count,
        "player_change": snapshot.player_change,
        "current_players": snapshot.current_players,
        "daily_peak": snapshot.daily_peak,
        "upcoming_hot_score": snapshot.upcoming_hot_score,
        "is_upcoming": bool(snapshot.is_upcoming),
        "release_date": snapshot.release_date.isoformat() if snapshot.release_date else None,
        "release_date_text": snapshot.release_date_text,
        "deal_detected_at": snapshot.deal_detected_at.isoformat() if snapshot.deal_detected_at else None,
        "price_sparkline_90d": snapshot.price_sparkline_90d or [],
        "sale_events_compact": snapshot.sale_events_compact or [],
    }


def _as_aware_utc(dt: datetime.datetime | None) -> datetime.datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _coerce_game_id(snapshot: GameSnapshot | None, latest_row: LatestGamePrice | None) -> int | None:
    candidate = snapshot.game_id if snapshot is not None else (latest_row.game_id if latest_row is not None else None)
    try:
        parsed = int(candidate) if candidate is not None else 0
    except Exception:
        parsed = 0
    return parsed if parsed > 0 else None


def _build_deal_radar_item(
    snapshot: GameSnapshot | None,
    latest_row: LatestGamePrice | None,
    signal_type: str,
    signal_text: str,
    timestamp: datetime.datetime | None,
    metadata: dict[str, Any] | None = None,
) -> dict | None:
    game_id = _coerce_game_id(snapshot, latest_row)
    if game_id is None:
        return None

    name = (
        (snapshot.game_name if snapshot is not None else None)
        or f"Game {game_id}"
    )
    banner_url = snapshot.banner_url if snapshot is not None else None
    if not banner_url and snapshot is not None and snapshot.steam_appid:
        banner_url = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{snapshot.steam_appid}/header.jpg"

    latest_price = (
        snapshot.latest_price
        if snapshot is not None and snapshot.latest_price is not None
        else latest_row.latest_price
        if latest_row is not None
        else None
    )
    latest_discount = (
        snapshot.latest_discount_percent
        if snapshot is not None and snapshot.latest_discount_percent is not None
        else latest_row.latest_discount_percent
        if latest_row is not None
        else None
    )
    players = snapshot.current_players if snapshot is not None else None
    buy_score = (
        snapshot.buy_score
        if snapshot is not None and snapshot.buy_score is not None
        else snapshot.worth_buying_score
        if snapshot is not None
        else None
    )

    resolved_ts = (
        _as_aware_utc(timestamp)
        or _as_aware_utc(snapshot.updated_at if snapshot is not None else None)
        or _as_aware_utc(latest_row.recorded_at if latest_row is not None else None)
        or utcnow()
    )
    sort_epoch = int(resolved_ts.timestamp())

    return {
        "game_id": game_id,
        "game_name": name,
        "image": banner_url,
        "banner_url": banner_url,
        "price": latest_price,
        "discount": latest_discount,
        "discount_percent": latest_discount,
        "signal_type": str(signal_type or "").upper(),
        "signal_text": signal_text,
        "timestamp": resolved_ts.isoformat(),
        "current_players": players,
        "buy_score": buy_score,
        "metadata": metadata or {},
        "_sort_ts": sort_epoch,
    }


def _deal_radar_signal_priority(signal_type: str | None) -> int:
    return int(DEAL_RADAR_SIGNAL_PRIORITY.get(str(signal_type or "").upper(), 0))


def _deal_radar_sort_key(item: dict) -> tuple[float, float, float, float, float]:
    return (
        float(_deal_radar_signal_priority(item.get("signal_type"))),
        float(int(item.get("_sort_ts") or 0)),
        float(safe_num(item.get("buy_score"), 0.0)),
        float(safe_num(item.get("discount"), 0.0)),
        float(safe_num(item.get("current_players"), 0.0)),
    )


def _deal_radar_signal_cap(limit: int) -> int:
    share_cap = int(math.ceil(max(1, int(limit)) * DEAL_RADAR_MAX_SIGNAL_SHARE))
    return max(2, min(share_cap, DEAL_RADAR_MAX_PER_SIGNAL))


def _deal_radar_diversity_target(available_signals: int, limit: int) -> int:
    if available_signals <= 0 or limit <= 0:
        return 0
    if available_signals >= 5 and limit >= 5:
        return 5
    if available_signals >= 4 and limit >= 4:
        return 4
    return min(available_signals, limit)


def _build_deal_radar_feed(session: Session, limit: int = DEAL_RADAR_LIMIT) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    lookback_cutoff = utcnow() - datetime.timedelta(days=DEAL_RADAR_LOOKBACK_DAYS)
    items: list[dict] = []
    seen_keys: set[tuple[int, str]] = set()

    def add_item(item: dict | None) -> None:
        if not item:
            return
        game_id = int(item.get("game_id") or 0)
        signal_type = str(item.get("signal_type") or "").upper()
        if game_id <= 0 or not signal_type:
            return
        key = (game_id, signal_type)
        if key in seen_keys:
            return
        seen_keys.add(key)
        items.append(item)

    alert_rows = (
        session.query(Alert, GameSnapshot, LatestGamePrice)
        .outerjoin(GameSnapshot, GameSnapshot.game_id == Alert.game_id)
        .outerjoin(LatestGamePrice, LatestGamePrice.game_id == Alert.game_id)
        .filter(
            Alert.created_at >= lookback_cutoff,
            Alert.alert_type.in_(
                [
                    ALERT_NEW_HISTORICAL_LOW,
                    ALERT_PRICE_DROP,
                    ALERT_PLAYER_SURGE,
                    ALERT_SALE_STARTED,
                ]
            ),
        )
        .order_by(Alert.created_at.desc(), Alert.id.desc())
        .limit(DEAL_RADAR_ALERT_SCAN_LIMIT)
        .all()
    )

    for alert_row, snapshot_row, latest_row in alert_rows:
        alert_type = str(alert_row.alert_type or "").upper()
        metadata = alert_row.metadata_json if isinstance(alert_row.metadata_json, dict) else {}

        signal_type: str | None = None
        signal_text = "Market signal detected."
        if alert_type == ALERT_NEW_HISTORICAL_LOW:
            signal_type = DEAL_RADAR_SIGNAL_NEW_HISTORICAL_LOW
            signal_text = "New all-time low price detected."
        elif alert_type == ALERT_PLAYER_SURGE:
            signal_type = DEAL_RADAR_SIGNAL_PLAYER_SURGE
            players = int(
                safe_num(
                    metadata.get("current_players"),
                    snapshot_row.current_players if snapshot_row is not None else 0,
                )
            )
            signal_text = (
                f"Player activity surge ({players:,} live players)."
                if players > 0
                else "Player activity is surging."
            )
        elif alert_type == ALERT_SALE_STARTED:
            signal_type = DEAL_RADAR_SIGNAL_SALE_STARTED
            discount = safe_num(
                metadata.get("discount_percent"),
                safe_num(
                    snapshot_row.latest_discount_percent if snapshot_row is not None else None,
                    safe_num(latest_row.latest_discount_percent if latest_row is not None else None, 0.0),
                ),
            )
            signal_text = (
                f"Sale just started at {discount:.0f}% off."
                if discount > 0
                else "A new sale just started."
            )
        elif alert_type == ALERT_PRICE_DROP:
            previous_price = metadata.get("old_price", metadata.get("previous_price"))
            new_price = metadata.get("new_price")
            old_val = safe_num(previous_price, -1.0)
            new_val = safe_num(new_price, -1.0)
            pct_drop = 0.0
            abs_drop = 0.0
            if old_val > 0 and new_val >= 0 and new_val < old_val:
                abs_drop = old_val - new_val
                pct_drop = (abs_drop / old_val) * 100.0

            discount = safe_num(
                snapshot_row.latest_discount_percent if snapshot_row is not None else None,
                safe_num(latest_row.latest_discount_percent if latest_row is not None else None, 0.0),
            )
            if abs_drop >= DEAL_RADAR_BIG_DROP_MIN_ABS or pct_drop >= DEAL_RADAR_BIG_DROP_MIN_PCT or discount >= 35.0:
                signal_type = DEAL_RADAR_SIGNAL_BIG_PRICE_DROP
                if old_val > 0 and new_val >= 0 and new_val < old_val:
                    signal_text = (
                        f"Price dropped {pct_drop:.0f}% (${new_val:.2f} now)."
                        if pct_drop >= DEAL_RADAR_BIG_DROP_MIN_PCT
                        else f"Price dropped to ${new_val:.2f}."
                    )
                else:
                    signal_text = "Notable price drop detected."

        if not signal_type:
            continue

        add_item(
            _build_deal_radar_item(
                snapshot=snapshot_row,
                latest_row=latest_row,
                signal_type=signal_type,
                signal_text=signal_text,
                timestamp=alert_row.created_at,
                metadata=metadata,
            )
        )

    discount_rows = (
        session.query(GameSnapshot, LatestGamePrice)
        .outerjoin(LatestGamePrice, LatestGamePrice.game_id == GameSnapshot.game_id)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.latest_discount_percent.isnot(None),
            GameSnapshot.latest_discount_percent > 0,
            GameSnapshot.latest_price.isnot(None),
            GameSnapshot.latest_price > 0,
        )
        .order_by(
            GameSnapshot.latest_discount_percent.desc().nullslast(),
            GameSnapshot.deal_score.desc().nullslast(),
            GameSnapshot.worth_buying_score.desc().nullslast(),
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.updated_at.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(DEAL_RADAR_DISCOUNT_POOL)
        .all()
    )
    for snapshot_row, latest_row in discount_rows:
        discount = safe_num(snapshot_row.latest_discount_percent, 0.0)
        worth_buying = safe_num(snapshot_row.worth_buying_score, 0.0)
        historical_status = str(snapshot_row.historical_status or "").lower()
        signal_type: str | None = None
        signal_text = "A noteworthy market signal was detected."
        if discount >= 65.0 or (discount >= 45.0 and worth_buying >= 55.0):
            signal_type = DEAL_RADAR_SIGNAL_BIG_DISCOUNT
            signal_text = f"Major discount live now ({discount:.0f}% off)."
        elif historical_status in {"near_historical_low", "matches_historical_low"} and discount >= 10.0:
            signal_type = DEAL_RADAR_SIGNAL_NEAR_HISTORICAL_LOW
            signal_text = "Price is near its all-time low during an active discount."
        if not signal_type:
            continue
        add_item(
            _build_deal_radar_item(
                snapshot=snapshot_row,
                latest_row=latest_row,
                signal_type=signal_type,
                signal_text=signal_text,
                timestamp=snapshot_row.updated_at,
                metadata={
                    "historical_status": snapshot_row.historical_status,
                    "discount_percent": snapshot_row.latest_discount_percent,
                    "worth_buying_score": snapshot_row.worth_buying_score,
                },
            )
        )

    trending_rows = (
        session.query(GameSnapshot, LatestGamePrice)
        .outerjoin(LatestGamePrice, LatestGamePrice.game_id == GameSnapshot.game_id)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.current_players.isnot(None),
            GameSnapshot.current_players > 0,
            (
                (GameSnapshot.player_change.isnot(None) & (GameSnapshot.player_change > 0))
                | (GameSnapshot.short_term_player_trend.isnot(None) & (GameSnapshot.short_term_player_trend > 0))
                | (GameSnapshot.trending_score.isnot(None) & (GameSnapshot.trending_score > 0))
            ),
        )
        .order_by(
            GameSnapshot.trending_score.desc().nullslast(),
            GameSnapshot.player_change.desc().nullslast(),
            GameSnapshot.short_term_player_trend.desc().nullslast(),
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.updated_at.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(DEAL_RADAR_TRENDING_POOL)
        .all()
    )
    for snapshot_row, latest_row in trending_rows:
        add_item(
            _build_deal_radar_item(
                snapshot=snapshot_row,
                latest_row=latest_row,
                signal_type=DEAL_RADAR_SIGNAL_TRENDING,
                signal_text=snapshot_row.trend_reason_summary or "Players are rising faster than baseline.",
                timestamp=snapshot_row.updated_at,
                metadata={
                    "player_change": snapshot_row.player_change,
                    "short_term_player_trend": snapshot_row.short_term_player_trend,
                },
            )
        )

    popular_rows = (
        session.query(GameSnapshot, LatestGamePrice)
        .outerjoin(LatestGamePrice, LatestGamePrice.game_id == GameSnapshot.game_id)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.current_players.isnot(None),
            GameSnapshot.current_players >= 250,
        )
        .order_by(
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.popularity_score.desc().nullslast(),
            GameSnapshot.updated_at.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(DEAL_RADAR_POPULAR_POOL)
        .all()
    )
    for snapshot_row, latest_row in popular_rows:
        add_item(
            _build_deal_radar_item(
                snapshot=snapshot_row,
                latest_row=latest_row,
                signal_type=DEAL_RADAR_SIGNAL_POPULAR_NOW,
                signal_text="High live player activity right now.",
                timestamp=snapshot_row.updated_at,
                metadata={
                    "current_players": snapshot_row.current_players,
                },
            )
        )

    items.sort(key=_deal_radar_sort_key, reverse=True)

    # Keep one representation per game in the feed. The global candidate sort
    # already encodes deterministic signal precedence and recency.
    deduped_items: list[dict] = []
    seen_game_ids: set[int] = set()
    for item in items:
        game_id = int(item.get("game_id") or 0)
        if game_id <= 0 or game_id in seen_game_ids:
            continue
        seen_game_ids.add(game_id)
        deduped_items.append(item)

    signal_queues: dict[str, list[dict]] = {}
    for item in deduped_items:
        signal_type = str(item.get("signal_type") or "").upper()
        if not signal_type:
            continue
        signal_queues.setdefault(signal_type, []).append(item)

    signal_order = sorted(
        signal_queues.keys(),
        key=lambda signal: (
            _deal_radar_signal_priority(signal),
            _deal_radar_sort_key(signal_queues[signal][0]),
            signal,
        ),
        reverse=True,
    )

    signal_cap = _deal_radar_signal_cap(limit)
    diversity_window = min(limit, DEAL_RADAR_DIVERSITY_WINDOW)
    diversity_target = _deal_radar_diversity_target(len(signal_order), diversity_window)

    selected: list[dict] = []
    selected_game_ids: set[int] = set()
    per_signal_counts: dict[str, int] = {}

    def take_from_signal(signal_type: str) -> bool:
        if per_signal_counts.get(signal_type, 0) >= signal_cap:
            return False
        queue = signal_queues.get(signal_type) or []
        while queue:
            item = queue.pop(0)
            game_id = int(item.get("game_id") or 0)
            if game_id <= 0 or game_id in selected_game_ids:
                continue
            selected.append(item)
            selected_game_ids.add(game_id)
            per_signal_counts[signal_type] = per_signal_counts.get(signal_type, 0) + 1
            return True
        return False

    # Ensure 4-5 distinct categories in top feed rows when enough signals exist.
    for signal_type in signal_order[:diversity_target]:
        if len(selected) >= diversity_window:
            break
        take_from_signal(signal_type)

    while len(selected) < diversity_window:
        added = False
        for signal_type in signal_order:
            if len(selected) >= diversity_window:
                break
            if take_from_signal(signal_type):
                added = True
        if not added:
            break

    leftovers: list[dict] = []
    for queue in signal_queues.values():
        leftovers.extend(queue)
    leftovers.sort(key=_deal_radar_sort_key, reverse=True)

    for item in leftovers:
        if len(selected) >= limit:
            break
        signal_type = str(item.get("signal_type") or "").upper()
        game_id = int(item.get("game_id") or 0)
        if not signal_type or game_id <= 0 or game_id in selected_game_ids:
            continue
        if per_signal_counts.get(signal_type, 0) >= signal_cap:
            continue
        selected.append(item)
        selected_game_ids.add(game_id)
        per_signal_counts[signal_type] = per_signal_counts.get(signal_type, 0) + 1

    for item in selected:
        item.pop("_sort_ts", None)

    return selected[:limit]


def _unique_snapshot_rows(rows: list[GameSnapshot]) -> list[GameSnapshot]:
    unique_rows: list[GameSnapshot] = []
    seen_ids: set[int] = set()
    for row in rows:
        game_id = int(row.game_id)
        if game_id in seen_ids:
            continue
        seen_ids.add(game_id)
        unique_rows.append(row)
    return unique_rows


def _take_diverse_rows(
    ranked_candidates: list[GameSnapshot],
    used_game_ids: set[int],
    section_limit: int,
    uniqueness_window: int,
) -> list[GameSnapshot]:
    selected: list[GameSnapshot] = []
    deferred: list[GameSnapshot] = []
    section_seen: set[int] = set()
    unique_target = max(0, min(section_limit, uniqueness_window))

    for row in ranked_candidates:
        game_id = int(row.game_id)
        if game_id in section_seen:
            continue
        if len(selected) < unique_target and game_id in used_game_ids:
            deferred.append(row)
            continue

        selected.append(row)
        section_seen.add(game_id)
        if len(selected) >= section_limit:
            return selected

    for row in deferred:
        game_id = int(row.game_id)
        if game_id in section_seen:
            continue
        selected.append(row)
        section_seen.add(game_id)
        if len(selected) >= section_limit:
            return selected

    return selected


def _apply_homepage_deal_diversity(
    rail_candidates: dict[str, list[GameSnapshot]],
    section_limit: int,
    uniqueness_window: int,
    rail_order: tuple[str, ...],
) -> dict[str, list[GameSnapshot]]:
    diversified: dict[str, list[GameSnapshot]] = {
        key: _unique_snapshot_rows(rows)
        for key, rows in rail_candidates.items()
    }

    used_game_ids: set[int] = set()
    for rail_key in rail_order:
        rows = diversified.get(rail_key, [])
        diversified_rows = _take_diverse_rows(
            ranked_candidates=rows,
            used_game_ids=used_game_ids,
            section_limit=section_limit,
            uniqueness_window=uniqueness_window,
        )
        diversified[rail_key] = diversified_rows
        for row in diversified_rows[:uniqueness_window]:
            used_game_ids.add(int(row.game_id))

    return diversified


def refresh_snapshots_once(session: Session, game_ids: list[int]) -> int:
    if not game_ids:
        return 0

    now = utcnow()
    refresh_price_aggregates_for_games(session, game_ids)

    games = session.query(Game).filter(Game.id.in_(game_ids)).all()
    game_by_id = {int(g.id): g for g in games}

    game_names = [g.name for g in games if g.name]
    wishlist_counts = {}
    watchlist_counts = {}
    if game_ids:
        wishlist_counts = {
            int(gid): int(count)
            for gid, count in (
                session.query(WishlistItem.game_id, func.count(WishlistItem.id))
                .filter(WishlistItem.game_id.in_(game_ids))
                .group_by(WishlistItem.game_id)
                .all()
            )
        }
    if game_ids:
        watchlist_counts = {
            int(gid): int(count)
            for gid, count in (
                session.query(Watchlist.game_id, func.count(Watchlist.id))
                .filter(Watchlist.game_id.in_(game_ids))
                .group_by(Watchlist.game_id)
                .all()
            )
        }
    legacy_watchlist_counts_by_name: dict[str, int] = {}
    if game_names:
        legacy_watchlist_counts_by_name = {
            str(name): int(count)
            for name, count in (
                session.query(WatchlistItem.game_name, func.count(WatchlistItem.id))
                .filter(WatchlistItem.game_name.in_(game_names))
                .group_by(WatchlistItem.game_name)
                .all()
            )
        }

    existing = {
        int(row.game_id): row
        for row in session.query(GameSnapshot).filter(GameSnapshot.game_id.in_(game_ids)).all()
    }
    latest_rows = {
        int(row.game_id): row
        for row in session.query(LatestGamePrice).filter(LatestGamePrice.game_id.in_(game_ids)).all()
    }
    low_rows = {
        int(row.game_id): row
        for row in session.query(GamePriceLow).filter(GamePriceLow.game_id.in_(game_ids)).all()
    }
    interest_rows = {
        int(row.game_id): row
        for row in session.query(GameInterestSignal).filter(GameInterestSignal.game_id.in_(game_ids)).all()
    }
    avg_players_last_24h_map = {
        int(gid): float(avg_players)
        for gid, avg_players in (
            session.query(GamePlayerHistory.game_id, func.avg(GamePlayerHistory.current_players))
            .filter(
                GamePlayerHistory.game_id.in_(game_ids),
                GamePlayerHistory.current_players.isnot(None),
                GamePlayerHistory.recorded_at >= now - datetime.timedelta(hours=24),
            )
            .group_by(GamePlayerHistory.game_id)
            .all()
        )
    }
    price_history_stats = {
        int(gid): {
            "history_point_count": int(point_count or 0),
            "max_discount": int(max_discount or 0),
            "last_discounted_at": last_discounted_at,
        }
        for gid, point_count, max_discount, last_discounted_at in (
            session.query(
                GamePrice.game_id,
                func.count(GamePrice.id),
                func.max(func.coalesce(GamePrice.discount_percent, 0)),
                func.max(case((GamePrice.discount_percent > 0, GamePrice.recorded_at), else_=None)),
            )
            .filter(
                GamePrice.game_id.in_(game_ids),
                GamePrice.price.isnot(None),
            )
            .group_by(GamePrice.game_id)
            .all()
        )
    }

    updated = 0

    for game_id in game_ids:
        game = game_by_id.get(int(game_id))
        if not game:
            continue

        snapshot = existing.get(int(game_id))
        previous_price = safe_num(snapshot.latest_price, default=0.0) if snapshot and snapshot.latest_price is not None else None
        previous_discount = (
            int(safe_num(snapshot.latest_discount_percent, default=0.0))
            if snapshot and snapshot.latest_discount_percent is not None
            else 0
        )
        previous_historical_low = (
            safe_num(snapshot.historical_low, default=0.0) if snapshot and snapshot.historical_low is not None else None
        )
        previous_player_momentum = (
            safe_num(snapshot.player_momentum, default=0.0) if snapshot and snapshot.player_momentum is not None else 0.0
        )
        previous_daily_peak = (
            int(safe_num(snapshot.daily_peak, default=0.0)) if snapshot and snapshot.daily_peak is not None else None
        )

        latest = latest_rows.get(int(game_id))

        latest_price = safe_num(latest.latest_price, default=0.0) if latest and latest.latest_price is not None else None
        latest_original_price = (
            safe_num(latest.original_price, default=0.0) if latest and latest.original_price is not None else None
        )
        latest_discount_percent = int(safe_num(latest.latest_discount_percent, default=-1.0)) if latest else None
        if latest_discount_percent is not None and latest_discount_percent < 0:
            latest_discount_percent = None

        if latest_discount_percent is None and latest_price and latest_original_price and latest_original_price > 0:
            latest_discount_percent = int(round((1.0 - (latest_price / latest_original_price)) * 100.0))
            latest_discount_percent = int(clamp(latest_discount_percent, 0.0, 100.0))

        low_row = low_rows.get(int(game_id))
        aggregate_historical_low = float(low_row.historical_low) if low_row and low_row.historical_low is not None else None
        effective_previous_historical_low = previous_historical_low
        if effective_previous_historical_low is None:
            effective_previous_historical_low = aggregate_historical_low

        is_new_historical_low = bool(
            latest_price is not None
            and latest_price > 0
            and (
                effective_previous_historical_low is None
                or latest_price < effective_previous_historical_low
            )
        )
        if is_new_historical_low:
            historical_low = float(latest_price)
        else:
            historical_low = aggregate_historical_low if aggregate_historical_low is not None else effective_previous_historical_low

        is_historical_low = bool(
            latest_price is not None
            and historical_low is not None
            and latest_price > 0
            and latest_price <= historical_low
        )
        history_stats = price_history_stats.get(int(game_id), {})
        history_point_count = int(history_stats.get("history_point_count", 0))
        max_discount = int(history_stats.get("max_discount", 0))
        last_discounted_at = history_stats.get("last_discounted_at")
        if last_discounted_at is not None and last_discounted_at.tzinfo is None:
            last_discounted_at = last_discounted_at.replace(tzinfo=UTC)
        days_since_last_sale = (now - last_discounted_at).days if last_discounted_at else None
        if days_since_last_sale is not None and days_since_last_sale < 0:
            days_since_last_sale = 0
        ever_discounted = bool(max_discount > 0 or last_discounted_at is not None)

        current_players = int(safe_num(latest.current_players, default=0.0)) if latest and latest.current_players is not None else None
        avg_player_count = current_players
        baseline_daily_peak = previous_daily_peak if previous_daily_peak and previous_daily_peak > 0 else (current_players or 1)
        avg_players_last_24h = avg_players_last_24h_map.get(int(game_id), safe_num(current_players, 1.0))
        momentum_score, player_growth_ratio, short_term_player_trend, trend_reason_summary = compute_momentum_score(
            discount_percent=latest_discount_percent,
            current_players=current_players,
            avg_players_last_24h=avg_players_last_24h,
        )
        trending_score = momentum_score
        player_momentum = max(0.0, short_term_player_trend)
        daily_peak = max(baseline_daily_peak, current_players or 0)

        deal_detected_at = None
        if latest and (latest_discount_percent or 0) > 0:
            deal_detected_at = latest.recorded_at
            if deal_detected_at and deal_detected_at.tzinfo is None:
                deal_detected_at = deal_detected_at.replace(tzinfo=UTC)

        since = now - datetime.timedelta(days=90)
        spark_rows = (
            session.query(GamePrice.recorded_at, GamePrice.price)
            .filter(
                GamePrice.game_id == game_id,
                GamePrice.recorded_at >= since,
                GamePrice.price.isnot(None),
                GamePrice.price > 0,
            )
            .order_by(GamePrice.recorded_at.asc(), GamePrice.id.asc())
            .all()
        )
        spark_points = [
            {
                "t": row[0].isoformat() if row[0] else None,
                "p": float(row[1]),
            }
            for row in spark_rows
        ]
        sparkline = downsample(spark_points, SPARKLINE_POINTS)

        prediction_sale_rows = (
            session.query(GamePrice.recorded_at, GamePrice.price, GamePrice.discount_percent)
            .filter(
                GamePrice.game_id == game_id,
                GamePrice.discount_percent.isnot(None),
                GamePrice.discount_percent > 0,
            )
            .order_by(GamePrice.recorded_at.desc(), GamePrice.id.desc())
            .limit(PREDICTION_SALE_HISTORY_LIMIT)
            .all()
        )
        sale_events_compact = [
            {
                "recorded_at": row[0].isoformat() if row[0] else None,
                "price": float(row[1]) if row[1] is not None else None,
                "discount_percent": int(row[2]) if row[2] is not None else 0,
            }
            for row in prediction_sale_rows[:SALE_EVENTS_MAX]
        ]

        review_score = safe_num(game.review_score, default=0.0)
        review_count = safe_num(game.review_total_count, default=0.0)
        deal_score = compute_deal_score(
            discount_percent=safe_num(latest_discount_percent, default=0.0),
            latest_price=latest_price,
            historical_low=historical_low,
            review_score=review_score,
            review_count=review_count,
            avg_player_count=safe_num(avg_player_count, default=0.0),
            player_momentum=player_momentum,
        )

        is_upcoming = int(game.is_released or 0) != 1
        release_date = game.release_date or _parse_release_date(game.release_date_text)
        upcoming_hot_score = 0.0
        wishlist_count = int(wishlist_counts.get(int(game_id), 0))
        watchlist_count = int(watchlist_counts.get(int(game_id), 0))
        watchlist_count += int(legacy_watchlist_counts_by_name.get(game.name, 0))
        interest = interest_rows.get(int(game_id))
        if interest is None:
            interest = GameInterestSignal(game_id=game_id)
            session.add(interest)
            interest_rows[int(game_id)] = interest
        interest.wishlist_count = wishlist_count
        interest.watchlist_count = watchlist_count
        interest.updated_at = now

        click_count = int(interest.click_count or 0)
        popularity_score = compute_popularity_score(
            click_count=click_count,
            wishlist_count=wishlist_count,
            watchlist_count=watchlist_count,
            last_clicked_at=interest.last_clicked_at,
        )
        if is_upcoming:
            upcoming_hot_score = compute_upcoming_hot_score(
                release_date=release_date,
                wishlist_count=wishlist_count,
                watchlist_count=watchlist_count,
                review_score=review_score,
                review_count=review_count,
            )
        recommended_score = compute_recommended_score(
            deal_score=deal_score,
            click_count=click_count,
            wishlist_count=wishlist_count,
            watchlist_count=watchlist_count,
            is_historical_low=is_historical_low,
            deal_detected_at=deal_detected_at,
        )
        worth_buying_score, worth_buying_components, worth_buying_reason_summary = compute_worth_buying_score(
            discount_percent=latest_discount_percent,
            review_score=game.review_score,
            review_count=game.review_total_count,
            avg_player_count=avg_player_count,
            player_growth_ratio=player_growth_ratio,
            latest_price=latest_price,
            historical_low_price=historical_low,
            historical_low_hit=is_new_historical_low,
        )
        deal_heat_level, deal_heat_reason, deal_heat_tags = compute_deal_heat(
            discount_percent=latest_discount_percent,
            review_score=game.review_score,
            current_players=current_players,
            player_growth_ratio=player_growth_ratio,
            historical_low_hit=is_new_historical_low,
            trend_reason_summary=trend_reason_summary,
        )
        buy_recommendation, buy_reason, price_vs_low_ratio = compute_buy_recommendation(
            current_price=latest_price,
            historical_low=historical_low,
            discount_percent=latest_discount_percent,
            days_since_last_sale=days_since_last_sale,
        )
        next_sale_prediction = compute_next_sale_prediction(
            current_price=latest_price,
            latest_original_price=latest_original_price,
            historical_low_price=historical_low,
            sale_rows=prediction_sale_rows,
        )

        if snapshot is None:
            snapshot = GameSnapshot(game_id=game_id)
            session.add(snapshot)

        snapshot.game_name = game.name
        snapshot.steam_appid = game.appid
        snapshot.store_url = game.store_url
        snapshot.banner_url = (
            f"https://cdn.cloudflare.steamstatic.com/steam/apps/{game.appid}/header.jpg" if game.appid else None
        )

        snapshot.latest_price = latest_price
        snapshot.latest_original_price = latest_original_price
        snapshot.latest_discount_percent = latest_discount_percent
        snapshot.current_players = current_players
        snapshot.avg_player_count = avg_player_count
        snapshot.player_change = 0
        snapshot.player_momentum = player_momentum
        snapshot.daily_peak = daily_peak

        snapshot.historical_low = historical_low
        if is_new_historical_low:
            snapshot.historical_status = "new_historical_low"
        elif is_historical_low:
            snapshot.historical_status = "matches_historical_low"
        else:
            snapshot.historical_status = None
        snapshot.is_historical_low = is_historical_low
        snapshot.historical_low_hit = is_new_historical_low
        snapshot.previous_historical_low_price = effective_previous_historical_low
        snapshot.historical_low_price = historical_low
        snapshot.history_point_count = history_point_count
        snapshot.ever_discounted = ever_discounted
        snapshot.max_discount = max_discount
        snapshot.last_discounted_at = last_discounted_at
        snapshot.historical_low_timestamp = now if is_new_historical_low else snapshot.historical_low_timestamp
        if is_new_historical_low:
            snapshot.historical_low_reason_summary = "New all-time low detected from latest snapshot"
        elif is_historical_low:
            snapshot.historical_low_reason_summary = "Price matches known all-time low"
        else:
            snapshot.historical_low_reason_summary = None

        snapshot.review_score = int(review_score) if game.review_score is not None else None
        snapshot.review_score_label = game.review_score_label
        snapshot.review_count = int(review_count) if game.review_total_count is not None else None

        snapshot.genres = game.genres
        snapshot.tags = game.tags
        snapshot.platforms = game.platforms

        snapshot.is_upcoming = is_upcoming
        snapshot.is_released = game.is_released or 0
        snapshot.release_date = release_date
        snapshot.release_date_text = game.release_date_text

        snapshot.deal_score = deal_score
        snapshot.popularity_score = popularity_score
        snapshot.recommended_score = recommended_score
        snapshot.trending_score = trending_score
        snapshot.buy_score = worth_buying_score
        snapshot.buy_recommendation = buy_recommendation
        snapshot.buy_reason = buy_reason
        snapshot.price_vs_low_ratio = price_vs_low_ratio
        snapshot.predicted_next_sale_price = next_sale_prediction.get("predicted_next_sale_price")
        snapshot.predicted_next_discount_percent = next_sale_prediction.get("predicted_next_discount_percent")
        snapshot.predicted_next_sale_window_days_min = next_sale_prediction.get("predicted_next_sale_window_days_min")
        snapshot.predicted_next_sale_window_days_max = next_sale_prediction.get("predicted_next_sale_window_days_max")
        snapshot.predicted_sale_confidence = next_sale_prediction.get("predicted_sale_confidence")
        snapshot.predicted_sale_reason = next_sale_prediction.get("predicted_sale_reason")
        snapshot.worth_buying_score = worth_buying_score
        snapshot.worth_buying_score_version = WORTH_BUYING_SCORE_VERSION
        snapshot.worth_buying_reason_summary = worth_buying_reason_summary
        snapshot.worth_buying_components = worth_buying_components
        snapshot.momentum_score = momentum_score
        snapshot.momentum_score_version = MOMENTUM_SCORE_VERSION
        snapshot.player_growth_ratio = player_growth_ratio
        snapshot.short_term_player_trend = short_term_player_trend
        snapshot.trend_reason_summary = trend_reason_summary
        snapshot.deal_heat_level = deal_heat_level
        snapshot.deal_heat_reason = deal_heat_reason
        snapshot.deal_heat_tags = deal_heat_tags
        snapshot.ranking_explanations = {
            "worth_buying": worth_buying_reason_summary,
            "momentum": trend_reason_summary,
            "heat": deal_heat_reason,
            "buy_timing": buy_reason,
            "next_sale_prediction": next_sale_prediction.get("predicted_sale_reason"),
        }
        snapshot.upcoming_hot_score = upcoming_hot_score
        snapshot.price_sparkline_90d = sparkline
        snapshot.sale_events_compact = sale_events_compact
        snapshot.deal_detected_at = deal_detected_at

        snapshot.updated_at = now

        if previous_discount == 0 and safe_num(latest_discount_percent, 0.0) > 0:
            insert_deal_event_and_user_alerts(
                session=session,
                game_id=game_id,
                game_name=game.name,
                steam_appid=game.appid,
                event_type=DEAL_EVENT_NEW_SALE,
                old_price=previous_price,
                new_price=latest_price,
                discount_percent=latest_discount_percent,
                event_reason_summary="Discount changed from 0 to active sale",
                metadata_json={"player_growth_ratio": player_growth_ratio},
                event_dedupe_key=(
                    f"new_sale:{game_id}:{int(safe_num(latest_discount_percent, 0.0))}:"
                    f"{latest.recorded_at.isoformat() if latest and latest.recorded_at else now.isoformat()}"
                ),
            )

        if (
            previous_price is not None
            and latest_price is not None
            and latest_price < previous_price
            and (
                effective_previous_historical_low is None
                or latest_price > effective_previous_historical_low
            )
        ):
            insert_deal_event_and_user_alerts(
                session=session,
                game_id=game_id,
                game_name=game.name,
                steam_appid=game.appid,
                event_type=DEAL_EVENT_PRICE_DROP,
                old_price=previous_price,
                new_price=latest_price,
                discount_percent=latest_discount_percent,
                event_reason_summary="Latest price dropped below previous snapshot price",
                metadata_json={
                    "previous_price": previous_price,
                    "new_price": latest_price,
                },
                event_dedupe_key=f"price_drop:{game_id}:{safe_num(previous_price, 0.0):.4f}:{safe_num(latest_price, 0.0):.4f}",
            )

        if is_new_historical_low:
            insert_deal_event_and_user_alerts(
                session=session,
                game_id=game_id,
                game_name=game.name,
                steam_appid=game.appid,
                event_type=DEAL_EVENT_HISTORICAL_LOW,
                old_price=previous_price,
                new_price=latest_price,
                discount_percent=latest_discount_percent,
                event_reason_summary=snapshot.historical_low_reason_summary,
                metadata_json={
                    "previous_historical_low_price": effective_previous_historical_low,
                    "historical_low_price": historical_low,
                },
                event_dedupe_key=f"historical_low:{game_id}:{safe_num(latest_price, 0.0):.4f}",
            )

        player_surge_threshold = 1.35
        player_surge_min_players = 500
        if (
            player_momentum >= player_surge_threshold
            and previous_player_momentum < player_surge_threshold
            and (current_players or 0) >= player_surge_min_players
        ):
            insert_deal_event_and_user_alerts(
                session=session,
                game_id=game_id,
                game_name=game.name,
                steam_appid=game.appid,
                event_type=DEAL_EVENT_PLAYER_SPIKE,
                old_price=None,
                new_price=None,
                discount_percent=None,
                event_reason_summary="Current players surged above rolling baseline",
                metadata_json={
                    "player_growth_ratio": player_growth_ratio,
                    "short_term_player_trend": short_term_player_trend,
                    "current_players": current_players,
                    "player_surge_threshold": player_surge_threshold,
                    "player_surge_min_players": player_surge_min_players,
                },
                event_dedupe_key=f"player_spike:{game_id}:{int(now.timestamp() // 3600)}",
            )

        process_watchlist_target_alerts(
            session=session,
            game_id=game_id,
            game_name=game.name,
            steam_appid=game.appid,
            latest_price=latest_price,
            latest_discount_percent=latest_discount_percent,
        )

        updated += 1

    return updated


def build_dashboard_filters(session: Session) -> dict[str, list[str]]:
    rows = (
        session.query(
            GameSnapshot.genres,
            GameSnapshot.tags,
            GameSnapshot.platforms,
            GameSnapshot.review_score_label,
        )
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
        )
        .all()
    )

    genre_counts: dict[str, int] = {}
    tag_counts: dict[str, int] = {}
    platform_counts: dict[str, int] = {}
    review_labels: set[str] = set()

    for genres_raw, tags_raw, platforms_raw, review_label in rows:
        for genre in split_csv_field(genres_raw):
            genre_counts[genre] = genre_counts.get(genre, 0) + 1
        for tag in split_csv_field(tags_raw):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        for platform in split_csv_field(platforms_raw):
            platform_counts[platform] = platform_counts.get(platform, 0) + 1
        if review_label:
            review_labels.add(str(review_label).strip())

    platforms = sorted(platform_counts.keys(), key=lambda p: (-platform_counts[p], p.lower()))
    normalized_platforms = {value.lower() for value in platforms}
    for option in EXTENDED_PLATFORM_FILTER_OPTIONS:
        if option.lower() not in normalized_platforms:
            platforms.append(option)
            normalized_platforms.add(option.lower())

    return {
        "genres": sorted(genre_counts.keys(), key=lambda g: (-genre_counts[g], g.lower())),
        "tags": sorted(tag_counts.keys(), key=lambda t: (-tag_counts[t], t.lower())),
        "platforms": platforms,
        "review_labels": sorted(review_labels),
    }


def _normalize_buy_recommendation(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"BUY_NOW", "WAIT"} else ""


def _snapshot_identity_key(row: dict, index: int = 0) -> str:
    if not isinstance(row, dict):
        return f"idx:{index}"
    game_id = row.get("game_id") or row.get("id")
    try:
        numeric_id = int(game_id)
    except Exception:
        numeric_id = 0
    if numeric_id > 0:
        return f"id:{numeric_id}"
    name = str(row.get("game_name") or row.get("name") or "").strip().lower()
    if name:
        return f"name:{name}"
    return f"idx:{index}"


def _dedupe_snapshot_rows(rows: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        key = _snapshot_identity_key(row, idx)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _build_decision_picks(rows: list[dict], recommendation: str, limit: int = HOMEPAGE_RAIL_LIMIT) -> list[dict]:
    target = _normalize_buy_recommendation(recommendation)
    if not target:
        return []
    picked: list[dict] = []
    for row in rows:
        if _normalize_buy_recommendation(row.get("buy_recommendation")) != target:
            continue
        picked.append(row)
        if len(picked) >= limit:
            break
    return picked


def _build_player_surges(alert_rows: list[dict], trending_rows: list[dict], limit: int = HOMEPAGE_RAIL_LIMIT) -> list[dict]:
    candidates: list[dict] = []
    for row in alert_rows:
        alert_type = str(row.get("alert_type") or row.get("signal_type") or "").strip().upper()
        if alert_type == ALERT_PLAYER_SURGE:
            candidates.append(row)
    for row in trending_rows:
        change = safe_num(row.get("player_change"), 0.0)
        short_term = safe_num(row.get("short_term_player_trend"), 0.0)
        if change > 0 or short_term > 0:
            candidates.append(row)
    return _dedupe_snapshot_rows(candidates)[:limit]


def rebuild_dashboard_cache(session: Session) -> None:
    hold_filter = func.upper(func.coalesce(Game.priority_tier, "")) == ROLLOUT_HOLD_TIER
    total_games = int(session.query(func.count(Game.id)).scalar() or 0)
    held_games = int(session.query(func.count(Game.id)).filter(hold_filter).scalar() or 0)
    tracked_games = max(0, total_games - held_games)
    released_tracked_games = int(
        session.query(func.count(Game.id))
        .filter(Game.is_released == 1)
        .filter(~hold_filter)
        .scalar()
        or 0
    )

    worth_buying_now = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.latest_discount_percent.isnot(None),
            GameSnapshot.latest_discount_percent > 0,
            GameSnapshot.is_upcoming.is_(False),
        )
        .order_by(GameSnapshot.worth_buying_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    recommended_deals = (
        session.query(GameSnapshot)
        .order_by(GameSnapshot.recommended_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    deal_ranked = (
        session.query(GameSnapshot)
        .order_by(GameSnapshot.deal_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    biggest_deals = (
        session.query(GameSnapshot)
        .order_by(GameSnapshot.latest_discount_percent.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    historical_lows = (
        session.query(GameSnapshot)
        .filter(GameSnapshot.is_historical_low.is_(True))
        .order_by(GameSnapshot.deal_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    top_reviewed = (
        session.query(GameSnapshot)
        .order_by(GameSnapshot.review_score.desc(), GameSnapshot.review_count.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    top_played = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            (GameSnapshot.current_players.isnot(None) | GameSnapshot.avg_player_count.isnot(None)),
            (
                (GameSnapshot.current_players > 0)
                | (GameSnapshot.avg_player_count > 0)
                | (GameSnapshot.daily_peak > 0)
            ),
        )
        .order_by(
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.avg_player_count.desc().nullslast(),
            GameSnapshot.daily_peak.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    trending = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.current_players.isnot(None),
            GameSnapshot.current_players > 0,
            (
                (GameSnapshot.player_change.isnot(None) & (GameSnapshot.player_change > 0))
                | (GameSnapshot.short_term_player_trend.isnot(None) & (GameSnapshot.short_term_player_trend > 0))
                | (GameSnapshot.momentum_score.isnot(None) & (GameSnapshot.momentum_score > 0))
            ),
        )
        .order_by(
            GameSnapshot.player_change.desc().nullslast(),
            GameSnapshot.short_term_player_trend.desc().nullslast(),
            GameSnapshot.momentum_score.desc().nullslast(),
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    leaderboard = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            (GameSnapshot.current_players.isnot(None) | GameSnapshot.avg_player_count.isnot(None)),
            (
                (GameSnapshot.current_players > 0)
                | (GameSnapshot.daily_peak > 0)
                | (GameSnapshot.avg_player_count > 0)
            ),
        )
        .order_by(
            GameSnapshot.current_players.desc().nullslast(),
            GameSnapshot.daily_peak.desc().nullslast(),
            GameSnapshot.avg_player_count.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    upcoming = (
        session.query(GameSnapshot)
        .filter(GameSnapshot.is_upcoming.is_(True))
        .order_by(GameSnapshot.upcoming_hot_score.desc(), GameSnapshot.release_date.asc(), GameSnapshot.game_id.asc())
        .limit(UPCOMING_LIMIT)
        .all()
    )
    trending_deals = (
        session.query(GameSnapshot)
        .filter(GameSnapshot.latest_discount_percent.isnot(None), GameSnapshot.latest_discount_percent > 0)
        .order_by(GameSnapshot.momentum_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )

    diversified_deal_rails = _apply_homepage_deal_diversity(
        rail_candidates={
            "deal_ranked": deal_ranked,
            "worth_buying_now": worth_buying_now,
            "recommended_deals": recommended_deals,
            "biggest_deals": biggest_deals,
            "trending_deals": trending_deals,
        },
        section_limit=HOMEPAGE_RAIL_LIMIT,
        uniqueness_window=HOMEPAGE_DIVERSITY_WINDOW,
        rail_order=HOMEPAGE_DIVERSITY_RAIL_ORDER,
    )
    deal_ranked = diversified_deal_rails.get("deal_ranked", [])
    worth_buying_now = diversified_deal_rails.get("worth_buying_now", [])
    recommended_deals = diversified_deal_rails.get("recommended_deals", [])
    biggest_deals = diversified_deal_rails.get("biggest_deals", [])
    trending_deals = diversified_deal_rails.get("trending_deals", [])
    new_historical_low_events = (
        session.query(DealEvent)
        .filter(
            DealEvent.event_type == DEAL_EVENT_HISTORICAL_LOW,
            DealEvent.created_at >= utcnow() - datetime.timedelta(hours=24),
        )
        .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
        .limit(24)
        .all()
    )
    new_historical_lows = []
    if new_historical_low_events:
        historical_low_game_ids = [int(row.game_id) for row in new_historical_low_events]
        snapshots_by_id = {
            int(row.game_id): row
            for row in session.query(GameSnapshot).filter(GameSnapshot.game_id.in_(historical_low_game_ids)).all()
        }
        for event in new_historical_low_events:
            row = snapshots_by_id.get(int(event.game_id))
            if row:
                new_historical_lows.append(_snapshot_row_to_dict(row))
    historical_lows_this_week_events = (
        session.query(DealEvent)
        .filter(
            DealEvent.event_type == DEAL_EVENT_HISTORICAL_LOW,
            DealEvent.created_at >= utcnow() - datetime.timedelta(days=7),
        )
        .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
        .limit(24)
        .all()
    )
    historical_lows_this_week = []
    if historical_lows_this_week_events:
        week_ids = [int(row.game_id) for row in historical_lows_this_week_events]
        week_snapshots = {
            int(row.game_id): row
            for row in session.query(GameSnapshot).filter(GameSnapshot.game_id.in_(week_ids)).all()
        }
        for event in historical_lows_this_week_events:
            snap = week_snapshots.get(int(event.game_id))
            if snap:
                historical_lows_this_week.append(_snapshot_row_to_dict(snap))
    biggest_price_drop_events = (
        session.query(DealEvent)
        .filter(DealEvent.event_type == DEAL_EVENT_PRICE_DROP)
        .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
        .limit(24)
        .all()
    )
    recent_alert_rows = (
        session.query(Alert, GameSnapshot)
        .outerjoin(GameSnapshot, GameSnapshot.game_id == Alert.game_id)
        .filter(Alert.created_at >= utcnow() - datetime.timedelta(days=2))
        .order_by(Alert.created_at.desc(), Alert.id.desc())
        .limit(256)
        .all()
    )
    alert_label_map = {
        ALERT_PRICE_DROP: "Price dropped",
        ALERT_NEW_HISTORICAL_LOW: "New historical low",
        ALERT_SALE_STARTED: "Sale started",
        ALERT_PLAYER_SURGE: "Major player increase",
    }
    alert_signals: list[dict] = []
    seen_alert_keys: set[tuple[int, str]] = set()
    for alert_row, snapshot_row in recent_alert_rows:
        if snapshot_row is None:
            continue
        alert_type = str(alert_row.alert_type or "").strip().upper()
        alert_key = (int(alert_row.game_id), alert_type)
        if alert_key in seen_alert_keys:
            continue
        if alert_type == ALERT_PLAYER_SURGE and int(safe_num(snapshot_row.current_players, 0.0)) <= 0:
            continue
        seen_alert_keys.add(alert_key)
        payload_row = _snapshot_row_to_dict(snapshot_row)
        payload_row["alert_type"] = alert_type
        payload_row["alert_label"] = alert_label_map.get(alert_type, "Market signal")
        payload_row["alert_created_at"] = alert_row.created_at.isoformat() if alert_row.created_at else None
        payload_row["alert_metadata"] = alert_row.metadata_json or {}
        alert_signals.append(payload_row)
        if len(alert_signals) >= 24:
            break

    deal_radar = _build_deal_radar_feed(session, limit=DEAL_RADAR_LIMIT)

    recommended_deals_rows = [_snapshot_row_to_dict(row) for row in recommended_deals]
    worth_buying_now_rows = [_snapshot_row_to_dict(row) for row in worth_buying_now]
    deal_ranked_rows = [_snapshot_row_to_dict(row) for row in deal_ranked]
    biggest_deals_rows = [_snapshot_row_to_dict(row) for row in biggest_deals]
    historical_lows_rows = [_snapshot_row_to_dict(row) for row in historical_lows]
    trending_deals_rows = [_snapshot_row_to_dict(row) for row in trending_deals]
    top_reviewed_rows = [_snapshot_row_to_dict(row) for row in top_reviewed]
    top_played_rows = [_snapshot_row_to_dict(row) for row in top_played]
    trending_rows = [_snapshot_row_to_dict(row) for row in trending]
    leaderboard_rows = [_snapshot_row_to_dict(row) for row in leaderboard]
    upcoming_rows = [_snapshot_row_to_dict(row) for row in upcoming]
    new_historical_lows_rows = _dedupe_snapshot_rows(new_historical_lows)

    decision_pool = _dedupe_snapshot_rows(
        [
            *worth_buying_now_rows,
            *recommended_deals_rows,
            *deal_ranked_rows,
            *biggest_deals_rows,
            *trending_deals_rows,
            *trending_rows,
            *new_historical_lows_rows,
        ]
    )
    buy_now_picks = _build_decision_picks(decision_pool, "BUY_NOW")
    wait_picks = _build_decision_picks(decision_pool, "WAIT")
    if not buy_now_picks:
        buy_now_picks = worth_buying_now_rows[:HOMEPAGE_RAIL_LIMIT]
    if not wait_picks:
        wait_candidates = [
            row
            for row in decision_pool
            if safe_num(row.get("price_vs_low_ratio"), 0.0) >= 1.08
            or safe_num(row.get("predicted_next_discount_percent"), 0.0) >= 35
        ]
        wait_picks = wait_candidates[:HOMEPAGE_RAIL_LIMIT]

    trending_now_rows = trending_rows if trending_rows else trending_deals_rows
    biggest_discounts_rows = biggest_deals_rows
    worth_buying_rows = worth_buying_now_rows
    player_surges = _build_player_surges(alert_signals, trending_rows)
    seasonal_summary = {}

    # Homepage dashboard cache is shared and not user-scoped; keep personal lists
    # empty here and hydrate them via user-scoped API calls in the frontend.
    wishlist: list[dict] = []
    watchlist: list[dict] = []

    payload = {
        "catalogSummary": {
            "total_games": total_games,
            "tracked_games": tracked_games,
            "held_games": held_games,
            "released_tracked_games": released_tracked_games,
            "rollout_hold_tier": ROLLOUT_HOLD_TIER,
            "updated_at": utcnow().isoformat(),
        },
        "recommendedDeals": recommended_deals_rows,
        "worthBuyingNow": worth_buying_now_rows,
        "home:worth_buying": worth_buying_now_rows,
        "topDealsToday": deal_ranked_rows,
        "dealRanked": deal_ranked_rows,
        "biggestDeals": biggest_deals_rows,
        "historicalLowsThisWeek": historical_lows_this_week,
        "historicalLows": historical_lows_rows,
        "trendingDeals": trending_deals_rows,
        "home:trending": trending_deals_rows,
        "newHistoricalLows": new_historical_lows_rows,
        "home:historical_lows": new_historical_lows_rows,
        "biggestPriceDrops": [
            {
                "id": int(row.id),
                "game_id": int(row.game_id),
                "event_type": row.event_type,
                "old_price": row.old_price,
                "new_price": row.new_price,
                "discount_percent": row.discount_percent,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in biggest_price_drop_events
        ],
        "topReviewed": top_reviewed_rows,
        "mostPlayedDeals": top_played_rows,
        "topPlayed": top_played_rows,
        "trending": trending_rows,
        "leaderboard": leaderboard_rows,
        "upcoming": upcoming_rows,
        "wishlist": wishlist,
        "watchlist": watchlist,
        "filters": build_dashboard_filters(session),
        "alertSignals": alert_signals,
        "dealRadar": deal_radar,
        "marketRadar": deal_radar,
        "worth_buying_now": worth_buying_rows,
        "biggest_discounts": biggest_discounts_rows,
        "buy_now_picks": buy_now_picks,
        "wait_picks": wait_picks,
        "new_historical_lows": new_historical_lows_rows,
        "trending_now": trending_now_rows,
        "deal_radar": deal_radar,
        "player_surges": player_surges,
        "seasonal_summary": seasonal_summary,
        "decision_dashboard": {
            "worth_buying_now": worth_buying_rows,
            "biggest_discounts": biggest_discounts_rows,
            "buy_now_picks": buy_now_picks,
            "wait_picks": wait_picks,
            "new_historical_lows": new_historical_lows_rows,
            "trending_now": trending_now_rows,
            "deal_radar": deal_radar,
            "player_surges": player_surges,
            "seasonal_summary": seasonal_summary,
        },
        "generated_at": utcnow().isoformat(),
    }

    section_payloads = {
        CACHE_KEY: payload,
        "home:worth_buying": {"items": payload.get("worthBuyingNow", []), "generated_at": payload["generated_at"]},
        "home:trending": {"items": payload.get("trendingDeals", []), "generated_at": payload["generated_at"]},
        "home:historical_lows": {"items": payload.get("newHistoricalLows", []), "generated_at": payload["generated_at"]},
        "home:biggest_price_drops": {"items": payload.get("biggestPriceDrops", []), "generated_at": payload["generated_at"]},
        "home:alerts": {"items": payload.get("alertSignals", []), "generated_at": payload["generated_at"]},
        "home:deal_radar": {"items": payload.get("dealRadar", []), "generated_at": payload["generated_at"]},
        "home:market_radar": {"items": payload.get("dealRadar", []), "generated_at": payload["generated_at"]},
        "home:top_played": {"items": payload.get("topPlayed", []), "generated_at": payload["generated_at"]},
        "home:upcoming": {"items": payload.get("upcoming", []), "generated_at": payload["generated_at"]},
    }
    for legacy_cache_key in LEGACY_CACHE_KEYS:
        section_payloads[legacy_cache_key] = payload
    now = utcnow()
    for cache_key, cache_payload in section_payloads.items():
        payload_json = json.dumps(cache_payload, ensure_ascii=False)
        cache_row = session.query(DashboardCache).filter(DashboardCache.cache_key == cache_key).first()
        if cache_row is None:
            session.add(DashboardCache(cache_key=cache_key, payload=payload_json, updated_at=now))
        else:
            cache_row.payload = payload_json
            cache_row.updated_at = now


def _print_pipeline_health(session: Session) -> None:
    latest_price_rows = session.execute(
        text("SELECT COUNT(*) FROM latest_game_prices")
    ).scalar() or 0
    snapshot_prices = session.execute(
        text("SELECT COUNT(*) FROM game_snapshots WHERE latest_price IS NOT NULL")
    ).scalar() or 0
    dirty_count = get_dirty_games_backlog(session)
    print(f"latest_game_prices rows: {latest_price_rows}")
    print(f"snapshots with price: {snapshot_prices}")
    print(f"dirty_games backlog: {dirty_count}")


def get_dirty_games_backlog(session: Session) -> int:
    return int(
        session.execute(text("SELECT COUNT(*) FROM dirty_games")).scalar()
        or 0
    )


def run_once() -> None:
    validate_settings()
    assert_scale_schema_ready(direct_engine, component_name="refresh_snapshots worker (--once)")
    print(
        "refresh_snapshots single-run started "
        f"batch_size={BATCH_SIZE} dirty_queue_fetch_size={DIRTY_QUEUE_FETCH_SIZE} "
        f"max_batch_size={MAX_BATCH_SIZE} "
        f"idle_sleep_seconds={IDLE_SLEEP_SECONDS} "
        f"cache_rebuild_every_batches={SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES} "
        f"retry_backoff={RETRY_BACKOFF_BASE_SECONDS:.1f}-{RETRY_BACKOFF_MAX_SECONDS:.1f}s "
        f"deal_radar_limit={DEAL_RADAR_LIMIT}"
    )
    print("running single refresh cycle...")
    session = DBSession()
    game_ids: list[int] = []
    try:
        print("checking dirty_games queue...")
        dirty_backlog_before = get_dirty_games_backlog(session)
        print(f"dirty_games backlog before claim: {dirty_backlog_before}")
        game_ids = claim_dirty_batch(session, BATCH_SIZE)
        print(f"dirty_games selected for cycle: {len(game_ids)}")

        if game_ids:
            print(f"processing {len(game_ids)} dirty games")
            update_job_status(session, "refresh_snapshots", started=True)
            updated_count = refresh_snapshots_once(session, game_ids)
            print(f"snapshots updated: {updated_count}")
            delete_dirty(session, game_ids)
        else:
            updated_count = 0

        priced_count = session.execute(
            text("SELECT COUNT(*) FROM game_snapshots WHERE latest_price IS NOT NULL")
        ).scalar() or 0
        print(f"snapshots with price: {priced_count}")

        print("rebuilding dashboard cache...")
        rebuild_dashboard_cache(session)
        print("dashboard cache rebuilt")

        update_job_status(
            session,
            "refresh_snapshots",
            completed_success=True,
            items_total=len(game_ids),
            items_success=updated_count,
            items_failed=max(0, len(game_ids) - updated_count),
        )
        _print_pipeline_health(session)
        session.commit()
        print("single refresh complete")
    except Exception as exc:
        session.rollback()
        try:
            mark_dirty_retry(session, game_ids, repr(exc))
            update_job_status(session, "refresh_snapshots", error_message=repr(exc))
            session.commit()
        except Exception:
            session.rollback()
        raise
    finally:
        session.close()


def run_worker_forever() -> None:
    validate_settings()
    assert_scale_schema_ready(direct_engine, component_name="refresh_snapshots worker")
    print(
        "refresh_snapshots worker started "
        f"batch_size={BATCH_SIZE} dirty_queue_fetch_size={DIRTY_QUEUE_FETCH_SIZE} "
        f"max_batch_size={MAX_BATCH_SIZE} "
        f"idle_sleep_seconds={IDLE_SLEEP_SECONDS} "
        f"cache_rebuild_every_batches={SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES} "
        f"retry_backoff={RETRY_BACKOFF_BASE_SECONDS:.1f}-{RETRY_BACKOFF_MAX_SECONDS:.1f}s "
        f"deal_radar_limit={DEAL_RADAR_LIMIT}"
    )
    first_cycle = True
    batches_since_cache_rebuild = 0
    while True:
        session = DBSession()
        game_ids: list[int] = []
        try:
            print("checking dirty_games queue...")
            dirty_backlog_before = get_dirty_games_backlog(session)
            print(f"dirty_games backlog before claim: {dirty_backlog_before}")
            game_ids = claim_dirty_batch(session, BATCH_SIZE)
            print(f"dirty_games selected for cycle: {len(game_ids)}")
            if not game_ids:
                if first_cycle or batches_since_cache_rebuild > 0:
                    print("rebuilding dashboard cache...")
                    rebuild_dashboard_cache(session)
                    print("dashboard cache rebuilt")
                    batches_since_cache_rebuild = 0
                    first_cycle = False
                _print_pipeline_health(session)
                session.commit()
                print("no dirty games, worker sleeping...")
                time.sleep(IDLE_SLEEP_SECONDS)
                continue

            print(f"processing {len(game_ids)} dirty games")
            batch_started_at = time.perf_counter()
            update_job_status(session, "refresh_snapshots", started=True)
            updated_count = refresh_snapshots_once(session, game_ids)
            print(f"snapshots updated: {updated_count}")
            delete_dirty(session, game_ids)
            batches_since_cache_rebuild += 1

            priced_count = session.execute(
                text("SELECT COUNT(*) FROM game_snapshots WHERE latest_price IS NOT NULL")
            ).scalar() or 0
            print(f"snapshots with price: {priced_count}")

            if batches_since_cache_rebuild >= SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES:
                print("rebuilding dashboard cache...")
                rebuild_dashboard_cache(session)
                print("dashboard cache rebuilt")
                batches_since_cache_rebuild = 0
            else:
                print(
                    "skipping dashboard cache rebuild this batch "
                    f"(pending_batches={batches_since_cache_rebuild}/{SNAPSHOT_CACHE_REBUILD_EVERY_BATCHES})"
                )

            elapsed = time.perf_counter() - batch_started_at
            update_job_status(
                session,
                "refresh_snapshots",
                completed_success=True,
                duration_ms=int(elapsed * 1000),
                items_total=len(game_ids),
                items_success=updated_count,
                items_failed=max(0, len(game_ids) - updated_count),
            )
            _print_pipeline_health(session)
            session.commit()
            print(
                f"refresh_snapshots batch={len(game_ids)} updated={updated_count} elapsed_ms={int(elapsed * 1000)}"
            )
            first_cycle = False
        except Exception as exc:
            session.rollback()
            try:
                mark_dirty_retry(session, game_ids, repr(exc))
                update_job_status(session, "refresh_snapshots", error_message=repr(exc))
                session.commit()
            except Exception:
                session.rollback()
            time.sleep(2)
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    print("starting refresh_snapshots worker...")
    if args.once:
        run_once()
    else:
        run_worker_forever()
