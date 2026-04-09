from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import argparse
import datetime
import hashlib
import json
import math
import os
import subprocess
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
    GameDiscoveryFeed,
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
CRITICAL_CACHE_KEY = "home_critical_v1"
LEGACY_CACHE_KEYS = ("home",)
INSIGHT_ENGINE_BRIDGE_SCRIPT = ROOT_DIR / "scripts" / "insight-engine" / "run_insight_engine.mjs"
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
HOMEPAGE_CRITICAL_RAIL_LIMIT = min(8, HOMEPAGE_RAIL_LIMIT)
HOMEPAGE_CRITICAL_DIGEST_LIMIT = 6
HOMEPAGE_CRITICAL_RADAR_LIMIT = min(8, HOMEPAGE_RAIL_LIMIT)
HOMEPAGE_DEAL_CANDIDATE_POOL = SNAPSHOT_HOMEPAGE_DEAL_CANDIDATE_POOL
HOMEPAGE_DIVERSITY_WINDOW = SNAPSHOT_HOMEPAGE_DIVERSITY_WINDOW
HOMEPAGE_COMPOSITION_POOL_MULTIPLIER = 5
HOMEPAGE_COMPOSITION_RAIL_LIMIT = max(
    HOMEPAGE_RAIL_LIMIT,
    min(HOMEPAGE_DEAL_CANDIDATE_POOL, HOMEPAGE_RAIL_LIMIT * HOMEPAGE_COMPOSITION_POOL_MULTIPLIER),
)
HOMEPAGE_DIVERSITY_LEAD_PROTECT = 12
HOMEPAGE_DIVERSITY_ROTATION_WINDOW = 9
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
HOMEPAGE_PRIMARY_DIVERSITY_RAIL_ORDER = (
    "deal_opportunities",
    "opportunity_radar",
    "biggest_discounts",
    "worth_buying_now",
    "trending_now",
)
HOMEPAGE_VISIBLE_DIVERSITY_RAIL_ORDER = (
    "deal_opportunities",
    "opportunity_radar",
    "deal_ranked",
    "biggest_discounts",
    "worth_buying_now",
    "trending_now",
)
HOMEPAGE_CROSS_RAIL_ORDER = (
    "deal_ranked",
    "worth_buying_now",
    "biggest_discounts",
    "trending_now",
    "deal_opportunities",
    "buy_now_picks",
    "opportunity_radar",
    "wait_picks",
)
HOMEPAGE_CROSS_RAIL_UNIQUENESS_WINDOW = max(1, min(8, HOMEPAGE_RAIL_LIMIT))
HOMEPAGE_CATALOG_SEED_LIMIT = 24
HOMEPAGE_ALL_DEALS_LIMIT = 96
HOMEPAGE_ALL_DEALS_CANDIDATE_POOL = 480
HOMEPAGE_ALL_DEALS_LEAD_COUNT = 12
HOMEPAGE_ALL_DEALS_MIN_DISCOUNT = 10
ALL_DEALS_FEED_CACHE_KEY = "home:all_deals_feed"
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
PLAYER_HISTORY_LOOKBACK_DAYS = 730
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


def _normalize_review_label(raw_label: Any, review_score: Any) -> str | None:
    label = str(raw_label or "").strip()
    if label:
        normalized = " ".join(label.split()).lower()
        canonical_labels = {
            "overwhelmingly negative": "Overwhelmingly Negative",
            "mostly negative": "Mostly Negative",
            "mixed": "Mixed",
            "mostly positive": "Mostly Positive",
            "very positive": "Very Positive",
            "overwhelmingly positive": "Overwhelmingly Positive",
        }
        if normalized in canonical_labels:
            return canonical_labels[normalized]
    score = safe_num(review_score, default=-1.0)
    if score < 0:
        return None
    if score >= 95:
        return "Overwhelmingly Positive"
    if score >= 80:
        return "Very Positive"
    if score >= 70:
        return "Mostly Positive"
    if score >= 40:
        return "Mixed"
    if score >= 20:
        return "Mostly Negative"
    return "Overwhelmingly Negative"


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


def _contract_buy_recommendation(value: Any) -> str | None:
    normalized = _normalize_buy_recommendation(value)
    if normalized == "BUY_NOW":
        return "Buy now"
    if normalized == "WAIT":
        return "Wait"
    if normalized == "AVOID":
        return "Avoid"
    return None


def _normalize_ranking_explanations(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _as_epoch_ms(value: datetime.datetime | None) -> int:
    timestamp = _as_aware_utc(value) or utcnow()
    return int(timestamp.timestamp() * 1000)


def _build_insight_engine_context(
    *,
    is_historical_low: bool,
    wishlist_count: int,
    watchlist_count: int,
    click_count: int,
    now: datetime.datetime,
) -> dict[str, Any]:
    return {
        "evaluationTimestamp": _as_epoch_ms(now),
        "userSignals": {
            "isDismissed": False,
            "isWishlisted": wishlist_count > 0,
            "isViewedOrTracked": watchlist_count > 0 or click_count > 0,
            "tasteMatch": "none",
        },
        "historicalContext": {
            "priceContext": "all_time_low" if is_historical_low else "normal",
        },
    }


def _run_insight_engine_subprocess(triggers: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    payload = {"triggers": triggers, "context": context}
    completed = subprocess.run(
        [
            "node",
            "--experimental-strip-types",
            str(INSIGHT_ENGINE_BRIDGE_SCRIPT),
        ],
        input=json.dumps(payload, ensure_ascii=False),
        text=True,
        capture_output=True,
        cwd=str(ROOT_DIR),
    )

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        message = [
            "insight engine subprocess failed",
            f"exit_code={completed.returncode}",
        ]
        if stderr:
            message.append(f"stderr={stderr}")
        if stdout:
            message.append(f"stdout={stdout}")
        raise RuntimeError(" | ".join(message))

    try:
        parsed = json.loads(completed.stdout or "")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "insight engine subprocess returned invalid JSON "
            f"stdout={completed.stdout!r} stderr={(completed.stderr or '').strip()!r}"
        ) from exc

    if not isinstance(parsed, dict):
        raise RuntimeError(f"insight engine subprocess returned non-object JSON: {type(parsed).__name__}")

    return parsed


def compute_deal_score(
    discount_percent: float,
    latest_price: float | None,
    historical_low: float | None,
    review_score: float,
    review_count: float,
    avg_player_count: float,
    player_momentum: float,
    history_confidence: float | None = None,
    medium_term_trend: float | None = None,
    long_term_trend: float | None = None,
    player_interest_state: str | None = None,
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
    if (
        history_confidence is None
        and medium_term_trend is None
        and long_term_trend is None
        and not player_interest_state
    ):
        momentum_component = clamp(player_momentum, -5.0, 10.0)
    else:
        confidence_factor = clamp(safe_num(history_confidence, 0.45), 0.2, 1.0)
        medium = safe_num(medium_term_trend, 0.0)
        long_term = safe_num(long_term_trend, 0.0)
        momentum_component = (
            clamp(player_momentum * 14.0, -6.0, 12.0)
            + clamp(medium * 9.0, -4.0, 4.0)
            + clamp(long_term * 7.0, -3.0, 3.0)
        ) * (0.5 + confidence_factor * 0.5)
        normalized_state = str(player_interest_state or "").strip().lower()
        if normalized_state in {"stable_evergreen", "resurging"}:
            momentum_component += 1.5
        elif normalized_state in {"declining", "launch_hype_dropoff"}:
            momentum_component -= 2.0
        elif normalized_state == "one_off_spike":
            momentum_component -= 1.5

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
    release_date_text: str | None,
    wishlist_count: int,
    watchlist_count: int,
    review_score: float,
    review_count: float,
    popularity_score: float,
    has_artwork: bool,
    has_genre: bool,
    has_platform: bool,
) -> float:
    def release_date_quality_score() -> float:
        text_value = str(release_date_text or "").strip()
        if not release_date and not text_value:
            return 0.15
        if text_value:
            lowered = text_value.lower()
            if lowered in {"coming soon", "coming soon!", "to be announced", "tba", "announced"}:
                return 0.1
            if lowered.startswith("q") and " " in lowered:
                return 0.62
            for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b, %Y", "%d %B, %Y"):
                try:
                    datetime.datetime.strptime(text_value, fmt)
                    return 1.0
                except ValueError:
                    continue
            for fmt in ("%b %Y", "%B %Y"):
                try:
                    datetime.datetime.strptime(text_value, fmt)
                    return 0.78
                except ValueError:
                    continue
            try:
                datetime.datetime.strptime(text_value, "%Y")
                return 0.52
            except ValueError:
                pass
        if release_date:
            return 0.66
        return 0.35

    now_date = utcnow().date()

    release_proximity = 0.0
    if release_date:
        days_out = (release_date - now_date).days
        if days_out < 0:
            release_proximity = -8.0
        elif days_out <= 365:
            release_proximity = 42.0 * (1.0 / (1.0 + (days_out / 35.0)))
        else:
            release_proximity = max(5.0, 42.0 * (1.0 / (1.0 + (days_out / 90.0))))

    demand_component = clamp(
        wishlist_count * 2.2 + watchlist_count * 1.5 + max(0.0, safe_num(popularity_score, 0.0)) * 0.7,
        0.0,
        120.0,
    )
    review_component = (
        clamp(review_score, 0.0, 100.0)
        * clamp(math.log10(max(review_count, 1.0)) / 4.0, 0.0, 1.0)
        * 0.22
    )
    quality_component = release_date_quality_score() * 16.0
    metadata_component = (4.0 if has_genre else 0.0) + (3.0 if has_platform else 0.0)
    artwork_component = 14.0 if has_artwork else -24.0
    total = 16.0 + release_proximity + demand_component + review_component + quality_component + metadata_component + artwork_component

    if not has_artwork:
        total *= 0.72
    if release_date is None and demand_component < 20.0:
        total -= 8.0

    return round(clamp(total, 0.0, 300.0), 2)


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


def compute_player_history_profile(
    *,
    current_players: int | None,
    avg_players_last_24h: float | None,
    avg_players_7d: float | None,
    avg_players_30d: float | None,
    avg_players_90d: float | None,
    avg_players_365d: float | None,
    peak_players_365d: float | None,
    min_players_365d: float | None,
    history_point_count: int | None,
    history_coverage_days: int | None,
) -> dict[str, Any]:
    current = max(0.0, safe_num(current_players, 0.0))
    avg_24h = max(0.0, safe_num(avg_players_last_24h, 0.0))
    avg_7d = max(0.0, safe_num(avg_players_7d, 0.0))
    avg_30d = max(0.0, safe_num(avg_players_30d, 0.0))
    avg_90d = max(0.0, safe_num(avg_players_90d, 0.0))
    avg_365d = max(0.0, safe_num(avg_players_365d, 0.0))
    peak_365d = max(0.0, safe_num(peak_players_365d, 0.0))
    min_365d = max(0.0, safe_num(min_players_365d, 0.0))
    sample_count = max(0, int(safe_num(history_point_count, 0.0)))
    coverage_days = max(0, int(safe_num(history_coverage_days, 0.0)))

    baseline_players = avg_30d or avg_7d or avg_24h or current
    short_baseline = avg_7d or avg_24h or baseline_players or 1.0
    medium_baseline = avg_30d or avg_90d or short_baseline or 1.0
    long_baseline = avg_90d or avg_365d or medium_baseline or 1.0
    year_baseline = avg_365d or long_baseline or 1.0

    short_term_delta = (current - short_baseline) / max(1.0, short_baseline) if short_baseline > 0 else 0.0
    medium_term_delta = (avg_7d - medium_baseline) / max(1.0, medium_baseline) if avg_7d > 0 else short_term_delta * 0.6
    long_term_delta = (avg_30d - long_baseline) / max(1.0, long_baseline) if avg_30d > 0 else medium_term_delta * 0.5
    long_tail_delta = (avg_90d - year_baseline) / max(1.0, year_baseline) if avg_90d > 0 else long_term_delta * 0.5

    peak_reference = max(1.0, avg_365d or avg_90d or avg_30d or peak_365d or 1.0)
    if peak_365d > 0 and min_365d > 0:
        volatility_ratio = (peak_365d - min_365d) / peak_reference
    else:
        volatility_ratio = 0.0
    stability_score = clamp(1.0 - (volatility_ratio / 3.2), 0.0, 1.0)

    sample_factor = clamp(math.log10(sample_count + 1.0) / 3.2, 0.0, 1.0)
    coverage_factor = clamp(coverage_days / 365.0, 0.0, 1.0)
    history_confidence = round(clamp((sample_factor * 0.62) + (coverage_factor * 0.38), 0.0, 1.0), 4)

    spike_ratio = current / max(1.0, avg_30d or short_baseline or 1.0)
    launch_hype_dropoff = (
        coverage_days >= 90
        and peak_365d > 0
        and avg_90d > 0
        and current > 0
        and peak_365d >= avg_90d * 1.8
        and current <= avg_90d * 0.45
    )
    one_off_spike = (
        spike_ratio >= 1.8
        and medium_term_delta < 0.12
        and long_term_delta <= 0.08
        and coverage_days >= 90
        and history_confidence >= 0.35
    )
    resurging = (
        medium_term_delta >= 0.14
        and (long_term_delta <= 0.02 or long_tail_delta < 0.0)
        and current >= max(1.0, short_baseline) * 1.05
    )
    declining = medium_term_delta <= -0.15 and long_term_delta <= -0.08
    stable_evergreen = (
        coverage_days >= 240
        and sample_count >= 120
        and stability_score >= 0.55
        and abs(long_term_delta) <= 0.12
        and max(avg_30d, avg_90d, avg_365d) >= 350
    )
    stable = (
        coverage_days >= 120
        and abs(medium_term_delta) <= 0.08
        and abs(long_term_delta) <= 0.1
        and stability_score >= 0.45
    )

    if launch_hype_dropoff:
        player_interest_state = "launch_hype_dropoff"
    elif one_off_spike:
        player_interest_state = "one_off_spike"
    elif resurging:
        player_interest_state = "resurging"
    elif declining:
        player_interest_state = "declining"
    elif stable_evergreen:
        player_interest_state = "stable_evergreen"
    elif stable:
        player_interest_state = "stable"
    else:
        player_interest_state = "mixed"

    return {
        "baseline_players": baseline_players,
        "avg_players_24h": avg_24h,
        "avg_players_7d": avg_7d,
        "avg_players_30d": avg_30d,
        "avg_players_90d": avg_90d,
        "avg_players_365d": avg_365d,
        "peak_players_365d": peak_365d,
        "min_players_365d": min_365d,
        "short_term_delta": round(short_term_delta, 6),
        "medium_term_delta": round(medium_term_delta, 6),
        "long_term_delta": round(long_term_delta, 6),
        "long_tail_delta": round(long_tail_delta, 6),
        "volatility_ratio": round(volatility_ratio, 6),
        "stability_score": round(stability_score, 6),
        "history_confidence": history_confidence,
        "history_point_count": sample_count,
        "history_coverage_days": coverage_days,
        "spike_ratio": round(spike_ratio, 6),
        "player_interest_state": player_interest_state,
    }


def compute_momentum_score(
    discount_percent: int | None,
    current_players: int | None,
    avg_players_last_24h: float | None,
    player_profile: dict[str, Any] | None = None,
) -> tuple[float, float, float, str]:
    discount = max(0.0, safe_num(discount_percent, 0.0))
    players = max(0.0, safe_num(current_players, 0.0))
    baseline = max(1.0, safe_num(avg_players_last_24h, 1.0))
    profile = player_profile or {}
    short_term_delta = safe_num(profile.get("short_term_delta"), 0.0)
    medium_term_delta = safe_num(profile.get("medium_term_delta"), 0.0)
    long_term_delta = safe_num(profile.get("long_term_delta"), 0.0)
    history_confidence = clamp(safe_num(profile.get("history_confidence"), 0.0), 0.0, 1.0)
    stability_score = clamp(safe_num(profile.get("stability_score"), 0.0), 0.0, 1.0)
    spike_ratio = safe_num(profile.get("spike_ratio"), 0.0)
    player_interest_state = str(profile.get("player_interest_state") or "").strip().lower()

    likely_transient_spike = (
        spike_ratio >= 1.8
        and medium_term_delta < 0.1
        and long_term_delta <= 0.08
        and history_confidence >= 0.4
    )

    if not profile:
        growth_ratio = players / baseline if players > 0 else 0.0
        short_term_trend = growth_ratio - 1.0
    else:
        growth_ratio = max(0.0, 1.0 + short_term_delta)
        short_term_trend = (
            short_term_delta * 0.62
            + medium_term_delta * 0.28
            + long_term_delta * 0.10
        )
        short_term_trend = clamp(short_term_trend, -0.95, 1.5)
        if likely_transient_spike:
            short_term_trend = min(short_term_trend, 0.55)

    # Avoid tiny-sample spikes dominating trend ranking.
    tiny_sample_guard = 0.4 if players < 300 else 1.0
    spike_bonus = 0.0
    if players >= 1000 and spike_ratio >= 1.8:
        spike_bonus = 10.0
    elif players >= 500 and spike_ratio >= 1.5:
        spike_bonus = 6.0

    if player_interest_state in {"one_off_spike", "launch_hype_dropoff"}:
        spike_bonus -= 4.0
    elif player_interest_state == "resurging":
        spike_bonus += 4.0
    if likely_transient_spike and player_interest_state != "resurging":
        spike_bonus -= 3.0

    raw_momentum = (
        discount * 0.30
        + math.log1p(max(players, baseline)) * 2.1
        + max(0.0, short_term_trend) * 20.0
        + max(0.0, medium_term_delta) * 14.0
        + max(0.0, long_term_delta) * 9.0
        + stability_score * 4.0
        + spike_bonus
    )
    if short_term_trend < -0.12:
        raw_momentum += short_term_trend * 10.0
    if medium_term_delta < -0.1:
        raw_momentum += medium_term_delta * 8.0
    if long_term_delta < -0.08:
        raw_momentum += long_term_delta * 6.0

    confidence_scale = 1.0 if not profile else (0.45 + history_confidence * 0.55)
    momentum_score = raw_momentum * tiny_sample_guard * confidence_scale
    momentum_score = round(clamp(momentum_score, 0.0, 100.0), 2)
    growth_ratio = round(max(0.0, growth_ratio), 6)
    short_term_trend = round(short_term_trend, 6)

    growth_pct = int(round(max(0.0, (growth_ratio - 1.0) * 100.0)))
    if history_confidence < 0.35 and player_interest_state in {"mixed", ""}:
        reason = "Player history is still sparse; trend confidence is limited"
    elif player_interest_state == "stable_evergreen":
        reason = "Stable long-term player base with consistent engagement"
    elif player_interest_state == "declining":
        reason = "Player activity is cooling across medium and long-term windows"
    elif player_interest_state == "resurging":
        reason = "Player activity is resurging against a weaker long-term baseline"
    elif player_interest_state == "one_off_spike":
        reason = "Recent player spike is sharp but not yet sustained"
    elif player_interest_state == "launch_hype_dropoff":
        reason = "Launch spike cooled; player interest remains below prior peak"
    elif growth_pct >= 150:
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
    history_confidence: float | None = None,
    player_interest_state: str | None = None,
) -> tuple[float, dict[str, float], str]:
    discount_value = clamp(safe_num(discount_percent, 0.0), 0.0, 100.0)
    review_value = clamp(safe_num(review_score, 0.0), 0.0, 100.0)
    review_count_value = max(0.0, safe_num(review_count, 0.0))
    players = max(0.0, safe_num(avg_player_count, 0.0))
    growth_ratio = max(0.0, safe_num(player_growth_ratio, 0.0))
    confidence_factor = clamp(safe_num(history_confidence, 0.45), 0.2, 1.0)
    interest_state = str(player_interest_state or "").strip().lower()

    discount_component = round(discount_value * 0.42, 2)
    review_confidence = clamp(math.log10(max(10.0, review_count_value)) / 4.0, 0.0, 1.0)
    review_component = round((review_value / 100.0) * review_confidence * 24.0, 2)
    player_activity_component = round(
        clamp(math.log10(players + 1.0) * 5.5, 0.0, 14.0) * (0.65 + confidence_factor * 0.35),
        2,
    )
    player_growth_component = round(
        clamp((growth_ratio - 1.0) * 18.0, -10.0, 16.0) * confidence_factor,
        2,
    )
    player_history_confidence_component = round(confidence_factor * 8.0, 2)
    player_state_adjustment = 0.0
    if interest_state in {"stable_evergreen", "resurging"}:
        player_state_adjustment = 3.0
    elif interest_state in {"declining", "launch_hype_dropoff"}:
        player_state_adjustment = -6.0
    elif interest_state == "one_off_spike":
        player_state_adjustment = -4.0

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
        "player_history_confidence_component": player_history_confidence_component,
        "player_state_adjustment": round(player_state_adjustment, 2),
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
    if interest_state in {"stable_evergreen", "resurging"}:
        reasons.append("durable player interest")
    if confidence_factor < 0.35:
        reasons.append("sparse player history")
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
    player_interest_state: str | None = None,
    history_confidence: float | None = None,
    short_term_player_trend: float | None = None,
    long_term_player_trend: float | None = None,
) -> tuple[str, str, float | None]:
    ratio = None
    interest_state = str(player_interest_state or "").strip().lower()
    confidence_factor = clamp(safe_num(history_confidence, 0.45), 0.0, 1.0)
    short_trend = safe_num(short_term_player_trend, 0.0)
    long_trend = safe_num(long_term_player_trend, 0.0)
    if current_price is not None and current_price > 0 and historical_low is not None and historical_low > 0:
        ratio = round(current_price / historical_low, 6)
        if current_price <= historical_low * 1.05:
            return "BUY_NOW", "Price near historical low", ratio

    normalized_discount = int(safe_num(discount_percent, -1.0)) if discount_percent is not None else None
    if (
        interest_state in {"declining", "launch_hype_dropoff"}
        and normalized_discount is not None
        and normalized_discount < 45
    ):
        return "WAIT", "Long-term player interest is cooling; wait for a deeper discount", ratio
    if (
        interest_state == "one_off_spike"
        and confidence_factor >= 0.35
        and normalized_discount is not None
        and normalized_discount < 35
    ):
        return "WAIT", "Recent player spike looks temporary; wait for pricing to stabilize", ratio
    if (
        interest_state in {"stable_evergreen", "resurging"}
        and ratio is not None
        and ratio <= 1.12
        and normalized_discount is not None
        and normalized_discount >= 20
    ):
        return "BUY_NOW", "Price is near low while long-term player interest remains durable", ratio

    if normalized_discount is not None and normalized_discount < 25:
        return "WAIT", "Discount depth historically larger", ratio

    if (
        confidence_factor < 0.35
        and ratio is not None
        and ratio > 1.15
        and normalized_discount is not None
        and normalized_discount < 35
    ):
        return "WAIT", "Player history is still sparse and price is not near historical low", ratio

    if confidence_factor >= 0.45 and long_trend <= -0.12 and short_trend <= -0.08 and normalized_discount is not None and normalized_discount < 40:
        return "WAIT", "Player trend is weakening across multiple windows", ratio

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


def compute_deal_opportunity_score(
    *,
    price_vs_low_ratio: float | None,
    predicted_sale_confidence: str | None,
    predicted_next_sale_window_days_min: int | None,
    predicted_next_sale_window_days_max: int | None,
    days_since_last_sale: int | None,
    max_discount: int | None,
    popularity_score: float | None,
    trending_score: float | None,
) -> tuple[float, str | None]:
    score = 0.0
    reasons: list[str] = []

    def push_reason(text: str) -> None:
        normalized = text.strip()
        if not normalized:
            return
        token = normalized.lower()
        if any(existing.lower() == token for existing in reasons):
            return
        reasons.append(normalized)

    ratio = safe_num(price_vs_low_ratio, 0.0)
    if ratio > 0:
        if ratio <= 1.05:
            score += 22.0
            push_reason("Close to historical low")
        elif ratio <= 1.12:
            score += 16.0
            push_reason("Near historical low")
        elif ratio <= 1.25:
            score += 9.0

    confidence = str(predicted_sale_confidence or "").strip().upper()
    confidence_weight = {
        "HIGH": 24.0,
        "MEDIUM": 16.0,
        "LOW": 8.0,
    }.get(confidence, 0.0)
    score += confidence_weight
    if confidence == "HIGH":
        push_reason("Sale timing confidence is high")
    elif confidence == "MEDIUM":
        push_reason("Sale timing confidence is medium")

    window_min = int(round(safe_num(predicted_next_sale_window_days_min, 0.0)))
    window_max = int(round(safe_num(predicted_next_sale_window_days_max, 0.0)))
    cadence_days = None
    if window_min > 0 and window_max > 0:
        cadence_days = int(round((window_min + window_max) / 2.0))
    elif window_min > 0:
        cadence_days = window_min
    elif window_max > 0:
        cadence_days = window_max

    since_sale = int(round(safe_num(days_since_last_sale, -1.0)))
    if since_sale < 0:
        since_sale = None

    if cadence_days is not None and since_sale is not None:
        cadence = max(14, cadence_days)
        overdue_ratio = since_sale / float(cadence)
        if overdue_ratio >= 1.2:
            score += 18.0
            push_reason(f"Historically discounts every ~{cadence_days} days")
        elif overdue_ratio >= 0.9:
            score += 11.0
            push_reason("Approaching a typical sale window")
        elif overdue_ratio >= 0.6:
            score += 6.0
    elif since_sale is not None:
        if since_sale >= 90:
            score += 12.0
            push_reason("Overdue for a new sale")
        elif since_sale >= 45:
            score += 7.0

    historical_max_discount = max(0.0, safe_num(max_discount, 0.0))
    if historical_max_discount >= 70:
        score += 10.0
        push_reason("Historically reaches deep discounts")
    elif historical_max_discount >= 50:
        score += 7.0
    elif historical_max_discount >= 30:
        score += 4.0

    popularity = clamp(safe_num(popularity_score, 0.0), 0.0, 100.0)
    trending = clamp(safe_num(trending_score, 0.0), 0.0, 100.0)
    score += (popularity / 100.0) * 10.0
    score += (trending / 100.0) * 10.0
    if popularity >= 68 and (since_sale is None or since_sale >= 45):
        push_reason("High popularity and overdue for sale")
    elif popularity >= 70:
        push_reason("High popularity")
    if trending >= 65:
        push_reason("Player momentum is rising")

    normalized_score = round(clamp(score, 0.0, 100.0), 2)
    reason = " and ".join(reasons[:2]) if reasons else None
    return normalized_score, reason


def compute_deal_heat(
    discount_percent: int | None,
    review_score: int | None,
    current_players: int | None,
    player_growth_ratio: float | None,
    historical_low_hit: bool,
    trend_reason_summary: str | None,
    player_interest_state: str | None = None,
    history_confidence: float | None = None,
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
    normalized_state = str(player_interest_state or "").strip().lower()
    confidence_factor = clamp(safe_num(history_confidence, 0.45), 0.0, 1.0)
    if normalized_state == "stable_evergreen":
        tags.append("evergreen")
    elif normalized_state == "resurging":
        tags.append("resurgence")
    elif normalized_state == "declining":
        tags.append("decline")
    elif normalized_state == "launch_hype_dropoff":
        tags.append("launch_dropoff")
    elif normalized_state == "one_off_spike":
        tags.append("spike_noise")
    if confidence_factor < 0.35:
        tags.append("sparse_history")

    if len(tags) >= 4:
        level = "viral"
    elif len(tags) >= 2:
        level = "hot"
    else:
        level = "warm"

    if historical_low_hit and discount >= 40:
        reason = "Now at a new historical low with a strong discount"
    elif normalized_state == "stable_evergreen":
        reason = "Stable long-term player base plus favorable deal timing"
    elif normalized_state == "resurging":
        reason = "Player activity is resurging while deal value is attractive"
    elif normalized_state in {"declining", "launch_hype_dropoff"}:
        reason = "Discount is active, but long-term player interest is cooling"
    elif normalized_state == "one_off_spike":
        reason = "Recent spike detected, but long-term trend is not yet confirmed"
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
        "buy_recommendation": _contract_buy_recommendation(snapshot.buy_recommendation),
        "buy_reason": snapshot.buy_reason,
        "price_vs_low_ratio": snapshot.price_vs_low_ratio,
        "predicted_next_sale_price": snapshot.predicted_next_sale_price,
        "predicted_next_discount_percent": snapshot.predicted_next_discount_percent,
        "predicted_next_sale_window_days_min": snapshot.predicted_next_sale_window_days_min,
        "predicted_next_sale_window_days_max": snapshot.predicted_next_sale_window_days_max,
        "predicted_sale_confidence": snapshot.predicted_sale_confidence,
        "predicted_sale_reason": snapshot.predicted_sale_reason,
        "deal_opportunity_score": snapshot.deal_opportunity_score,
        "deal_opportunity_reason": snapshot.deal_opportunity_reason,
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
        "review_score_label": _normalize_review_label(snapshot.review_score_label, snapshot.review_score),
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
    rail_key: str,
) -> list[GameSnapshot]:
    def _diversity_token(game_id: int) -> int:
        digest = hashlib.sha1(
            f"{utcnow().date().isoformat()}:{rail_key}:{game_id}".encode("utf-8")
        ).hexdigest()
        return int(digest[:12], 16)

    def _reorder_with_diversity_bias(rows: list[GameSnapshot]) -> list[GameSnapshot]:
        if len(rows) <= HOMEPAGE_DIVERSITY_LEAD_PROTECT + 1:
            return rows
        lead_count = min(HOMEPAGE_DIVERSITY_LEAD_PROTECT, len(rows))
        reordered: list[GameSnapshot] = list(rows[:lead_count])
        tail = list(rows[lead_count:])
        while tail:
            window_size = min(HOMEPAGE_DIVERSITY_ROTATION_WINDOW, len(tail))
            best_idx = min(
                range(window_size),
                key=lambda idx: _diversity_token(int(safe_num(tail[idx].game_id, 0.0))),
            )
            reordered.append(tail.pop(best_idx))
        return reordered

    ordered_candidates = _reorder_with_diversity_bias(ranked_candidates)
    selected: list[GameSnapshot] = []
    deferred: list[GameSnapshot] = []
    section_seen: set[int] = set()
    unique_target = max(0, min(section_limit, uniqueness_window))

    for row in ordered_candidates:
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
            rail_key=rail_key,
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
    discovery_rows = {
        int(row.game_id): row
        for row in session.query(GameDiscoveryFeed).filter(GameDiscoveryFeed.game_id.in_(game_ids)).all()
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
    player_history_cutoff = now - datetime.timedelta(days=PLAYER_HISTORY_LOOKBACK_DAYS)
    player_history_stats_map: dict[int, dict[str, Any]] = {}
    player_history_rows = (
        session.query(
            GamePlayerHistory.game_id,
            func.count(GamePlayerHistory.id),
            func.min(GamePlayerHistory.recorded_at),
            func.max(GamePlayerHistory.recorded_at),
            func.avg(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(hours=24), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.avg(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=7), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.avg(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=30), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.avg(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=90), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.avg(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=365), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.max(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=365), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
            func.min(
                case(
                    (GamePlayerHistory.recorded_at >= now - datetime.timedelta(days=365), GamePlayerHistory.current_players),
                    else_=None,
                )
            ),
        )
        .filter(
            GamePlayerHistory.game_id.in_(game_ids),
            GamePlayerHistory.current_players.isnot(None),
            GamePlayerHistory.recorded_at >= player_history_cutoff,
        )
        .group_by(GamePlayerHistory.game_id)
        .all()
    )
    for (
        gid,
        history_count,
        first_seen_at,
        last_seen_at,
        avg_players_24h,
        avg_players_7d,
        avg_players_30d,
        avg_players_90d,
        avg_players_365d,
        peak_players_365d,
        min_players_365d,
    ) in player_history_rows:
        coverage_days = 0
        if first_seen_at and last_seen_at:
            coverage_days = max(0, int((last_seen_at - first_seen_at).days))
        player_history_stats_map[int(gid)] = {
            "history_point_count": int(safe_num(history_count, 0.0)),
            "history_coverage_days": coverage_days,
            "avg_players_24h": safe_num(avg_players_24h, 0.0),
            "avg_players_7d": safe_num(avg_players_7d, 0.0),
            "avg_players_30d": safe_num(avg_players_30d, 0.0),
            "avg_players_90d": safe_num(avg_players_90d, 0.0),
            "avg_players_365d": safe_num(avg_players_365d, 0.0),
            "peak_players_365d": safe_num(peak_players_365d, 0.0),
            "min_players_365d": safe_num(min_players_365d, 0.0),
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
        player_history_stats = player_history_stats_map.get(int(game_id), {})
        player_profile = compute_player_history_profile(
            current_players=current_players,
            avg_players_last_24h=player_history_stats.get("avg_players_24h"),
            avg_players_7d=player_history_stats.get("avg_players_7d"),
            avg_players_30d=player_history_stats.get("avg_players_30d"),
            avg_players_90d=player_history_stats.get("avg_players_90d"),
            avg_players_365d=player_history_stats.get("avg_players_365d"),
            peak_players_365d=player_history_stats.get("peak_players_365d"),
            min_players_365d=player_history_stats.get("min_players_365d"),
            history_point_count=player_history_stats.get("history_point_count"),
            history_coverage_days=player_history_stats.get("history_coverage_days"),
        )
        avg_player_count_float = max(
            0.0,
            safe_num(player_profile.get("baseline_players"), safe_num(current_players, 0.0)),
        )
        avg_player_count = int(round(avg_player_count_float)) if avg_player_count_float > 0 else current_players
        baseline_daily_peak = previous_daily_peak if previous_daily_peak and previous_daily_peak > 0 else (current_players or 1)
        avg_players_last_24h = max(
            1.0,
            safe_num(player_profile.get("avg_players_24h"), safe_num(current_players, 1.0)),
        )
        momentum_score, player_growth_ratio, short_term_player_trend, trend_reason_summary = compute_momentum_score(
            discount_percent=latest_discount_percent,
            current_players=current_players,
            avg_players_last_24h=avg_players_last_24h,
            player_profile=player_profile,
        )
        medium_term_player_trend = safe_num(player_profile.get("medium_term_delta"), 0.0)
        long_term_player_trend = safe_num(player_profile.get("long_term_delta"), 0.0)
        history_confidence = clamp(safe_num(player_profile.get("history_confidence"), 0.0), 0.0, 1.0)
        player_interest_state = str(player_profile.get("player_interest_state") or "").strip().lower()
        trending_score = round(
            clamp(
                safe_num(momentum_score, 0.0)
                + clamp(medium_term_player_trend * 18.0, -6.0, 8.0)
                + clamp(long_term_player_trend * 12.0, -4.0, 6.0),
                0.0,
                100.0,
            ),
            2,
        )
        player_momentum = round(clamp(short_term_player_trend, -1.0, 1.5), 6)
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
            history_confidence=history_confidence,
            medium_term_trend=medium_term_player_trend,
            long_term_trend=long_term_player_trend,
            player_interest_state=player_interest_state,
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
        has_upcoming_artwork = bool(str(game.appid or "").strip())
        has_genre_metadata = bool(split_csv_field(game.genres))
        has_platform_metadata = bool(split_csv_field(game.platforms))
        if is_upcoming:
            upcoming_hot_score = compute_upcoming_hot_score(
                release_date=release_date,
                release_date_text=game.release_date_text,
                wishlist_count=wishlist_count,
                watchlist_count=watchlist_count,
                review_score=review_score,
                review_count=review_count,
                popularity_score=popularity_score,
                has_artwork=has_upcoming_artwork,
                has_genre=has_genre_metadata,
                has_platform=has_platform_metadata,
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
            history_confidence=history_confidence,
            player_interest_state=player_interest_state,
        )
        deal_heat_level, deal_heat_reason, deal_heat_tags = compute_deal_heat(
            discount_percent=latest_discount_percent,
            review_score=game.review_score,
            current_players=current_players,
            player_growth_ratio=player_growth_ratio,
            historical_low_hit=is_new_historical_low,
            trend_reason_summary=trend_reason_summary,
            player_interest_state=player_interest_state,
            history_confidence=history_confidence,
        )
        buy_recommendation, buy_reason, price_vs_low_ratio = compute_buy_recommendation(
            current_price=latest_price,
            historical_low=historical_low,
            discount_percent=latest_discount_percent,
            days_since_last_sale=days_since_last_sale,
            player_interest_state=player_interest_state,
            history_confidence=history_confidence,
            short_term_player_trend=short_term_player_trend,
            long_term_player_trend=long_term_player_trend,
        )
        next_sale_prediction = compute_next_sale_prediction(
            current_price=latest_price,
            latest_original_price=latest_original_price,
            historical_low_price=historical_low,
            sale_rows=prediction_sale_rows,
        )
        deal_opportunity_score, deal_opportunity_reason = compute_deal_opportunity_score(
            price_vs_low_ratio=price_vs_low_ratio,
            predicted_sale_confidence=next_sale_prediction.get("predicted_sale_confidence"),
            predicted_next_sale_window_days_min=next_sale_prediction.get("predicted_next_sale_window_days_min"),
            predicted_next_sale_window_days_max=next_sale_prediction.get("predicted_next_sale_window_days_max"),
            days_since_last_sale=days_since_last_sale,
            max_discount=max_discount,
            popularity_score=popularity_score,
            trending_score=trending_score,
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
        snapshot.review_score_label = _normalize_review_label(game.review_score_label, review_score)
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
        snapshot.buy_recommendation = _contract_buy_recommendation(buy_recommendation)
        snapshot.buy_reason = buy_reason
        snapshot.price_vs_low_ratio = price_vs_low_ratio
        snapshot.predicted_next_sale_price = next_sale_prediction.get("predicted_next_sale_price")
        snapshot.predicted_next_discount_percent = next_sale_prediction.get("predicted_next_discount_percent")
        snapshot.predicted_next_sale_window_days_min = next_sale_prediction.get("predicted_next_sale_window_days_min")
        snapshot.predicted_next_sale_window_days_max = next_sale_prediction.get("predicted_next_sale_window_days_max")
        snapshot.predicted_sale_confidence = next_sale_prediction.get("predicted_sale_confidence")
        snapshot.predicted_sale_reason = next_sale_prediction.get("predicted_sale_reason")
        snapshot.deal_opportunity_score = deal_opportunity_score
        snapshot.deal_opportunity_reason = deal_opportunity_reason
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
        ranking_explanations = _normalize_ranking_explanations(snapshot.ranking_explanations)
        insight_engine_triggers: list[dict[str, Any]] = []
        insight_engine_timestamp = _as_epoch_ms(latest.recorded_at if latest and latest.recorded_at else now)

        if previous_price is not None and latest_price is not None and previous_price != latest_price:
            insight_engine_triggers.append(
                {
                    "type": "price_change",
                    "gameId": str(game_id),
                    "timestamp": insight_engine_timestamp,
                    "previous": float(previous_price),
                    "current": float(latest_price),
                }
            )

        previous_review_label = _normalize_review_label(snapshot.review_score_label if snapshot else None, snapshot.review_score if snapshot else None)
        current_review_label = _normalize_review_label(game.review_score_label, review_score)
        if previous_review_label and current_review_label and previous_review_label != current_review_label:
            insight_engine_triggers.append(
                {
                    "type": "review_change",
                    "gameId": str(game_id),
                    "timestamp": _as_epoch_ms(now),
                    "previous": previous_review_label,
                    "current": current_review_label,
                }
            )

        if snapshot is not None and bool(snapshot.is_upcoming) and not is_upcoming:
            insight_engine_triggers.append(
                {
                    "type": "release_event",
                    "gameId": str(game_id),
                    "timestamp": _as_epoch_ms(now),
                    "previous": None,
                    "current": "released",
                }
            )

        if snapshot is not None and snapshot.popularity_score is not None:
            previous_relevance = clamp(safe_num(snapshot.popularity_score, 0.0) / 100.0, 0.0, 1.0)
            current_relevance = clamp(safe_num(popularity_score, 0.0) / 100.0, 0.0, 1.0)
            if previous_relevance < 0.6 <= current_relevance:
                insight_engine_triggers.append(
                    {
                        "type": "relevance_increase",
                        "gameId": str(game_id),
                        "timestamp": insight_engine_timestamp,
                        "previous": float(previous_relevance),
                        "current": float(current_relevance),
                    }
                )

        insight_engine_context = _build_insight_engine_context(
            is_historical_low=is_historical_low,
            wishlist_count=wishlist_count,
            watchlist_count=watchlist_count,
            click_count=click_count,
            now=now,
        )
        insight_engine_result = _run_insight_engine_subprocess(insight_engine_triggers, insight_engine_context)
        ranking_explanations.update(
            {
                "worth_buying": worth_buying_reason_summary,
                "momentum": trend_reason_summary,
                "heat": deal_heat_reason,
                "buy_timing": buy_reason,
                "next_sale_prediction": next_sale_prediction.get("predicted_sale_reason"),
                "deal_opportunity": deal_opportunity_reason,
                "player_interest_state": player_interest_state,
                "player_history_confidence": history_confidence,
                "player_trends": {
                    "short_term": short_term_player_trend,
                    "medium_term": medium_term_player_trend,
                    "long_term": long_term_player_trend,
                    "coverage_days": int(safe_num(player_profile.get("history_coverage_days"), 0.0)),
                    "history_point_count": int(safe_num(player_profile.get("history_point_count"), 0.0)),
                },
                "insight_engine": insight_engine_result,
            }
        )
        snapshot.ranking_explanations = ranking_explanations
        snapshot.upcoming_hot_score = upcoming_hot_score
        snapshot.price_sparkline_90d = sparkline
        snapshot.sale_events_compact = sale_events_compact
        snapshot.deal_detected_at = deal_detected_at

        snapshot.updated_at = now

        discovery_row = discovery_rows.get(int(game_id))
        if discovery_row is None:
            discovery_row = GameDiscoveryFeed(game_id=game_id)
            session.add(discovery_row)
            discovery_rows[int(game_id)] = discovery_row

        normalized_recommendation = _normalize_buy_recommendation(buy_recommendation)
        normalized_discount = int(safe_num(latest_discount_percent, 0.0)) if latest_discount_percent is not None else 0
        is_strong_buy = normalized_recommendation == "BUY_NOW" and safe_num(worth_buying_score, 0.0) >= 70.0
        is_wait_pick = normalized_recommendation == "WAIT"
        is_big_discount = normalized_discount >= 50
        is_trending_now = (
            safe_num(trending_score, 0.0) >= 58.0
            or safe_num(short_term_player_trend, 0.0) >= 0.06
            or (
                safe_num(momentum_score, 0.0) >= 58.0
                and safe_num(current_players, 0.0) >= 250.0
            )
        )
        if player_interest_state == "one_off_spike" and history_confidence >= 0.35 and medium_term_player_trend < 0.05:
            is_trending_now = False

        discovery_row.game_name = game.name
        discovery_row.steam_appid = game.appid
        discovery_row.store_url = game.store_url
        discovery_row.banner_url = snapshot.banner_url
        discovery_row.latest_price = latest_price
        discovery_row.latest_original_price = latest_original_price
        discovery_row.latest_discount_percent = latest_discount_percent
        discovery_row.historical_low = historical_low
        discovery_row.historical_status = snapshot.historical_status
        discovery_row.historical_low_hit = is_new_historical_low
        discovery_row.buy_recommendation = _contract_buy_recommendation(buy_recommendation)
        discovery_row.buy_reason = buy_reason
        discovery_row.deal_score = deal_score
        discovery_row.buy_score = worth_buying_score
        discovery_row.worth_buying_score = worth_buying_score
        discovery_row.momentum_score = momentum_score
        discovery_row.trending_score = trending_score
        discovery_row.deal_opportunity_score = deal_opportunity_score
        discovery_row.deal_opportunity_reason = deal_opportunity_reason
        discovery_row.predicted_next_sale_price = next_sale_prediction.get("predicted_next_sale_price")
        discovery_row.predicted_next_discount_percent = next_sale_prediction.get("predicted_next_discount_percent")
        discovery_row.predicted_next_sale_window_days_min = next_sale_prediction.get("predicted_next_sale_window_days_min")
        discovery_row.predicted_next_sale_window_days_max = next_sale_prediction.get("predicted_next_sale_window_days_max")
        discovery_row.predicted_sale_confidence = next_sale_prediction.get("predicted_sale_confidence")
        discovery_row.predicted_sale_reason = next_sale_prediction.get("predicted_sale_reason")
        discovery_row.popularity_score = popularity_score
        discovery_row.price_vs_low_ratio = price_vs_low_ratio
        discovery_row.max_discount = max_discount
        discovery_row.current_players = current_players
        discovery_row.player_growth_ratio = player_growth_ratio
        discovery_row.short_term_player_trend = short_term_player_trend
        discovery_row.review_score = int(review_score) if game.review_score is not None else None
        discovery_row.review_score_label = _normalize_review_label(game.review_score_label, review_score)
        discovery_row.review_count = int(review_count) if game.review_total_count is not None else None
        discovery_row.genres = game.genres
        discovery_row.tags = game.tags
        discovery_row.platforms = game.platforms
        discovery_row.worth_buying_reason_summary = worth_buying_reason_summary
        discovery_row.trend_reason_summary = trend_reason_summary
        discovery_row.deal_heat_reason = deal_heat_reason
        discovery_row.is_released = game.is_released or 0
        discovery_row.is_upcoming = is_upcoming
        discovery_row.release_date = release_date
        discovery_row.is_strong_buy = is_strong_buy
        discovery_row.is_wait_pick = is_wait_pick
        discovery_row.is_new_historical_low = is_new_historical_low
        discovery_row.is_big_discount = is_big_discount
        discovery_row.is_trending_now = is_trending_now
        discovery_row.updated_at = now

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
    normalized = str(value or "").strip().upper().replace(" ", "_")
    return normalized if normalized in {"BUY_NOW", "WAIT", "AVOID"} else ""


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


def _take_diverse_payload_rows(
    ranked_candidates: list[dict],
    used_keys: set[str],
    section_limit: int,
    uniqueness_window: int,
) -> list[dict]:
    selected: list[dict] = []
    deferred: list[dict] = []
    section_seen: set[str] = set()
    unique_target = max(0, min(section_limit, uniqueness_window))

    for idx, row in enumerate(ranked_candidates):
        if not isinstance(row, dict):
            continue
        key = _snapshot_identity_key(row, idx)
        if key in section_seen:
            continue
        if len(selected) < unique_target and key in used_keys:
            deferred.append(row)
            continue
        selected.append(row)
        section_seen.add(key)
        if len(selected) >= section_limit:
            return selected

    for idx, row in enumerate(deferred):
        if not isinstance(row, dict):
            continue
        key = _snapshot_identity_key(row, idx)
        if key in section_seen:
            continue
        selected.append(row)
        section_seen.add(key)
        if len(selected) >= section_limit:
            return selected

    return selected


def _apply_homepage_payload_diversity(
    rail_candidates: dict[str, list[dict]],
    section_limit: int,
    uniqueness_window: int,
    rail_order: tuple[str, ...],
) -> dict[str, list[dict]]:
    diversified: dict[str, list[dict]] = {
        key: _dedupe_snapshot_rows(rows)
        for key, rows in rail_candidates.items()
    }
    used_keys: set[str] = set()
    for rail_key in rail_order:
        rows = diversified.get(rail_key, [])
        diversified_rows = _take_diverse_payload_rows(
            ranked_candidates=rows,
            used_keys=used_keys,
            section_limit=section_limit,
            uniqueness_window=uniqueness_window,
        )
        diversified[rail_key] = diversified_rows
        for idx, row in enumerate(diversified_rows[:uniqueness_window]):
            used_keys.add(_snapshot_identity_key(row, idx))
    return diversified


def _score_homepage_opportunity_row(row: dict) -> float:
    buy_score = safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0))
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return (
        safe_num(row.get("deal_opportunity_score"), 0.0) * 0.9
        + buy_score * 0.45
        + safe_num(row.get("deal_score"), 0.0) * 0.35
        + safe_num(row.get("momentum_score"), 0.0) * 0.2
        + safe_num(row.get("trending_score"), 0.0) * 0.15
        + discount * 0.12
    )


def _build_homepage_opportunity_rails(decision_pool: list[dict], limit: int) -> tuple[list[dict], list[dict]]:
    ranked_candidates = sorted(
        _dedupe_snapshot_rows(decision_pool),
        key=lambda row: (
            _score_homepage_opportunity_row(row),
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("latest_discount_percent"), safe_num(row.get("discount_percent"), 0.0)),
        ),
        reverse=True,
    )
    opportunity_rows = ranked_candidates[:limit]
    opportunity_keys = {
        _snapshot_identity_key(row, idx)
        for idx, row in enumerate(opportunity_rows)
        if isinstance(row, dict)
    }

    radar_candidates = sorted(
        _dedupe_snapshot_rows(decision_pool),
        key=lambda row: (
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("momentum_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("latest_discount_percent"), safe_num(row.get("discount_percent"), 0.0)),
        ),
        reverse=True,
    )

    radar_rows: list[dict] = []
    radar_keys: set[str] = set()
    for idx, row in enumerate(radar_candidates):
        if not isinstance(row, dict):
            continue
        key = _snapshot_identity_key(row, idx)
        if key in opportunity_keys:
            continue
        radar_rows.append(row)
        radar_keys.add(key)
        if len(radar_rows) >= limit:
            break

    if len(radar_rows) < limit:
        for idx, row in enumerate(radar_candidates):
            if not isinstance(row, dict):
                continue
            key = _snapshot_identity_key(row, idx)
            if key in radar_keys:
                continue
            radar_rows.append(row)
            radar_keys.add(key)
            if len(radar_rows) >= limit:
                break

    return opportunity_rows, radar_rows


def _seasonal_sale_window(now_date: datetime.date) -> dict:
    year = now_date.year
    windows = [
        {
            "name": "Steam Spring Sale",
            "slug": "spring_sale",
            "start": datetime.date(year, 3, 10),
            "end": datetime.date(year, 3, 24),
        },
        {
            "name": "Steam Summer Sale",
            "slug": "summer_sale",
            "start": datetime.date(year, 6, 20),
            "end": datetime.date(year, 7, 4),
        },
        {
            "name": "Steam Autumn Sale",
            "slug": "autumn_sale",
            "start": datetime.date(year, 11, 20),
            "end": datetime.date(year, 12, 3),
        },
        {
            "name": "Steam Winter Sale",
            "slug": "winter_sale",
            "start": datetime.date(year, 12, 18),
            "end": datetime.date(year + 1, 1, 5),
        },
    ]
    for window in windows:
        if window["start"] <= now_date <= window["end"]:
            return {**window, "status": "live", "days_until_start": 0}
    upcoming_windows = [window for window in windows if window["start"] > now_date]
    if not upcoming_windows:
        next_window = {
            "name": "Steam Spring Sale",
            "slug": "spring_sale",
            "start": datetime.date(year + 1, 3, 10),
            "end": datetime.date(year + 1, 3, 24),
        }
    else:
        next_window = upcoming_windows[0]
    return {
        **next_window,
        "status": "upcoming",
        "days_until_start": (next_window["start"] - now_date).days,
    }


def _build_cached_seasonal_summary(decision_pool: list[dict], limit: int = 24) -> dict:
    sale_window = _seasonal_sale_window(utcnow().date())
    mode = "active_sale" if sale_window.get("status") == "live" else "potential_sale"
    if mode == "active_sale":
        candidates = [row for row in decision_pool if safe_num(row.get("discount_percent"), 0.0) > 0]
    else:
        candidates = [row for row in decision_pool if safe_num(row.get("discount_percent"), 0.0) <= 0]
    if not candidates:
        candidates = decision_pool
    raw_items = _dedupe_snapshot_rows(candidates)[:max(1, int(limit))]
    items: list[dict] = []
    for row in raw_items:
        compact = _compact_catalog_seed_row(row)
        if not compact:
            continue
        compact["deal_score"] = row.get("deal_score")
        compact["buy_recommendation"] = row.get("buy_recommendation")
        compact["historical_status"] = row.get("historical_status")
        compact["predicted_sale_score"] = row.get("predicted_sale_score")
        compact["seasonal_relevance_score"] = row.get("seasonal_relevance_score")
        items.append(
            {
                key: value
                for key, value in compact.items()
                if value is not None and value != "" and value != []
            }
        )
    return {
        "sale_event": {
            "name": sale_window["name"],
            "slug": sale_window["slug"],
            "status": sale_window["status"],
            "start_date": sale_window["start"].isoformat(),
            "end_date": sale_window["end"].isoformat(),
            "days_until_start": sale_window["days_until_start"],
        },
        "mode": mode,
        "items": items,
        "expected_games": items,
    }


def _compact_catalog_seed_row(row: dict) -> dict:
    if not isinstance(row, dict):
        return {}
    game_id = row.get("game_id") or row.get("id")
    if safe_num(game_id, 0.0) <= 0:
        return {}
    game_name = str(row.get("game_name") or row.get("name") or "").strip()
    if not game_name:
        return {}
    slimmed = {
        "game_id": int(game_id),
        "id": int(game_id),
        "game_name": game_name,
        "steam_appid": row.get("steam_appid"),
        "banner_url": row.get("banner_url") or row.get("image") or row.get("header_image"),
        "price": row.get("price"),
        "original_price": row.get("original_price"),
        "discount_percent": row.get("discount_percent"),
        "review_score": row.get("review_score"),
        "review_score_label": _normalize_review_label(
            row.get("review_score_label") or row.get("review_label"),
            row.get("review_score"),
        ),
        "current_players": row.get("current_players"),
        "genres": row.get("genres") if isinstance(row.get("genres"), list) else [],
        "tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
        "platforms": row.get("platforms") if isinstance(row.get("platforms"), list) else [],
    }
    return {
        key: value
        for key, value in slimmed.items()
        if value is not None and value != "" and value != []
    }


def _build_homepage_critical_payload(payload: dict) -> dict:
    allowed_keys = (
        "catalogSummary",
        "dealRanked",
        "biggest_discounts",
        "worth_buying_now",
        "trending_now",
        "new_historical_lows",
        "buy_now_picks",
        "wait_picks",
        "deal_opportunities",
        "opportunity_radar",
        "deal_radar",
        "daily_digest",
    )
    def slim_row(row: dict) -> dict:
        if not isinstance(row, dict):
            return {}
        price = row.get("price")
        if price is None:
            price = row.get("latest_price")
        original_price = row.get("original_price")
        if original_price is None:
            original_price = row.get("latest_original_price")
        discount_percent = row.get("discount_percent")
        if discount_percent is None:
            discount_percent = row.get("latest_discount_percent")
        banner_url = row.get("banner_url") or row.get("image") or row.get("header_image")
        game_id = row.get("game_id") or row.get("id")
        buy_score = row.get("buy_score")
        if buy_score is None:
            buy_score = row.get("worth_buying_score")
        review_score_label = row.get("review_score_label") or row.get("review_label")
        review_count = row.get("review_count") or row.get("review_total_count")
        is_upcoming = row.get("is_upcoming")
        is_released = row.get("is_released")
        if is_released is None:
            is_released = 0 if bool(is_upcoming) else 1
        slimmed = {
            "game_id": game_id,
            "id": row.get("id") or game_id,
            "game_name": row.get("game_name") or row.get("name"),
            "steam_appid": row.get("steam_appid"),
            "is_released": is_released,
            "is_upcoming": is_upcoming,
            "release_date": row.get("release_date"),
            "banner_url": banner_url,
            "price": price,
            "original_price": original_price,
            "discount_percent": discount_percent,
            "historical_low": row.get("historical_low"),
            "historical_status": row.get("historical_status"),
            "price_vs_low_ratio": row.get("price_vs_low_ratio"),
            "current_players": row.get("current_players"),
            "player_change": row.get("player_change"),
            "short_term_player_trend": row.get("short_term_player_trend"),
            "deal_score": row.get("deal_score"),
            "buy_score": buy_score,
            "worth_buying_score": row.get("worth_buying_score"),
            "deal_opportunity_score": row.get("deal_opportunity_score"),
            "buy_recommendation": _contract_buy_recommendation(row.get("buy_recommendation")),
            "buy_reason": row.get("buy_reason"),
            "predicted_next_discount_percent": row.get("predicted_next_discount_percent"),
            "predicted_sale_reason": row.get("predicted_sale_reason"),
            "worth_buying_reason_summary": row.get("worth_buying_reason_summary"),
            "trend_reason_summary": row.get("trend_reason_summary"),
            "deal_heat_reason": row.get("deal_heat_reason"),
            "review_score": row.get("review_score"),
            "review_score_label": _normalize_review_label(review_score_label, row.get("review_score")),
            "deal_detected_at": row.get("deal_detected_at"),
            "alert_type": row.get("alert_type"),
            "alert_label": row.get("alert_label"),
            "alert_created_at": row.get("alert_created_at") or row.get("created_at"),
            "timestamp": row.get("timestamp"),
            "signal_type": row.get("signal_type"),
            "signal_text": row.get("signal_text"),
            "image": row.get("image"),
            "opportunity_reason": row.get("opportunity_reason"),
            "opportunity_reasons": row.get("opportunity_reasons"),
        }
        return {
            key: value
            for key, value in slimmed.items()
            if value is not None and value != "" and value != [] and value != {}
        }

    def slim_rows(rows: list[dict], limit: int | None = None) -> list[dict]:
        if limit is None:
            limit = HOMEPAGE_CRITICAL_RAIL_LIMIT
        slimmed_rows: list[dict] = []
        for row in rows[:limit]:
            slimmed = slim_row(row)
            if slimmed:
                slimmed_rows.append(slimmed)
        return slimmed_rows

    def slim_daily_digest(digest: dict) -> dict:
        if not isinstance(digest, dict):
            return {}
        sections = digest.get("sections")
        slim_sections: dict[str, list[dict]] = {}
        if isinstance(sections, dict):
            for key, value in sections.items():
                if isinstance(value, list):
                    slim_sections[str(key)] = slim_rows(value, HOMEPAGE_CRITICAL_DIGEST_LIMIT)
        highlights = digest.get("highlights")
        slim_highlights = (
            slim_rows(highlights, HOMEPAGE_CRITICAL_DIGEST_LIMIT)
            if isinstance(highlights, list)
            else []
        )
        counts = digest.get("counts")
        if not isinstance(counts, dict):
            counts = {key: len(value) for key, value in slim_sections.items()}
        return {
            "personalized": bool(digest.get("personalized")),
            "window_hours": digest.get("window_hours"),
            "window_start": digest.get("window_start"),
            "window_end": digest.get("window_end"),
            "generated_at": digest.get("generated_at"),
            "counts": counts,
            "sections": slim_sections,
            "highlights": slim_highlights,
        }

    trimmed: dict[str, Any] = {}
    for key in allowed_keys:
        if key not in payload:
            continue
        value = payload[key]
        if isinstance(value, list):
            list_limit = HOMEPAGE_CRITICAL_RAIL_LIMIT
            if key in {"deal_radar"}:
                list_limit = HOMEPAGE_CRITICAL_RADAR_LIMIT
            trimmed[key] = slim_rows(value, list_limit)
        elif key in {"daily_digest"} and isinstance(value, dict):
            trimmed[key] = slim_daily_digest(value)
        else:
            trimmed[key] = value
    return trimmed


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


def _is_released_snapshot_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if bool(row.get("is_upcoming")):
        return False
    released_value = row.get("is_released")
    if released_value is None:
        return True
    try:
        return int(released_value) == 1
    except Exception:
        return bool(released_value)


def _snapshot_row_has_actual_sale(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    price = safe_num(row.get("price"), safe_num(row.get("latest_price"), 0.0))
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    return price > 0 and discount > 0


def _released_snapshot_rows(rows: list[dict]) -> list[dict]:
    return [row for row in _dedupe_snapshot_rows(rows) if _is_released_snapshot_row(row)]


def _released_deal_snapshot_rows(rows: list[dict]) -> list[dict]:
    return [
        row
        for row in _dedupe_snapshot_rows(rows)
        if _is_released_snapshot_row(row) and _snapshot_row_has_actual_sale(row)
    ]


def _compose_unique_snapshot_rows(
    primary_rows: list[dict],
    fallback_rows: list[dict],
    blocked_keys: set[str],
    limit: int,
) -> list[dict]:
    bounded_limit = max(1, int(limit))
    selected: list[dict] = []
    seen_keys: set[str] = set()
    for source_rows in (primary_rows, fallback_rows):
        for idx, row in enumerate(_dedupe_snapshot_rows(source_rows)):
            if not isinstance(row, dict):
                continue
            row_key = _snapshot_identity_key(row, idx)
            if not row_key or row_key in seen_keys or row_key in blocked_keys:
                continue
            selected.append(row)
            seen_keys.add(row_key)
            blocked_keys.add(row_key)
            if len(selected) >= bounded_limit:
                return selected
    return selected


def _is_wait_candidate_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if _normalize_buy_recommendation(row.get("buy_recommendation")) == "WAIT":
        return True
    return (
        safe_num(row.get("price_vs_low_ratio"), 0.0) >= 1.08
        or safe_num(row.get("predicted_next_discount_percent"), 0.0) >= 35
    )


def _snapshot_sort_game_id(row: dict) -> int:
    return int(safe_num((row or {}).get("game_id") or (row or {}).get("id"), 0.0))


def _snapshot_diversity_token(row: dict, rail_key: str, idx: int = 0) -> int:
    identity = _snapshot_identity_key(row, idx)
    digest = hashlib.sha1(
        f"{utcnow().date().isoformat()}:{rail_key}:{identity}".encode("utf-8")
    ).hexdigest()
    return int(digest[:12], 16)


def _reorder_ranked_snapshot_rows_for_diversity(
    ranked_rows: list[dict],
    *,
    rail_key: str,
    lead_protect: int = HOMEPAGE_DIVERSITY_LEAD_PROTECT,
    rotation_window: int = HOMEPAGE_DIVERSITY_ROTATION_WINDOW,
) -> list[dict]:
    if len(ranked_rows) <= lead_protect + 1:
        return ranked_rows
    lead_count = min(max(0, int(lead_protect)), len(ranked_rows))
    reordered: list[dict] = list(ranked_rows[:lead_count])
    tail = list(ranked_rows[lead_count:])
    while tail:
        window_size = min(max(1, int(rotation_window)), len(tail))
        best_idx = min(
            range(window_size),
            key=lambda idx: _snapshot_diversity_token(tail[idx], rail_key, idx),
        )
        reordered.append(tail.pop(best_idx))
    return reordered


def _is_all_deals_floor_snapshot_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    if not _is_released_snapshot_row(row):
        return False
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    price = safe_num(row.get("price"), safe_num(row.get("latest_price"), 0.0))
    if price <= 0 or discount <= 0:
        return False
    if discount < HOMEPAGE_ALL_DEALS_MIN_DISCOUNT:
        return False
    deal_score = safe_num(row.get("deal_score"), 0.0)
    buy_score = safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0))
    opportunity_score = safe_num(row.get("deal_opportunity_score"), 0.0)
    review_score = safe_num(row.get("review_score"), 0.0)
    current_players = safe_num(row.get("current_players"), 0.0)
    historical_status = str(row.get("historical_status") or "").strip().lower()
    has_historical_signal = historical_status in {"new_historical_low", "matches_historical_low", "near_historical_low"}
    return bool(
        discount >= 55
        or deal_score >= 38
        or buy_score >= 40
        or opportunity_score >= 42
        or review_score >= 72
        or current_players >= 300
        or has_historical_signal
    )


def _score_all_deals_snapshot_row(row: dict) -> float:
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    deal_score = safe_num(row.get("deal_score"), 0.0)
    buy_score = safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0))
    opportunity_score = safe_num(row.get("deal_opportunity_score"), 0.0)
    momentum_score = safe_num(row.get("momentum_score"), 0.0)
    review_score = safe_num(row.get("review_score"), 0.0)
    current_players = safe_num(row.get("current_players"), 0.0)
    player_signal = clamp(math.log10(current_players + 1.0) * 6.5, 0.0, 18.0)
    return (
        deal_score * 0.5
        + buy_score * 0.38
        + opportunity_score * 0.24
        + momentum_score * 0.18
        + review_score * 0.12
        + clamp(discount, 0.0, 90.0) * 0.2
        + player_signal
    )


def _all_deals_snapshot_discount_band(row: dict) -> int:
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    if discount >= 70:
        return 0
    if discount >= 50:
        return 1
    if discount >= 35:
        return 2
    if discount >= 20:
        return 3
    if discount >= HOMEPAGE_ALL_DEALS_MIN_DISCOUNT:
        return 4
    return 5


def _build_homepage_all_deals_rows(
    primary_rows: list[dict],
    fallback_rows: list[dict],
    *,
    exposure_counts: dict[str, int],
    limit: int = HOMEPAGE_ALL_DEALS_LIMIT,
    lead_count: int = HOMEPAGE_ALL_DEALS_LEAD_COUNT,
) -> list[dict]:
    bounded_limit = max(1, int(limit))
    bounded_lead_count = max(1, min(int(lead_count), bounded_limit))
    max_exposed_rows = max(4, min(8, bounded_limit // 3))
    candidate_rows = _released_deal_snapshot_rows([*primary_rows, *fallback_rows])
    floor_rows = [row for row in candidate_rows if _is_all_deals_floor_snapshot_row(row)]
    ranked_rows = floor_rows if floor_rows else candidate_rows
    ranked_rows = sorted(
        ranked_rows,
        key=lambda row: (
            _score_all_deals_snapshot_row(row),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
            safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0)),
            -_snapshot_sort_game_id(row),
        ),
        reverse=True,
    )
    ranked_rows = _reorder_ranked_snapshot_rows_for_diversity(
        ranked_rows,
        rail_key="all_deals",
        lead_protect=min(6, bounded_lead_count),
        rotation_window=10,
    )
    if not ranked_rows:
        return []

    selected: list[dict] = []
    seen_keys: set[str] = set()
    exposed_rows_selected = 0

    def _try_add(row: dict, *, allow_exposed: bool) -> bool:
        nonlocal exposed_rows_selected
        row_key = _snapshot_identity_key(row, 0)
        if not row_key or row_key in seen_keys:
            return False
        repeat_count = int(exposure_counts.get(row_key, 0))
        if repeat_count >= 2:
            return False
        if not allow_exposed and repeat_count > 0:
            return False
        if allow_exposed and repeat_count > 0 and exposed_rows_selected >= max_exposed_rows:
            return False
        selected.append(row)
        seen_keys.add(row_key)
        if repeat_count > 0:
            exposed_rows_selected += 1
        exposure_counts[row_key] = repeat_count + 1
        return True

    for row in ranked_rows:
        if _try_add(row, allow_exposed=False) and len(selected) >= bounded_lead_count:
            break

    remaining_rows = [row for row in ranked_rows if _snapshot_identity_key(row, 0) not in seen_keys]
    bands: dict[int, list[dict]] = {band: [] for band in range(6)}
    for row in remaining_rows:
        bands[_all_deals_snapshot_discount_band(row)].append(row)
    for band_rows in bands.values():
        band_rows.sort(
            key=lambda row: (
                _score_all_deals_snapshot_row(row),
                safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
                safe_num(row.get("deal_score"), 0.0),
                -_snapshot_sort_game_id(row),
            ),
            reverse=True,
        )

    while len(selected) < bounded_limit:
        progressed = False
        for band in (0, 1, 2, 3, 4, 5):
            band_rows = bands.get(band, [])
            while band_rows:
                row = band_rows.pop(0)
                if _try_add(row, allow_exposed=False):
                    progressed = True
                    break
            if len(selected) >= bounded_limit:
                break
        if not progressed:
            break

    if len(selected) < bounded_limit:
        for row in ranked_rows:
            if _try_add(row, allow_exposed=True) and len(selected) >= bounded_limit:
                break

    return selected[:bounded_limit]


def _is_exceptional_homepage_repeat_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    discount = safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0))
    deal_score = safe_num(row.get("deal_score"), 0.0)
    buy_score = safe_num(row.get("buy_score"), safe_num(row.get("worth_buying_score"), 0.0))
    opportunity_score = safe_num(row.get("deal_opportunity_score"), 0.0)
    momentum_score = safe_num(row.get("momentum_score"), 0.0)
    historical_status = str(row.get("historical_status") or "").strip().lower()
    historical_elite = historical_status in {"new_historical_low", "matches_historical_low"}

    return bool(
        (discount >= 82 and deal_score >= 90)
        or (buy_score >= 93 and deal_score >= 88)
        or (opportunity_score >= 93 and momentum_score >= 88 and discount >= 40)
        or (historical_elite and discount >= 55 and deal_score >= 84)
    )


def _compose_cross_rail_snapshot_rows(
    primary_rows: list[dict],
    fallback_rows: list[dict],
    *,
    exposure_counts: dict[str, int],
    limit: int,
    uniqueness_window: int,
) -> list[dict]:
    bounded_limit = max(1, int(limit))
    bounded_window = max(1, min(int(uniqueness_window), bounded_limit))
    selected: list[dict] = []
    seen_keys: set[str] = set()
    deferred_repeats: list[dict] = []

    for source_rows in (primary_rows, fallback_rows):
        for idx, row in enumerate(_dedupe_snapshot_rows(source_rows)):
            if not isinstance(row, dict):
                continue
            row_key = _snapshot_identity_key(row, idx)
            if not row_key or row_key in seen_keys:
                continue

            repeat_count = int(exposure_counts.get(row_key, 0))
            if repeat_count > 0:
                if repeat_count >= 2:
                    seen_keys.add(row_key)
                    continue
                deferred_repeats.append(row)
                seen_keys.add(row_key)
                continue

            selected.append(row)
            seen_keys.add(row_key)
            if len(selected) >= bounded_limit:
                break
        if len(selected) >= bounded_limit:
            break

    if len(selected) < bounded_limit:
        for idx, row in enumerate(deferred_repeats):
            row_key = _snapshot_identity_key(row, idx)
            if not row_key:
                continue
            repeat_count = int(exposure_counts.get(row_key, 0))
            if repeat_count >= 2:
                continue
            if len(selected) < bounded_window and len(selected) > 0:
                continue
            selected.append(row)
            if len(selected) >= bounded_limit:
                break

    if len(selected) < bounded_limit and len(selected) < bounded_window:
        for row in deferred_repeats:
            if row in selected:
                continue
            row_key = _snapshot_identity_key(row, 0)
            if not row_key:
                continue
            repeat_count = int(exposure_counts.get(row_key, 0))
            if repeat_count >= 2:
                continue
            selected.append(row)
            if len(selected) >= bounded_limit:
                break

    for idx, row in enumerate(selected):
        if idx >= bounded_window:
            continue
        row_key = _snapshot_identity_key(row, idx)
        if not row_key:
            continue
        exposure_counts[row_key] = int(exposure_counts.get(row_key, 0)) + 1

    return selected


def _diversify_homepage_cross_rails(
    rails: dict[str, list[dict]],
    *,
    fallback_rows: list[dict],
    rail_order: tuple[str, ...],
    limit: int,
    uniqueness_window: int,
) -> tuple[dict[str, list[dict]], dict[str, int]]:
    normalized_limit = max(1, int(limit))
    normalized_window = max(1, int(uniqueness_window))
    fallback_pool = _dedupe_snapshot_rows(fallback_rows)
    exposure_counts: dict[str, int] = {}
    diversified: dict[str, list[dict]] = {}

    for rail_key in rail_order:
        primary_rows = _dedupe_snapshot_rows(rails.get(rail_key, []))
        diversified[rail_key] = _compose_cross_rail_snapshot_rows(
            primary_rows,
            fallback_pool,
            exposure_counts=exposure_counts,
            limit=normalized_limit,
            uniqueness_window=normalized_window,
        )

    for rail_key, rows in rails.items():
        if rail_key in diversified:
            continue
        diversified[rail_key] = _dedupe_snapshot_rows(rows)

    return diversified, exposure_counts


def _allocate_homepage_protected_deal_rails(
    candidate_pool: list[dict],
    limit: int,
) -> tuple[dict[str, list[dict]], set[str]]:
    eligible_pool = _released_deal_snapshot_rows(candidate_pool)
    if not eligible_pool:
        return {
            "deal_opportunities": [],
            "opportunity_radar": [],
            "wait_picks": [],
        }, set()

    bounded_limit = max(1, int(limit))
    ranked_opportunities = sorted(
        eligible_pool,
        key=lambda row: (
            _score_homepage_opportunity_row(row),
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
            -_snapshot_sort_game_id(row),
        ),
        reverse=True,
    )
    ranked_radar = sorted(
        eligible_pool,
        key=lambda row: (
            safe_num(row.get("deal_opportunity_score"), 0.0),
            safe_num(row.get("momentum_score"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("discount_percent"), safe_num(row.get("latest_discount_percent"), 0.0)),
            -_snapshot_sort_game_id(row),
        ),
        reverse=True,
    )
    ranked_wait = sorted(
        [row for row in eligible_pool if _is_wait_candidate_row(row)],
        key=lambda row: (
            safe_num(row.get("predicted_next_discount_percent"), 0.0),
            safe_num(row.get("price_vs_low_ratio"), 0.0),
            safe_num(row.get("deal_score"), 0.0),
            safe_num(row.get("momentum_score"), 0.0),
            -_snapshot_sort_game_id(row),
        ),
        reverse=True,
    )

    used_keys: set[str] = set()
    allocated: dict[str, list[dict]] = {}
    for rail_key, ranked_rows in (
        ("deal_opportunities", ranked_opportunities),
        ("opportunity_radar", ranked_radar),
        ("wait_picks", ranked_wait),
    ):
        diverse_ranked_rows = _reorder_ranked_snapshot_rows_for_diversity(
            ranked_rows,
            rail_key=rail_key,
        )
        allocated[rail_key] = _compose_unique_snapshot_rows(
            _released_deal_snapshot_rows(diverse_ranked_rows),
            eligible_pool,
            used_keys,
            bounded_limit,
        )
    return allocated, used_keys


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
            GameSnapshot.is_released == 1,
        )
        .order_by(GameSnapshot.worth_buying_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    recommended_deals = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
        )
        .order_by(GameSnapshot.recommended_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    deal_ranked = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
        )
        .order_by(GameSnapshot.deal_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    biggest_deals = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
            GameSnapshot.latest_discount_percent.isnot(None),
            GameSnapshot.latest_discount_percent > 0,
        )
        .order_by(GameSnapshot.latest_discount_percent.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_DEAL_CANDIDATE_POOL)
        .all()
    )
    historical_lows = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_historical_low.is_(True),
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
        )
        .order_by(GameSnapshot.deal_score.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    top_reviewed = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
        )
        .order_by(GameSnapshot.review_score.desc(), GameSnapshot.review_count.desc(), GameSnapshot.game_id.asc())
        .limit(HOMEPAGE_RAIL_LIMIT)
        .all()
    )
    top_played = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
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
            GameSnapshot.is_released == 1,
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
            GameSnapshot.is_released == 1,
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
    upcoming_artwork_priority = case((GameSnapshot.banner_url.isnot(None), 1), else_=0)
    upcoming = (
        session.query(GameSnapshot)
        .filter(GameSnapshot.is_upcoming.is_(True))
        .order_by(
            upcoming_artwork_priority.desc(),
            GameSnapshot.upcoming_hot_score.desc(),
            GameSnapshot.release_date.asc(),
            GameSnapshot.game_id.asc(),
        )
        .limit(UPCOMING_LIMIT)
        .all()
    )
    trending_deals = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
            GameSnapshot.latest_discount_percent.isnot(None),
            GameSnapshot.latest_discount_percent > 0,
        )
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
        section_limit=HOMEPAGE_COMPOSITION_RAIL_LIMIT,
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
            if row and not bool(row.is_upcoming) and int(row.is_released or 0) == 1:
                new_historical_lows.append(_snapshot_row_to_dict(row))
    biggest_price_drop_events = (
        session.query(DealEvent)
        .filter(DealEvent.event_type == DEAL_EVENT_PRICE_DROP)
        .order_by(DealEvent.created_at.desc(), DealEvent.id.desc())
        .limit(24)
        .all()
    )
    price_drop_snapshots_by_id: dict[int, GameSnapshot] = {}
    if biggest_price_drop_events:
        price_drop_game_ids = [int(row.game_id) for row in biggest_price_drop_events if row.game_id is not None]
        if price_drop_game_ids:
            price_drop_snapshots_by_id = {
                int(row.game_id): row
                for row in session.query(GameSnapshot).filter(GameSnapshot.game_id.in_(price_drop_game_ids)).all()
            }
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
    upcoming_rows = [_snapshot_row_to_dict(row) for row in upcoming][:HOMEPAGE_RAIL_LIMIT]
    new_historical_lows_rows = _dedupe_snapshot_rows(new_historical_lows)
    all_discounted_snapshot_rows = (
        session.query(GameSnapshot)
        .filter(
            GameSnapshot.is_upcoming.is_(False),
            GameSnapshot.is_released == 1,
            GameSnapshot.latest_discount_percent.isnot(None),
            GameSnapshot.latest_discount_percent > 0,
        )
        .order_by(
            GameSnapshot.deal_score.desc().nullslast(),
            GameSnapshot.latest_discount_percent.desc().nullslast(),
            GameSnapshot.worth_buying_score.desc().nullslast(),
            GameSnapshot.game_id.asc(),
        )
        .limit(HOMEPAGE_ALL_DEALS_CANDIDATE_POOL)
        .all()
    )
    all_discounted_rows = [_snapshot_row_to_dict(row) for row in all_discounted_snapshot_rows]

    canonical_deal_pool = _released_deal_snapshot_rows(
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
    trending_now_rows = _released_deal_snapshot_rows(trending_rows if trending_rows else trending_deals_rows)
    allocated_protected_rails, protected_visible_keys = _allocate_homepage_protected_deal_rails(
        [*canonical_deal_pool, *trending_now_rows, *new_historical_lows_rows],
        HOMEPAGE_COMPOSITION_RAIL_LIMIT,
    )
    deal_opportunities_rows = allocated_protected_rails.get("deal_opportunities", [])
    opportunity_radar_rows = allocated_protected_rails.get("opportunity_radar", [])
    worth_buying_rows = _released_deal_snapshot_rows(worth_buying_now_rows)
    biggest_discounts_rows = _released_deal_snapshot_rows(biggest_deals_rows)
    if not biggest_discounts_rows:
        biggest_discounts_rows = _released_deal_snapshot_rows(deal_ranked_rows)
    wait_picks = allocated_protected_rails.get("wait_picks", [])

    buy_now_candidates = _released_deal_snapshot_rows(
        _build_decision_picks(canonical_deal_pool, "BUY_NOW", limit=HOMEPAGE_COMPOSITION_RAIL_LIMIT)
    )
    buy_now_picks = _compose_unique_snapshot_rows(
        buy_now_candidates,
        _released_deal_snapshot_rows([*worth_buying_rows, *canonical_deal_pool]),
        protected_visible_keys,
        HOMEPAGE_COMPOSITION_RAIL_LIMIT,
    )
    if not buy_now_picks:
        buy_now_picks = _compose_unique_snapshot_rows(
            _released_deal_snapshot_rows(worth_buying_now_rows),
            _released_deal_snapshot_rows(canonical_deal_pool),
            protected_visible_keys,
            HOMEPAGE_COMPOSITION_RAIL_LIMIT,
        )

    deal_ranked_rows = _compose_unique_snapshot_rows(
        _released_deal_snapshot_rows(deal_ranked_rows),
        _released_deal_snapshot_rows([*canonical_deal_pool, *biggest_discounts_rows]),
        set(),
        HOMEPAGE_COMPOSITION_RAIL_LIMIT,
    )
    diversified_cross_rails, cross_rail_exposure_counts = _diversify_homepage_cross_rails(
        {
            "deal_opportunities": deal_opportunities_rows,
            "buy_now_picks": buy_now_picks,
            "biggest_discounts": biggest_discounts_rows,
            "worth_buying_now": worth_buying_rows,
            "trending_now": trending_now_rows,
            "opportunity_radar": opportunity_radar_rows,
            "deal_ranked": deal_ranked_rows,
            "wait_picks": wait_picks,
        },
        fallback_rows=_released_deal_snapshot_rows(
            [
                *canonical_deal_pool,
                *trending_now_rows,
                *new_historical_lows_rows,
                *worth_buying_rows,
                *biggest_discounts_rows,
                *deal_ranked_rows,
                *buy_now_picks,
                *wait_picks,
            ]
        ),
        rail_order=HOMEPAGE_CROSS_RAIL_ORDER,
        limit=HOMEPAGE_RAIL_LIMIT,
        uniqueness_window=HOMEPAGE_CROSS_RAIL_UNIQUENESS_WINDOW,
    )
    deal_opportunities_rows = diversified_cross_rails.get("deal_opportunities", [])
    buy_now_picks = diversified_cross_rails.get("buy_now_picks", [])
    biggest_discounts_rows = diversified_cross_rails.get("biggest_discounts", [])
    worth_buying_rows = diversified_cross_rails.get("worth_buying_now", [])
    trending_now_rows = diversified_cross_rails.get("trending_now", [])
    opportunity_radar_rows = diversified_cross_rails.get("opportunity_radar", [])
    deal_ranked_rows = diversified_cross_rails.get("deal_ranked", [])
    wait_picks = diversified_cross_rails.get("wait_picks", [])
    decision_pool = _released_deal_snapshot_rows(
        [
            *deal_opportunities_rows,
            *buy_now_picks,
            *biggest_discounts_rows,
            *worth_buying_rows,
            *trending_now_rows,
            *opportunity_radar_rows,
            *deal_ranked_rows,
            *wait_picks,
            *canonical_deal_pool,
        ]
    )
    player_surges = _build_player_surges(alert_signals, trending_rows)
    seasonal_summary = _build_cached_seasonal_summary(decision_pool, limit=24)
    all_deals_rows = _build_homepage_all_deals_rows(
        all_discounted_rows,
        _released_deal_snapshot_rows(
            [
                *decision_pool,
                *canonical_deal_pool,
                *deal_ranked_rows,
                *worth_buying_rows,
                *biggest_discounts_rows,
                *buy_now_picks,
                *deal_opportunities_rows,
                *trending_now_rows,
                *opportunity_radar_rows,
                *wait_picks,
                *new_historical_lows_rows,
            ]
        ),
        exposure_counts=dict(cross_rail_exposure_counts),
        limit=HOMEPAGE_ALL_DEALS_LIMIT,
        lead_count=HOMEPAGE_ALL_DEALS_LEAD_COUNT,
    )
    if not all_deals_rows:
        all_deals_rows = _released_deal_snapshot_rows(
            [
                *decision_pool,
                *canonical_deal_pool,
                *deal_ranked_rows,
                *worth_buying_rows,
                *biggest_discounts_rows,
            ]
        )[:HOMEPAGE_ALL_DEALS_LIMIT]
    catalog_seed_rows = all_deals_rows[:HOMEPAGE_ALL_DEALS_LIMIT]
    biggest_price_drops_rows: list[dict] = []
    for row in biggest_price_drop_events:
        game_id = int(row.game_id) if row.game_id is not None else 0
        if game_id <= 0:
            continue
        snapshot = price_drop_snapshots_by_id.get(game_id)
        payload_row = _snapshot_row_to_dict(snapshot) if snapshot is not None else {
            "id": game_id,
            "game_id": game_id,
        }
        payload_row.update(
            {
                "event_type": row.event_type,
                "old_price": row.old_price,
                "new_price": row.new_price,
                "discount_percent": row.discount_percent,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
        )
        biggest_price_drops_rows.append(payload_row)
    digest_now = utcnow()
    digest_window_start = digest_now - datetime.timedelta(hours=24)
    digest_limit = min(12, HOMEPAGE_RAIL_LIMIT)
    digest_sections = {
        "biggest_price_drops": biggest_price_drops_rows[:digest_limit],
        "new_historical_lows": new_historical_lows_rows[:digest_limit],
        "buy_now_opportunities": buy_now_picks[:digest_limit],
        "trending_games": trending_rows[:digest_limit],
        "radar_signals": deal_radar[:digest_limit],
    }
    daily_digest = {
        "personalized": False,
        "window_hours": 24,
        "window_start": digest_window_start.isoformat(),
        "window_end": digest_now.isoformat(),
        "generated_at": digest_now.isoformat(),
        "counts": {key: len(value) for key, value in digest_sections.items()},
        "sections": digest_sections,
        "highlights": [],
    }

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
        "dealRanked": deal_ranked_rows,
        "topDealsToday": deal_ranked_rows or biggest_discounts_rows,
        "biggest_discounts": biggest_discounts_rows,
        "biggestDeals": biggest_discounts_rows,
        "worth_buying_now": worth_buying_rows,
        "worthBuyingNow": worth_buying_rows,
        "trending_now": trending_now_rows,
        "trendingDeals": trending_now_rows,
        "new_historical_lows": new_historical_lows_rows,
        "newHistoricalLows": new_historical_lows_rows,
        "buy_now_picks": buy_now_picks,
        "buyNowPicks": buy_now_picks,
        "wait_picks": wait_picks,
        "waitPicks": wait_picks,
        "deal_opportunities": deal_opportunities_rows,
        "dealOpportunities": deal_opportunities_rows,
        "opportunity_radar": opportunity_radar_rows,
        "opportunityRadar": opportunity_radar_rows,
        "deal_radar": deal_radar,
        "dealRadar": deal_radar,
        "marketRadar": deal_radar,
        "daily_digest": daily_digest,
        "historicalLows": historical_lows_rows,
        "biggestPriceDrops": biggest_price_drops_rows,
        "topReviewed": top_reviewed_rows,
        "topPlayed": top_played_rows,
        "trending": trending_rows,
        "leaderboard": leaderboard_rows,
        "upcoming": upcoming_rows,
        "wishlist": wishlist,
        "watchlist": watchlist,
        "filters": build_dashboard_filters(session),
        "alertSignals": alert_signals,
        "player_surges": player_surges,
        "seasonal_summary": seasonal_summary,
        "all_deals": all_deals_rows,
        "allDeals": all_deals_rows,
        "releasedGames": all_deals_rows,
        "released": all_deals_rows,
        "generated_at": utcnow().isoformat(),
    }

    critical_payload = _build_homepage_critical_payload(payload)
    section_payloads = {
        CACHE_KEY: payload,
        CRITICAL_CACHE_KEY: critical_payload,
        "home:worth_buying": {"items": payload.get("worth_buying_now", []), "generated_at": payload["generated_at"]},
        "home:trending": {"items": payload.get("trending_now", []), "generated_at": payload["generated_at"]},
        "home:historical_lows": {"items": payload.get("historicalLows", []), "generated_at": payload["generated_at"]},
        "home:biggest_price_drops": {"items": payload.get("biggestPriceDrops", []), "generated_at": payload["generated_at"]},
        "home:alerts": {"items": payload.get("alertSignals", []), "generated_at": payload["generated_at"]},
        "home:deal_radar": {"items": payload.get("deal_radar", []), "generated_at": payload["generated_at"]},
        "home:market_radar": {"items": payload.get("deal_radar", []), "generated_at": payload["generated_at"]},
        "home:deal_opportunities": {"items": payload.get("deal_opportunities", []), "generated_at": payload["generated_at"]},
        "home:opportunity_radar": {"items": payload.get("opportunity_radar", []), "generated_at": payload["generated_at"]},
        "home:seasonal_summary": {"items": payload.get("seasonal_summary", {}).get("items", []), **payload.get("seasonal_summary", {}), "generated_at": payload["generated_at"]},
        "home:top_reviewed": {"items": payload.get("topReviewed", []), "generated_at": payload["generated_at"]},
        "home:top_played": {"items": payload.get("topPlayed", []), "generated_at": payload["generated_at"]},
        "home:leaderboard": {"items": payload.get("leaderboard", []), "generated_at": payload["generated_at"]},
        "home:upcoming": {"items": payload.get("upcoming", []), "generated_at": payload["generated_at"]},
        ALL_DEALS_FEED_CACHE_KEY: {"items": all_deals_rows, "total": len(all_deals_rows), "total_pages": 1, "generated_at": payload["generated_at"]},
        "home:catalog_seed": {"items": catalog_seed_rows, "total": len(catalog_seed_rows), "total_pages": 1, "generated_at": payload["generated_at"]},
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
