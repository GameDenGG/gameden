import time
import random
import math
import datetime
from typing import Dict, Any, Optional

from sqlalchemy import case, or_

from config import (
    COLD_REFRESH_MINUTES,
    HOT_PLAYER_THRESHOLD,
    HOT_REFRESH_MINUTES,
    INGESTION_GAMES_PER_RUN,
    INGESTION_GAMES_PER_RUN_LIMIT,
    INGESTION_INCLUDE_ROLLOUT_HOLD,
    INGESTION_MAX_DELAY_SECONDS,
    INGESTION_MIN_DELAY_SECONDS,
    INGESTION_RAW_GAMES_PER_RUN,
    INGESTION_REQUEST_RETRIES,
    INGESTION_ROLLOUT_HOLD_TIER,
    INGESTION_SHARD_INDEX,
    INGESTION_SHARD_TOTAL,
    MEDIUM_PLAYER_THRESHOLD,
    MEDIUM_REFRESH_MINUTES,
    validate_settings,
)
from database.job_status import normalize_counter_triplet
from database.dirty_games import mark_game_dirty
from database.models import (
    Session,
    DealEvent,
    Game,
    GamePrice,
    GameLatestPrice,
    GamePlayerHistory,
    JobStatus,
    PushSubscription,
    UserAlert,
    WishlistItem,
)
from scraper.steam_scraper import get_game_price_data
from scraper.steam_players import get_current_players
from logger_config import setup_logger
from services.push_notifications import send_push_notification

logger = setup_logger("tracker")

validate_settings()

RAW_GAMES_PER_RUN = INGESTION_RAW_GAMES_PER_RUN
TRACK_GAMES_PER_RUN_LIMIT = INGESTION_GAMES_PER_RUN_LIMIT
GAMES_PER_RUN = INGESTION_GAMES_PER_RUN
MIN_DELAY_SECONDS = INGESTION_MIN_DELAY_SECONDS
MAX_DELAY_SECONDS = INGESTION_MAX_DELAY_SECONDS
REQUEST_RETRIES = INGESTION_REQUEST_RETRIES
TRACK_SHARD_TOTAL = INGESTION_SHARD_TOTAL
TRACK_SHARD_INDEX = INGESTION_SHARD_INDEX
ROLLOUT_HOLD_TIER = INGESTION_ROLLOUT_HOLD_TIER
TRACK_INCLUDE_ROLLOUT_HOLD = INGESTION_INCLUDE_ROLLOUT_HOLD

TIER_HOT = "HOT"
TIER_MEDIUM = "MEDIUM"
TIER_COLD = "COLD"
VALID_PRIORITY_TIERS = {TIER_HOT, TIER_MEDIUM, TIER_COLD}

TRACK_HOT_MIN_PLAYERS = HOT_PLAYER_THRESHOLD
TRACK_MEDIUM_MIN_PLAYERS = MEDIUM_PLAYER_THRESHOLD

TRACK_HOT_REFRESH_MINUTES = HOT_REFRESH_MINUTES
TRACK_MEDIUM_REFRESH_MINUTES = MEDIUM_REFRESH_MINUTES
TRACK_COLD_REFRESH_MINUTES = COLD_REFRESH_MINUTES


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def safe_int(value: Optional[Any], default: Optional[int] = 0) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_text(value: Optional[Any], default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def normalize_featured_media(value: Optional[Any]) -> Optional[dict]:
    if not isinstance(value, dict):
        return None

    kind = safe_text(value.get("kind")).lower()
    provider = safe_text(value.get("provider")).lower()
    embed_url = safe_text(value.get("embed_url"))
    if kind not in {"embed", "video"} or provider not in {"steam", "youtube"} or not embed_url:
        return None

    poster_url = safe_text(value.get("poster_url")) or None
    title = safe_text(value.get("title")) or None
    return {
        "kind": kind,
        "provider": provider,
        "embed_url": embed_url,
        "poster_url": poster_url,
        "title": title,
    }


def normalize_priority_tier(value: Optional[Any]) -> Optional[str]:
    tier = safe_text(value).upper()
    return tier if tier in VALID_PRIORITY_TIERS else None


def is_rollout_hold_tier(value: Optional[Any]) -> bool:
    return safe_text(value).upper() == ROLLOUT_HOLD_TIER


def compute_priority_tier(game: Game, observed_players: Optional[int]) -> str:
    if safe_int(game.is_released, default=1) == 0:
        return TIER_COLD

    player_count = safe_int(observed_players, default=None)
    if player_count is None:
        player_count = safe_int(game.last_player_count, default=None)

    if player_count is None:
        existing = normalize_priority_tier(game.priority_tier)
        return existing or TIER_MEDIUM

    if player_count >= TRACK_HOT_MIN_PLAYERS:
        return TIER_HOT
    if player_count >= TRACK_MEDIUM_MIN_PLAYERS:
        return TIER_MEDIUM
    return TIER_COLD


def refresh_interval_for_tier(tier: str) -> datetime.timedelta:
    normalized = normalize_priority_tier(tier) or TIER_MEDIUM
    if normalized == TIER_HOT:
        return datetime.timedelta(minutes=TRACK_HOT_REFRESH_MINUTES)
    if normalized == TIER_COLD:
        return datetime.timedelta(minutes=TRACK_COLD_REFRESH_MINUTES)
    return datetime.timedelta(minutes=TRACK_MEDIUM_REFRESH_MINUTES)


def compute_popularity_score(game: Game) -> float:
    players = max(0.0, float(safe_int(game.last_player_count, default=0) or 0))
    review_count = max(0.0, float(safe_int(game.review_total_count, default=0) or 0))
    priority_boost = max(0.0, float(safe_int(game.priority, default=0) or 0))

    players_component = min(72.0, math.log10(players + 1.0) * 18.0)
    reviews_component = min(22.0, math.log10(review_count + 1.0) * 6.0)
    priority_component = min(8.0, priority_boost * 2.0)
    return round(players_component + reviews_component + priority_component, 3)


def apply_ingestion_schedule(game: Game, observed_players: Optional[int]) -> None:
    now = utc_now()
    tier = compute_priority_tier(game, observed_players)
    game.priority_tier = tier
    if observed_players is not None:
        game.last_player_count = int(observed_players)
    game.popularity_score = compute_popularity_score(game)
    game.next_refresh_at = now + refresh_interval_for_tier(tier)
    game.last_checked_at = now


def tier_rank_expression():
    return case(
        (Game.priority_tier == TIER_HOT, 3),
        (Game.priority_tier == TIER_MEDIUM, 2),
        (Game.priority_tier == TIER_COLD, 1),
        else_=case(
            (Game.last_player_count >= TRACK_HOT_MIN_PLAYERS, 3),
            (Game.last_player_count >= TRACK_MEDIUM_MIN_PLAYERS, 2),
            else_=1,
        ),
    )


def call_with_retry(fn, *args, retries: int = REQUEST_RETRIES, base_sleep: float = 0.35, **kwargs):
    attempts = max(0, int(retries)) + 1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs), attempt
        except Exception as exc:  # scraper functions usually swallow errors; keep this for defensive safety.
            last_error = exc
            if attempt >= attempts:
                break
            delay = base_sleep * attempt + random.uniform(0.0, 0.25)
            logger.warning("Retrying %s attempt=%s/%s delay=%.2fs", getattr(fn, "__name__", "call"), attempt, attempts, delay)
            time.sleep(delay)
    if last_error:
        raise last_error
    return None, attempts


def get_price_data_with_retry(store_url: str, retries: int = REQUEST_RETRIES):
    attempts = max(0, int(retries)) + 1
    for attempt in range(1, attempts + 1):
        data = get_game_price_data(store_url)
        if data is not None:
            return data, attempt
        if attempt < attempts:
            delay = 0.4 * attempt + random.uniform(0.0, 0.2)
            logger.warning("Steam appdetails empty response. retry=%s/%s delay=%.2fs", attempt, attempts, delay)
            time.sleep(delay)
    return None, attempts


def get_players_with_retry(store_url: str, retries: int = REQUEST_RETRIES):
    attempts = max(0, int(retries)) + 1
    for attempt in range(1, attempts + 1):
        players = get_current_players(store_url)
        if players is not None:
            return players, attempt
        if attempt < attempts:
            delay = 0.25 * attempt + random.uniform(0.0, 0.2)
            logger.warning("Steam players empty response. retry=%s/%s delay=%.2fs", attempt, attempts, delay)
            time.sleep(delay)
    return None, attempts


def update_ingestion_job_status(
    session,
    *,
    started: bool = False,
    completed_success: bool = False,
    error_message: str | None = None,
    duration_ms: int | None = None,
    items_total: int | None = None,
    items_success: int | None = None,
    items_failed: int | None = None,
) -> None:
    now = utc_now()
    row = session.get(JobStatus, "price_ingestion")
    if row is None:
        row = JobStatus(job_name="price_ingestion")
        session.add(row)
        session.flush()

    if started:
        row.last_started_at = now
        row.last_error = None
    if completed_success:
        row.last_completed_at = now
        row.last_success_at = now
        row.last_error = None
        row.last_duration_ms = int(duration_ms) if duration_ms is not None else None
        if items_total is not None or items_success is not None or items_failed is not None:
            normalized_total, normalized_success, normalized_failed = normalize_counter_triplet(
                items_total,
                items_success,
                items_failed,
            )
            row.last_items_total = normalized_total
            row.last_items_success = normalized_success
            row.last_items_failed = normalized_failed
    if error_message:
        row.last_completed_at = now
        row.last_error = str(error_message)[:2000]
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


def get_games_for_run(session) -> list[Game]:
    now = utc_now()
    query = session.query(Game)
    if TRACK_SHARD_TOTAL > 1:
        shard_index = TRACK_SHARD_INDEX % TRACK_SHARD_TOTAL
        # Safe modulo sharding for horizontally scaling ingestion workers.
        query = query.filter((Game.id % TRACK_SHARD_TOTAL) == shard_index)

    query = query.filter(
        or_(
            Game.next_refresh_at.is_(None),
            Game.next_refresh_at <= now,
        )
    )
    if not TRACK_INCLUDE_ROLLOUT_HOLD:
        query = query.filter(
            or_(
                Game.priority_tier.is_(None),
                Game.priority_tier != ROLLOUT_HOLD_TIER,
            )
        )

    tier_rank = tier_rank_expression()
    return (
        query.order_by(
            Game.next_refresh_at.asc().nullsfirst(),
            tier_rank.desc(),
            Game.popularity_score.desc().nullslast(),
            Game.last_checked_at.asc().nullsfirst(),
            Game.priority.desc(),
            Game.name.asc(),
        )
        .limit(GAMES_PER_RUN)
        .all()
    )


def apply_game_updates(game: Game, price_data: Dict[str, Any]) -> Dict[str, bool]:
    changes = {
        "release_changed": False,
        "metadata_changed": False,
    }

    new_is_released = safe_int(price_data.get("is_released"), default=1)
    new_release_date_text = safe_text(price_data.get("release_date_text"))
    new_genres = safe_text(price_data.get("genres"))
    new_tags = safe_text(price_data.get("tags"))
    new_platforms = safe_text(price_data.get("platforms"))
    new_review_score = safe_int(price_data.get("review_score"), default=None)
    new_review_score_label = safe_text(price_data.get("review_score_label"))
    new_review_total_count = safe_int(price_data.get("review_total_count"), default=None)
    new_developer = safe_text(price_data.get("developer"))
    new_publisher = safe_text(price_data.get("publisher"))
    new_featured_media = normalize_featured_media(price_data.get("featured_media"))

    if game.is_released != new_is_released:
        game.is_released = new_is_released
        changes["release_changed"] = True

    if (game.release_date_text or "") != new_release_date_text:
        game.release_date_text = new_release_date_text
        changes["release_changed"] = True

    if (game.genres or "") != new_genres:
        game.genres = new_genres or None
        changes["metadata_changed"] = True

    if (game.tags or "") != new_tags:
        game.tags = new_tags or None
        changes["metadata_changed"] = True

    if (game.platforms or "") != new_platforms:
        game.platforms = new_platforms or None
        changes["metadata_changed"] = True

    if game.review_score != new_review_score:
        game.review_score = new_review_score
        changes["metadata_changed"] = True

    if (game.review_score_label or "") != new_review_score_label:
        game.review_score_label = new_review_score_label or None
        changes["metadata_changed"] = True

    if game.review_total_count != new_review_total_count:
        game.review_total_count = new_review_total_count
        changes["metadata_changed"] = True

    if (game.developer or "") != new_developer:
        game.developer = new_developer or None
        changes["metadata_changed"] = True

    if (game.publisher or "") != new_publisher:
        game.publisher = new_publisher or None
        changes["metadata_changed"] = True

    if game.featured_media != new_featured_media:
        game.featured_media = new_featured_media
        changes["metadata_changed"] = True

    return changes


def create_price_snapshot(
    game: Game,
    price_data: Dict[str, Any],
    current_players: Optional[int],
) -> GamePrice:
    return GamePrice(
        game_id=game.id,
        game_name=game.name,
        price=price_data["price"],
        original_price=price_data.get("original_price"),
        discount_percent=price_data.get("discount_percent"),
        current_players=current_players,
        store_url=game.store_url,
        recorded_at=utc_now(),
    )


def should_save_price_snapshot(price_data: Dict[str, Any]) -> bool:
    return price_data.get("price") is not None


def log_run_summary(summary: Dict[str, int], started_at: datetime.datetime) -> None:
    duration_seconds = round((utc_now() - started_at).total_seconds(), 2)
    games_per_second = round((summary["updated"] / duration_seconds), 2) if duration_seconds > 0 else 0.0

    logger.info(
        (
            "Tracking summary | selected=%s checked=%s updated=%s "
            "release_updates=%s metadata_updates=%s snapshots_saved=%s "
            "no_data=%s no_price=%s player_checks=%s retries=%s failed=%s duration_seconds=%s games_per_second=%s"
        ),
        summary["selected"],
        summary["checked"],
        summary["updated"],
        summary["release_updates"],
        summary["metadata_updates"],
        summary["snapshots_saved"],
        summary["no_data"],
        summary["no_price"],
        summary["player_checks"],
        summary["retries"],
        summary["failed"],
        duration_seconds,
        games_per_second,
    )


def track_all_games() -> None:
    started_at = utc_now()
    if TRACK_GAMES_PER_RUN_LIMIT > 0 and RAW_GAMES_PER_RUN > TRACK_GAMES_PER_RUN_LIMIT:
        logger.warning(
            "TRACK_GAMES_PER_RUN=%s exceeds TRACK_GAMES_PER_RUN_LIMIT=%s. Using %s.",
            RAW_GAMES_PER_RUN,
            TRACK_GAMES_PER_RUN_LIMIT,
            GAMES_PER_RUN,
        )
    logger.info(
        (
            "Starting game tracking job. games_per_run=%s raw_games_per_run=%s "
            "games_per_run_limit=%s min_delay=%.2f max_delay=%.2f shard_total=%s shard_index=%s "
            "hot_players>=%s medium_players>=%s intervals[min]=%s/%s/%s "
            "rollout_hold_tier=%s include_rollout_hold=%s"
        ),
        GAMES_PER_RUN,
        RAW_GAMES_PER_RUN,
        TRACK_GAMES_PER_RUN_LIMIT,
        MIN_DELAY_SECONDS,
        MAX_DELAY_SECONDS,
        TRACK_SHARD_TOTAL,
        TRACK_SHARD_INDEX,
        TRACK_HOT_MIN_PLAYERS,
        TRACK_MEDIUM_MIN_PLAYERS,
        TRACK_HOT_REFRESH_MINUTES,
        TRACK_MEDIUM_REFRESH_MINUTES,
        TRACK_COLD_REFRESH_MINUTES,
        ROLLOUT_HOLD_TIER,
        TRACK_INCLUDE_ROLLOUT_HOLD,
    )

    session = Session()
    summary = {
        "selected": 0,
        "checked": 0,
        "updated": 0,
        "release_updates": 0,
        "metadata_updates": 0,
        "snapshots_saved": 0,
        "no_data": 0,
        "no_price": 0,
        "player_checks": 0,
        "retries": 0,
        "failed": 0,
    }

    try:
        update_ingestion_job_status(session, started=True)
        session.commit()

        if GAMES_PER_RUN <= 0:
            logger.warning("GAMES_PER_RUN is %s. Nothing to do.", GAMES_PER_RUN)
            return

        if MIN_DELAY_SECONDS < 0 or MAX_DELAY_SECONDS < 0:
            logger.warning(
                "Negative delay detected. Resetting delays to defaults: %.2f / %.2f",
                INGESTION_MIN_DELAY_SECONDS,
                INGESTION_MAX_DELAY_SECONDS,
            )
            min_delay = INGESTION_MIN_DELAY_SECONDS
            max_delay = INGESTION_MAX_DELAY_SECONDS
        else:
            min_delay = MIN_DELAY_SECONDS
            max_delay = MAX_DELAY_SECONDS

        if min_delay > max_delay:
            logger.warning(
                "MIN_DELAY_SECONDS (%.2f) > MAX_DELAY_SECONDS (%.2f). Swapping values.",
                min_delay,
                max_delay,
            )
            min_delay, max_delay = max_delay, min_delay

        games = get_games_for_run(session)
        summary["selected"] = len(games)

        tier_mix = {TIER_HOT: 0, TIER_MEDIUM: 0, TIER_COLD: 0}
        for game in games:
            tier = normalize_priority_tier(game.priority_tier) or compute_priority_tier(game, observed_players=None)
            tier_mix[tier] = tier_mix.get(tier, 0) + 1

        logger.info(
            "Selected %s games to track this run. tier_mix=%s",
            len(games),
            tier_mix,
        )

        for index, game in enumerate(games, start=1):
            summary["checked"] += 1
            logger.info("Checking game %s/%s: %s", index, len(games), game.name)

            try:
                price_data, price_attempts = get_price_data_with_retry(game.store_url)
                summary["retries"] += max(0, int(price_attempts) - 1)

                if price_data is None:
                    summary["no_data"] += 1
                    logger.warning("No usable Steam data found for %s", game.name)
                    apply_ingestion_schedule(game, observed_players=None)
                    session.commit()
                    continue

                change_flags = apply_game_updates(game, price_data)

                if change_flags["release_changed"]:
                    summary["release_updates"] += 1

                if change_flags["metadata_changed"]:
                    summary["metadata_updates"] += 1

                current_players = None
                if game.is_released:
                    summary["player_checks"] += 1
                    current_players, player_attempts = get_players_with_retry(game.store_url)
                    summary["retries"] += max(0, int(player_attempts) - 1)

                logger.info("Parsed Steam data for %s: %s", game.name, price_data)
                logger.info("Current players for %s: %s", game.name, current_players)

                if current_players is not None:
                    previous_player_row = (
                        session.query(GamePlayerHistory)
                        .filter(GamePlayerHistory.game_id == game.id)
                        .order_by(GamePlayerHistory.recorded_at.desc(), GamePlayerHistory.id.desc())
                        .first()
                    )
                    if (
                        previous_player_row
                        and previous_player_row.current_players is not None
                        and previous_player_row.current_players > 0
                        and current_players > previous_player_row.current_players * 1.5
                    ):
                        dedupe_key = f"player_spike:{game.id}:{int(utc_now().timestamp() // 3600)}"
                        existing_event = session.query(DealEvent.id).filter(DealEvent.event_dedupe_key == dedupe_key).first()
                        if existing_event is None:
                            session.add(
                                DealEvent(
                                    game_id=game.id,
                                    event_type="PLAYER_SPIKE",
                                    old_price=None,
                                    new_price=None,
                                    discount_percent=None,
                                    event_dedupe_key=dedupe_key,
                                    event_reason_summary="Ingestion player spike signal",
                                    metadata_json={"current_players": int(current_players)},
                                )
                            )
                        session.flush()
                        if existing_event is None:
                            user_rows = (
                                session.query(WishlistItem.user_id)
                                .filter(WishlistItem.game_id == game.id, WishlistItem.user_id.isnot(None))
                                .distinct()
                                .all()
                            )
                            for (user_id,) in user_rows:
                                user_id_text = str(user_id)
                                recent_alert = (
                                    session.query(UserAlert.id)
                                    .filter(
                                        UserAlert.user_id == user_id_text,
                                        UserAlert.game_id == game.id,
                                        UserAlert.alert_type == "PLAYER_SPIKE",
                                        UserAlert.created_at >= utc_now() - datetime.timedelta(hours=6),
                                    )
                                    .first()
                                )
                                if recent_alert:
                                    continue
                                session.add(
                                    UserAlert(
                                        user_id=user_id_text,
                                        game_id=game.id,
                                        alert_type="PLAYER_SPIKE",
                                        price=None,
                                        discount_percent=None,
                                    )
                                )
                                subscriptions = (
                                    session.query(PushSubscription)
                                    .filter(PushSubscription.user_id == user_id_text)
                                    .all()
                                )
                                for sub in subscriptions:
                                    send_push_notification(
                                        {
                                            "endpoint": sub.endpoint,
                                            "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                                        },
                                        {
                                            "title": "Trending Game!",
                                            "body": f"{game.name} player activity is spiking",
                                            "url": f"/games/{game.appid}" if game.appid else f"/games/{game.id}",
                                        },
                                    )
                    session.add(
                        GamePlayerHistory(
                            game_id=game.id,
                            current_players=int(current_players),
                        )
                    )
                    mark_game_dirty(session, game.id, reason="player_history_update")

                if should_save_price_snapshot(price_data):
                    snapshot = create_price_snapshot(game, price_data, current_players)
                    session.add(snapshot)

                    latest_row = (
                        session.query(GameLatestPrice)
                        .filter(GameLatestPrice.game_id == game.id)
                        .first()
                    )

                    if latest_row:
                        latest_row.game_name = game.name
                        latest_row.price = price_data["price"]
                        latest_row.original_price = price_data.get("original_price")
                        latest_row.discount_percent = price_data.get("discount_percent")
                        latest_row.current_players = current_players
                        latest_row.store_url = game.store_url
                        latest_row.timestamp = snapshot.timestamp
                    else:
                        latest_row = GameLatestPrice(
                            game_id=game.id,
                            game_name=game.name,
                            price=price_data["price"],
                            original_price=price_data.get("original_price"),
                            discount_percent=price_data.get("discount_percent"),
                            current_players=current_players,
                            store_url=game.store_url,
                            timestamp=snapshot.timestamp,
                        )
                        session.add(latest_row)

                    summary["snapshots_saved"] += 1
                    logger.info("Saved price record for %s", game.name)
                    mark_game_dirty(session, game.id, reason="price_snapshot_update")
                else:
                    summary["no_price"] += 1
                    logger.info(
                        "Skipping price snapshot for unreleased/no-price game %s",
                        game.name,
                    )
                    if change_flags["release_changed"] or change_flags["metadata_changed"]:
                        mark_game_dirty(session, game.id, reason="metadata_or_release_update")

                apply_ingestion_schedule(game, observed_players=current_players)
                summary["updated"] += 1
                session.commit()

            except Exception:
                summary["failed"] += 1
                session.rollback()
                logger.exception("Failed while tracking %s", game.name)

                try:
                    apply_ingestion_schedule(game, observed_players=None)
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Failed updating last_checked_at for %s", game.name)

            if index < len(games):
                delay = random.uniform(min_delay, max_delay)
                logger.info("Sleeping %.2f seconds before next game.", delay)
                time.sleep(delay)

        logger.info("Tracking job completed.")
        duration_ms = int((utc_now() - started_at).total_seconds() * 1000)
        items_total = int(summary["selected"])
        items_success = max(0, min(int(summary["updated"]), items_total))
        # Keep persisted counters internally consistent for operational readiness checks.
        items_failed = max(0, items_total - items_success)
        update_ingestion_job_status(
            session,
            completed_success=True,
            duration_ms=duration_ms,
            items_total=items_total,
            items_success=items_success,
            items_failed=items_failed,
        )
        session.commit()

    except Exception:
        session.rollback()
        try:
            update_ingestion_job_status(session, error_message="price_ingestion_failed")
            session.commit()
        except Exception:
            session.rollback()
        logger.exception("Tracking job failed. Rolled back database session.")
        raise

    finally:
        try:
            log_run_summary(summary, started_at)
        finally:
            session.close()
            logger.info("Database session closed.")


if __name__ == "__main__":
    track_all_games()
