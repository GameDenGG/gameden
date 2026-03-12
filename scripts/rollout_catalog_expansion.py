from __future__ import annotations

import argparse
import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
import sys
from typing import Iterable

from sqlalchemy import case, func

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database import direct_engine
from database.dirty_games import mark_games_dirty
from database.models import DirtyGame, Game, Session
from database.schema_guard import validate_scale_schema
from scripts.seed_steam_games import build_seed_rows, fetch_steam_applist, insert_missing_games_and_queue_dirty


DEFAULT_HOLD_TIER = "ROLLOUT_HOLD"
DEFAULT_ACTIVE_TIER = "MEDIUM"
DEFAULT_HOLD_UNTIL = datetime.datetime(2100, 1, 1, tzinfo=datetime.timezone.utc)
DEFAULT_ACTIVATION_SPREAD_MINUTES = 240
CHUNK_SIZE = 1000

PHASE_TARGETS: dict[str, int] = {
    "baseline_10k": 10_000,
    "phase1_25k": 25_000,
    "phase2_50k": 50_000,
}
PHASE_SEED_LIMITS: dict[str, int] = {
    "baseline_10k": 10_000,
    "phase1_25k": 25_000,
    "phase2_50k": 50_000,
}

PHASE_RECOMMENDED_ENV: dict[str, dict[str, str]] = {
    "baseline_10k": {
        "TRACK_GAMES_PER_RUN": "600",
        "TRACK_SHARD_TOTAL": "1",
        "SNAPSHOT_BATCH_SIZE": "200",
        "HOMEPAGE_DEAL_CANDIDATE_POOL": "384",
    },
    "phase1_25k": {
        "TRACK_GAMES_PER_RUN": "450",
        "TRACK_SHARD_TOTAL": "1",
        "SNAPSHOT_BATCH_SIZE": "250",
        "HOMEPAGE_DEAL_CANDIDATE_POOL": "512",
    },
    "phase2_50k": {
        "TRACK_GAMES_PER_RUN": "350",
        "TRACK_SHARD_TOTAL": "1",
        "SNAPSHOT_BATCH_SIZE": "300",
        "HOMEPAGE_DEAL_CANDIDATE_POOL": "640",
    },
}


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def normalize_tier(value: str | None, default: str = DEFAULT_HOLD_TIER) -> str:
    normalized = str(value or "").strip().upper()
    return normalized or default


def parse_hold_until(value: str | None) -> datetime.datetime:
    if not value:
        return DEFAULT_HOLD_UNTIL
    parsed = datetime.datetime.fromisoformat(value.strip())
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def chunked(items: list[int], size: int = CHUNK_SIZE) -> Iterable[list[int]]:
    step = max(1, int(size))
    for idx in range(0, len(items), step):
        yield items[idx : idx + step]


def split_even(items: list[int], bucket_count: int) -> list[list[int]]:
    if not items:
        return []
    count = max(1, int(bucket_count))
    buckets: list[list[int]] = [[] for _ in range(count)]
    for idx, item in enumerate(items):
        buckets[idx % count].append(item)
    return [bucket for bucket in buckets if bucket]


@dataclass
class RolloutStatus:
    total_games: int
    tracked_games: int
    held_games: int
    released_games: int
    upcoming_games: int
    dirty_games_total: int
    dirty_games_tracked: int
    dirty_games_held: int
    dirty_games_eligible_now: int
    hold_tier: str

    def to_dict(self) -> dict:
        return asdict(self)


def _is_hold_filter(hold_tier: str):
    return func.upper(func.coalesce(Game.priority_tier, "")) == hold_tier


def fetch_rollout_status(session, hold_tier: str) -> RolloutStatus:
    hold_filter = _is_hold_filter(hold_tier)
    total_games = int(session.query(func.count(Game.id)).scalar() or 0)
    held_games = int(session.query(func.count(Game.id)).filter(hold_filter).scalar() or 0)
    tracked_games = max(0, total_games - held_games)
    released_games = int(session.query(func.count(Game.id)).filter(Game.is_released == 1).scalar() or 0)
    upcoming_games = max(0, total_games - released_games)

    dirty_games_total = int(session.query(func.count(DirtyGame.game_id)).scalar() or 0)
    dirty_games_held = int(
        session.query(func.count(DirtyGame.game_id))
        .join(Game, Game.id == DirtyGame.game_id)
        .filter(hold_filter)
        .scalar()
        or 0
    )
    dirty_games_tracked = max(0, dirty_games_total - dirty_games_held)
    dirty_games_eligible_now = int(
        session.query(func.count(DirtyGame.game_id))
        .filter((DirtyGame.next_attempt_at.is_(None)) | (DirtyGame.next_attempt_at <= utc_now()))
        .scalar()
        or 0
    )

    return RolloutStatus(
        total_games=total_games,
        tracked_games=tracked_games,
        held_games=held_games,
        released_games=released_games,
        upcoming_games=upcoming_games,
        dirty_games_total=dirty_games_total,
        dirty_games_tracked=dirty_games_tracked,
        dirty_games_held=dirty_games_held,
        dirty_games_eligible_now=dirty_games_eligible_now,
        hold_tier=hold_tier,
    )


def ranked_game_ids(session) -> list[int]:
    metadata_completeness = (
        case((Game.review_score.isnot(None), 1), else_=0)
        + case((Game.review_total_count.isnot(None), 1), else_=0)
        + case((Game.developer.isnot(None), 1), else_=0)
        + case((Game.publisher.isnot(None), 1), else_=0)
        + case((Game.release_date.isnot(None), 1), else_=0)
    )
    rows = (
        session.query(Game.id)
        .order_by(
            case((Game.is_released == 1, 1), else_=0).desc(),
            metadata_completeness.desc(),
            func.coalesce(Game.popularity_score, 0.0).desc(),
            func.coalesce(Game.last_player_count, 0).desc(),
            func.coalesce(Game.priority, 0).desc(),
            Game.id.asc(),
        )
        .all()
    )
    return [int(row[0]) for row in rows]


def fetch_active_and_held_sets(session, hold_tier: str) -> tuple[set[int], set[int]]:
    rows = session.query(Game.id, Game.priority_tier).all()
    active_ids: set[int] = set()
    held_ids: set[int] = set()
    for game_id, priority_tier in rows:
        game_id_int = int(game_id)
        if normalize_tier(priority_tier, default="") == hold_tier:
            held_ids.add(game_id_int)
        else:
            active_ids.add(game_id_int)
    return active_ids, held_ids


def _resolve_target(total_games: int, target_tracked: int | None, phase: str | None) -> tuple[int, str]:
    if phase:
        resolved = int(PHASE_TARGETS[phase])
        return min(total_games, max(0, resolved)), phase
    if target_tracked is None:
        raise ValueError("Either --target-tracked or --phase is required.")
    resolved = int(target_tracked)
    return min(total_games, max(0, resolved)), f"target_{resolved}"


def _recommended_profile(phase_or_target: str) -> dict[str, str] | None:
    return PHASE_RECOMMENDED_ENV.get(phase_or_target)


def run_catalog_seed(
    *,
    target_limit: int,
    hold_new_games: bool,
    hold_tier: str,
    hold_until: datetime.datetime,
    queue_held_games: bool,
) -> dict:
    apps = fetch_steam_applist(limit=target_limit)
    seed_rows = build_seed_rows(apps, limit=target_limit)
    inserted_count, queued_count = insert_missing_games_and_queue_dirty(
        seed_rows,
        hold_new_games=hold_new_games,
        hold_tier=hold_tier,
        hold_until=hold_until,
        queue_held_games=queue_held_games,
    )
    return {
        "catalog_fetch_count": len(apps),
        "seed_candidate_count": len(seed_rows),
        "inserted_new_games": int(inserted_count),
        "queued_dirty_games": int(queued_count),
        "hold_new_games": bool(hold_new_games),
        "queue_held_games": bool(queue_held_games),
        "seed_limit": int(target_limit),
    }


def apply_rollout_target(
    session,
    *,
    hold_tier: str,
    active_tier: str,
    target_tracked: int,
    hold_until: datetime.datetime,
    activation_spread_minutes: int,
    enqueue_activated: bool,
    dry_run: bool,
) -> dict:
    ranked_ids = ranked_game_ids(session)
    active_ids, _ = fetch_active_and_held_sets(session, hold_tier)

    target_ids = set(ranked_ids[:target_tracked])
    to_activate = sorted(target_ids - active_ids)
    to_hold = sorted(active_ids - target_ids)
    unchanged = max(0, len(target_ids) - len(to_activate))

    summary = {
        "target_tracked": int(target_tracked),
        "catalog_total": len(ranked_ids),
        "activate_count": len(to_activate),
        "hold_count": len(to_hold),
        "unchanged_count": unchanged,
        "enqueue_activated": bool(enqueue_activated),
        "dry_run": bool(dry_run),
    }

    if dry_run:
        return summary

    for chunk in chunked(to_hold):
        session.query(Game).filter(Game.id.in_(chunk)).update(
            {
                Game.priority_tier: hold_tier,
                Game.next_refresh_at: hold_until,
            },
            synchronize_session=False,
        )

    if to_activate:
        spread_minutes = max(0, int(activation_spread_minutes))
        if spread_minutes == 0:
            buckets = [to_activate]
        else:
            bucket_count = min(max(1, spread_minutes // 15), len(to_activate))
            buckets = split_even(to_activate, bucket_count)

        now = utc_now()
        for idx, bucket in enumerate(buckets):
            if len(buckets) <= 1:
                scheduled_refresh = now
            else:
                offset_ratio = idx / max(1, len(buckets) - 1)
                scheduled_refresh = now + datetime.timedelta(minutes=int(round(spread_minutes * offset_ratio)))
            for chunk in chunked(bucket):
                session.query(Game).filter(Game.id.in_(chunk)).update(
                    {
                        Game.priority_tier: active_tier,
                        Game.next_refresh_at: scheduled_refresh,
                    },
                    synchronize_session=False,
                )

    removed_dirty = 0
    for chunk in chunked(to_hold):
        removed_dirty += int(
            session.query(DirtyGame).filter(DirtyGame.game_id.in_(chunk)).delete(synchronize_session=False)
        )

    if enqueue_activated and to_activate:
        mark_games_dirty(session, to_activate, reason="rollout_activation")

    session.commit()
    summary["removed_dirty_rows_for_held_games"] = int(removed_dirty)
    return summary


def print_status(status: RolloutStatus) -> None:
    print("=== Catalog Rollout Status ===")
    print(f"hold_tier={status.hold_tier}")
    print(f"catalog_total={status.total_games}")
    print(f"tracked_games={status.tracked_games}")
    print(f"held_games={status.held_games}")
    print(f"released_games={status.released_games}")
    print(f"upcoming_games={status.upcoming_games}")
    print(f"dirty_games_total={status.dirty_games_total}")
    print(f"dirty_games_tracked={status.dirty_games_tracked}")
    print(f"dirty_games_held={status.dirty_games_held}")
    print(f"dirty_games_eligible_now={status.dirty_games_eligible_now}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply staged tracked-catalog rollout controls (10k -> 25k -> 50k)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Show tracked vs held catalog rollout status.")
    status_parser.add_argument("--hold-tier", default=DEFAULT_HOLD_TIER)

    apply_parser = subparsers.add_parser("apply", help="Apply a tracked-catalog target safely.")
    apply_parser.add_argument("--target-tracked", type=int, default=None)
    apply_parser.add_argument("--phase", choices=sorted(PHASE_TARGETS.keys()), default=None)
    apply_parser.add_argument("--hold-tier", default=DEFAULT_HOLD_TIER)
    apply_parser.add_argument("--active-tier", default=DEFAULT_ACTIVE_TIER)
    apply_parser.add_argument("--hold-until", default=DEFAULT_HOLD_UNTIL.isoformat())
    apply_parser.add_argument(
        "--activation-spread-minutes",
        type=int,
        default=DEFAULT_ACTIVATION_SPREAD_MINUTES,
        help="Spread activation refresh scheduling across this many minutes to avoid burst load.",
    )
    apply_parser.add_argument(
        "--enqueue-activated",
        action="store_true",
        help="Also enqueue newly activated games into dirty_games immediately.",
    )
    apply_parser.add_argument("--dry-run", action="store_true")

    expand_parser = subparsers.add_parser(
        "expand",
        help="Fast safe phase command: seed catalog to target then apply staged activation.",
    )
    expand_parser.add_argument("--target-tracked", type=int, default=None)
    expand_parser.add_argument("--phase", choices=sorted(PHASE_TARGETS.keys()), default=None)
    expand_parser.add_argument(
        "--seed-limit",
        type=int,
        default=0,
        help="Steam catalog seed target. Default resolves to the selected phase/target.",
    )
    expand_parser.add_argument("--hold-tier", default=DEFAULT_HOLD_TIER)
    expand_parser.add_argument("--active-tier", default=DEFAULT_ACTIVE_TIER)
    expand_parser.add_argument("--hold-until", default=DEFAULT_HOLD_UNTIL.isoformat())
    expand_parser.add_argument(
        "--activation-spread-minutes",
        type=int,
        default=DEFAULT_ACTIVATION_SPREAD_MINUTES,
        help="Spread activation refresh scheduling across this many minutes to avoid burst load.",
    )
    expand_parser.add_argument(
        "--queue-held-games",
        action="store_true",
        help="Also enqueue held seeded games into dirty_games (normally disabled).",
    )
    expand_parser.add_argument(
        "--skip-seed",
        action="store_true",
        help="Skip Steam app-list seeding and only apply activation target.",
    )
    expand_parser.add_argument(
        "--dry-run-rollout",
        action="store_true",
        help="Run activation apply as dry-run after seeding.",
    )

    args = parser.parse_args()
    hold_tier = normalize_tier(getattr(args, "hold_tier", None), default=DEFAULT_HOLD_TIER)

    schema_report = validate_scale_schema(direct_engine)
    if not schema_report.is_ready:
        print("Schema readiness check failed. Run: python setup_database.py")
        print(schema_report.to_dict())
        return 1

    session = Session()
    try:
        if args.command == "status":
            status = fetch_rollout_status(session, hold_tier=hold_tier)
            print_status(status)
            return 0

        status_before = fetch_rollout_status(session, hold_tier=hold_tier)
        seed_summary: dict | None = None
        if args.command == "expand":
            target_hint, target_label_hint = _resolve_target(
                total_games=max(status_before.total_games, PHASE_TARGETS.get(args.phase, 0)),
                target_tracked=args.target_tracked,
                phase=args.phase,
            )
            default_seed_limit = PHASE_SEED_LIMITS.get(target_label_hint, target_hint)
            resolved_seed_limit = int(args.seed_limit) if int(args.seed_limit or 0) > 0 else int(default_seed_limit)
            if not args.skip_seed:
                seed_summary = run_catalog_seed(
                    target_limit=resolved_seed_limit,
                    hold_new_games=True,
                    hold_tier=hold_tier,
                    hold_until=parse_hold_until(args.hold_until),
                    queue_held_games=bool(args.queue_held_games),
                )
            session.expire_all()

        status_before_apply = fetch_rollout_status(session, hold_tier=hold_tier)
        target_tracked, target_label = _resolve_target(
            total_games=status_before_apply.total_games,
            target_tracked=args.target_tracked,
            phase=args.phase,
        )
        summary = apply_rollout_target(
            session,
            hold_tier=hold_tier,
            active_tier=normalize_tier(args.active_tier, default=DEFAULT_ACTIVE_TIER),
            target_tracked=target_tracked,
            hold_until=parse_hold_until(args.hold_until),
            activation_spread_minutes=int(args.activation_spread_minutes),
            enqueue_activated=bool(getattr(args, "enqueue_activated", False)),
            dry_run=bool(getattr(args, "dry_run", False) or getattr(args, "dry_run_rollout", False)),
        )
        status_after = fetch_rollout_status(session, hold_tier=hold_tier)

        print("=== Catalog Rollout Apply ===")
        print(f"command={args.command}")
        print(f"target_label={target_label}")
        if seed_summary is not None:
            print("seed_summary:")
            for key, value in seed_summary.items():
                print(f"  {key}={value}")
        for key, value in summary.items():
            print(f"{key}={value}")
        print("\nStatus before:")
        print_status(status_before)
        if args.command == "expand":
            print("\nStatus before apply:")
            print_status(status_before_apply)
        print("\nStatus after:")
        print_status(status_after)

        profile = _recommended_profile(target_label)
        if profile:
            print("\nRecommended runtime profile for this phase:")
            for key, value in profile.items():
                print(f"{key}={value}")

        return 0
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
