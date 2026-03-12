import argparse
import datetime
import time
from pathlib import Path
import sys

from sqlalchemy import func

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database.dirty_games import mark_game_dirty
from database.models import DirtyGame, Game, Session
from jobs.refresh_snapshots import rebuild_dashboard_cache, refresh_snapshots_once


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def count_catalog(session):
    released = int(session.query(func.count(Game.id)).filter(Game.is_released == 1).scalar() or 0)
    upcoming = int(session.query(func.count(Game.id)).filter(Game.is_released != 1).scalar() or 0)
    total = int(session.query(func.count(Game.id)).scalar() or 0)
    return total, released, upcoming


def seed_missing_games(session, target_released: int, target_upcoming: int) -> tuple[int, int]:
    _, released, upcoming = count_catalog(session)
    add_released = max(0, target_released - released)
    add_upcoming = max(0, target_upcoming - upcoming)
    created_released = 0
    created_upcoming = 0

    base_appid = 9000000 + int(session.query(func.max(Game.id)).scalar() or 0)

    for i in range(add_released):
        appid = str(base_appid + i + 1)
        session.add(
            Game(
                appid=appid,
                name=f"[SCALE] Released Game {appid}",
                store_url=f"https://store.steampowered.com/app/{appid}/",
                is_released=1,
                release_date_text="Jan 1, 2020",
                genres="Action",
                tags="ScaleTest",
                platforms="Windows",
            )
        )
        created_released += 1

    for i in range(add_upcoming):
        appid = str(base_appid + add_released + i + 1)
        session.add(
            Game(
                appid=appid,
                name=f"[SCALE] Upcoming Game {appid}",
                store_url=f"https://store.steampowered.com/app/{appid}/",
                is_released=0,
                release_date_text="Dec 2026",
                genres="Adventure",
                tags="ScaleTest",
                platforms="Windows",
            )
        )
        created_upcoming += 1

    session.commit()
    return created_released, created_upcoming


def enqueue_all_games(session) -> int:
    game_ids = [int(row[0]) for row in session.query(Game.id).all()]
    for game_id in game_ids:
        mark_game_dirty(session, game_id, reason="scale_validation_enqueue_all")
    session.commit()
    return len(game_ids)


def run_snapshot_batch(session, batch_size: int) -> dict:
    dirty_ids = [int(row[0]) for row in session.query(DirtyGame.game_id).order_by(DirtyGame.updated_at.asc()).limit(batch_size).all()]
    if not dirty_ids:
        return {"batch_size": 0, "updated": 0, "elapsed_ms": 0}

    started = time.perf_counter()
    updated = refresh_snapshots_once(session, dirty_ids)
    session.query(DirtyGame).filter(DirtyGame.game_id.in_(dirty_ids)).delete(synchronize_session=False)
    rebuild_dashboard_cache(session)
    session.commit()
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    return {"batch_size": len(dirty_ids), "updated": int(updated), "elapsed_ms": elapsed_ms}


def main():
    parser = argparse.ArgumentParser(description="Scale validation helper for NEWWORLD Steam deals backend.")
    parser.add_argument("--target-released", type=int, default=10000)
    parser.add_argument("--target-upcoming", type=int, default=500)
    parser.add_argument("--seed-missing", action="store_true")
    parser.add_argument("--enqueue-all", action="store_true")
    parser.add_argument("--run-snapshot-batch", action="store_true")
    parser.add_argument("--snapshot-batch-size", type=int, default=500)
    args = parser.parse_args()

    session = Session()
    try:
        started_at = utc_now().isoformat()
        total, released, upcoming = count_catalog(session)
        print(f"started_at={started_at}")
        print(f"catalog_total={total} released={released} upcoming={upcoming}")

        if args.seed_missing:
            created_released, created_upcoming = seed_missing_games(
                session,
                target_released=args.target_released,
                target_upcoming=args.target_upcoming,
            )
            total, released, upcoming = count_catalog(session)
            print(
                f"seeded_released={created_released} seeded_upcoming={created_upcoming} "
                f"catalog_total={total} released={released} upcoming={upcoming}"
            )

        if args.enqueue_all:
            enqueued = enqueue_all_games(session)
            print(f"dirty_enqueued={enqueued}")

        dirty_count = int(session.query(func.count(DirtyGame.game_id)).scalar() or 0)
        print(f"dirty_games_count={dirty_count}")

        if args.run_snapshot_batch:
            result = run_snapshot_batch(session, args.snapshot_batch_size)
            print(
                f"snapshot_batch_size={result['batch_size']} "
                f"updated={result['updated']} elapsed_ms={result['elapsed_ms']}"
            )
            dirty_count_after = int(session.query(func.count(DirtyGame.game_id)).scalar() or 0)
            print(f"dirty_games_count_after={dirty_count_after}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
