from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database import DirectSessionLocal
from database.job_status import is_counter_triplet_consistent, repair_job_status_rows
from database.models import JobStatus


def _collect_inconsistencies(session, job_names: list[str]) -> list[dict]:
    query = session.query(JobStatus)
    if job_names:
        query = query.filter(JobStatus.job_name.in_(job_names))

    rows = query.order_by(JobStatus.job_name.asc()).all()
    inconsistencies: list[dict] = []
    for row in rows:
        total = int(row.last_items_total or 0)
        success = int(row.last_items_success or 0)
        failed = int(row.last_items_failed or 0)
        if not is_counter_triplet_consistent(total, success, failed):
            inconsistencies.append(
                {
                    "job_name": row.job_name,
                    "last_items_total": total,
                    "last_items_success": success,
                    "last_items_failed": failed,
                }
            )
    return inconsistencies


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair inconsistent job_status counters (success/failed/total)."
    )
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        default=[],
        help="Optional job_name filter. Repeat for multiple jobs.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show repairs without writing changes.")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON summary.")
    args = parser.parse_args()

    job_names = sorted({str(name).strip() for name in args.jobs if str(name).strip()})
    session = DirectSessionLocal()
    try:
        before = _collect_inconsistencies(session, job_names)
        repair_summary = repair_job_status_rows(
            session,
            job_names=job_names,
            dry_run=bool(args.dry_run),
        )
        if not args.dry_run:
            session.commit()
        after = _collect_inconsistencies(session, job_names)

        payload = {
            "dry_run": bool(args.dry_run),
            "jobs": job_names,
            "inconsistent_before": before,
            "repair_summary": repair_summary,
            "inconsistent_after": after,
            "success": len(after) == 0,
        }

        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print("=== job_status counter repair ===")
            print(f"dry_run={payload['dry_run']}")
            if job_names:
                print(f"jobs={','.join(job_names)}")
            print(f"inconsistent_before={len(before)}")
            print(f"checked_rows={repair_summary['checked_rows']}")
            print(f"repaired_rows={repair_summary['repaired_rows']}")
            print(f"inconsistent_after={len(after)}")
            if before:
                print("before:")
                for row in before:
                    print(
                        f"- {row['job_name']}: total={row['last_items_total']} "
                        f"success={row['last_items_success']} failed={row['last_items_failed']}"
                    )
            if repair_summary["repairs"]:
                print("repairs:")
                for row in repair_summary["repairs"]:
                    before_row = row["before"]
                    after_row = row["after"]
                    print(
                        f"- {row['job_name']}: "
                        f"({before_row['last_items_total']}, {before_row['last_items_success']}, {before_row['last_items_failed']}) "
                        f"-> ({after_row['last_items_total']}, {after_row['last_items_success']}, {after_row['last_items_failed']})"
                    )
            if after:
                print("remaining inconsistent rows:")
                for row in after:
                    print(
                        f"- {row['job_name']}: total={row['last_items_total']} "
                        f"success={row['last_items_success']} failed={row['last_items_failed']}"
                    )
            print("status=ok" if payload["success"] else "status=needs_attention")

        return 0 if payload["success"] else 1
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
