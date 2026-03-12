from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy.orm import Session

from database.models import JobStatus


def _as_non_negative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def normalize_counter_triplet(
    items_total: Any,
    items_success: Any,
    items_failed: Any,
) -> tuple[int, int, int]:
    total = _as_non_negative_int(items_total)
    success = _as_non_negative_int(items_success)
    failed = _as_non_negative_int(items_failed)

    # If total is missing but success/failed are present, recover a coherent total.
    if total <= 0 and (success > 0 or failed > 0):
        total = max(success + failed, success, failed)

    if success > total:
        success = total
    if failed > total:
        failed = total
    if success + failed > total:
        failed = max(0, total - success)

    return total, success, failed


def is_counter_triplet_consistent(
    items_total: Any,
    items_success: Any,
    items_failed: Any,
) -> bool:
    total = _as_non_negative_int(items_total)
    success = _as_non_negative_int(items_success)
    failed = _as_non_negative_int(items_failed)
    if total <= 0:
        return True
    return success <= total and failed <= total and (success + failed) <= total


def repair_job_status_rows(
    session: Session,
    *,
    job_names: Iterable[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    query = session.query(JobStatus)
    normalized_job_names = [str(name).strip() for name in (job_names or []) if str(name).strip()]
    if normalized_job_names:
        query = query.filter(JobStatus.job_name.in_(normalized_job_names))

    rows = query.order_by(JobStatus.job_name.asc()).all()
    repairs: list[dict[str, Any]] = []

    for row in rows:
        # Leave untouched rows with no counters yet.
        if (
            row.last_items_total is None
            and row.last_items_success is None
            and row.last_items_failed is None
        ):
            continue

        raw_total = _as_non_negative_int(row.last_items_total)
        raw_success = _as_non_negative_int(row.last_items_success)
        raw_failed = _as_non_negative_int(row.last_items_failed)
        normalized_total, normalized_success, normalized_failed = normalize_counter_triplet(
            raw_total,
            raw_success,
            raw_failed,
        )

        if (
            normalized_total == raw_total
            and normalized_success == raw_success
            and normalized_failed == raw_failed
        ):
            continue

        repairs.append(
            {
                "job_name": row.job_name,
                "before": {
                    "last_items_total": raw_total,
                    "last_items_success": raw_success,
                    "last_items_failed": raw_failed,
                },
                "after": {
                    "last_items_total": normalized_total,
                    "last_items_success": normalized_success,
                    "last_items_failed": normalized_failed,
                },
            }
        )
        if not dry_run:
            row.last_items_total = normalized_total
            row.last_items_success = normalized_success
            row.last_items_failed = normalized_failed

    if repairs and not dry_run:
        session.flush()

    return {
        "checked_rows": len(rows),
        "repaired_rows": len(repairs),
        "repairs": repairs,
    }
