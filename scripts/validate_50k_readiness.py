from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from database import DirectSessionLocal, direct_engine
from database.job_status import repair_job_status_rows
from database.schema_guard import SchemaReadinessReport, validate_scale_schema
from jobs import refresh_snapshots
import main as ingestion_main


CORE_COUNT_QUERIES = {
    "games": "SELECT COUNT(*) FROM games",
    "game_prices": "SELECT COUNT(*) FROM game_prices",
    "game_player_history": "SELECT COUNT(*) FROM game_player_history",
    "latest_game_prices": "SELECT COUNT(*) FROM latest_game_prices",
    "game_snapshots": "SELECT COUNT(*) FROM game_snapshots",
    "dirty_games": "SELECT COUNT(*) FROM dirty_games",
}

ROLLOUT_PHASE_TARGETS = {
    "baseline_10k": 10_000,
    "phase1_25k": 25_000,
    "phase2_50k": 50_000,
}


@dataclass
class ReadinessResult:
    schema_ready: bool
    schema_report: dict[str, Any]
    db_counts_ok: bool
    db_counts: dict[str, int] = field(default_factory=dict)
    queue_health: dict[str, Any] = field(default_factory=dict)
    job_status: dict[str, Any] = field(default_factory=dict)
    job_status_repair: dict[str, Any] = field(default_factory=dict)
    config_checks: dict[str, Any] = field(default_factory=dict)
    api_checks: dict[str, Any] = field(default_factory=dict)
    rollout_checks: dict[str, Any] = field(default_factory=dict)
    phase: str | None = None
    checks: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["ready"] = self.ready
        return payload


def _record_check(result: ReadinessResult, name: str, ok: bool, detail: str) -> None:
    result.checks.append({"name": name, "ok": bool(ok), "detail": detail})
    if ok:
        return
    result.errors.append(f"{name}: {detail}")


def _run_core_db_counts(result: ReadinessResult) -> None:
    counts: dict[str, int] = {}
    try:
        with direct_engine.connect() as conn:
            for table_name, sql in CORE_COUNT_QUERIES.items():
                value = conn.execute(text(sql)).scalar()
                counts[table_name] = int(value or 0)
        result.db_counts = counts
        result.db_counts_ok = True
        _record_check(result, "db_core_counts", True, "core table counts queryable")
    except Exception as exc:
        result.db_counts_ok = False
        _record_check(result, "db_core_counts", False, f"failed running core counts: {exc}")


def _run_queue_health_checks(result: ReadinessResult) -> None:
    if not result.db_counts_ok:
        return

    try:
        with direct_engine.connect() as conn:
            queue_stats = {
                "eligible_now": int(
                    conn.execute(
                        text(
                            """
                            SELECT COUNT(*) FROM dirty_games
                            WHERE next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP
                            """
                        )
                    ).scalar()
                    or 0
                ),
                "retry_ge_5": int(
                    conn.execute(
                        text("SELECT COUNT(*) FROM dirty_games WHERE COALESCE(retry_count, 0) >= 5")
                    ).scalar()
                    or 0
                ),
                "max_retry_count": int(
                    conn.execute(text("SELECT COALESCE(MAX(retry_count), 0) FROM dirty_games")).scalar()
                    or 0
                ),
                "oldest_updated_at": conn.execute(
                    text("SELECT MIN(updated_at) FROM dirty_games")
                ).scalar(),
            }
    except Exception as exc:
        _record_check(result, "queue_health", False, f"failed querying dirty queue health: {exc}")
        return

    if queue_stats["retry_ge_5"] > 0:
        result.warnings.append(
            f"dirty_games has {queue_stats['retry_ge_5']} rows at retry_count >= 5 (check problematic titles)."
        )

    result.queue_health = queue_stats
    _record_check(result, "queue_health", True, "dirty queue health queryable")


def _run_job_status_checks(result: ReadinessResult, *, repair_job_status: bool = False) -> None:
    if not result.db_counts_ok:
        return

    if repair_job_status:
        repair_session = DirectSessionLocal()
        try:
            repair_payload = repair_job_status_rows(repair_session, dry_run=False)
            repair_session.commit()
            result.job_status_repair = repair_payload
            _record_check(
                result,
                "job_status_repair",
                True,
                (
                    "auto-repair applied"
                    f" (checked={repair_payload.get('checked_rows', 0)}, repaired={repair_payload.get('repaired_rows', 0)})"
                ),
            )
        except Exception as exc:
            repair_session.rollback()
            _record_check(result, "job_status_repair", False, f"job_status auto-repair failed: {exc}")
        finally:
            repair_session.close()

    try:
        with direct_engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        job_name,
                        last_started_at,
                        last_completed_at,
                        last_success_at,
                        last_error,
                        last_duration_ms,
                        last_items_total,
                        last_items_success,
                        last_items_failed,
                        updated_at
                    FROM job_status
                    WHERE job_name IN ('price_ingestion', 'refresh_snapshots')
                    """
                )
            ).mappings().all()
    except Exception as exc:
        _record_check(result, "job_status", False, f"failed querying job_status: {exc}")
        return

    status_payload: dict[str, Any] = {}
    for row in rows:
        row_dict = dict(row)
        duration_ms = int(row_dict.get("last_duration_ms") or 0)
        success_items = int(row_dict.get("last_items_success") or 0)
        row_dict["items_per_second"] = (
            round(success_items / (duration_ms / 1000.0), 3)
            if duration_ms > 0 and success_items > 0
            else 0.0
        )
        status_payload[str(row_dict["job_name"])] = row_dict

    result.job_status = status_payload

    ingestion_present = "price_ingestion" in status_payload
    snapshots_present = "refresh_snapshots" in status_payload
    _record_check(
        result,
        "job_status_rows_present",
        ingestion_present and snapshots_present,
        "job_status should contain both price_ingestion and refresh_snapshots rows",
    )

    consistency_issues: list[str] = []
    for job_name, row in status_payload.items():
        total = int(row.get("last_items_total") or 0)
        success = int(row.get("last_items_success") or 0)
        failed = int(row.get("last_items_failed") or 0)
        if total <= 0:
            continue
        if success > total or failed > total or (success + failed) > total:
            consistency_issues.append(
                f"{job_name}(total={total}, success={success}, failed={failed})"
            )
    _record_check(
        result,
        "job_status_consistency",
        not consistency_issues,
        "job_status totals should not report impossible success/failed counts",
    )
    if consistency_issues:
        result.warnings.append("job_status consistency issues: " + "; ".join(consistency_issues))
        result.warnings.append("Repair with: python scripts/repair_job_status.py")

    stale_jobs = []
    for job_name, row in status_payload.items():
        if row.get("last_error"):
            stale_jobs.append(f"{job_name}:last_error={row['last_error']}")
    if stale_jobs:
        result.warnings.append("Recent worker errors found in job_status: " + "; ".join(stale_jobs))


def _run_config_checks(result: ReadinessResult) -> None:
    checks: dict[str, Any] = {
        "ingestion_games_per_run": int(ingestion_main.GAMES_PER_RUN),
        "ingestion_games_per_run_limit": int(ingestion_main.TRACK_GAMES_PER_RUN_LIMIT),
        "ingestion_shard_total": int(ingestion_main.TRACK_SHARD_TOTAL),
        "ingestion_shard_index": int(ingestion_main.TRACK_SHARD_INDEX),
        "ingestion_delays_seconds": {
            "min": float(ingestion_main.MIN_DELAY_SECONDS),
            "max": float(ingestion_main.MAX_DELAY_SECONDS),
        },
        "priority_intervals_minutes": {
            "hot": int(ingestion_main.TRACK_HOT_REFRESH_MINUTES),
            "medium": int(ingestion_main.TRACK_MEDIUM_REFRESH_MINUTES),
            "cold": int(ingestion_main.TRACK_COLD_REFRESH_MINUTES),
        },
        "rollout_hold_tier": str(getattr(ingestion_main, "ROLLOUT_HOLD_TIER", "ROLLOUT_HOLD")),
        "track_include_rollout_hold": bool(getattr(ingestion_main, "TRACK_INCLUDE_ROLLOUT_HOLD", False)),
        "snapshot_batch_size": int(refresh_snapshots.BATCH_SIZE),
        "snapshot_max_batch_size": int(refresh_snapshots.MAX_BATCH_SIZE),
        "snapshot_retry_backoff": {
            "base_seconds": float(refresh_snapshots.RETRY_BACKOFF_BASE_SECONDS),
            "max_seconds": float(refresh_snapshots.RETRY_BACKOFF_MAX_SECONDS),
        },
        "homepage_candidate_pool": int(refresh_snapshots.HOMEPAGE_DEAL_CANDIDATE_POOL),
        "homepage_rail_limit": int(refresh_snapshots.HOMEPAGE_RAIL_LIMIT),
    }
    result.config_checks = checks

    interval_ok = (
        checks["priority_intervals_minutes"]["hot"]
        < checks["priority_intervals_minutes"]["medium"]
        < checks["priority_intervals_minutes"]["cold"]
    )
    _record_check(
        result,
        "priority_tier_intervals",
        interval_ok,
        "expected HOT < MEDIUM < COLD refresh cadence",
    )

    pool_ok = checks["homepage_candidate_pool"] >= checks["homepage_rail_limit"]
    _record_check(
        result,
        "homepage_candidate_pool",
        pool_ok,
        "candidate pool should be >= rail limit for diversity",
    )

    shard_ok = checks["ingestion_shard_total"] >= 1 and checks["ingestion_shard_index"] >= 0
    _record_check(
        result,
        "ingestion_sharding_config",
        shard_ok,
        "TRACK_SHARD_TOTAL must be >=1 and TRACK_SHARD_INDEX must be >=0",
    )

    hold_tier_ok = bool(str(checks["rollout_hold_tier"]).strip())
    _record_check(
        result,
        "rollout_hold_tier_config",
        hold_tier_ok,
        "TRACK_ROLLOUT_HOLD_TIER must be configured",
    )

    if checks["ingestion_shard_total"] > 3:
        result.warnings.append(
            f"TRACK_SHARD_TOTAL={checks['ingestion_shard_total']} is aggressive; start with 1 and scale carefully."
        )
    if checks["track_include_rollout_hold"]:
        result.warnings.append(
            "TRACK_INCLUDE_ROLLOUT_HOLD=true bypasses staged hold controls; disable for normal rollout phases."
        )


def _run_rollout_phase_checks(
    result: ReadinessResult,
    *,
    expected_catalog_min: int,
    expected_tracked_min: int,
    max_dirty_games: int | None,
) -> None:
    if not result.db_counts_ok:
        return

    hold_tier = str(result.config_checks.get("rollout_hold_tier") or "ROLLOUT_HOLD").strip().upper()
    try:
        with direct_engine.connect() as conn:
            tracked_games = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM games
                        WHERE upper(COALESCE(priority_tier, '')) <> :hold_tier
                        """
                    ),
                    {"hold_tier": hold_tier},
                ).scalar()
                or 0
            )
            held_games = int(
                conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM games
                        WHERE upper(COALESCE(priority_tier, '')) = :hold_tier
                        """
                    ),
                    {"hold_tier": hold_tier},
                ).scalar()
                or 0
            )
    except Exception as exc:
        _record_check(result, "rollout_phase_counts", False, f"failed rollout count query: {exc}")
        return

    total_games = int(result.db_counts.get("games") or 0)
    dirty_games = int(result.db_counts.get("dirty_games") or 0)
    rollout_checks = {
        "phase": result.phase,
        "hold_tier": hold_tier,
        "total_games": total_games,
        "tracked_games": tracked_games,
        "held_games": held_games,
        "dirty_games": dirty_games,
        "expected_catalog_min": int(expected_catalog_min),
        "expected_tracked_min": int(expected_tracked_min),
        "max_dirty_games": (int(max_dirty_games) if max_dirty_games is not None else None),
    }
    result.rollout_checks = rollout_checks

    _record_check(
        result,
        "rollout_counts_consistency",
        (tracked_games + held_games) == total_games,
        "tracked + held game counts should equal total games",
    )

    if expected_catalog_min > 0:
        _record_check(
            result,
            "rollout_catalog_size",
            total_games >= expected_catalog_min,
            f"catalog total should be >= {expected_catalog_min}",
        )

    if expected_tracked_min > 0:
        _record_check(
            result,
            "rollout_tracked_size",
            tracked_games >= expected_tracked_min,
            f"tracked games should be >= {expected_tracked_min}",
        )

    if max_dirty_games is not None and max_dirty_games >= 0:
        _record_check(
            result,
            "rollout_dirty_backlog_bound",
            dirty_games <= int(max_dirty_games),
            f"dirty_games should remain <= {int(max_dirty_games)}",
        )

    if tracked_games == 0:
        result.warnings.append("Tracked catalog appears empty; ingestion will not progress.")
    if held_games == 0:
        result.warnings.append("No held catalog detected; staged rollout control may be disabled.")


def _run_api_checks(result: ReadinessResult, base_url: str | None, timeout_seconds: float) -> None:
    if not base_url:
        result.api_checks = {"skipped": True}
        result.warnings.append("API checks skipped (no --base-url provided).")
        return

    api_checks: dict[str, Any] = {"base_url": base_url}
    try:
        dashboard = requests.get(f"{base_url}/dashboard/home", timeout=timeout_seconds)
        api_checks["dashboard_status"] = dashboard.status_code
        if dashboard.ok:
            dashboard_payload = dashboard.json()
            dashboard_keys_ok = (
                isinstance(dashboard_payload, dict)
                and "dealRanked" in dashboard_payload
                and "worthBuyingNow" in dashboard_payload
                and "trendingDeals" in dashboard_payload
            )
            _record_check(
                result,
                "api_dashboard_home",
                dashboard_keys_ok,
                "dashboard payload should include dealRanked/worthBuyingNow/trendingDeals",
            )
            if dashboard_keys_ok:
                diversity_keys = [
                    "dealRanked",
                    "worthBuyingNow",
                    "recommendedDeals",
                    "biggestDeals",
                    "trendingDeals",
                ]
                top_window = 8
                key_to_ids: dict[str, set[str]] = {}
                for key in diversity_keys:
                    rows = dashboard_payload.get(key) or []
                    top_rows = rows[:top_window] if isinstance(rows, list) else []
                    row_ids: set[str] = set()
                    for row in top_rows:
                        if not isinstance(row, dict):
                            continue
                        game_id = row.get("game_id")
                        game_name = row.get("game_name")
                        if game_id is not None:
                            row_ids.add(f"id:{game_id}")
                        elif game_name:
                            row_ids.add(f"name:{str(game_name).strip().lower()}")
                    key_to_ids[key] = row_ids

                overlap_counts: dict[str, int] = {}
                max_overlap = 0
                for idx, left in enumerate(diversity_keys):
                    for right in diversity_keys[idx + 1:]:
                        overlap = len(key_to_ids.get(left, set()) & key_to_ids.get(right, set()))
                        overlap_counts[f"{left}__{right}"] = overlap
                        if overlap > max_overlap:
                            max_overlap = overlap
                api_checks["homepage_overlap_counts_top8"] = overlap_counts
                _record_check(
                    result,
                    "api_homepage_diversity",
                    max_overlap <= 3,
                    "top deal rails should not heavily duplicate lead titles",
                )
        else:
            _record_check(
                result,
                "api_dashboard_home",
                False,
                f"/dashboard/home returned {dashboard.status_code}",
            )

        catalog = requests.get(
            f"{base_url}/games/released?page=1&page_size=100&sort=alpha-asc&include_free=true",
            timeout=timeout_seconds,
        )
        api_checks["released_status"] = catalog.status_code
        if catalog.ok:
            catalog_payload = catalog.json()
            catalog_ok = (
                isinstance(catalog_payload, dict)
                and "items" in catalog_payload
                and "total" in catalog_payload
                and "total_pages" in catalog_payload
                and isinstance(catalog_payload.get("items"), list)
            )
            _record_check(
                result,
                "api_released_catalog_contract",
                catalog_ok,
                "released catalog should include items/total/total_pages",
            )
            api_checks["released_total"] = int(catalog_payload.get("total") or 0)
            api_checks["released_total_pages"] = int(catalog_payload.get("total_pages") or 0)
            api_checks["released_page_size"] = int(catalog_payload.get("page_size") or 0)
            pagination_ok = (
                api_checks["released_total"] >= 0
                and api_checks["released_page_size"] > 0
                and (
                    api_checks["released_total"] == 0
                    or api_checks["released_total_pages"] >= 1
                )
            )
            _record_check(
                result,
                "api_released_catalog_pagination",
                pagination_ok,
                "released catalog pagination metadata should be valid",
            )
        else:
            _record_check(
                result,
                "api_released_catalog_contract",
                False,
                f"/games/released returned {catalog.status_code}",
            )

        radar_response = requests.get(
            f"{base_url}/api/market-radar?limit=50",
            timeout=timeout_seconds,
        )
        if radar_response.status_code == 404:
            radar_response = requests.get(
                f"{base_url}/api/deal-radar?limit=50",
                timeout=timeout_seconds,
            )
        api_checks["market_radar_status"] = radar_response.status_code
        if radar_response.ok:
            radar_payload = radar_response.json()
            radar_items = radar_payload.get("items", []) if isinstance(radar_payload, dict) else []
            radar_contract_ok = isinstance(radar_items, list)
            _record_check(
                result,
                "api_market_radar_contract",
                radar_contract_ok,
                "market radar payload should include an items list",
            )
            if radar_contract_ok:
                signal_counts: dict[str, int] = {}
                game_ids: set[int] = set()
                duplicate_games = 0
                top_window = radar_items[:20]
                for item in top_window:
                    if not isinstance(item, dict):
                        continue
                    signal_type = str(item.get("signal_type") or "").upper()
                    if signal_type:
                        signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
                    game_id = item.get("game_id")
                    try:
                        parsed_game_id = int(game_id)
                    except Exception:
                        parsed_game_id = 0
                    if parsed_game_id > 0:
                        if parsed_game_id in game_ids:
                            duplicate_games += 1
                        game_ids.add(parsed_game_id)
                api_checks["market_radar_signal_counts"] = signal_counts
                api_checks["market_radar_top_window"] = len(top_window)
                api_checks["market_radar_duplicate_games_top_window"] = duplicate_games
                max_per_signal = max(signal_counts.values()) if signal_counts else 0
                api_checks["market_radar_max_per_signal_top_window"] = max_per_signal
                top_window_count = max(1, len(top_window))
                dominant_signal_share = round(max_per_signal / top_window_count, 3)
                api_checks["market_radar_dominant_signal_share"] = dominant_signal_share
                diversity_ok = (
                    len(signal_counts) >= 4
                    and duplicate_games == 0
                    and dominant_signal_share <= 0.45
                )
                _record_check(
                    result,
                    "api_market_radar_diversity",
                    diversity_ok,
                    "market radar top feed should contain 4+ signal categories with no duplicate games and balanced signal share",
                )
        else:
            _record_check(
                result,
                "api_market_radar_contract",
                False,
                f"/api/market-radar returned {radar_response.status_code}",
            )
    except Exception as exc:
        _record_check(result, "api_checks", False, f"API check failed: {exc}")

    result.api_checks = api_checks


def run_readiness_validation(
    base_url: str | None,
    timeout_seconds: float,
    *,
    phase: str | None = None,
    expected_catalog_min: int = 0,
    expected_tracked_min: int = 0,
    max_dirty_games: int | None = None,
    repair_job_status: bool = False,
) -> ReadinessResult:
    schema_report: SchemaReadinessReport
    try:
        schema_report = validate_scale_schema(direct_engine)
    except Exception as exc:
        result = ReadinessResult(
            schema_ready=False,
            schema_report={"error": str(exc)},
            db_counts_ok=False,
        )
        _record_check(result, "schema_validation", False, f"schema introspection failed: {exc}")
        return result

    result = ReadinessResult(
        schema_ready=schema_report.is_ready,
        schema_report=schema_report.to_dict(),
        db_counts_ok=False,
        phase=phase,
    )
    _record_check(
        result,
        "schema_validation",
        schema_report.is_ready,
        "required scale columns/indexes/queue PK present",
    )

    _run_core_db_counts(result)
    _run_queue_health_checks(result)
    _run_job_status_checks(result, repair_job_status=repair_job_status)
    _run_config_checks(result)
    _run_rollout_phase_checks(
        result,
        expected_catalog_min=max(0, int(expected_catalog_min)),
        expected_tracked_min=max(0, int(expected_tracked_min)),
        max_dirty_games=(None if max_dirty_games is None else int(max_dirty_games)),
    )
    _run_api_checks(result, base_url=base_url, timeout_seconds=timeout_seconds)
    return result


def _print_human_report(result: ReadinessResult) -> None:
    print("=== 50k Readiness Validation ===")
    print(f"ready={result.ready}")

    print("\nChecks:")
    for item in result.checks:
        status = "PASS" if item["ok"] else "FAIL"
        print(f"- [{status}] {item['name']}: {item['detail']}")

    if result.db_counts:
        print("\nCore table counts:")
        for table, count in result.db_counts.items():
            print(f"- {table}: {count}")

    if result.queue_health:
        print("\nDirty queue health:")
        for key, value in result.queue_health.items():
            print(f"- {key}: {value}")

    if result.job_status:
        print("\nWorker status:")
        print(json.dumps(result.job_status, indent=2, default=str))
    if result.job_status_repair:
        print("\njob_status repair:")
        print(json.dumps(result.job_status_repair, indent=2, default=str))

    if result.config_checks:
        print("\nRuntime config snapshot:")
        print(json.dumps(result.config_checks, indent=2, default=str))

    if result.rollout_checks:
        print("\nRollout checks:")
        print(json.dumps(result.rollout_checks, indent=2, default=str))

    if result.api_checks:
        print("\nAPI checks:")
        print(json.dumps(result.api_checks, indent=2, default=str))

    if result.warnings:
        print("\nWarnings:")
        for warning in result.warnings:
            print(f"- {warning}")

    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"- {error}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate NEWWORLD 50k-game readiness.")
    parser.add_argument(
        "--base-url",
        default="",
        help="Optional API base URL for endpoint checks, e.g. http://127.0.0.1:8000",
    )
    parser.add_argument("--timeout-seconds", type=float, default=4.0)
    parser.add_argument(
        "--phase",
        choices=sorted(ROLLOUT_PHASE_TARGETS.keys()),
        default="",
        help="Optional rollout phase label for stage-specific readiness checks.",
    )
    parser.add_argument(
        "--expected-catalog-min",
        type=int,
        default=0,
        help="Optional minimum required games row count.",
    )
    parser.add_argument(
        "--expected-tracked-min",
        type=int,
        default=0,
        help="Optional minimum required tracked (non-held) games.",
    )
    parser.add_argument(
        "--max-dirty-games",
        type=int,
        default=-1,
        help="Optional upper bound for dirty_games backlog. Set <0 to skip bound check.",
    )
    parser.add_argument(
        "--repair-job-status",
        action="store_true",
        help="Repair inconsistent job_status item counters before running readiness checks.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON payload only.")
    args = parser.parse_args()

    base_url = args.base_url.strip() or None
    phase = args.phase.strip() or None
    expected_catalog_min = int(args.expected_catalog_min or 0)
    expected_tracked_min = int(args.expected_tracked_min or 0)
    if phase:
        phase_target = int(ROLLOUT_PHASE_TARGETS.get(phase) or 0)
        expected_catalog_min = max(expected_catalog_min, phase_target)
        expected_tracked_min = max(expected_tracked_min, phase_target)
    max_dirty_games = None if int(args.max_dirty_games) < 0 else int(args.max_dirty_games)

    result = run_readiness_validation(
        base_url=base_url,
        timeout_seconds=float(args.timeout_seconds),
        phase=phase,
        expected_catalog_min=expected_catalog_min,
        expected_tracked_min=expected_tracked_min,
        max_dirty_games=max_dirty_games,
        repair_job_status=bool(args.repair_job_status),
    )

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        _print_human_report(result)

    return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
