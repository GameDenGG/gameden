from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_GUARD = ROOT / "database" / "schema_guard.py"
INGEST_LOOP = ROOT / "jobs" / "run_price_ingestion_loop.py"
SNAPSHOT_WORKER = ROOT / "jobs" / "refresh_snapshots.py"
READINESS_SCRIPT = ROOT / "scripts" / "validate_50k_readiness.py"
READINESS_DOC = ROOT / "docs" / "READINESS_50K_CHECKLIST.md"


class ReadinessContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema_guard_text = SCHEMA_GUARD.read_text(encoding="utf-8")
        cls.ingest_loop_text = INGEST_LOOP.read_text(encoding="utf-8")
        cls.snapshot_worker_text = SNAPSHOT_WORKER.read_text(encoding="utf-8")
        cls.readiness_script_text = READINESS_SCRIPT.read_text(encoding="utf-8")
        cls.readiness_doc_text = READINESS_DOC.read_text(encoding="utf-8")

    def test_schema_guard_tracks_required_scale_tables(self) -> None:
        self.assertIn('"games"', self.schema_guard_text)
        self.assertIn('"dirty_games"', self.schema_guard_text)
        self.assertIn('"latest_game_prices"', self.schema_guard_text)
        self.assertIn('"game_snapshots"', self.schema_guard_text)
        self.assertIn("REQUIRED_INDEXES_COMMON", self.schema_guard_text)

    def test_workers_fail_fast_if_scale_schema_missing(self) -> None:
        self.assertIn("assert_scale_schema_ready(direct_engine, component_name=", self.ingest_loop_text)
        self.assertIn("assert_scale_schema_ready(direct_engine, component_name=", self.snapshot_worker_text)
        self.assertIn("refresh_snapshots worker (--once)", self.snapshot_worker_text)

    def test_readiness_script_checks_core_counts_and_queue_health(self) -> None:
        self.assertIn('"game_prices": "SELECT COUNT(*) FROM game_prices"', self.readiness_script_text)
        self.assertIn('"game_player_history": "SELECT COUNT(*) FROM game_player_history"', self.readiness_script_text)
        self.assertIn('"dirty_games": "SELECT COUNT(*) FROM dirty_games"', self.readiness_script_text)
        self.assertIn("SELECT COALESCE(MAX(retry_count), 0) FROM dirty_games", self.readiness_script_text)
        self.assertIn("job_status_consistency", self.readiness_script_text)
        self.assertIn("--repair-job-status", self.readiness_script_text)
        self.assertIn("repair_job_status_rows", self.readiness_script_text)
        self.assertIn("validate_scale_schema", self.readiness_script_text)

    def test_readiness_script_checks_snapshot_cache_backed_endpoints(self) -> None:
        self.assertIn("/dashboard/home", self.readiness_script_text)
        self.assertIn("/games/released?page=1&page_size=100&sort=alpha-asc&include_free=true", self.readiness_script_text)
        self.assertIn("api_homepage_diversity", self.readiness_script_text)
        self.assertIn("api_released_catalog_pagination", self.readiness_script_text)

    def test_readiness_doc_contains_go_no_go_commands(self) -> None:
        self.assertIn("python setup_database.py", self.readiness_doc_text)
        self.assertIn("python scripts/validate_50k_readiness.py", self.readiness_doc_text)
        self.assertIn("python scripts/repair_job_status.py", self.readiness_doc_text)
        self.assertIn("Go / No-Go Rule", self.readiness_doc_text)


if __name__ == "__main__":
    unittest.main()
