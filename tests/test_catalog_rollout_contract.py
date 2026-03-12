from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_FILE = ROOT / "main.py"
ROLLOUT_SCRIPT = ROOT / "scripts" / "rollout_catalog_expansion.py"
VALIDATOR_SCRIPT = ROOT / "scripts" / "validate_50k_readiness.py"
SEED_SCRIPT = ROOT / "scripts" / "seed_steam_games.py"
REPAIR_SCRIPT = ROOT / "scripts" / "repair_job_status.py"
ROLLOUT_DOC = ROOT / "docs" / "CATALOG_ROLLOUT_PLAN.md"


class CatalogRolloutContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.main_text = MAIN_FILE.read_text(encoding="utf-8")
        cls.rollout_text = ROLLOUT_SCRIPT.read_text(encoding="utf-8")
        cls.validator_text = VALIDATOR_SCRIPT.read_text(encoding="utf-8")
        cls.seed_text = SEED_SCRIPT.read_text(encoding="utf-8")
        cls.repair_text = REPAIR_SCRIPT.read_text(encoding="utf-8")
        cls.rollout_doc_text = ROLLOUT_DOC.read_text(encoding="utf-8")

    def test_ingestion_supports_rollout_hold_filter(self) -> None:
        self.assertIn("ROLLOUT_HOLD_TIER", self.main_text)
        self.assertIn("TRACK_INCLUDE_ROLLOUT_HOLD", self.main_text)
        self.assertIn("Game.priority_tier != ROLLOUT_HOLD_TIER", self.main_text)

    def test_rollout_script_defines_phase_targets(self) -> None:
        self.assertIn('"phase1_25k": 25_000', self.rollout_text)
        self.assertIn('"phase2_50k": 50_000', self.rollout_text)
        self.assertIn('add_parser("status"', self.rollout_text)
        self.assertIn('add_parser("apply"', self.rollout_text)
        self.assertIn('add_parser(\n        "expand"', self.rollout_text)
        self.assertIn("validate_scale_schema", self.rollout_text)

    def test_seed_script_can_hold_new_games_for_staged_rollout(self) -> None:
        self.assertIn("--hold-new-games", self.seed_text)
        self.assertIn("--queue-held-games", self.seed_text)
        self.assertIn("priority_tier", self.seed_text)
        self.assertIn("next_refresh_at", self.seed_text)

    def test_readiness_validator_supports_phase_checks(self) -> None:
        self.assertIn("ROLLOUT_PHASE_TARGETS", self.validator_text)
        self.assertIn("--phase", self.validator_text)
        self.assertIn("--expected-tracked-min", self.validator_text)
        self.assertIn("rollout_tracked_size", self.validator_text)
        self.assertIn("rollout_dirty_backlog_bound", self.validator_text)

    def test_rollout_doc_has_copy_paste_phase_commands(self) -> None:
        self.assertIn("rollout_catalog_expansion.py expand --phase phase1_25k", self.rollout_doc_text)
        self.assertIn("rollout_catalog_expansion.py expand --phase phase2_50k", self.rollout_doc_text)
        self.assertIn("rollout_catalog_expansion.py apply --phase phase1_25k", self.rollout_doc_text)
        self.assertIn("rollout_catalog_expansion.py apply --phase phase2_50k", self.rollout_doc_text)
        self.assertIn("validate_50k_readiness.py --phase phase1_25k", self.rollout_doc_text)
        self.assertIn("validate_50k_readiness.py --phase phase2_50k", self.rollout_doc_text)

    def test_job_status_repair_script_exists_with_expected_entrypoints(self) -> None:
        self.assertIn("repair_job_status_rows", self.repair_text)
        self.assertIn("--dry-run", self.repair_text)
        self.assertIn("raise SystemExit(main())", self.repair_text)


if __name__ == "__main__":
    unittest.main()
