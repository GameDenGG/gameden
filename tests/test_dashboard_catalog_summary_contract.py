from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFRESH_SNAPSHOTS = ROOT / "jobs" / "refresh_snapshots.py"
WEB_INDEX = ROOT / "web" / "index.html"


class DashboardCatalogSummaryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.refresh_text = REFRESH_SNAPSHOTS.read_text(encoding="utf-8")
        cls.web_text = WEB_INDEX.read_text(encoding="utf-8")

    def test_dashboard_cache_payload_contains_catalog_summary(self) -> None:
        self.assertIn('"catalogSummary": {', self.refresh_text)
        self.assertIn('"tracked_games": tracked_games', self.refresh_text)
        self.assertIn('"total_games": total_games', self.refresh_text)
        self.assertIn("ROLLOUT_HOLD_TIER", self.refresh_text)

    def test_homepage_badge_is_not_hardcoded_to_10k(self) -> None:
        self.assertNotIn("~10,000 games tracked", self.web_text)
        self.assertIn('id="trackedGamesChip"', self.web_text)
        self.assertIn("normalizeCatalogSummary", self.web_text)
        self.assertIn("dashboardPayload?.catalogSummary", self.web_text)
        self.assertIn("${fmtInt(trackedCount)} games tracked", self.web_text)


if __name__ == "__main__":
    unittest.main()
