from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_SERVER = ROOT / "api" / "server.py"
WEB_INDEX = ROOT / "web" / "index.html"


class SeasonalSaleContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.api_text = API_SERVER.read_text(encoding="utf-8")
        cls.web_text = WEB_INDEX.read_text(encoding="utf-8")

    def test_backend_has_active_sale_row_builder(self) -> None:
        self.assertIn("def build_active_sale_rows", self.api_text)
        self.assertIn("row.discount_percent is None or row.discount_percent <= 0", self.api_text)
        self.assertIn("serialized[\"seasonal_relevance_score\"] = relevance_score", self.api_text)
        self.assertIn("return _dedupe_serialized_rows(ranked_rows)", self.api_text)

    def test_seasonal_summary_switches_mode_by_sale_status(self) -> None:
        self.assertIn('if is_live:', self.api_text)
        self.assertIn('seasonal_mode = "active_sale"', self.api_text)
        self.assertIn('seasonal_mode = "potential_sale"', self.api_text)
        self.assertIn('"mode": seasonal_mode', self.api_text)
        self.assertIn('"items": seasonal_items', self.api_text)
        self.assertIn('"expected_games": seasonal_items', self.api_text)

    def test_frontend_supports_seasonal_mode_aware_rendering(self) -> None:
        self.assertIn('const mode = String(source.mode || "")', self.web_text)
        self.assertIn('state.seasonalSale.mode === "active_sale"', self.web_text)
        self.assertIn("function rankSeasonalItemsByMode", self.web_text)
        self.assertIn("scoreSeasonalItem", self.web_text)
        self.assertIn("On-Sale Games", self.web_text)
        self.assertIn("No currently discounted games found for the active seasonal sale.", self.web_text)

    def test_frontend_live_copy_no_longer_potential_only(self) -> None:
        self.assertRegex(
            self.web_text,
            re.compile(r"window is live\.\s*Showing games that are currently discounted right now\.", re.IGNORECASE),
        )


if __name__ == "__main__":
    unittest.main()
