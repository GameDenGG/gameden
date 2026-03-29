import unittest
from pathlib import Path

from api.server import parse_csv_field


ROOT = Path(__file__).resolve().parents[1]


class BecauseYouOwnStabilityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.home_text = (ROOT / "web" / "index.html").read_text(encoding="utf-8")

    def test_restore_flow_no_longer_schedules_delayed_bundle_refresh(self) -> None:
        self.assertIn("if (shouldFetchCriticalBundle) {", self.home_text)
        self.assertNotIn("criticalRefreshDelayMs", self.home_text)
        self.assertIn("if (!restoredSnapshot || shouldFetchDeferredBundle) {", self.home_text)
        self.assertNotIn("scheduleDeferredHomepageStage({ delayMs });", self.home_text)

    def test_because_you_own_requires_stronger_overlap_or_controlled_fallback(self) -> None:
        self.assertIn("meaningfulOverlapCount >= 2", self.home_text)
        self.assertIn("hasControlledWeakFallback", self.home_text)
        self.assertIn("recommendations.reason === \"weak_fallback\"", self.home_text)

    def test_parse_csv_field_accepts_json_arrays_and_iterables(self) -> None:
        self.assertEqual(parse_csv_field('["Action","RPG"]'), ["Action", "RPG"])
        self.assertEqual(parse_csv_field(["Action", "RPG"]), ["Action", "RPG"])
        self.assertEqual(parse_csv_field("Action,RPG"), ["Action", "RPG"])


if __name__ == "__main__":
    unittest.main()
