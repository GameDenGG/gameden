from __future__ import annotations

import unittest

from database.job_status import is_counter_triplet_consistent, normalize_counter_triplet


class JobStatusCounterTests(unittest.TestCase):
    def test_normalize_caps_failed_against_total(self) -> None:
        total, success, failed = normalize_counter_triplet(600, 597, 578)
        self.assertEqual(total, 600)
        self.assertEqual(success, 597)
        self.assertEqual(failed, 3)
        self.assertTrue(is_counter_triplet_consistent(total, success, failed))

    def test_normalize_recovers_missing_total(self) -> None:
        total, success, failed = normalize_counter_triplet(0, 5, 2)
        self.assertEqual((total, success, failed), (7, 5, 2))
        self.assertTrue(is_counter_triplet_consistent(total, success, failed))

    def test_normalize_handles_negative_and_non_numeric(self) -> None:
        total, success, failed = normalize_counter_triplet(-10, "8", None)
        self.assertEqual((total, success, failed), (8, 8, 0))
        self.assertTrue(is_counter_triplet_consistent(total, success, failed))

    def test_consistency_helper_flags_impossible_triplets(self) -> None:
        self.assertFalse(is_counter_triplet_consistent(10, 8, 5))
        self.assertFalse(is_counter_triplet_consistent(10, 12, 0))
        self.assertTrue(is_counter_triplet_consistent(10, 8, 2))


if __name__ == "__main__":
    unittest.main()
