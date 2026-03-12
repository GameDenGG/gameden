from __future__ import annotations

import unittest

from jobs import refresh_snapshots


class SnapshotWorkerScalingTests(unittest.TestCase):
    def test_batch_size_is_clamped(self) -> None:
        self.assertGreaterEqual(refresh_snapshots.BATCH_SIZE, 1)
        self.assertLessEqual(refresh_snapshots.BATCH_SIZE, refresh_snapshots.MAX_BATCH_SIZE)
        self.assertEqual(refresh_snapshots.clamp_batch_size(refresh_snapshots.MAX_BATCH_SIZE + 5000), refresh_snapshots.MAX_BATCH_SIZE)

    def test_retry_backoff_grows_exponentially_until_cap(self) -> None:
        first = refresh_snapshots.compute_retry_backoff_seconds(1)
        second = refresh_snapshots.compute_retry_backoff_seconds(2)
        third = refresh_snapshots.compute_retry_backoff_seconds(3)
        capped = refresh_snapshots.compute_retry_backoff_seconds(10_000)

        self.assertGreaterEqual(second, first)
        self.assertGreaterEqual(third, second)
        self.assertLessEqual(capped, int(refresh_snapshots.RETRY_BACKOFF_MAX_SECONDS))

    def test_homepage_candidate_pool_is_large_enough_for_diversity(self) -> None:
        self.assertGreaterEqual(
            refresh_snapshots.HOMEPAGE_DEAL_CANDIDATE_POOL,
            refresh_snapshots.HOMEPAGE_RAIL_LIMIT,
        )


if __name__ == "__main__":
    unittest.main()
