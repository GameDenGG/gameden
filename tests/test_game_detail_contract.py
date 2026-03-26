from __future__ import annotations

import datetime
import unittest
from pathlib import Path

from api import server


ROOT = Path(__file__).resolve().parents[1]


class GameDetailContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server_text = (ROOT / "api" / "server.py").read_text(encoding="utf-8")
        cls.game_text = (ROOT / "web" / "game.html").read_text(encoding="utf-8")

    def test_player_history_backend_exposes_authoritative_range_series(self) -> None:
        self.assertIn('"7d": 7', self.server_text)
        self.assertIn('"30d": 30', self.server_text)
        self.assertIn('"3m": 90', self.server_text)
        self.assertIn('"1y": 365', self.server_text)
        self.assertIn('"all": None', self.server_text)
        self.assertIn('"display_series_by_range": display_series_by_range', self.server_text)
        self.assertIn('"display_series": selected_display.get("points", [])', self.server_text)

    def test_player_display_series_are_evenly_progressive_by_bucket(self) -> None:
        base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        source_points = []
        for hour_offset in range(0, 40 * 24 + 1, 6):
            timestamp = base + datetime.timedelta(hours=hour_offset)
            ts_ms = int(timestamp.timestamp() * 1000)
            source_points.append(
                {
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "ts": ts_ms,
                    "players": 100 + hour_offset,
                }
            )

        series = server._build_player_display_series(source_points, "30d")
        points = series.get("points", [])
        bucket_ms = server.PLAYER_HISTORY_DISPLAY_BUCKET_MS["30d"]
        deltas = [
            int(points[idx + 1]["ts"]) - int(points[idx]["ts"])
            for idx in range(len(points) - 1)
        ]
        self.assertTrue(points)
        self.assertTrue(all(delta == bucket_ms for delta in deltas))

    def test_game_resolve_contract_normalizes_review_summary_and_counts(self) -> None:
        self.assertIn('"review_summary": review_summary,', self.server_text)
        self.assertIn('normalized["review_summary"] = review_summary or review_label or review_score_summary', self.server_text)
        self.assertIn('normalized["review_count"] = normalized_review_count', self.server_text)
        self.assertIn('normalized["review_total_count"] = normalized_review_count', self.server_text)
        self.assertIn('normalized["review"] = {', self.server_text)

    def test_game_page_uses_backend_player_series_and_review_order(self) -> None:
        self.assertIn("function getAuthoritativePlayerDisplaySeries(", self.game_text)
        self.assertIn("historyPayload?.display_series_by_range", self.game_text)
        self.assertNotIn("function buildDisplayedPlayerSeries(", self.game_text)
        self.assertNotIn("Review summary pending", self.game_text)
        self.assertIn("if (review.summary) return review.summary;", self.game_text)
        self.assertIn("if (review.score_text) return review.score_text;", self.game_text)
        self.assertIn("if (review.label) return review.label;", self.game_text)


if __name__ == "__main__":
    unittest.main()
