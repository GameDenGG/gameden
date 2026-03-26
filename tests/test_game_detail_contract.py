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
        self.assertIn('"is_interpolated": index in interpolated_indexes', self.server_text)
        self.assertIn("PLAYER_HISTORY_INCOMPATIBLE_SOURCES", self.server_text)

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

    def test_player_display_series_non_all_ranges_fill_interior_gaps_only(self) -> None:
        base = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

        def make_point(day_offset: int, players: int) -> dict:
            timestamp = base + datetime.timedelta(days=day_offset)
            return {
                "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                "ts": int(timestamp.timestamp() * 1000),
                "players": players,
            }

        source_points = [
            make_point(0, 120),
            make_point(20, 130),
            make_point(360, 1000),
            make_point(361, 980),
            make_point(392, 2010),
            make_point(398, 2140),
            make_point(400, 2080),
        ]

        for range_key in ("30d", "3m", "1y"):
            with self.subTest(range_key=range_key):
                series = server._build_player_display_series(source_points, range_key)
                points = series.get("points", [])
                min_ts, max_ts = server._resolve_player_range_bounds(source_points, range_key)
                bucket_starts = server._build_player_bucket_timestamps(min_ts, max_ts, range_key)
                self.assertEqual(len(points), len(bucket_starts))
                self.assertEqual([int(point["ts"]) for point in points], bucket_starts)
                if points[0]["players"] is None:
                    self.assertFalse(bool(points[0]["is_interpolated"]))
                else:
                    self.assertTrue(bool(points[0]["is_interpolated"]))
                interpolated_points = [point for point in points if point.get("is_interpolated")]
                self.assertTrue(all(point["players"] is not None for point in interpolated_points))
                for point_index, point in enumerate(points):
                    if point["players"] is not None:
                        continue
                    has_left_real = any(candidate["players"] is not None for candidate in points[:point_index])
                    has_right_real = any(candidate["players"] is not None for candidate in points[point_index + 1 :])
                    self.assertFalse(has_left_real and has_right_real)

        self.assertEqual(
            server._build_player_display_series(source_points, "30d").get("point_count"),
            31,
        )
        self.assertEqual(
            server._build_player_display_series(source_points, "3m").get("point_count"),
            14,
        )
        self.assertEqual(
            server._build_player_display_series(source_points, "1y").get("point_count"),
            13,
        )

    def test_player_display_series_uses_nearby_left_edge_seed_for_continuity(self) -> None:
        base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        source_points = [
            {
                "timestamp": (base + datetime.timedelta(days=29)).isoformat().replace("+00:00", "Z"),
                "ts": int((base + datetime.timedelta(days=29)).timestamp() * 1000),
                "players": 1000,
            },
            {
                "timestamp": (base + datetime.timedelta(days=35)).isoformat().replace("+00:00", "Z"),
                "ts": int((base + datetime.timedelta(days=35)).timestamp() * 1000),
                "players": 1200,
            },
            {
                "timestamp": (base + datetime.timedelta(days=60)).isoformat().replace("+00:00", "Z"),
                "ts": int((base + datetime.timedelta(days=60)).timestamp() * 1000),
                "players": 1300,
            },
        ]
        series = server._build_player_display_series(source_points, "30d")
        points = series.get("points", [])
        self.assertTrue(points)
        self.assertEqual(points[0]["players"], 1000)
        self.assertTrue(points[0]["is_interpolated"])
        self.assertTrue(any(point["players"] is not None for point in points[:5]))

    def test_player_display_series_all_range_keeps_coarse_monthly_shape(self) -> None:
        base = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        start = {
            "timestamp": base.isoformat().replace("+00:00", "Z"),
            "ts": int(base.timestamp() * 1000),
            "players": 100,
        }
        end_dt = base + datetime.timedelta(days=180)
        end = {
            "timestamp": end_dt.isoformat().replace("+00:00", "Z"),
            "ts": int(end_dt.timestamp() * 1000),
            "players": 700,
        }
        source_points = [start, end]

        series = server._build_player_display_series(source_points, "all")
        min_ts, max_ts = server._resolve_player_range_bounds(source_points, "all")
        bucket_starts = server._build_player_bucket_timestamps(
            min_ts,
            max_ts,
            "all",
        )
        points = series.get("points", [])

        self.assertEqual(len(points), len(bucket_starts))
        self.assertEqual(points[0]["players"], 100)
        self.assertEqual(points[-1]["ts"], bucket_starts[-1])
        self.assertFalse(any(point["players"] is None for point in points))
        self.assertTrue(any(point.get("is_interpolated") for point in points))

    def test_player_display_series_buckets_align_to_calendar_boundaries(self) -> None:
        base = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        source_points = []
        for day_offset in range(0, 420, 5):
            timestamp = base + datetime.timedelta(days=day_offset, hours=11, minutes=17)
            source_points.append(
                {
                    "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
                    "ts": int(timestamp.timestamp() * 1000),
                    "players": 500 + day_offset,
                }
            )

        checks = {
            "7d": lambda dt: dt.minute == 0 and dt.second == 0 and dt.microsecond == 0 and (dt.hour % 3 == 0),
            "30d": lambda dt: dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0,
            "3m": lambda dt: dt.weekday() == 0 and dt.hour == 0 and dt.minute == 0 and dt.second == 0,
            "1y": lambda dt: dt.day == 1 and dt.hour == 0 and dt.minute == 0 and dt.second == 0,
            "all": lambda dt: dt.day == 1 and dt.hour == 0 and dt.minute == 0 and dt.second == 0,
        }

        for range_key, predicate in checks.items():
            with self.subTest(range_key=range_key):
                series = server._build_player_display_series(source_points, range_key)
                points = series.get("points", [])
                self.assertTrue(points)
                for point in points:
                    dt = datetime.datetime.fromtimestamp(int(point["ts"]) / 1000, tz=datetime.timezone.utc)
                    self.assertTrue(predicate(dt))

    def test_game_resolve_contract_normalizes_review_summary_and_counts(self) -> None:
        self.assertIn('"review_summary": review_summary,', self.server_text)
        self.assertIn('normalized["review_summary"] = review_summary or review_score_summary or review_label', self.server_text)
        self.assertIn('normalized["review_count"] = normalized_review_count', self.server_text)
        self.assertIn('normalized["review_total_count"] = normalized_review_count', self.server_text)
        self.assertIn('"review_total_count": normalized_review_count,', self.server_text)
        self.assertIn('normalized["review"] = {', self.server_text)

    def test_game_page_uses_backend_player_series_and_review_order(self) -> None:
        self.assertIn("function getAuthoritativePlayerDisplaySeries(", self.game_text)
        self.assertIn("historyPayload?.display_series_by_range", self.game_text)
        self.assertIn("const displayPoints = backendPoints;", self.game_text)
        self.assertIn("const comparableDisplayedPoints = displayedPoints.filter((point) => Number.isFinite(Number(point?.players)));", self.game_text)
        self.assertIn("const comparableRealDisplayedPoints = comparableDisplayedPoints.filter((point) => !point?.isInterpolated);", self.game_text)
        self.assertIn("best = { index, delta, ratio, timestamp: current.timestamp, ts: current.ts };", self.game_text)
        self.assertIn("point.getProps([\"x\", \"y\"], true)", self.game_text)
        self.assertNotIn("function buildDisplayedPlayerSeries(", self.game_text)
        self.assertNotIn("Review summary pending", self.game_text)
        self.assertIn("if (review.summary) return review.summary;", self.game_text)
        self.assertIn("if (review.score_text) return review.score_text;", self.game_text)
        self.assertIn("if (review.label) return review.label;", self.game_text)


if __name__ == "__main__":
    unittest.main()
