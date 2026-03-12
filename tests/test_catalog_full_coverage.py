from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_SERVER = ROOT / "api" / "server.py"
WEB_INDEX = ROOT / "web" / "index.html"
SETUP_DATABASE = ROOT / "setup_database.py"
MAIN_FILE = ROOT / "main.py"


class CatalogFullCoverageContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.api_text = API_SERVER.read_text(encoding="utf-8")
        cls.web_text = WEB_INDEX.read_text(encoding="utf-8")
        cls.setup_text = SETUP_DATABASE.read_text(encoding="utf-8")
        cls.main_text = MAIN_FILE.read_text(encoding="utf-8")

    def test_released_endpoint_uses_outer_join_and_released_filter(self) -> None:
        self.assertIn(".outerjoin(GameSnapshot, GameSnapshot.game_id == Game.id)", self.api_text)
        self.assertIn(".filter(Game.is_released == 1)", self.api_text)
        self.assertIn("GameSnapshot.is_upcoming.is_(False)", self.api_text)
        self.assertIn("GameSnapshot.is_upcoming.is_(None)", self.api_text)
        self.assertIn("GameSnapshot.game_id.is_(None)", self.api_text)

    def test_released_endpoint_handles_missing_snapshot_rows(self) -> None:
        self.assertIn("snapshot.steam_appid if snapshot else None", self.api_text)
        self.assertIn("\"price\": snapshot.latest_price if snapshot else None", self.api_text)
        self.assertIn("\"genres\": parse_csv_field(snapshot.genres) if snapshot else []", self.api_text)

    def test_released_endpoint_review_fields_fallback_to_game_values(self) -> None:
        self.assertIn("else game.review_score", self.api_text)
        self.assertIn("else game.review_score_label", self.api_text)
        self.assertIn("else game.review_total_count", self.api_text)

    def test_all_games_no_longer_excludes_featured_deals(self) -> None:
        self.assertIn("const filtered = source.filter(matchesFilters);", self.web_text)
        self.assertNotIn("biggestDealNames", self.web_text)

    def test_include_free_defaults_on_for_full_catalog(self) -> None:
        self.assertIn("includeFree: true", self.web_text)
        self.assertIn("ui.includeFreeCheckbox.checked = state.filters.includeFree;", self.web_text)

    def test_setup_database_adds_scaling_catalog_columns_and_indexes(self) -> None:
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS developer TEXT;", self.setup_text)
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS publisher TEXT;", self.setup_text)
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS next_refresh_at TIMESTAMPTZ;", self.setup_text)
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS priority_tier TEXT;", self.setup_text)
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS last_player_count INTEGER;", self.setup_text)
        self.assertIn("ALTER TABLE games ADD COLUMN IF NOT EXISTS popularity_score DOUBLE PRECISION DEFAULT 0;", self.setup_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_games_developer ON games (developer);", self.setup_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_games_publisher ON games (publisher);", self.setup_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_games_is_released_name ON games (is_released, name);", self.setup_text)
        self.assertIn("CREATE INDEX IF NOT EXISTS ix_games_next_refresh_at ON games (next_refresh_at);", self.setup_text)

    def test_ingestion_uses_priority_tier_schedule_and_next_refresh(self) -> None:
        self.assertIn("TRACK_HOT_MIN_PLAYERS", self.main_text)
        self.assertIn("TRACK_MEDIUM_MIN_PLAYERS", self.main_text)
        self.assertIn("def apply_ingestion_schedule", self.main_text)
        self.assertIn("Game.next_refresh_at.is_(None)", self.main_text)
        self.assertIn("Game.next_refresh_at <= now", self.main_text)

    def test_search_queries_support_developer_and_publisher_fields(self) -> None:
        self.assertIn("g.developer", self.api_text)
        self.assertIn("g.publisher", self.api_text)
        self.assertIn("Game.developer.ilike", self.api_text)
        self.assertIn("Game.publisher.ilike", self.api_text)

    def test_platform_filters_include_extended_options_and_virtual_predicates(self) -> None:
        self.assertIn("EXTENDED_PLATFORM_FILTER_OPTIONS = (\"Steam Deck\", \"VR Compatibility\")", self.api_text)
        self.assertIn("def _build_platform_filter_predicate(platform_value: str):", self.api_text)
        self.assertIn("GameSnapshot.tags.ilike(\"%deck verified%\")", self.api_text)
        self.assertIn("GameSnapshot.tags.ilike(\"%virtual reality%\")", self.api_text)
        self.assertIn("platform_predicate = _build_platform_filter_predicate(platform)", self.api_text)


if __name__ == "__main__":
    unittest.main()
