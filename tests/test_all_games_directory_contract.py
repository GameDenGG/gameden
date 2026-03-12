from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ALL_RESULTS = ROOT / "web" / "all-results.html"
INDEX_HTML = ROOT / "web" / "index.html"


class AllGamesDirectoryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = ALL_RESULTS.read_text(encoding="utf-8")
        cls.index_text = INDEX_HTML.read_text(encoding="utf-8")

    def test_directory_metadata_and_navigation_are_present(self) -> None:
        self.assertIn("<title>GameDen.gg - All Games Directory</title>", self.text)
        self.assertIn('<link rel="canonical" href="https://gameden.gg/web/all-results.html">', self.text)
        self.assertIn('<meta property="og:url" content="https://gameden.gg/web/all-results.html">', self.text)
        self.assertIn('href="/web/index.html">Back to Dashboard</a>', self.text)

    def test_directory_uses_released_catalog_endpoint_and_filters_endpoint(self) -> None:
        self.assertIn('return `/games/released?${q.toString()}`;', self.text)
        self.assertIn('const payload = await fetchJson("/games/filters");', self.text)
        self.assertIn('q.set("sort", state.filters.sort || "alpha-asc");', self.text)
        self.assertIn('state.filters.sort = s || (state.view === RELEASED_VIEW ? "alpha-asc" : "deal-score");', self.text)

    def test_directory_promotes_genre_and_platform_filters(self) -> None:
        self.assertIn('<label for="genre">Genre</label>', self.text)
        self.assertIn('<label for="platform">Platform</label>', self.text)
        self.assertIn('const EXTENDED_PLATFORM_FILTER_OPTIONS = ["Steam Deck", "VR Compatibility"];', self.text)

    def test_homepage_show_all_link_for_directory_forces_alpha_default(self) -> None:
        self.assertIn('const releasedSortValue = view === "released-games"', self.index_text)
        self.assertIn('? "alpha-asc"', self.index_text)

    def test_directory_contains_no_mojibake_literals(self) -> None:
        self.assertNotIn("â", self.text)
        self.assertNotIn("Ã", self.text)
        self.assertNotIn("ðŸ", self.text)
        self.assertNotIn("â†", self.text)


if __name__ == "__main__":
    unittest.main()
