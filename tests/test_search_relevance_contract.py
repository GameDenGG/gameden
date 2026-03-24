from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_SERVER = ROOT / "api" / "server.py"
WEB_INDEX = ROOT / "web" / "index.html"


class SearchRelevanceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = API_SERVER.read_text(encoding="utf-8")
        cls.web_text = WEB_INDEX.read_text(encoding="utf-8")

    def test_games_released_uses_name_relevance_order_when_q_present(self) -> None:
        self.assertIn("def _build_name_relevance_order_columns(search_text: str, include_similarity: bool):", self.text)
        self.assertIn("if search_text:", self.text)
        self.assertIn("relevance_order_columns = _build_name_relevance_order_columns(search_text, include_similarity=include_similarity)", self.text)
        self.assertIn("released_query = released_query.order_by(*relevance_order_columns, *order_by_columns)", self.text)

    def test_games_released_has_similarity_fallback(self) -> None:
        self.assertIn("can_use_similarity = (", self.text)
        self.assertIn("Falling back to non-similarity released search ranking", self.text)
        self.assertIn("query_with_order = build_released_query(include_similarity=False)", self.text)

    def test_search_endpoint_prefers_exact_and_prefix_before_tiebreakers(self) -> None:
        self.assertIn("CASE WHEN lower(g.name) = :normalized_q THEN 0 ELSE 1 END", self.text)
        self.assertIn("CASE WHEN lower(g.name) LIKE (:normalized_q || '%') THEN 0 ELSE 1 END", self.text)
        self.assertIn("sim DESC", self.text)
        self.assertNotIn("COALESCE(s.deal_score, 0) DESC", self.text)
        self.assertNotIn("lexical_score += deal_score", self.text)
        self.assertIn("activity_tiebreak = popularity_score", self.text)
        self.assertIn("if lexical_tier <= 4:", self.text)
        self.assertIn("math.log10(current_players + 1.0)", self.text)
        self.assertIn("math.log10(upcoming_hot_score + 1.0)", self.text)
        self.assertIn("should_run_broad_pass = len(rows) < min(2, normalized_limit)", self.text)

    def test_home_search_dropdown_uses_search_endpoint(self) -> None:
        self.assertIn("function refreshSearchResults(options = {})", self.web_text)
        self.assertIn("const payload = await fetchJson(`/search?${params.toString()}`", self.web_text)
        self.assertNotIn("fetchJson(`/games/search?", self.web_text)

    def test_home_search_dropdown_rows_use_anchor_navigation(self) -> None:
        self.assertIn("function updateSearchActiveIndexStateOnly(nextIndex)", self.web_text)
        self.assertIn("function findSearchResultOptionFromEvent(event)", self.web_text)
        self.assertIn("const rowHref = String(option.getAttribute(\"href\") || option.href || \"\").trim();", self.web_text)
        self.assertIn("const option = findSearchResultOptionFromEvent(event);", self.web_text)
        self.assertIn("navigateToSearchHref(rowHref);", self.web_text)
        self.assertNotIn("renderedNode.click();", self.web_text)

    def test_frontend_search_fallback_does_not_use_deal_score(self) -> None:
        start = self.web_text.index("function searchFallbackPool(query, limit = 20)")
        end = self.web_text.index("function resolveSearchResultHref(row)", start)
        fallback_source = self.web_text[start:end]
        self.assertNotIn("deal_score", fallback_source)
        self.assertIn("popularityScore", fallback_source)


if __name__ == "__main__":
    unittest.main()
