from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_SERVER = ROOT / "api" / "server.py"


class SearchRelevanceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = API_SERVER.read_text(encoding="utf-8")

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
        self.assertIn("COALESCE(s.deal_score, 0) DESC", self.text)


if __name__ == "__main__":
    unittest.main()

