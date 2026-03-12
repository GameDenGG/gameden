from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AgentGuardrailContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root_agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
        cls.web_agents = (ROOT / "web" / "AGENTS.md").read_text(encoding="utf-8")

    def test_root_agents_protects_major_ui_structures(self) -> None:
        self.assertIn(
            "Do not remove major UI sections, menus, panels, routes, or primary navigation unless the user explicitly requests removal.",
            self.root_agents,
        )
        self.assertIn(
            "Treat Wishlist, Watchlist, and core discovery sections as protected structures; prefer repositioning/refining over deletion.",
            self.root_agents,
        )

    def test_web_agents_protects_homepage_menus_and_panels(self) -> None:
        self.assertIn(
            "do not remove major homepage/menu sections (including Wishlist/Watchlist and key discovery panels) unless removal is explicitly requested",
            self.web_agents,
        )
        self.assertIn(
            "when adjusting layout, preserve existing menu/panel functionality and interactions unless the task explicitly changes them",
            self.web_agents,
        )


if __name__ == "__main__":
    unittest.main()
