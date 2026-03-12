from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WatchlistAndDiscoveryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server_text = (ROOT / "api" / "server.py").read_text(encoding="utf-8")
        cls.models_text = (ROOT / "database" / "models.py").read_text(encoding="utf-8")
        cls.snapshot_text = (ROOT / "jobs" / "refresh_snapshots.py").read_text(encoding="utf-8")
        cls.home_text = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
        cls.game_text = (ROOT / "web" / "game.html").read_text(encoding="utf-8")
        cls.watchlist_text = (ROOT / "web" / "watchlist.html").read_text(encoding="utf-8")

    def test_watchlist_api_endpoints_exist(self) -> None:
        self.assertIn('@app.get("/api/watchlist")', self.server_text)
        self.assertIn('@app.post("/api/watchlist")', self.server_text)
        self.assertIn('@app.delete("/api/watchlist/{game_id}")', self.server_text)

    def test_watchlist_page_route_exists(self) -> None:
        self.assertIn('@app.get("/watchlist")', self.server_text)
        self.assertIn('return FileResponse("web/watchlist.html")', self.server_text)

    def test_buy_score_is_modeled_and_written(self) -> None:
        self.assertIn("buy_score = Column(Float, default=0.0)", self.models_text)
        self.assertIn("snapshot.buy_score = worth_buying_score", self.snapshot_text)

    def test_alert_signal_model_and_worker_contract(self) -> None:
        self.assertIn('class Alert(Base):', self.models_text)
        self.assertIn('__tablename__ = "alerts"', self.models_text)
        self.assertIn("EVENT_TO_ALERT_TYPE", self.snapshot_text)
        self.assertIn("def upsert_alert_signal(", self.snapshot_text)
        self.assertIn("ALERT_DEDUPE_HOURS", self.snapshot_text)
        self.assertIn('"alertSignals": alert_signals', self.snapshot_text)
        self.assertIn("def _build_deal_radar_feed(", self.snapshot_text)
        self.assertIn('"dealRadar": deal_radar', self.snapshot_text)

    def test_homepage_watchlist_uses_api_contract(self) -> None:
        self.assertIn('fetchJson(`/api/watchlist?user_id=${encodeURIComponent(CURRENT_USER_ID)}`)', self.home_text)
        self.assertIn('await fetchJson("/api/watchlist", {', self.home_text)
        self.assertIn('`/api/watchlist/${encodeURIComponent(target.game_id)}?user_id=', self.home_text)
        self.assertIn("getStructuredAlertCandidates()", self.home_text)
        self.assertIn("state.alertSignals = alertSignals;", self.home_text)

    def test_game_page_has_deep_discovery_sections(self) -> None:
        self.assertIn('id="watchButton"', self.game_text)
        self.assertIn('id="playerChart"', self.game_text)
        self.assertIn('id="relatedGames"', self.game_text)
        self.assertIn('const gameIdParam = getQueryParam("game_id");', self.game_text)
        self.assertIn('fetchJson(`/games/${requestedGameId}`)', self.game_text)
        self.assertIn("fetchJson(`/games/by-name?game_name=${encodeURIComponent(gameName)}`)", self.game_text)
        self.assertIn("fetchJson(`/games/${gameId}/related?limit=8`)", self.game_text)
        self.assertIn("fetchJson(`/games/${gameId}/player-history`)", self.game_text)
        self.assertNotIn("/games/detail?game_name=", self.game_text)
        self.assertNotIn("â€”", self.game_text)

    def test_watchlist_page_uses_watchlist_api(self) -> None:
        self.assertIn("GameDen.gg Watchlist", self.watchlist_text)
        self.assertIn('localStorage.setItem(WATCHLIST_STORAGE_KEY, fallback);', self.watchlist_text)
        self.assertIn("fetchJson(`/api/watchlist?user_id=", self.watchlist_text)
        self.assertIn("fetchJson(`/api/watchlist/${encodeURIComponent(gameId)}?user_id=", self.watchlist_text)
        self.assertIn("fetchJson(`/api/alerts?user_id=", self.watchlist_text)

    def test_watchlist_alert_feed_endpoint_exists(self) -> None:
        self.assertIn('@app.get("/api/alerts")', self.server_text)
        self.assertIn("def build_user_watchlist_alert_feed(", self.server_text)
        self.assertIn(".join(", self.server_text)
        self.assertIn("Watchlist.game_id == Alert.game_id", self.server_text)

    def test_deal_radar_endpoint_exists(self) -> None:
        self.assertIn('@app.get("/api/deal-radar")', self.server_text)
        self.assertIn("def list_deal_radar_feed(", self.server_text)
        self.assertIn('DEAL_RADAR_CACHE_KEY = "home:deal_radar"', self.server_text)


if __name__ == "__main__":
    unittest.main()
