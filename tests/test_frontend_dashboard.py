from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "web" / "index.html"


class DashboardFrontendContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = INDEX_HTML.read_text(encoding="utf-8")

    def test_all_games_label_and_search_empty_copy(self) -> None:
        self.assertIn("All Games", self.text)
        self.assertIn("No matching games found.", self.text)
        self.assertNotIn("No matching released games found", self.text)

    def test_homepage_branding_uses_gameden_market_radar_identity(self) -> None:
        self.assertIn("<title>GameDen.gg</title>", self.text)
        self.assertIn('name: "GameDen.gg"', self.text)
        self.assertIn('subtitle: "The Game Market Radar"', self.text)
        self.assertIn('id="heroEyebrow">GameDen.gg: The Game Market Radar</div>', self.text)
        self.assertNotIn("NEWWORLD", self.text)

    def test_analytics_section_is_cleanly_separated_from_alerts(self) -> None:
        self.assertIn("Analytics", self.text)
        self.assertIn("Top Played", self.text)
        self.assertIn("Trending Activity", self.text)
        self.assertIn("Active Player Leaders", self.text)
        self.assertNotIn('<h3 class="analytics-group-title">Alerts</h3>', self.text)
        self.assertIn("Alerts & Deal Signals", self.text)

    def test_market_radar_section_and_endpoint_wiring_exist(self) -> None:
        self.assertIn("Market Radar", self.text)
        self.assertIn('id="dealRadarFeed"', self.text)
        self.assertIn('id="dealRadarMoreBtn"', self.text)
        self.assertIn('fetchJson("/api/market-radar?limit=50")', self.text)
        self.assertIn('fetchJson("/api/deal-radar?limit=50")', self.text)
        self.assertIn("function renderDealRadar()", self.text)
        self.assertIn("function dealRadarSignalPriority(signalType)", self.text)
        self.assertIn("function compareDealRadarItems(a, b)", self.text)
        self.assertIn("const bestByGame = new Map();", self.text)

    def test_market_radar_signal_labels_cover_new_types(self) -> None:
        self.assertIn('case "SALE_STARTED":', self.text)
        self.assertIn('case "BIG_DISCOUNT":', self.text)
        self.assertIn('case "NEAR_HISTORICAL_LOW":', self.text)
        self.assertIn('return "Sale Started";', self.text)
        self.assertIn('return "Major Discount";', self.text)
        self.assertIn('return "Near Historical Low";', self.text)

    def test_catalog_loader_uses_bounded_concurrency_and_page_cache(self) -> None:
        self.assertIn("const CATALOG_FETCH_CONCURRENCY = 3;", self.text)
        self.assertIn("pageCache: new Map()", self.text)
        self.assertIn("inflightControllers: new Map()", self.text)
        self.assertIn("function getCatalogPageCache()", self.text)
        self.assertIn("function getCatalogInFlightControllers()", self.text)
        self.assertIn("await Promise.all(pagesToFetch.map(async (page) => {", self.text)

    def test_catalog_loader_abort_and_stale_guard_exist(self) -> None:
        self.assertIn("function cancelCatalogLoad()", self.text)
        self.assertIn("function abortCatalogInFlightControllers()", self.text)
        self.assertIn("state.catalog.controller.abort();", self.text)
        self.assertIn("controller.abort();", self.text)
        self.assertIn("function isCurrentCatalogRequest(", self.text)
        self.assertIn("function isCurrentCatalogContext(", self.text)
        self.assertIn("window.addEventListener(\"pagehide\", stop);", self.text)
        self.assertIn("window.addEventListener(\"beforeunload\", stop);", self.text)

    def test_catalog_merge_is_deterministic_and_deduped(self) -> None:
        self.assertIn("function getHighestContiguousCatalogPage", self.text)
        self.assertIn("function rebuildCatalogFromPageCache()", self.text)
        self.assertIn("for (let page = 1; page <= highestContiguous; page += 1)", self.text)
        self.assertIn("state.allGames = uniqueGamesByName(merged);", self.text)

    def test_discovery_order_places_market_radar_below_personal_panels(self) -> None:
        self.assertIn("function applyDiscoverySectionOrder()", self.text)
        self.assertIn(
            '"bestDealsPanel",\n                "worthBuyingPanel"',
            self.text,
        )
        self.assertIn(
            'const sideOrder = ["wishlistPanel", "watchlistPanel", "dealRadarPanel", "alertsPanel"];',
            self.text,
        )
        self.assertIn(
            "const wishlistLoading = !!state.personalListStatus?.wishlistLoading;",
            self.text,
        )
        self.assertIn(
            "const watchlistLoading = !!state.personalListStatus?.watchlistLoading;",
            self.text,
        )
        self.assertIn(
            "setPanelVisibility(ui.wishlistPanel, wishlistItems.length > 0 || wishlistLoading || wishlistError);",
            self.text,
        )
        self.assertIn(
            "setPanelVisibility(ui.watchlistPanel, watchlistItems.length > 0 || watchlistLoading || watchlistError);",
            self.text,
        )

    def test_seasonal_mode_copy_supports_active_and_potential(self) -> None:
        self.assertIn("On-Sale Games", self.text)
        self.assertIn("Potential Sales", self.text)
        self.assertIn("No currently discounted games found for the active seasonal sale.", self.text)

    def test_seasonal_row_nav_controls_are_expansion_and_overflow_gated(self) -> None:
        self.assertIn("function updateSeasonalRowNavVisibility", self.text)
        self.assertIn("const shouldShow = expanded && panelVisible && contentVisible && hasOverflow;", self.text)
        self.assertIn('rowNav.classList.toggle("hidden", !shouldShow);', self.text)
        self.assertIn('rowNav.setAttribute("aria-hidden", shouldShow ? "false" : "true");', self.text)

    def test_scroll_behavior_dims_search_and_filters_without_hiding(self) -> None:
        self.assertIn(".search-bar-wrap.scrolled", self.text)
        self.assertIn(".global-filters-shell.scrolled", self.text)
        self.assertIn("function wireScrollBehavior()", self.text)
        self.assertIn('ui.searchBarWrap.classList.toggle("scrolled", isScrolled);', self.text)
        self.assertIn('ui.filtersShell.classList.toggle("scrolled", isScrolled);', self.text)
        self.assertIn('ui.filtersShell.classList.remove("hidden-on-scroll");', self.text)

    def test_filter_top_row_promotes_genre_and_platform(self) -> None:
        self.assertIn('<label for="genreSelect">Genre</label>', self.text)
        self.assertIn('<label for="platformSelect">Platform</label>', self.text)
        self.assertIn("grid-template-columns: minmax(120px, 170px) minmax(150px, 1fr) minmax(170px, 1fr)", self.text)
        self.assertIn("const EXTENDED_PLATFORM_FILTER_OPTIONS = Object.freeze([\"Steam Deck\", \"VR Compatibility\"]);", self.text)

    def test_filter_options_have_cache_and_endpoint_fallback(self) -> None:
        self.assertIn("dashboardData.filters = buildFilterOptions(", self.text)
        self.assertIn('await fetchJson("/games/filters").catch(() => ({}));', self.text)
        self.assertIn("function gameMatchesPlatformFilter(game, platformFilter)", self.text)

    def test_recently_updated_shows_first_three_with_expand_toggle(self) -> None:
        self.assertIn('id="recentlyUpdatedToggleBtn"', self.text)
        self.assertIn("const initialVisible = 3;", self.text)
        self.assertIn("wireRecentlyUpdatedToggle();", self.text)

    def test_search_review_label_mapping_and_fallback(self) -> None:
        self.assertIn("function getReviewLabel(game)", self.text)
        self.assertIn("row.review_score_label ?? row.review_label ?? row.review_summary ?? row.reviewSummary ?? null", self.text)
        self.assertIn("Review data unavailable", self.text)

    def test_homepage_personal_lists_are_user_scoped_and_api_hydrated(self) -> None:
        self.assertIn("state.wishlist = [];", self.text)
        self.assertIn("state.watchlist = [];", self.text)
        self.assertIn("await Promise.all([", self.text)
        self.assertIn("loadUserWishlist().catch((error) => {", self.text)
        self.assertIn("loadUserWatchlist().catch((error) => {", self.text)
        self.assertIn("state.watchlist = items;", self.text)

    def test_all_games_section_uses_featured_rotation_and_directory_link(self) -> None:
        self.assertIn('id="releasedGamesDirectoryBtn"', self.text)
        self.assertIn("function getFeaturedAllGames()", self.text)
        self.assertIn("function scoreTopSellerCandidate(game)", self.text)
        self.assertIn("function updateAllGamesDirectoryLink()", self.text)
        self.assertNotIn('id="releasedGamesToggleBtn"', self.text)

    def test_biggest_deals_section_removed_from_homepage(self) -> None:
        self.assertNotIn('<h2 class="panel-title">Biggest Deals</h2>', self.text)
        self.assertNotIn("function renderBiggestDeals()", self.text)
        self.assertNotIn('id="biggestDealsGrid"', self.text)

    def test_wishlist_watchlist_panels_and_navigation_controls_exist(self) -> None:
        self.assertIn('id="wishlistPanel"', self.text)
        self.assertIn('id="watchlistPanel"', self.text)
        self.assertIn('id="wishlistList"', self.text)
        self.assertIn('id="watchlistList"', self.text)
        self.assertIn('href="/watchlist"', self.text)
        self.assertIn('const sideOrder = ["wishlistPanel", "watchlistPanel", "dealRadarPanel", "alertsPanel"];', self.text)

    def test_card_actions_prevent_overlay_navigation_and_still_toggle_lists(self) -> None:
        self.assertIn('.game-card > .card-action-row {', self.text)
        self.assertIn('document.addEventListener("pointerdown", (event) => {', self.text)
        self.assertIn('target.closest("button[data-action]")', self.text)
        self.assertIn('event.stopImmediatePropagation();', self.text)
        self.assertIn('if (action === "wishlist") {', self.text)
        self.assertIn('if (action === "watchlist") {', self.text)

    def test_game_links_use_shared_detail_href_with_game_id_support(self) -> None:
        self.assertIn("function buildGameDetailHref(gameLike)", self.text)
        self.assertIn('params.set("game_id", String(gameId));', self.text)
        self.assertIn('const detailHref = buildGameDetailHref(game);', self.text)

    def test_empty_state_copy_and_trending_labels_are_human_readable(self) -> None:
        self.assertIn("No new all-time low prices in the last 24 hours.", self.text)
        self.assertIn("No new deals have appeared since your last visit.", self.text)
        self.assertIn('"Players increasing"', self.text)
        self.assertIn('"Players decreasing"', self.text)
        self.assertIn('"Player count stable"', self.text)
        self.assertNotIn('"steady"', self.text)

    def test_top_summary_stat_grid_removed(self) -> None:
        self.assertNotIn('<section class="stats-grid">', self.text)
        self.assertNotIn('id="hero-released-count"', self.text)


if __name__ == "__main__":
    unittest.main()
