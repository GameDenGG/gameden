import json
import datetime
import math
import unittest
from collections import Counter

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.models import Alert, Base, DashboardCache, GameSnapshot, LatestGamePrice
from jobs.refresh_snapshots import (
    DEAL_RADAR_DIVERSITY_WINDOW,
    DEAL_RADAR_MAX_PER_SIGNAL,
    DEAL_RADAR_MAX_SIGNAL_SHARE,
    _build_deal_radar_feed,
    rebuild_dashboard_cache,
)


def _build_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)()


class DashboardCacheAlignmentTests(unittest.TestCase):
    def test_dashboard_cache_model_maps_pk_to_key_column(self):
        self.assertIn("key", DashboardCache.__table__.c)
        self.assertTrue(DashboardCache.__table__.c["key"].primary_key)
        self.assertEqual(DashboardCache.cache_key.key, "cache_key")

    def test_dashboard_cache_construct_with_cache_key_sets_key(self):
        row = DashboardCache(cache_key="home", payload="{}", updated_at=None)
        self.assertEqual(row.key, "home")
        self.assertEqual(row.cache_key, "home")

    def test_rebuild_dashboard_cache_writes_rows_and_reads_by_cache_key_alias(self):
        session = _build_session()
        try:
            # Minimal snapshot seed so payload sections can serialize.
            session.add(
                GameSnapshot(
                    game_id=1,
                    game_name="Test Game",
                    steam_appid="1",
                    latest_price=9.99,
                    latest_original_price=19.99,
                    latest_discount_percent=50,
                    review_score=90,
                    review_score_label="Very Positive",
                    review_count=10000,
                    current_players=1500,
                    avg_player_count=1100,
                    player_change=300,
                    daily_peak=1800,
                    momentum_score=70.0,
                    is_upcoming=False,
                    is_released=1,
                )
            )
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()

            home = session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first()
            self.assertIsNotNone(home)
            parsed = json.loads(home.payload)
            self.assertIn("dealRanked", parsed)
            self.assertIn("topPlayed", parsed)
            self.assertIn("trending", parsed)
            self.assertIn("leaderboard", parsed)
            self.assertIn("dealRadar", parsed)
            self.assertIn("filters", parsed)
            self.assertIn("platforms", parsed["filters"])
            self.assertIn("Steam Deck", parsed["filters"]["platforms"])
            self.assertIn("VR Compatibility", parsed["filters"]["platforms"])
            self.assertTrue(isinstance(parsed.get("dealRadar"), list))
            self.assertEqual(parsed["topPlayed"][0]["daily_peak"], 1800)
            self.assertEqual(parsed["topPlayed"][0]["avg_30d"], 1100)
            self.assertEqual(parsed["topPlayed"][0]["review_score_label"], "Very Positive")

            trending = session.query(DashboardCache).filter(DashboardCache.cache_key == "home:trending").first()
            self.assertIsNotNone(trending)
            deal_radar = session.query(DashboardCache).filter(DashboardCache.cache_key == "home:deal_radar").first()
            self.assertIsNotNone(deal_radar)
            parsed_radar = json.loads(deal_radar.payload)
            self.assertIn("items", parsed_radar)
        finally:
            session.close()

    def test_rebuild_dashboard_cache_diversifies_primary_deal_rails(self):
        session = _build_session()
        try:
            rows = []
            for idx in range(1, 90):
                rows.append(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Game {idx}",
                        steam_appid=str(idx),
                        latest_price=19.99 + (idx / 100.0),
                        latest_original_price=49.99 + (idx / 100.0),
                        latest_discount_percent=max(5, 95 - idx),
                        review_score=max(50, 98 - (idx % 40)),
                        review_score_label="Very Positive",
                        review_count=5000 + idx,
                        current_players=max(100, 20000 - idx * 120),
                        avg_player_count=max(90, 18000 - idx * 100),
                        player_change=max(1, 4000 - idx * 20),
                        daily_peak=max(120, 22000 - idx * 130),
                        momentum_score=float(300 - idx),
                        worth_buying_score=float(500 - idx),
                        recommended_score=float(480 - idx),
                        deal_score=float(520 - idx),
                        is_upcoming=False,
                        is_released=1,
                    )
                )
            session.add_all(rows)
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()

            home = session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first()
            self.assertIsNotNone(home)
            payload = json.loads(home.payload)

            def top_ids(key: str, count: int = 8) -> set[int]:
                return {int(item["game_id"]) for item in payload.get(key, [])[:count]}

            deal_ranked_ids = top_ids("dealRanked")
            worth_buying_ids = top_ids("worthBuyingNow")
            recommended_ids = top_ids("recommendedDeals")
            biggest_ids = top_ids("biggestDeals")
            trending_ids = top_ids("trendingDeals")

            self.assertTrue(deal_ranked_ids)
            self.assertTrue(worth_buying_ids)
            self.assertTrue(recommended_ids)
            self.assertTrue(biggest_ids)
            self.assertTrue(trending_ids)

            self.assertTrue(deal_ranked_ids.isdisjoint(worth_buying_ids))
            self.assertTrue(deal_ranked_ids.isdisjoint(recommended_ids))
            self.assertTrue(worth_buying_ids.isdisjoint(recommended_ids))
            self.assertTrue(recommended_ids.isdisjoint(biggest_ids))
            self.assertTrue(biggest_ids.isdisjoint(trending_ids))
        finally:
            session.close()

    def test_rebuild_dashboard_cache_diversity_is_deterministic(self):
        session = _build_session()
        try:
            for idx in range(1, 70):
                session.add(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Deterministic {idx}",
                        steam_appid=str(idx),
                        latest_price=14.99 + (idx / 100.0),
                        latest_original_price=39.99 + (idx / 100.0),
                        latest_discount_percent=max(5, 90 - idx),
                        review_score=max(55, 95 - (idx % 30)),
                        review_score_label="Mostly Positive",
                        review_count=2000 + idx,
                        current_players=max(80, 12000 - idx * 90),
                        avg_player_count=max(75, 10000 - idx * 80),
                        player_change=max(1, 1800 - idx * 15),
                        daily_peak=max(90, 14000 - idx * 100),
                        momentum_score=float(220 - idx),
                        worth_buying_score=float(260 - idx),
                        recommended_score=float(240 - idx),
                        deal_score=float(280 - idx),
                        is_upcoming=False,
                        is_released=1,
                    )
                )
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()
            first_payload = json.loads(
                session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first().payload
            )

            rebuild_dashboard_cache(session)
            session.commit()
            second_payload = json.loads(
                session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first().payload
            )

            for key in ("dealRanked", "worthBuyingNow", "recommendedDeals", "biggestDeals", "trendingDeals"):
                first_ids = [int(item["game_id"]) for item in first_payload.get(key, [])[:24]]
                second_ids = [int(item["game_id"]) for item in second_payload.get(key, [])[:24]]
                self.assertEqual(first_ids, second_ids)
        finally:
            session.close()

    def test_rebuild_dashboard_cache_preserves_deal_rank_quality_for_lead_slots(self):
        session = _build_session()
        try:
            for idx in range(1, 80):
                session.add(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Quality {idx}",
                        steam_appid=str(idx),
                        latest_price=9.99 + (idx / 100.0),
                        latest_original_price=59.99 + (idx / 100.0),
                        latest_discount_percent=max(1, 99 - idx),
                        review_score=80 + (idx % 10),
                        review_score_label="Very Positive",
                        review_count=9000 + idx,
                        current_players=max(100, 30000 - idx * 200),
                        avg_player_count=max(90, 25000 - idx * 170),
                        player_change=max(1, 3500 - idx * 25),
                        daily_peak=max(120, 35000 - idx * 210),
                        momentum_score=float(500 - idx),
                        worth_buying_score=float(520 - idx),
                        recommended_score=float(510 - idx),
                        deal_score=float(600 - idx),
                        is_upcoming=False,
                        is_released=1,
                    )
                )
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()
            payload = json.loads(session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first().payload)

            deal_ranked = payload.get("dealRanked", [])[:12]
            deal_ranked_ids = [int(item["game_id"]) for item in deal_ranked]
            self.assertEqual(deal_ranked_ids, list(range(1, 13)))

            deal_scores = [float(item.get("deal_score") or 0.0) for item in deal_ranked]
            self.assertEqual(deal_scores, sorted(deal_scores, reverse=True))
        finally:
            session.close()

    def test_rebuild_dashboard_cache_limits_cross_rail_repeat_exposure(self):
        session = _build_session()
        try:
            for idx in range(1, 120):
                session.add(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Exposure {idx}",
                        steam_appid=str(idx),
                        latest_price=7.99 + (idx / 100.0),
                        latest_original_price=59.99 + (idx / 100.0),
                        latest_discount_percent=max(1, 99 - idx),
                        review_score=70 + (idx % 25),
                        review_score_label="Very Positive",
                        review_count=3000 + idx,
                        current_players=max(100, 26000 - idx * 150),
                        avg_player_count=max(90, 22000 - idx * 130),
                        player_change=max(1, 2800 - idx * 20),
                        daily_peak=max(120, 32000 - idx * 190),
                        momentum_score=float(450 - idx),
                        worth_buying_score=float(470 - idx),
                        recommended_score=float(460 - idx),
                        deal_score=float(520 - idx),
                        buy_score=float(460 - idx),
                        deal_opportunity_score=float(440 - idx),
                        is_upcoming=False,
                        is_released=1,
                    )
                )
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()
            payload = json.loads(session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first().payload)

            rail_keys = (
                "deal_opportunities",
                "buyNowPicks",
                "biggest_discounts",
                "worth_buying_now",
                "trending_now",
                "opportunity_radar",
                "dealRanked",
                "wait_picks",
            )
            top_window = 8
            exposure_counter: Counter[int] = Counter()
            total_slots = 0

            for key in rail_keys:
                rows = payload.get(key, [])[:top_window]
                for row in rows:
                    game_id = int(row.get("game_id") or 0)
                    if game_id <= 0:
                        continue
                    total_slots += 1
                    exposure_counter[game_id] += 1

            self.assertGreater(total_slots, 0)
            self.assertTrue(exposure_counter)
            self.assertLessEqual(max(exposure_counter.values()), 2)
            unique_ratio = len(exposure_counter) / float(total_slots)
            self.assertGreaterEqual(unique_ratio, 0.55)
        finally:
            session.close()

    def test_rebuild_dashboard_cache_catalog_seed_prioritizes_non_rail_inventory(self):
        session = _build_session()
        try:
            for idx in range(1, 110):
                session.add(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Catalog Seed {idx}",
                        steam_appid=str(idx),
                        latest_price=8.99 + (idx / 100.0),
                        latest_original_price=49.99 + (idx / 100.0),
                        latest_discount_percent=max(1, 98 - idx),
                        review_score=72 + (idx % 22),
                        review_score_label="Very Positive",
                        review_count=2500 + idx,
                        current_players=max(80, 18000 - idx * 110),
                        avg_player_count=max(70, 16000 - idx * 95),
                        player_change=max(1, 2200 - idx * 14),
                        daily_peak=max(90, 23000 - idx * 140),
                        momentum_score=float(360 - idx),
                        worth_buying_score=float(390 - idx),
                        recommended_score=float(380 - idx),
                        deal_score=float(430 - idx),
                        buy_score=float(370 - idx),
                        deal_opportunity_score=float(350 - idx),
                        is_upcoming=False,
                        is_released=1,
                    )
                )
            session.commit()

            rebuild_dashboard_cache(session)
            session.commit()
            payload = json.loads(session.query(DashboardCache).filter(DashboardCache.cache_key == "home").first().payload)

            lead_rail_ids: set[int] = set()
            for key in (
                "deal_opportunities",
                "buyNowPicks",
                "biggest_discounts",
                "worth_buying_now",
                "trending_now",
                "opportunity_radar",
                "dealRanked",
            ):
                for row in payload.get(key, [])[:8]:
                    game_id = int(row.get("game_id") or 0)
                    if game_id > 0:
                        lead_rail_ids.add(game_id)

            released_seed_ids = [
                int(row.get("game_id") or 0)
                for row in payload.get("releasedGames", [])[:24]
                if int(row.get("game_id") or 0) > 0
            ]
            self.assertTrue(released_seed_ids)
            overlap = [game_id for game_id in released_seed_ids if game_id in lead_rail_ids]
            self.assertLessEqual(len(overlap), 8)
        finally:
            session.close()

    def test_deal_radar_feed_limits_duplicate_games_and_balances_signals(self):
        session = _build_session()
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            for idx in range(1, 46):
                discount = 0
                historical_status = None
                player_change = 0
                short_term_trend = 0.0
                trending_score = 0.0
                current_players = 220 + idx * 11

                if 9 <= idx <= 16:
                    discount = 72
                elif 17 <= idx <= 24:
                    discount = 18
                    historical_status = "near_historical_low"
                elif 25 <= idx <= 32:
                    player_change = 450 - idx
                    short_term_trend = 0.65
                    trending_score = 80 - idx
                    current_players = 900 + idx * 20
                elif 33 <= idx <= 40:
                    current_players = 3200 + idx * 30

                session.add(
                    GameSnapshot(
                        game_id=idx,
                        game_name=f"Radar {idx}",
                        steam_appid=str(idx),
                        latest_price=19.99,
                        latest_original_price=49.99 if discount > 0 else 19.99,
                        latest_discount_percent=discount if discount > 0 else 0,
                        current_players=current_players,
                        avg_player_count=max(100, current_players - 80),
                        player_change=player_change,
                        short_term_player_trend=short_term_trend,
                        trending_score=trending_score,
                        historical_status=historical_status,
                        worth_buying_score=70.0 if discount >= 45 else 45.0,
                        buy_score=72.0 if discount >= 45 else 42.0,
                        deal_score=75.0 if discount >= 45 else 38.0,
                        popularity_score=float(current_players / 100.0),
                        is_upcoming=False,
                        is_released=1,
                        updated_at=now - datetime.timedelta(minutes=idx),
                    )
                )
                session.add(
                    LatestGamePrice(
                        game_id=idx,
                        latest_price=19.99,
                        original_price=49.99 if discount > 0 else 19.99,
                        latest_discount_percent=discount if discount > 0 else 0,
                        current_players=current_players,
                        recorded_at=now - datetime.timedelta(minutes=idx),
                    )
                )

            alert_defs = [
                (1, "NEW_HISTORICAL_LOW", {}),
                (2, "PRICE_DROP", {"old_price": 49.99, "new_price": 19.99}),
                (3, "PLAYER_SURGE", {"current_players": 5200}),
                (4, "SALE_STARTED", {"discount_percent": 40}),
                (5, "PRICE_DROP", {"old_price": 39.99, "new_price": 19.99}),
                (6, "PRICE_DROP", {"old_price": 59.99, "new_price": 29.99}),
                (7, "PRICE_DROP", {"old_price": 44.99, "new_price": 21.99}),
                (8, "NEW_HISTORICAL_LOW", {}),
                (9, "SALE_STARTED", {"discount_percent": 72}),
                (10, "PLAYER_SURGE", {"current_players": 6100}),
            ]
            for offset, (game_id, alert_type, metadata) in enumerate(alert_defs, start=1):
                session.add(
                    Alert(
                        id=offset,
                        game_id=game_id,
                        alert_type=alert_type,
                        metadata_json=metadata,
                        created_at=now - datetime.timedelta(minutes=game_id),
                    )
                )

            session.commit()

            limit = 30
            feed = _build_deal_radar_feed(session, limit=limit)
            self.assertTrue(feed)

            game_ids = [int(item.get("game_id")) for item in feed]
            self.assertEqual(len(game_ids), len(set(game_ids)))

            signal_counts = Counter(str(item.get("signal_type") or "").upper() for item in feed)
            expected_signal_cap = max(
                2,
                min(int(math.ceil(limit * DEAL_RADAR_MAX_SIGNAL_SHARE)), DEAL_RADAR_MAX_PER_SIGNAL),
            )
            self.assertLessEqual(max(signal_counts.values()), expected_signal_cap)

            top_window = feed[: min(len(feed), DEAL_RADAR_DIVERSITY_WINDOW)]
            top_categories = {str(item.get("signal_type") or "").upper() for item in top_window}
            self.assertGreaterEqual(len(top_categories), 5)
        finally:
            session.close()

    def test_deal_radar_selects_single_highest_priority_signal_for_same_game(self):
        session = _build_session()
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            session.add(
                GameSnapshot(
                    game_id=1,
                    game_name="Priority Test",
                    steam_appid="1",
                    latest_price=19.99,
                    latest_original_price=39.99,
                    latest_discount_percent=50,
                    current_players=2500,
                    avg_player_count=1800,
                    player_change=250,
                    short_term_player_trend=0.4,
                    trending_score=60.0,
                    worth_buying_score=75.0,
                    buy_score=75.0,
                    deal_score=80.0,
                    popularity_score=30.0,
                    is_upcoming=False,
                    is_released=1,
                    updated_at=now,
                )
            )
            session.add(
                LatestGamePrice(
                    game_id=1,
                    latest_price=19.99,
                    original_price=39.99,
                    latest_discount_percent=50,
                    current_players=2500,
                    recorded_at=now,
                )
            )
            session.add(
                Alert(
                    id=1,
                    game_id=1,
                    alert_type="PRICE_DROP",
                    metadata_json={"old_price": 39.99, "new_price": 19.99},
                    created_at=now,
                )
            )
            session.add(
                Alert(
                    id=2,
                    game_id=1,
                    alert_type="SALE_STARTED",
                    metadata_json={"discount_percent": 50},
                    created_at=now - datetime.timedelta(seconds=30),
                )
            )
            session.commit()

            feed = _build_deal_radar_feed(session, limit=10)
            self.assertEqual(len(feed), 1)
            self.assertEqual(feed[0]["game_id"], 1)
            self.assertEqual(feed[0]["signal_type"], "SALE_STARTED")
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
