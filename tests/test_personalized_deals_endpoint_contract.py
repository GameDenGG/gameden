import datetime
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.responses import JSONResponse
from starlette.requests import Request

import api.server as server
from api.cache import _cache_store


AUTH_USER_ID = "acct_12345678-1234-1234-1234-1234567890ab"
ANON_VIEWER_ID = "anon_" + ("a" * 32)


class _FakeQuery:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter(self, *args, **kwargs):
        return self

    def group_by(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeSession:
    def __init__(self, queued_results):
        self._queued_results = [list(rows) for rows in queued_results]

    def query(self, *args, **kwargs):
        rows = self._queued_results.pop(0) if self._queued_results else []
        return _FakeQuery(rows)

    def close(self):
        return None


def _build_request(query: str = "user_id=acct&limit=12&include_owned=0") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/personalized-deals",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 12345),
        "headers": [(b"host", b"testserver")],
        "query_string": query.encode("utf-8"),
    }
    request = Request(scope)
    request.state.authenticated_user_id = AUTH_USER_ID
    request.state.viewer_id = ANON_VIEWER_ID
    return request


def _make_snapshot(
    game_id: int,
    *,
    tags: str,
    genres: str,
    deal_score: float = 82.0,
    deal_opportunity_score: float = 80.0,
    buy_score: float = 80.0,
    discount: int = 55,
    updated_at: datetime.datetime | None = None,
):
    now = updated_at or datetime.datetime(2026, 3, 29, 12, 0, tzinfo=datetime.timezone.utc)
    return SimpleNamespace(
        game_id=game_id,
        game_name=f"Game {game_id}",
        steam_appid=str(100000 + game_id),
        store_url=f"https://store.steampowered.com/app/{100000 + game_id}",
        banner_url=f"https://cdn.example.com/{game_id}.jpg",
        latest_price=19.99,
        latest_original_price=39.99,
        latest_discount_percent=discount,
        historical_low=9.99,
        historical_status="near_historical_low",
        price_vs_low_ratio=1.04,
        buy_recommendation="BUY_NOW",
        buy_reason="Strong buy setup",
        deal_score=deal_score,
        buy_score=buy_score,
        worth_buying_score=buy_score,
        momentum_score=61.0,
        trending_score=66.0,
        deal_opportunity_score=deal_opportunity_score,
        popularity_score=68.0,
        current_players=4200,
        player_growth_ratio=1.14,
        short_term_player_trend=0.09,
        review_score=88,
        review_score_label="Very Positive",
        is_released=1,
        is_upcoming=False,
        release_date=datetime.date(2020, 1, (game_id % 27) + 1),
        tags=tags,
        genres=genres,
        updated_at=now,
    )


class PersonalizedDealsEndpointContractTests(unittest.TestCase):
    def setUp(self) -> None:
        _cache_store.clear()
        self._core_endpoint = server.list_personalized_deals.__wrapped__.__wrapped__

    def _session_factory(self, *, owned_game_id: int, seed_tags: str, seed_genres: str, event_rows=None):
        if event_rows is None:
            event_rows = []

        def _factory():
            return _FakeSession(
                [
                    [(owned_game_id, datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc))],  # owned
                    [],  # watchlist
                    [],  # target watchlist
                    [(owned_game_id, seed_tags, seed_genres)],  # seed metadata rows
                    event_rows,  # recent DealEvent counts
                ]
            )

        return _factory

    def test_endpoint_returns_200_and_json_safe_temporal_fields(self):
        strong_snapshot = _make_snapshot(
            201,
            tags="roguelike,deckbuilder",
            genres="rpg,indie",
        )
        request = _build_request("user_id=acct&limit=12&include_owned=0")

        with (
            patch.object(server, "resolve_request_user_id", return_value=AUTH_USER_ID),
            patch.object(server, "_read_dashboard_cache", return_value=(None, {})),
            patch.object(
                server,
                "ReadSessionLocal",
                self._session_factory(
                    owned_game_id=101,
                    seed_tags="roguelike,deckbuilder,turn-based",
                    seed_genres="rpg,indie",
                ),
            ),
            patch.object(server, "_query_release_feed_rows", side_effect=[[strong_snapshot], []]),
        ):
            response = server.list_personalized_deals(
                request=request,
                user_id=AUTH_USER_ID,
                limit=12,
                summary=False,
                include_owned=False,
            )

        self.assertIsInstance(response, JSONResponse)
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertIn("items", payload)
        self.assertEqual(payload.get("count"), len(payload.get("items", [])))
        self.assertEqual(payload["items"][0]["release_date"], strong_snapshot.release_date.isoformat())

    def test_one_genre_weak_match_is_filtered_when_strong_overlap_exists(self):
        strong_snapshot = _make_snapshot(
            301,
            tags="roguelike,deckbuilder",
            genres="rpg,indie",
            updated_at=datetime.datetime(2026, 3, 29, 12, 10, tzinfo=datetime.timezone.utc),
        )
        weak_snapshot = _make_snapshot(
            302,
            tags="action",
            genres="action",
            deal_score=92.0,
            deal_opportunity_score=91.0,
            buy_score=90.0,
            discount=70,
            updated_at=datetime.datetime(2026, 3, 29, 12, 9, tzinfo=datetime.timezone.utc),
        )
        request = _build_request("user_id=acct&limit=12&include_owned=0")

        with (
            patch.object(server, "resolve_request_user_id", return_value=AUTH_USER_ID),
            patch.object(server, "_read_dashboard_cache", return_value=(None, {})),
            patch.object(
                server,
                "ReadSessionLocal",
                self._session_factory(
                    owned_game_id=101,
                    seed_tags="roguelike,deckbuilder,action",
                    seed_genres="rpg,indie",
                ),
            ),
            patch.object(server, "_query_release_feed_rows", side_effect=[[strong_snapshot, weak_snapshot], []]),
        ):
            payload = self._core_endpoint(
                request=request,
                user_id=AUTH_USER_ID,
                limit=12,
                summary=False,
                include_owned=False,
            )

        ids = [int(item["game_id"]) for item in payload["items"]]
        self.assertIn(301, ids)
        self.assertNotIn(302, ids)

    def test_only_weak_similarity_returns_clean_limited_payload(self):
        weak_snapshot = _make_snapshot(
            401,
            tags="action",
            genres="action",
            deal_score=91.0,
            deal_opportunity_score=90.0,
            buy_score=88.0,
            discount=68,
        )
        request = _build_request("user_id=acct&limit=12&include_owned=0")

        with (
            patch.object(server, "resolve_request_user_id", return_value=AUTH_USER_ID),
            patch.object(server, "_read_dashboard_cache", return_value=(None, {})),
            patch.object(
                server,
                "ReadSessionLocal",
                self._session_factory(
                    owned_game_id=101,
                    seed_tags="action",
                    seed_genres="action",
                ),
            ),
            patch.object(server, "_query_release_feed_rows", side_effect=[[weak_snapshot], []]),
        ):
            payload = self._core_endpoint(
                request=request,
                user_id=AUTH_USER_ID,
                limit=12,
                summary=False,
                include_owned=False,
            )

        self.assertFalse(payload["personalized"])
        self.assertTrue(payload["fallback_mode"])
        self.assertTrue(isinstance(payload.get("items"), list))
        self.assertLessEqual(payload["count"], 12)
        self.assertEqual(payload.get("fallback_reason"), "We don't have strong matches yet. Add more owned games and we'll improve this.")

    def test_stable_ordering_when_inputs_do_not_change(self):
        first_snapshot = _make_snapshot(
            501,
            tags="roguelike,deckbuilder",
            genres="rpg,indie",
            deal_score=85.0,
            deal_opportunity_score=84.0,
            buy_score=84.0,
            updated_at=datetime.datetime(2026, 3, 29, 12, 0, tzinfo=datetime.timezone.utc),
        )
        second_snapshot = _make_snapshot(
            502,
            tags="roguelike,deckbuilder",
            genres="rpg,indie",
            deal_score=85.0,
            deal_opportunity_score=84.0,
            buy_score=84.0,
            updated_at=datetime.datetime(2026, 3, 29, 12, 0, tzinfo=datetime.timezone.utc),
        )

        request_one = _build_request("user_id=acct&limit=12&include_owned=0")
        request_two = _build_request("user_id=acct&limit=12&include_owned=0")

        with (
            patch.object(server, "resolve_request_user_id", return_value=AUTH_USER_ID),
            patch.object(server, "_read_dashboard_cache", return_value=(None, {})),
            patch.object(
                server,
                "ReadSessionLocal",
                self._session_factory(
                    owned_game_id=101,
                    seed_tags="roguelike,deckbuilder,turn-based",
                    seed_genres="rpg,indie",
                ),
            ),
            patch.object(server, "_query_release_feed_rows", return_value=[first_snapshot, second_snapshot]),
        ):
            payload_one = self._core_endpoint(
                request=request_one,
                user_id=AUTH_USER_ID,
                limit=12,
                summary=False,
                include_owned=False,
            )
            payload_two = self._core_endpoint(
                request=request_two,
                user_id=AUTH_USER_ID,
                limit=12,
                summary=False,
                include_owned=False,
            )

        ids_one = [int(item["game_id"]) for item in payload_one["items"]]
        ids_two = [int(item["game_id"]) for item in payload_two["items"]]
        self.assertEqual(ids_one, ids_two)


if __name__ == "__main__":
    unittest.main()
