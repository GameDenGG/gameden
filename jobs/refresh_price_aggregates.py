from sqlalchemy import text

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from database.models import Session


def refresh_all_price_aggregates() -> None:
    session = Session()
    try:
        if session.bind and session.bind.dialect.name == "postgresql":
            session.execute(
                text(
                    """
                    INSERT INTO latest_game_prices (
                        game_id,
                        latest_price,
                        original_price,
                        latest_discount_percent,
                        current_players,
                        recorded_at
                    )
                    SELECT DISTINCT ON (gp.game_id)
                        gp.game_id,
                        gp.price AS latest_price,
                        gp.original_price,
                        gp.discount_percent AS latest_discount_percent,
                        gp.current_players,
                        gp.recorded_at
                    FROM game_prices gp
                    WHERE gp.game_id IS NOT NULL
                      AND gp.price IS NOT NULL
                    ORDER BY gp.game_id, gp.recorded_at DESC, gp.id DESC
                    ON CONFLICT (game_id) DO UPDATE SET
                        latest_price = EXCLUDED.latest_price,
                        original_price = EXCLUDED.original_price,
                        latest_discount_percent = EXCLUDED.latest_discount_percent,
                        current_players = EXCLUDED.current_players,
                        recorded_at = EXCLUDED.recorded_at
                    """
                )
            )
            session.execute(
                text(
                    """
                    INSERT INTO game_price_lows (game_id, historical_low)
                    SELECT gp.game_id, MIN(gp.price) AS historical_low
                    FROM game_prices gp
                    WHERE gp.game_id IS NOT NULL
                      AND gp.price IS NOT NULL
                      AND gp.price > 0
                    GROUP BY gp.game_id
                    ON CONFLICT (game_id) DO UPDATE SET
                        historical_low = EXCLUDED.historical_low
                    """
                )
            )
        else:
            session.execute(
                text(
                    """
                    INSERT INTO latest_game_prices (
                        game_id,
                        latest_price,
                        original_price,
                        latest_discount_percent,
                        current_players,
                        recorded_at
                    )
                    SELECT gp.game_id, gp.price, gp.original_price, gp.discount_percent, gp.current_players, gp.recorded_at
                    FROM game_prices gp
                    JOIN (
                        SELECT game_id, MAX(recorded_at) AS max_recorded_at
                        FROM game_prices
                        WHERE game_id IS NOT NULL AND price IS NOT NULL
                        GROUP BY game_id
                    ) latest ON latest.game_id = gp.game_id AND latest.max_recorded_at = gp.recorded_at
                    WHERE gp.game_id IS NOT NULL AND gp.price IS NOT NULL
                    ON CONFLICT(game_id) DO UPDATE SET
                        latest_price = excluded.latest_price,
                        original_price = excluded.original_price,
                        latest_discount_percent = excluded.latest_discount_percent,
                        current_players = excluded.current_players,
                        recorded_at = excluded.recorded_at
                    """
                )
            )
            session.execute(
                text(
                    """
                    INSERT INTO game_price_lows (game_id, historical_low)
                    SELECT game_id, MIN(price) AS historical_low
                    FROM game_prices
                    WHERE game_id IS NOT NULL AND price IS NOT NULL AND price > 0
                    GROUP BY game_id
                    ON CONFLICT(game_id) DO UPDATE SET
                        historical_low = excluded.historical_low
                    """
                )
            )

        session.commit()

        latest_count = session.execute(text("SELECT COUNT(*) FROM latest_game_prices")).scalar() or 0
        low_count = session.execute(text("SELECT COUNT(*) FROM game_price_lows")).scalar() or 0
        print(f"latest_game_prices rows={latest_count}")
        print(f"game_price_lows rows={low_count}")
    finally:
        session.close()


if __name__ == "__main__":
    refresh_all_price_aggregates()
