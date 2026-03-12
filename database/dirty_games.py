from sqlalchemy import text
from sqlalchemy.orm import Session


def mark_game_dirty(session: Session, game_id: int, reason: str | None = None) -> None:
    if session.bind and session.bind.dialect.name == "postgresql":
        sql = """
            INSERT INTO dirty_games (
                game_id,
                reason,
                first_seen_at,
                last_seen_at,
                updated_at,
                retry_count,
                next_attempt_at
            )
            VALUES (:game_id, :reason, now(), now(), now(), 0, now())
            ON CONFLICT (game_id) DO UPDATE SET
                reason = COALESCE(EXCLUDED.reason, dirty_games.reason),
                last_seen_at = now(),
                updated_at = now(),
                next_attempt_at = now()
        """
    else:
        sql = """
            INSERT INTO dirty_games (
                game_id,
                reason,
                first_seen_at,
                last_seen_at,
                updated_at,
                retry_count,
                next_attempt_at
            )
            VALUES (
                :game_id,
                :reason,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                0,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(game_id) DO UPDATE SET
                reason = COALESCE(excluded.reason, dirty_games.reason),
                last_seen_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                next_attempt_at = CURRENT_TIMESTAMP
        """

    session.execute(text(sql), {"game_id": int(game_id), "reason": reason})


def mark_games_dirty(session: Session, game_ids: list[int], reason: str | None = None) -> None:
    for game_id in game_ids:
        mark_game_dirty(session, game_id, reason=reason)
