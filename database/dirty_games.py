from sqlalchemy import text
from sqlalchemy.orm import Session


def _db_now_expression(session: Session) -> str:
    if session.bind and session.bind.dialect.name == "postgresql":
        return "now()"
    return "CURRENT_TIMESTAMP"


def mark_game_dirty(session: Session, game_id: int, reason: str | None = None) -> None:
    now_expression = _db_now_expression(session)
    sql = f"""
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
            {now_expression},
            {now_expression},
            {now_expression},
            0,
            {now_expression}
        )
        ON CONFLICT (game_id) DO UPDATE SET
            reason = COALESCE(EXCLUDED.reason, dirty_games.reason),
            last_seen_at = {now_expression},
            updated_at = {now_expression},
            next_attempt_at = {now_expression}
    """

    session.execute(text(sql), {"game_id": int(game_id), "reason": reason})


def mark_games_dirty(session: Session, game_ids: list[int], reason: str | None = None) -> None:
    for game_id in game_ids:
        mark_game_dirty(session, game_id, reason=reason)
