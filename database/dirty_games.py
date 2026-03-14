from sqlalchemy import text
from sqlalchemy.orm import Session


def _db_now_expression(session: Session) -> str:
    if session.bind and session.bind.dialect.name == "postgresql":
        return "now()"
    return "CURRENT_TIMESTAMP"


def _normalize_game_id(game_id: int) -> int:
    try:
        normalized = int(game_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"dirty queue game_id must be an integer (got {game_id!r})") from exc
    if normalized <= 0:
        raise ValueError(f"dirty queue game_id must be > 0 (got {normalized})")
    return normalized


def _normalize_reason(reason: str | None) -> str | None:
    if reason is None:
        return None
    normalized = str(reason).strip()
    if not normalized:
        return None
    return normalized[:255]


def mark_game_dirty(session: Session, game_id: int, reason: str | None = None) -> None:
    normalized_game_id = _normalize_game_id(game_id)
    normalized_reason = _normalize_reason(reason)
    now_expression = _db_now_expression(session)
    # Keep dirty queue semantics stable:
    # - one logical row per game_id
    # - first_seen_at is insert-only
    # - last_seen_at/updated_at/next_attempt_at advance on subsequent marks
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

    session.execute(text(sql), {"game_id": normalized_game_id, "reason": normalized_reason})


def mark_games_dirty(session: Session, game_ids: list[int], reason: str | None = None) -> None:
    for game_id in game_ids:
        mark_game_dirty(session, game_id, reason=reason)
