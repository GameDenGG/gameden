# Dirty Queue Lifecycle

`dirty_games` is a dedupe-safe, one-row-per-game work queue.

## Producer behavior

- Producers call `mark_game_dirty(session, game_id, reason=...)`.
- Existing rows are updated in place:
  - `last_seen_at` and `updated_at` move forward.
  - `next_attempt_at` resets to immediate.
  - `reason` updates when provided.
- Duplicate work for the same game does not create new rows.

## Consumer behavior

- Snapshot workers claim batches ordered by retry readiness (`next_attempt_at`, `updated_at`).
- Claimed rows are tagged with `locked_at` and `locked_by`.
- Successful processing deletes the rows.
- Failed processing increments `retry_count` and sets `next_attempt_at` with backoff.

## Operational signals

- Backlog size: `COUNT(*) FROM dirty_games`
- Oldest backlog age: `MIN(updated_at)`
- Retry pressure: `SUM(retry_count)` / high `retry_count` rows
- Stuck checks: rows with old `locked_at` and no recent worker success

## Backoff and locking policy

- Failed rows increment `retry_count`.
- `next_attempt_at` is set with progressive backoff to avoid hot-loop retries.
- Workers only claim rows where `next_attempt_at <= now()` and lock timeout has expired.
- `locked_at` older than 10 minutes is treated as stale and reclaimable.
