# NEWWORLD 50k Readiness Go/No-Go Checklist

Use this checklist before declaring NEWWORLD ready to scale from ~10k to 50k tracked games.

Use [CATALOG_ROLLOUT_PLAN.md](/docs/CATALOG_ROLLOUT_PLAN.md) for the staged 10k -> 25k -> 50k execution flow.

## 1. Migration / Schema

1. Run schema bootstrap:

```bash
python setup_database.py
```

2. Run readiness validator (DB checks):

```bash
python scripts/validate_50k_readiness.py
```

If this fails on `job_status_consistency`, repair legacy counters:

```bash
python scripts/repair_job_status.py
python scripts/validate_50k_readiness.py --repair-job-status
```

3. Optional API contract checks (when API is running):

```bash
python scripts/validate_50k_readiness.py --base-url http://127.0.0.1:8000
```

Required outcomes:
- no schema validation failures
- required scale columns/indexes present
- dirty queue primary key on `dirty_games.game_id` present

4. Optional phase-bound checks:

```bash
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000
python scripts/validate_50k_readiness.py --phase phase2_50k --max-dirty-games 60000
```

## 2. Worker Startup Safety

Workers now perform startup schema checks and fail fast with actionable guidance if migrations are missing:
- `python -m jobs.run_price_ingestion_loop`
- `python -m jobs.refresh_snapshots`

If startup fails with schema readiness errors, run `python setup_database.py` against the same DB used by workers and restart.

## 3. Operational Health

Run these SQL checks:

```sql
SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM game_prices;
SELECT COUNT(*) FROM game_player_history;
SELECT COUNT(*) FROM latest_game_prices;
SELECT COUNT(*) FROM game_snapshots;
SELECT COUNT(*) FROM dirty_games;
```

Healthy pattern:
- `game_prices` and `game_player_history` continue increasing
- `latest_game_prices` and `game_snapshots` approach catalog coverage
- `dirty_games` fluctuates and drains (does not grow unbounded)

## 4. Ingestion Safety

Confirm:
- priority tiers are active (`HOT`, `MEDIUM`, `COLD`)
- `next_refresh_at` is being updated on ingestion
- 429 cooldown/backoff is active in logs when rate-limited
- one ingestion worker is baseline; increase workers only after stable 429 behavior

## 5. Snapshot / Queue Safety

Confirm:
- snapshot worker claims batches from `dirty_games`
- `retry_count` and `next_attempt_at` are updated on failures
- backlog drains over time under steady state
- dashboard cache rebuild continues to run

## 6. Catalog / Search / Frontend

Confirm:
- All Games can traverse full paginated catalog (not first page only)
- client loader uses bounded page-fetch concurrency with cancellation/stale-request guards
- search/filter works against full loaded catalog
- cancellation/stale-request protection is active
- infinite scroll/load-more does not duplicate items

## 7. Homepage Quality

Confirm:
- homepage rails remain snapshot/cache-backed
- Market Radar appears near the top of discovery flow and uses snapshot/cache-backed signals
- first visible rows are diverse (not the same titles repeated across deal rails)
- Seasonal Sale mode/copy matches active vs inactive sale state
- analytics widgets do not surface low-signal zero-only junk when better records exist

## 8. Architecture Integrity (Must Stay True)

- APIs serve discovery/home from `game_snapshots` and `dashboard_cache`
- no raw-history request-time ranking scans
- append-only history model preserved
- dirty queue dedupe semantics preserved (`ON CONFLICT`, one logical row per game)

## 9. Go / No-Go Rule

Go only if all are true:
- schema validation passes
- workers start cleanly after migration
- queue drain is healthy
- catalog/search/homepage checks pass
- no architecture integrity violations
- current rollout phase checks pass (`rollout_catalog_expansion.py` status + phase validator)

Otherwise: No-Go. Fix failed checks, rerun validator, and retest.
