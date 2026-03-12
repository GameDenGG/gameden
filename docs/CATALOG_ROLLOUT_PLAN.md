# GameDen Catalog Rollout Plan (10k -> 25k -> 50k)

This document defines the controlled catalog expansion flow for GameDen.

Use this plan to scale tracked games in phases without bypassing dirty-queue/snapshot architecture.

## 1. Safety Model

- Catalog rows can exist beyond the active tracked set.
- Active tracking is controlled by `games.priority_tier` and `games.next_refresh_at`.
- `ROLLOUT_HOLD` games are excluded from ingestion by default (`TRACK_INCLUDE_ROLLOUT_HOLD=false`).
- Snapshot/API request paths remain snapshot/cache-backed.

## 2. Commands Reference

Rollout status:

```bash
python scripts/rollout_catalog_expansion.py status
```

Fastest safe non-manual phase command (seed + activate):

```bash
python scripts/rollout_catalog_expansion.py expand --phase phase1_25k --activation-spread-minutes 240
python scripts/rollout_catalog_expansion.py expand --phase phase2_50k --activation-spread-minutes 360
```

Phase apply (example):

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --activation-spread-minutes 240
```

Dry run first:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --dry-run
```

Phase validation:

```bash
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000
```

Optional API contract checks:

```bash
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000 --base-url http://127.0.0.1:8000
```

## 3. Pre-Rollout Checklist

1. Apply schema/index bootstrap:

```bash
python setup_database.py
```

2. Restart API and workers after migration.
3. Verify baseline health:

```sql
SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM game_prices;
SELECT COUNT(*) FROM game_player_history;
SELECT COUNT(*) FROM latest_game_prices;
SELECT COUNT(*) FROM game_snapshots;
SELECT COUNT(*) FROM dirty_games;
```

4. Confirm ingestion baseline remains conservative:
`TRACK_SHARD_TOTAL=1`, bounded `TRACK_GAMES_PER_RUN`, 429 backoff/cooldown active.

## 4. Phase 1 (10k -> 25k)

Fast path (recommended):

```bash
python scripts/rollout_catalog_expansion.py expand --phase phase1_25k --activation-spread-minutes 240
```

Manual split flow (if you want explicit separate steps):

1. Expand catalog rows to 25k while holding new rows by default:

```bash
python scripts/seed_steam_games.py --limit 25000 --hold-new-games
```

2. Preview changes:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --dry-run
```

3. Activate tracked set:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --activation-spread-minutes 240
```

4. Validate go/no-go:

```bash
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000 --base-url http://127.0.0.1:8000
```

Proceed only if:
- ingestion runs cleanly with controlled 429 behavior
- dirty queue fluctuates and drains (no runaway growth)
- snapshot worker keeps pace (no sustained lag growth)
- catalog search/pagination/homepage quality remain healthy

## 5. Phase 2 (25k -> 50k)

Fast path (recommended):

```bash
python scripts/rollout_catalog_expansion.py expand --phase phase2_50k --activation-spread-minutes 360
```

Manual split flow:

1. Expand catalog rows to 50k while holding new rows:

```bash
python scripts/seed_steam_games.py --limit 50000 --hold-new-games
```

2. Preview target activation:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase2_50k --dry-run
```

3. Activate tracked set with a slower spread:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase2_50k --activation-spread-minutes 360
```

4. Validate go/no-go:

```bash
python scripts/validate_50k_readiness.py --phase phase2_50k --max-dirty-games 60000 --base-url http://127.0.0.1:8000
```

Proceed only if Phase 1 remained stable and all checks pass again at 50k.

## 6. Stop Conditions (No-Go)

Stop rollout and hold current phase if any of these occur:

- persistent 429 storms despite conservative pacing
- dirty queue grows continuously across multiple cycles
- snapshot backlog grows and does not recover
- catalog or homepage APIs regress in latency/quality
- major frontend catalog/search instability

## 7. Rollback / Stabilization

You can reduce active tracked size quickly:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k
```

or explicit target:

```bash
python scripts/rollout_catalog_expansion.py apply --target-tracked 10000
```

Then re-run readiness checks and keep one-worker conservative defaults until stable.

## 8. job_status Readiness Blocker Repair

If readiness fails on `job_status_consistency`, repair legacy inconsistent counters:

```bash
python scripts/repair_job_status.py
python scripts/validate_50k_readiness.py --repair-job-status
```
