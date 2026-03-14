# NEWWORLD Operations Runbook

## 1. Purpose

This runbook describes how to operate and troubleshoot the NEWWORLD ingestion and analytics system.

It is intended for:

- engineers
- AI agents
- operators

This document should be consulted when:

- ingestion stalls
- Steam rate limits occur
- the dirty queue grows unexpectedly
- snapshot refresh fails
- dashboards appear stale

---

## 2. System Components

NEWWORLD runs with two primary background workers.

### Ingestion Worker

Command:

```bash
python -m jobs.run_price_ingestion_loop
```

Responsibilities:

- fetch Steam appdetails
- fetch player counts
- write to:

`game_prices`  
`game_player_history`  
`latest_game_prices`

- enqueue dirty rows for snapshot refresh

---

### Snapshot Worker

Command:

```bash
python -m jobs.refresh_snapshots
```

Responsibilities:

- read `dirty_games` queue
- rebuild snapshot rows
- refresh dashboard cache

---

## 3. Local Development Startup

Typical development startup sequence:

1.

```bash
python setup_database.py
```

2.

```bash
python scripts/seed_steam_games.py
```

3.

Start ingestion worker

```bash
python -m jobs.run_price_ingestion_loop
```

4.

Start snapshot worker

```bash
python -m jobs.refresh_snapshots
```

---

## 4. Environment Variables

Important environment variables:

`DATABASE_URL`

Example:

`postgresql://postgres:PASSWORD@localhost:5432/newworld`

`REQUIRE_DATABASE_URL`

Optional safety switch. When `true`, startup fails if `DATABASE_URL` is not set from environment.
This is recommended for deployed runtimes.

`STEAM_API_KEY`

Used by seed scripts and ingestion.

`TRACK_GAMES_PER_RUN`

Controls ingestion batch size.

Recommended safe value:

`25`

`INGESTION_BATCH_SIZE`

Optional alias for `TRACK_GAMES_PER_RUN` used by `jobs.run_price_ingestion_loop`.

`SNAPSHOT_BATCH_SIZE`

Rows processed per snapshot worker cycle (default: `1000`).

`DIRTY_QUEUE_FETCH_SIZE`

Optional cap on dirty queue rows claimed per snapshot cycle (defaults to `SNAPSHOT_BATCH_SIZE`).

Domain/metadata runtime variables for production:

- `DISPLAY_SITE_NAME` (set to `GameDen.gg`)
- `SITE_URL` (set to `https://gameden.gg`)
- `CANONICAL_HOST_REDIRECT` and `CANONICAL_REDIRECT_HOSTS`
- `CORS_ALLOW_ORIGINS` (must include `https://gameden.gg`)

Use [GAMEDEN_DOMAIN_GO_LIVE_CHECKLIST.md](/docs/GAMEDEN_DOMAIN_GO_LIVE_CHECKLIST.md)
for the complete domain launch verification flow.

---

## 5. System Health Checks

Run these queries in PostgreSQL.

```sql
SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM game_prices;
SELECT COUNT(*) FROM game_player_history;
SELECT COUNT(*) FROM latest_game_prices;
SELECT COUNT(*) FROM game_snapshots;
SELECT COUNT(*) FROM dirty_games;
```

Healthy patterns:

- `game_prices` increasing
- `game_player_history` increasing
- `latest_game_prices` increasing
- `game_snapshots` approximately equal to number of games
- `dirty_games` fluctuates but does not grow uncontrollably

---

## 6. Handling Steam 429 Rate Limits

Steam frequently returns:

`429 Too Many Requests`

Symptoms:

- ingestion logs show repeated retries
- price updates stall

Mitigation:

1. reduce batch size

`TRACK_GAMES_PER_RUN=25`

2. restart ingestion worker

3. allow cooldown period

4. avoid running multiple ingestion workers initially

---

## 7. Dirty Queue Issues

If `dirty_games` grows uncontrollably:

Check:

```sql
SELECT COUNT(*) FROM dirty_games;
```

Possible causes:

- ingestion repeatedly marking games dirty
- snapshot worker not running
- schema mismatch

Recovery:

restart snapshot worker

```bash
python -m jobs.refresh_snapshots
```

---

## 8. Snapshot Staleness

If dashboards appear stale:

Check:

```sql
SELECT COUNT(*) FROM game_snapshots;
```

Expected:

approximately equal to number of games

Ensure snapshot worker is running.

---

## 9. Database Issues

Common problems:

Postgres connection errors

Check `DATABASE_URL`.

If local development:

`postgresql://postgres:PASSWORD@localhost:5432/newworld`

---

## 10. Scaling Workers

Safe progression:

1. ingestion worker
2. ingestion workers
3. ingestion workers

Increase only if Steam rate limits remain acceptable.

---

## 11. Emergency Reset

If ingestion becomes inconsistent:

Stop workers.

Restart:

```bash
python -m jobs.run_price_ingestion_loop
python -m jobs.refresh_snapshots
```

Verify database health.

---

## 12. When to Escalate

Investigate deeper if:

- `game_prices` remains zero
- `latest_game_prices` remains zero
- `dirty_games` grows without draining
- Steam endpoints return persistent errors

---

## 13. 50k Readiness Validation

Before scaling beyond the baseline catalog size, run:

```bash
python scripts/validate_50k_readiness.py
```

If API is running locally, include endpoint checks:

```bash
python scripts/validate_50k_readiness.py --base-url http://127.0.0.1:8000
```

See [READINESS_50K_CHECKLIST.md](/docs/READINESS_50K_CHECKLIST.md) for go/no-go criteria.

## 14. Controlled Catalog Rollout (10k -> 25k -> 50k)

Use staged activation, not a single jump.

1. Check current rollout status:

```bash
python scripts/rollout_catalog_expansion.py status
```

2. Fastest safe non-manual expansion (seed + activate in one command):

```bash
python scripts/rollout_catalog_expansion.py expand --phase phase1_25k --activation-spread-minutes 240
python scripts/rollout_catalog_expansion.py expand --phase phase2_50k --activation-spread-minutes 360
```

3. Manual split flow (optional) - expand catalog rows while holding newly seeded games:

```bash
python scripts/seed_steam_games.py --limit 25000 --hold-new-games
python scripts/seed_steam_games.py --limit 50000 --hold-new-games
```

4. Activate phase target with spread:

```bash
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --activation-spread-minutes 240
python scripts/rollout_catalog_expansion.py apply --phase phase2_50k --activation-spread-minutes 360
```

5. Validate go/no-go after each phase:

```bash
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000 --base-url http://127.0.0.1:8000
python scripts/validate_50k_readiness.py --phase phase2_50k --max-dirty-games 60000 --base-url http://127.0.0.1:8000
```

6. If readiness fails on inconsistent `job_status` counters, repair then rerun:

```bash
python scripts/repair_job_status.py
python scripts/validate_50k_readiness.py --repair-job-status
```

7. If instability appears, reduce target immediately:

```bash
python scripts/rollout_catalog_expansion.py apply --target-tracked 10000
```

Detailed workflow and stop criteria are documented in
[CATALOG_ROLLOUT_PLAN.md](/docs/CATALOG_ROLLOUT_PLAN.md).
