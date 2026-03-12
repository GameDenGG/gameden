# NEWWORLD System Bootstrap Guide

## 1. System Overview

NEWWORLD is a Steam deal discovery and analytics platform that tracks game prices, discounts, player counts, historical trends, and deal signals across about 10,000 Steam games.

Core architecture layers:

1. Catalog (`games`)
2. Dirty Queue (`dirty_games`)
3. Raw History (`game_prices`, `game_player_history`)
4. Materialized Snapshots (`latest_game_prices`, `game_snapshots`)
5. API Layer (`api/server.py`, backed by `dashboard_cache` + snapshots)

Pipeline model:

```text
Steam API -> ingestion worker -> raw history -> latest/materialized tables -> snapshot worker -> dashboard_cache -> API
```

Ingestion writes facts first. Snapshot refresh computes derived intelligence later. APIs should read snapshots/cache, not recompute heavy rankings.

## 2. Repository Layout

Key directories:

- `api/`: FastAPI application (`server.py`), cache helpers, metrics helpers.
- `database/`: SQLAlchemy models, DB engine/session setup, dirty queue helpers.
- `jobs/`: Long-running worker entrypoints (`run_price_ingestion_loop`, `refresh_snapshots`).
- `scraper/`: Steam fetch clients (`steam_scraper.py`, `steam_players.py`) with retry/rate-limit handling.
- `scripts/`: Operational scripts (`seed_steam_games.py`, diagnostics, scale validation).
- `docs/`: Architecture, AI guidance, and operational runbooks.

## 3. Database Architecture

Major tables and purpose:

- `games`: canonical game catalog.
- `dirty_games`: deduplicated queue of games needing recomputation.
- `game_prices`: append-only raw price history.
- `game_player_history`: append-only player count history.
- `latest_game_prices`: one-row-per-game latest materialized pricing state.
- `game_snapshots`: derived game intelligence (scores, trend/deal metadata, historical low state).

Data flow:

```text
Steam -> ingestion worker -> game_prices + game_player_history -> dirty_games
      -> snapshot worker -> latest_game_prices + game_snapshots -> dashboard_cache -> API
```

## 4. Local Development Setup

Estimated setup time: 10-15 minutes.

1. Create and activate a virtual environment:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

2. Install Python dependencies (project has no pinned requirements file yet):

```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary requests python-dotenv pywebpush
```

3. Install PostgreSQL locally and create the database:

```sql
CREATE DATABASE newworld;
```

4. Configure environment in `.env`:

```env
DATABASE_URL=postgresql://postgres:PASSWORD@localhost:5432/newworld
STEAM_USER_AGENT=Mozilla/5.0
STEAM_API_KEY=your_steam_api_key
```

`DATABASE_URL` fallback behavior:
- If `DATABASE_URL` is set, it is used.
- If not set, `config.py` falls back to:
  `postgresql://postgres:YOURPASSWORD@localhost:5432/newworld`

## 5. Database Initialization

Initialize schema/indexes:

```bash
python setup_database.py
```

Notes:
- This script is additive and designed to be idempotent.
- Safe to rerun during local development.
- Applies `Base.metadata.create_all(...)` plus dialect-specific `ALTER TABLE`/index SQL.

## 6. Seeding the Game Catalog

Run:

```bash
python scripts/seed_steam_games.py --limit 10000
```

Required env var:

```env
STEAM_API_KEY=your_steam_api_key
```

What this seed does:

- Pulls Steam app list pages.
- Filters likely game entries.
- Inserts missing rows into `games`.
- Enqueues corresponding `dirty_games` entries (deduplicated via upsert).

## 7. Running the Ingestion Pipeline

Run workers in separate terminals.

Worker 1 (ingestion loop):

```bash
python -m jobs.run_price_ingestion_loop
```

Responsibilities:
- fetch Steam appdetails
- store `game_prices` history
- store `game_player_history`
- update latest row in `game_latest_prices` (legacy/latest mirror)
- enqueue `dirty_games`

Worker 2 (snapshot refresh):

```bash
python -m jobs.refresh_snapshots
```

Responsibilities:
- claim and process `dirty_games`
- refresh `latest_game_prices`
- rebuild `game_snapshots`
- emit deduplicated deal events/alerts
- rebuild `dashboard_cache`

## 8. Rate Limiting Considerations

Steam appdetails may return HTTP `429 Too Many Requests`.

Current behavior (`scraper/steam_scraper.py`):
- global cooldown after 429 (`STEAM_APPDETAILS_429_COOLDOWN_SECONDS`, default 45s)
- exponential backoff with jitter (`STEAM_APPDETAILS_429_BACKOFF_BASE_SECONDS`, `...MAX_SECONDS`)
- per-request pacing (`STEAM_APPDETAILS_REQUEST_DELAY_SECONDS`, default 0.25s)

Safe ingestion tuning for local stability:

```env
TRACK_GAMES_PER_RUN=25
TRACK_MIN_DELAY_SECONDS=0.10
TRACK_MAX_DELAY_SECONDS=0.30
TRACK_REQUEST_RETRIES=2
```

Use this conservative profile first, then increase throughput only after stable runs.

## 9. Verifying System Health

Run these SQL checks:

```sql
SELECT COUNT(*) FROM games;
SELECT COUNT(*) FROM game_prices;
SELECT COUNT(*) FROM game_player_history;
SELECT COUNT(*) FROM latest_game_prices;
SELECT COUNT(*) FROM game_snapshots;
SELECT COUNT(*) FROM dirty_games;
```

Healthy patterns:
- `games` is large after seed (target around 10k).
- `game_prices` and `game_player_history` steadily increase during ingestion.
- `latest_game_prices` and `game_snapshots` approach the number of tracked games.
- `dirty_games` fluctuates but should not grow unbounded under steady-state.

Quick debugging workflow:

1. Check worker logs for 429 bursts or DB errors.
2. Hit API health endpoint: `GET /health`.
3. Check dirty queue age/size and retry counts.
4. If queue stalls, run snapshot worker once (`python -m jobs.refresh_snapshots --once`) and inspect failures.

## 10. Common Issues

1. Steam rate limits (`429`)
- Symptom: ingestion slows; repeated retries.
- Fix: reduce `TRACK_GAMES_PER_RUN`, increase request delays, allow cooldown to clear.

2. Dirty queue schema mismatches
- Symptom: worker errors on `dirty_games` columns (`next_attempt_at`, locks, retries).
- Fix: rerun `python setup_database.py` to apply additive schema updates.

3. Missing environment variables
- Symptom: seed fails (`STEAM_API_KEY` missing) or DB/auth failures.
- Fix: verify `.env` values and restart processes.

4. PostgreSQL connection issues
- Symptom: startup errors from SQLAlchemy.
- Fix: confirm Postgres is running, DB exists, and `DATABASE_URL` is correct.

## 11. Scaling the Ingestion Pipeline

Safe scaling tiers:

1. `1` ingestion worker (default, safest)
2. `2` ingestion workers (moderate)
3. `3` ingestion workers (max safe starting point)

Sharding approach (implemented):

```env
TRACK_SHARD_TOTAL=2
TRACK_SHARD_INDEX=0
```

Run another worker with `TRACK_SHARD_INDEX=1`. Workers shard on `Game.id % TRACK_SHARD_TOTAL` to avoid duplicate scanning.

Controlled catalog expansion (recommended):

```bash
python scripts/rollout_catalog_expansion.py expand --phase phase1_25k --activation-spread-minutes 240
python scripts/rollout_catalog_expansion.py expand --phase phase2_50k --activation-spread-minutes 360
```

Manual split flow (seed + apply):

```bash
python scripts/seed_steam_games.py --limit 25000 --hold-new-games
python scripts/rollout_catalog_expansion.py apply --phase phase1_25k --activation-spread-minutes 240
python scripts/validate_50k_readiness.py --phase phase1_25k --max-dirty-games 30000
```

Then repeat for 50k:

```bash
python scripts/seed_steam_games.py --limit 50000 --hold-new-games
python scripts/rollout_catalog_expansion.py apply --phase phase2_50k --activation-spread-minutes 360
python scripts/validate_50k_readiness.py --phase phase2_50k --max-dirty-games 60000
```

## 12. AI Development Workflow

NEWWORLD AI workflow:

- Use Codex/ChatGPT prompts that include:
  - repository analysis
  - files to change
  - implementation plan
  - validation steps
- Prefer minimal, additive, architecture-safe changes.
- Keep heavy logic in workers and snapshots.
- Do not compute rankings in APIs.
- For queue/event work, inspect schema and constraints first (`database/models.py`, `setup_database.py`, `database/dirty_games.py`, `jobs/refresh_snapshots.py`).

Codex prompt checklist for changes:

```text
1) Analyze repo structure
2) Summarize architecture impact
3) List exact files
4) Confirm migration needs
5) Implement
6) Validate (tests + SQL + runtime checks)
```

## 13. Future Scaling Plans

Near/mid-term roadmap:

- multi-worker ingestion hardening
- richer deal detection/event pipeline
- expanded analytics dashboards and trend explainability
- alerting/notification depth improvements
- production deployment hardening (process supervision, observability, infra automation)
