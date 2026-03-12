# NEWWORLD Codex Working Rules

Codex must follow all applicable `AGENTS.md` instructions before making changes.
Instruction discovery is root-to-leaf from repository root to working directory.

## Required Read Order Before Implementation

Read these documents first:

1. `docs/AI_SYSTEM_BOOTSTRAP.md`
2. `docs/ARCHITECTURE_DECISIONS.md`
3. `docs/OPERATIONS_RUNBOOK.md`

If the task touches ingestion, queues, snapshots, analytics flow, caching, API data flow, or frontend data wiring, also read:

4. `docs/DATA_PIPELINE_DIAGRAM.md`

Before making schema or pipeline assumptions, inspect:

- `database/models.py`
- `database/dirty_games.py`
- `jobs/run_price_ingestion_loop.py`
- `jobs/refresh_snapshots.py`

Do not assume schema details without code inspection.

## Required Behavior Before Coding

Before implementation, summarize:

- architecture constraints that must be preserved
- dirty queue semantics
- intended frontend/API data path
- whether the task affects ingestion, snapshots, or API contracts

Then implement changes directly unless the user explicitly asks for planning only.

## System Overview

Primary pipeline:

Steam APIs  
-> ingestion worker  
-> raw history tables  
-> dirty queue  
-> snapshot worker  
-> materialized snapshots/cache  
-> API  
-> frontend

Request-time analytics should be served from `game_snapshots` and `dashboard_cache`, not recomputed from raw history.

## Key Table Roles

- `games`: canonical Steam catalog.
- `dirty_games`: refresh queue driving snapshot recomputation.
- `game_prices`: append-only price history.
- `game_player_history`: append-only player history.
- `latest_game_prices`: latest materialized price state.
- `game_snapshots`: per-game derived analytics state.
- `dashboard_cache`: cached homepage/leaderboard payloads.

## Dirty Queue Semantics

`dirty_games` is the canonical refresh trigger.

Required semantics:

- one logical row per game
- enqueue writes use `ON CONFLICT`
- `retry_count` initialized on insert
- `first_seen_at` set only once
- `last_seen_at`/`updated_at` advance on subsequent marks

Do not change queue behavior without verifying model and writer compatibility.

## Architecture Constraints To Preserve

Preserve:

- append-only history tables
- dirty queue semantics
- snapshot-based APIs
- idempotent setup/migration behavior
- environment-based DB configuration (`DATABASE_URL` first, local fallback second)

Never:

- bypass `dirty_games`
- run heavy history scans in APIs
- aggressively parallelize Steam ingestion without controls
- hardcode production DB URLs

## Frontend/API Data Rules

- Frontend and dashboard fixes must preserve snapshot/cache-backed request paths.
- If payload contracts drift, normalize in one place.
- Do not patch UI bugs by introducing raw-history request-time analytics.
- Keep loading, empty, and error states distinct and accurate.
- Do not remove major UI sections, menus, panels, routes, or primary navigation unless the user explicitly requests removal.
- Treat Wishlist, Watchlist, and core discovery sections as protected structures; prefer repositioning/refining over deletion.

## Validation Expectations

After changes, validate as applicable:

- lint/format checks
- tests for touched files
- type checks (if configured)
- no parsing/runtime regressions
- no violation of snapshot/cache-backed data flow

## Expected Implementation Response Format

1. Repository analysis
2. Files to modify
3. Implementation plan
4. Code changes
5. Validation steps
6. Expected outcome
