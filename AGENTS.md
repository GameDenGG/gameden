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

## Frontend Runtime Identification Rules

Before modifying any UI, template, chart, page logic, view wiring, or frontend data flow, Codex must identify the exact active runtime path.

Required before editing:
- the route being debugged
- the template/page file actually rendered for that route
- the JS function actually used at runtime
- all duplicate or similar implementations in the repo
- why those duplicates are not the active path

If multiple similar implementations exist, Codex must list them with file paths before editing anything.

Do not modify inactive or similarly named page/chart implementations.

For game chart issues:
- treat the game detail page and `/history` page as separate implementations
- never use `/history` page code as evidence for game detail page bugs unless the user explicitly says the bug is on `/history`
- if duplicate chart code exists, the live runtime path must be identified from the actual route/template/script chain before any patch is made

## Visualization Bug Rules

For visualization-only bugs:
- do not begin with ingestion, dirty queue, snapshot, schema, or API changes unless the active runtime path proves the issue originates there
- first verify the rendered dataset, active chart config, and live function path
- if exact marker placement is required, marker indices must come from the exact rendered dataset used by Chart.js
- do not compute marker indices from filtered, compressed, or alternate arrays when the rendered dataset differs
- if line smoothing makes visible extrema differ from actual datapoints, disable smoothing only for that chart
- do not apply chart rendering changes globally unless the same requirement exists for that chart too

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

- `games`: canonical Steam catalog
- `dirty_games`: refresh queue driving snapshot recomputation
- `game_prices`: append-only price history
- `game_player_history`: append-only player history
- `latest_game_prices`: latest materialized price state
- `game_snapshots`: per-game derived analytics state
- `dashboard_cache`: cached homepage/leaderboard payloads

## Dirty Queue Semantics

`dirty_games` is the canonical refresh trigger.

Required semantics:
- one logical row per game
- enqueue writes use `ON CONFLICT`
- `retry_count` initialized on insert
- `first_seen_at` set only once
- `last_seen_at` / `updated_at` advance on subsequent marks

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

- Frontend and dashboard fixes must preserve snapshot/cache-backed request paths
- If payload contracts drift, normalize in one place
- Do not patch UI bugs by introducing raw-history request-time analytics
- Keep loading, empty, and error states distinct and accurate
- Do not remove major UI sections, menus, panels, routes, or primary navigation unless the user explicitly requests removal
- Treat Wishlist, Watchlist, and core discovery sections as protected structures; prefer repositioning/refining over deletion

For frontend data bugs:
- verify whether the page consumes `game_snapshots`, `dashboard_cache`, API display payloads, or raw history-derived data before editing
- prefer fixing data normalization at the single active boundary where payloads enter the page
- do not patch the wrong page just because symbol names are similar

## Validation Expectations

After changes, validate as applicable:
- lint/format checks
- tests for touched files
- type checks if configured
- no parsing/runtime regressions
- no violation of snapshot/cache-backed data flow
- no edits to inactive duplicate implementations
- if the task is a frontend bug, validate on the exact affected route/page
- if duplicate implementations exist, explicitly confirm which one was left untouched

For chart and visualization bugs, validate as applicable:
- rendered dataset is non-empty when expected
- active chart config matches intended behavior
- marker indices correspond to rendered datapoints
- range/time-window coverage is correct
- loading, empty, and error states still behave correctly

## Expected Implementation Response Format

1. Repository analysis
2. Active runtime path
3. Files to modify
4. Implementation plan
5. Code changes
6. Validation steps
7. Expected outcome

If duplicate implementations were found, include:
- all candidate files
- the one confirmed active
- why the others were not modified

## DATABASE MIGRATION SAFETY

1. Any change to SQLAlchemy models that alters database structure MUST include an Alembic migration.

2. Structural changes include:
   - new columns
   - removed columns
   - column renames
   - type changes
   - index changes
   - table changes

3. Do not make structural model changes without checking existing Alembic state and migration ordering.

4. Do not infer schema drift from docs alone; verify in code first.