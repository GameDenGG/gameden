# NEWWORLD Architecture Decisions

## 1. Purpose of This Document

This file records architecture decisions that were intentionally chosen for NEWWORLD.

Read this before making major changes to:

- ingestion
- dirty queue
- snapshots
- caching
- API query patterns
- local/production database configuration

The goal is to prevent accidental regressions in core system design.

## 2. System Overview

NEWWORLD tracks Steam game prices, discounts, player counts, and market signals across about 10,000 games.

Current pipeline:

```text
Steam API / Store endpoints
-> ingestion worker
-> raw history tables
-> dirty queue
-> snapshot worker
-> latest/materialized state
-> dashboard cache
-> API
```

This pipeline is implemented in `main.py`, `jobs/run_price_ingestion_loop.py`, `jobs/refresh_snapshots.py`, and the database models in `database/models.py`.

## 3. Decision: Append-Only Raw History

Decision:

- `game_prices` and `game_player_history` are append-only historical observation tables.
- They are for durable fact capture over time.
- They should not be rewritten with heavy update logic.
- Request handlers should not scan them for every request.

Why this was chosen:

- preserves full historical context
- enables trend/analytics recomputation
- supports future scoring model changes
- aligns with snapshot-based serving

Tradeoff:

- tables grow continuously
- indexing and future partitioning will be needed at larger scale

## 4. Decision: Dirty Queue as Refresh Trigger

Decision:

`dirty_games` is the canonical refresh trigger for snapshot recomputation.

Queue semantics that must be preserved:

- one logical queue row per game (`game_id` primary key)
- idempotent writes via conflict handling (`ON CONFLICT (game_id) DO UPDATE`)
- `first_seen_at`, `last_seen_at`, and `updated_at` are meaningful lifecycle fields
- `retry_count` must exist and be initialized
- queue inserts must satisfy all NOT NULL columns
- queue write shape must stay compatible across seed scripts, ingestion code, and snapshot worker

Why this was chosen:

- avoids full-catalog refresh every cycle
- scales recomputation with changed games
- reduces DB load and worker cycle time

Tradeoff:

- schema drift in any writer/reader can break ingestion or refresh
- queue contract must stay synchronized across code paths

## 5. Decision: Snapshot-Based Product Reads

Decision:

- APIs and dashboards should primarily read `game_snapshots`, `latest_game_prices`, and cache state.
- Heavy analytics/scoring should not run at request time.
- Historical-derived fields should be computed in snapshot refresh jobs.

Why this was chosen:

- keeps API latency stable
- avoids repeated scans of large history tables
- supports 10k+ catalog scale with predictable performance

Tradeoff:

- snapshot worker is critical infrastructure
- snapshot schema must evolve as product intelligence expands

## 6. Decision: Dashboard Cache for Homepage / Leaderboards

Decision:

Homepage and repeated leaderboard-like surfaces are served from precomputed cache/materialized state.

Current implementation details:

- cache stored in `dashboard_cache`
- stable section keys are written during snapshot refresh (for example `home`, `home:trending`, `home:worth_buying`)
- cache is rebuilt in `jobs/refresh_snapshots.py` (`rebuild_dashboard_cache`)

Why this was chosen:

- faster homepage/API reads
- more stable performance under traffic
- avoids recomputing ranking sections per request

Tradeoff:

- cache schema/model alignment matters
- refresh correctness directly affects user-facing freshness

Important compatibility note:

`dashboard_cache` had `key` vs `cache_key` compatibility drift in the past. Keep model/schema compatibility explicit (`key` as physical column, `cache_key` alias) and avoid introducing naming drift.

## 7. Decision: Local PostgreSQL for Development

Decision:

- local development defaults to local PostgreSQL
- production remains `DATABASE_URL` driven
- code stays environment-switchable
- local is preferred for sustained ingestion development

Why this was chosen:

- avoids hosted free-tier transfer/quota interruptions during ingestion loops
- improves local iteration speed
- keeps production portability intact

Tradeoff:

- local setup requires more initial environment work
- onboarding docs must remain accurate

## 8. Decision: Environment-Driven Database Configuration

Decision rule:

1. use `DATABASE_URL` from environment when present
2. otherwise fall back to local Postgres URL

Current implementation:

- `config.py` selects environment value first, fallback second
- `database/__init__.py` builds pooled/direct/read engines from that config
- startup logs DB host/port/database and source (`environment` or `local_fallback`)

Why this was chosen:

- smooth local bootstrap
- simple production deployment
- easy future provider switch (Neon/other hosted Postgres)

Tradeoff:

- misconfigured env values fail startup
- safe target logging is required for fast diagnosis

## 9. Decision: Conservative Steam Ingestion

Decision:

Steam ingestion is intentionally conservative due to appdetails rate limits.

Operational approach:

- start with one ingestion worker
- keep `TRACK_GAMES_PER_RUN` modest
- use retry and paced requests
- use cooldown + exponential backoff on HTTP 429
- add more workers only after stability
- expand tracked catalog in phases (10k -> 25k -> 50k) using rollout hold controls rather than one-step activation

Current implementation anchors:

- worker pacing/tuning: `main.py` (`TRACK_GAMES_PER_RUN`, delays, retries, sharding)
- appdetails rate-limit logic: `scraper/steam_scraper.py` (`APPDETAILS_429_COOLDOWN_SECONDS`, backoff base/max, request delay)

Why this was chosen:

- Steam appdetails returns 429 under aggressive patterns
- aggressive concurrency can cause cascading retry/cooldown behavior

Tradeoff:

- slower full-catalog coverage per cycle
- throughput tuning must be iterative and monitored
- rollout execution needs explicit operator discipline and phase validation

## 10. Decision: Seed Catalog First, Then Ingest

Decision:

Seed `games` first from Steam catalog, then let ingestion enrich over time.

Current implementation:

- `scripts/seed_steam_games.py` populates `games`
- same script queues `dirty_games` for initial snapshot processing
- ingestion (`main.py`) subsequently builds history and metadata progressively

Why this was chosen:

- fast initial catalog coverage
- progressive enrichment without blocking on full metadata completeness
- avoids discovery bottlenecks

Tradeoff:

- some catalog rows can exist before price/player history exists
- snapshots may exist with partial fields before repeated ingestion cycles fill them

## 11. Decision: Safe, Idempotent Setup Script

Decision:

`setup_database.py` is a rerunnable bootstrap/migration script and must remain safe to rerun.

Required behavior to preserve:

- additive DDL and index updates
- heavy use of `IF NOT EXISTS`
- per-statement logging for visibility
- commit/rollback per statement in `run_sql_statements`
- fail-fast on non-tolerable errors instead of hidden transaction-aborted cascades

Why this was chosen:

- easier local debugging
- safer bootstrap in mixed schema states
- smoother incremental evolution without full migration framework

Tradeoff:

- script grows over time
- migration discipline is required to avoid drift and duplication

## 12. Decision: AI / Codex Prompt-Driven Development

Decision:

- implementation tasks are commonly run via Codex prompts
- prompts should include sufficient context and be copy-paste ready
- AI agents must inspect schema and queue contracts before changing ingestion/dirty queue logic
- docs are part of the system: `AGENTS.md`, `SYSTEM_CONTEXT.md`, `AI_SYSTEM_MAP.md`, `LIVE_REPO_PATHS.md`, and bootstrap docs

Why this was chosen:

- improves implementation consistency
- reduces repeated context reconstruction
- makes AI-assisted changes safer in critical areas

Tradeoff:

- docs must stay current
- stale docs increase risk of incorrect AI changes

## 13. Non-Negotiable Constraints

Do not casually break these:

- do not bypass dirty queue
- do not move heavy history scans into APIs
- do not make ingestion aggressively concurrent without rate-limit controls
- do not hardcode production DB URLs
- do not break idempotent queue writes
- do not remove append-only history model
- do not break snapshot-based serving model

## 14. Future Evolution

Likely next steps that preserve current design:

- 2-worker / 3-worker ingestion sharding using existing shard env settings
- partitioning strategy for history tables at higher scale
- richer alert/event logic and dedupe coverage
- more advanced section caching and cache invalidation strategy
- production deployment on hosted Postgres with operational guardrails
- stronger rate-limit-aware ingestion scheduling and adaptive throttling

## 15. How to Use This Document

Before large architecture changes:

- read this file first
- verify the proposed change preserves these decisions
- explicitly justify any intentional deviation

If a change must deviate from these choices, document:

- what is changing
- why the existing decision is no longer sufficient
- migration/rollback plan
