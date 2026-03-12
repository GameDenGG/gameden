# NEWWORLD Codebase Constraints

This document defines **hard engineering guardrails** for the NEWWORLD
repository.

Its purpose is to prevent AI coding agents or contributors from
introducing architectural mistakes, unsafe refactors, or performance
regressions.

This file should be read together with:

-   AI_ENGINEERING_GUIDELINES.md
-   AI_SYSTEM_MAP.md
-   LIVE_REPO_PATHS.md
-   SYSTEM_ARCHITECTURE.md

------------------------------------------------------------------------

# 1. Architecture Must Remain Pipeline-Based

The system uses a **data pipeline architecture**.

    Steam API
    → ingestion worker
    → game_prices
    → latest_game_prices
    → snapshot refresh worker
    → game_snapshots
    → dashboard_cache
    → API
    → frontend

AI agents must **not collapse or bypass this pipeline**.

Examples of forbidden changes:

❌ Computing deal rankings inside API requests\
❌ Querying historical tables for every page request\
❌ Removing the snapshot layer

Correct pattern:

✔ Compute intelligence in workers\
✔ Store results in `game_snapshots`\
✔ Serve from snapshots or cache

------------------------------------------------------------------------

# 2. Snapshot Layer Is the Source of Product Intelligence

`game_snapshots` must remain the **central derived-state table**.

All ranking logic should live there.

Examples:

-   deal_score
-   trending_score
-   worth_buying_score
-   historical_low_flag
-   explanation metadata

Do NOT implement ranking systems that bypass this table.

------------------------------------------------------------------------

# 3. Dashboard Cache Must Serve Public Pages

Public discovery surfaces should rely on cached data.

Examples:

-   homepage sections
-   leaderboard pages
-   trending lists

Allowed sources:

-   `dashboard_cache`
-   `game_snapshots`

Forbidden:

❌ scanning historical tables during page requests

------------------------------------------------------------------------

# 4. Workers Must Be Idempotent

Background workers must be safe to rerun.

Rules:

-   running the worker twice must not corrupt state
-   workers must tolerate partial failures
-   retries must not generate duplicate events

Particularly important for:

-   snapshot refresh worker
-   ingestion worker

------------------------------------------------------------------------

# 5. Dirty Queue Must Be Deduplicated

`dirty_games` must behave like a queue.

It must not allow unlimited duplicates.

Correct behavior:

-   one queued row per game
-   updates modify timestamps instead of inserting duplicates

Incorrect behavior:

❌ inserting thousands of dirty rows for the same game

------------------------------------------------------------------------

# 6. Event Generation Must Be Stable

`deal_events` must represent meaningful state changes.

Examples:

-   NEW_SALE
-   PRICE_DROP
-   HISTORICAL_LOW
-   PLAYER_SPIKE

Do not generate duplicate events across snapshot refresh cycles.

Use dedupe keys such as:

    game_id + event_type + price_state

------------------------------------------------------------------------

# 7. Historical Tables Must Remain Append-Only

Large history tables:

-   `game_prices`
-   `game_player_history`

must remain append-only whenever possible.

Avoid:

❌ expensive updates\
❌ mass rewrites

Prefer:

✔ inserts only\
✔ indexed history lookups

------------------------------------------------------------------------

# 8. Migrations Must Be Safe

Database schema changes must follow safe patterns.

Allowed:

-   additive columns
-   new indexes
-   nullable fields

Avoid:

❌ destructive schema rewrites\
❌ large blocking migrations\
❌ manual production edits

If a migration is large:

-   split into phases
-   backfill separately

------------------------------------------------------------------------

# 9. Frontend Must Not Own Business Logic

Frontend components should **display signals, not compute them**.

Ranking logic must remain in backend snapshot generation.

Frontend may:

-   render scores
-   render explanation tags
-   display badges

Frontend must NOT:

❌ calculate rankings\
❌ infer deal scores from raw fields

------------------------------------------------------------------------

# 10. SEO Pages Must Remain Server-Friendly

Discovery pages should remain SEO-friendly.

Requirements:

-   canonical URLs
-   descriptive titles
-   meta descriptions
-   Open Graph tags
-   structured data where possible

AI agents should not convert SEO pages to purely client-rendered
experiences.

------------------------------------------------------------------------

# 11. Performance Constraints

Request handlers must avoid expensive operations.

Bad patterns:

❌ scanning large tables\
❌ recomputing rankings\
❌ fetching full history data

Good patterns:

✔ query `game_snapshots`\
✔ query `dashboard_cache`

------------------------------------------------------------------------

# 12. Scaling Expectations

The system must support growth stages:

    10k games   (current milestone)
    25k games   (near-term scale)
    50k games   (future growth)
    100k+ games (long-term architecture evolution)

AI agents must avoid decisions that block this scaling path.

------------------------------------------------------------------------

# 13. When Refactors Are Allowed

Refactors are allowed only when:

-   they simplify architecture
-   they reduce duplicated logic
-   they improve reliability
-   they preserve the pipeline model

Major structural changes must include:

1.  explanation of the problem
2.  safer alternative proposal
3.  migration strategy

------------------------------------------------------------------------

# 14. AI Agent Safety Rule

Before implementing changes, AI agents must:

1.  read `AI_ENGINEERING_GUIDELINES.md`
2.  read `AI_SYSTEM_MAP.md`
3.  read `LIVE_REPO_PATHS.md`
4.  inspect the repository
5.  list files that will change
6.  confirm migrations
7.  then implement code

Agents should never immediately rewrite large systems.

------------------------------------------------------------------------

# 15. Golden Rule

Prefer:

**minimal, high-confidence improvements**

over

**large speculative refactors**.

This repository prioritizes **reliability and scalability** over rapid
experimentation.
