# NEWWORLD AI System Map

This document helps AI coding agents understand the NEWWORLD codebase quickly and safely.

Its goal is to reduce bad assumptions, prevent unnecessary rewrites, and improve implementation quality across backend, frontend, workers, database changes, and deployment work.

Use this document together with:

- SYSTEM_ARCHITECTURE.md
- NEWWORLD_QUICK_START_UPDATED.md
- DEVELOPER_ROADMAP.md
- NEWWORLD_FEATURE_IMPLEMENTATION_GUIDE.md
- ENGINEERING_PLAYBOOK.md
- GROWTH_PLAYBOOK.md
- AI_ENGINEERING_GUIDELINES.md

---

# 1. What NEWWORLD Is

NEWWORLD is a Steam deals and discovery platform.

It tracks:

- game prices
- discounts
- player counts
- price history
- deal events
- historical lows
- trending games
- wishlist alerts
- push notifications
- leaderboard and discovery surfaces

The product is not just a discount tracker.

Its long-term value is:

- deal intelligence
- discovery
- alerts
- trend visibility
- SEO pages
- shareable deal insights

---

# 2. Core Architecture

The platform follows a pipeline model.

```text
Steam API / Steam Player Data
    ↓
Price Ingestion Worker
    ↓
game_prices
    ↓
latest_game_prices
    ↓
Snapshot Refresh Worker
    ↓
game_snapshots
    ↓
dashboard_cache
    ↓
API
    ↓
Frontend
```

Key rule:

**Raw data is collected first. Derived intelligence is computed later.**

That means:

- ingestion writes raw facts
- snapshot worker computes rankings, events, and cached sections
- APIs should read precomputed state
- frontend should not depend on expensive live computations

---

# 3. Mental Model for the Data Layer

Think of the database in three layers.

## A. Raw historical data

These tables grow continuously.

- `game_prices`
- `game_player_history`

Purpose:
- record historical facts
- support charts and trends
- allow recomputation of derived values later

Rules:
- prefer append-only writes
- avoid expensive updates
- index for history lookups

---

## B. Current derived state

These tables power product experiences directly.

- `latest_game_prices`
- `game_snapshots`

Purpose:
- keep the latest usable game state
- store scores, rankings, trend signals, historical low status, and explanation metadata

Rules:
- one row per game or per scope
- deterministic refresh logic
- safe to rebuild from upstream data

---

## C. Presentation cache

These tables power fast public responses.

- `dashboard_cache`

Purpose:
- homepage sections
- leaderboard sections
- public discovery surfaces

Rules:
- request handlers should prefer cache over recomputing
- cache can be section-based, not only page-based

---

# 4. Major Runtime Processes

## 4.1 Price Ingestion Worker

Typical command:

```bash
python -m jobs.run_price_ingestion_loop
```

Responsibilities:
- fetch Steam pricing
- fetch discount data
- fetch player counts
- write rows to `game_prices`
- write rows to `game_player_history`
- mark changed games as dirty

Important rules:
- must tolerate partial failures
- must be idempotent
- should log throughput, failures, retries, and cycle time
- must not crash the whole cycle because a subset of games fails

---

## 4.2 Snapshot Refresh Worker

Typical command:

```bash
python -m jobs.refresh_snapshots
```

Responsibilities:
- consume dirty games
- refresh `latest_game_prices`
- refresh `game_snapshots`
- detect deal events
- compute ranking scores
- rebuild dashboard cache

Important rules:
- should process changed games only when possible
- must avoid duplicate events
- must avoid duplicate alerts
- must be safe to rerun

---

## 4.3 Optional Catalog Sync / Metadata Jobs

Possible future role:
- discover newly released games
- discover upcoming games
- refresh metadata
- keep release dates current

Important note:
upcoming games do not necessarily behave exactly like released games.

---

# 5. Important Tables and What They Mean

## `games`
Canonical catalog table.

Contains:
- game identity
- metadata
- release state
- genres/tags if available
- release date
- store identifiers

AI rule:
treat this as the canonical source for which games exist.

---

## `game_prices`
Historical record of prices and discounts over time.

Typical use:
- price history chart
- historical low detection
- sale event detection

AI rule:
do not build product features by querying this table directly on every request.

---

## `latest_game_prices`
Most recent materialized price state.

Typical use:
- current price display
- current discount
- latest snapshot inputs

AI rule:
should usually contain one row per game or one row per supported scope.

---

## `game_player_history`
Historical player count events.

Typical use:
- player charts
- trend detection
- momentum scoring

AI rule:
use this for worker-side calculations, not heavy request-time scans.

---

## `game_snapshots`
The most important product-serving table.

This is where derived intelligence should live.

Expected types of fields:
- current price
- original price
- discount percent
- current players
- review score
- deal score
- trending score
- worth buying score
- historical low flags
- explanation fields
- heat / popularity metadata

AI rule:
when implementing rankings, leaderboard logic, or homepage sections, prefer extending `game_snapshots`.

---

## `dashboard_cache`
Stores prebuilt homepage or leaderboard sections.

AI rule:
if a feature belongs on the homepage or a recurring public list, prefer rebuilding it in workers and serving it from cache.

---

## `dirty_games`
Queue-like table for games that need refresh.

AI rule:
treat this like a real queue, not just a loose marker list.

Recommended properties:
- dedupe-safe
- observable
- safe locking
- retryable
- avoids queue explosion

---

## `deal_events`
Stores meaningful product events such as:
- new sale
- price drop
- historical low
- player spike

AI rule:
events must be deduplicated and stable enough to power alerts, pages, and social sharing.

---

## User-facing tables
Examples:
- `wishlist_items`
- `deal_watchlists`
- `user_alerts`
- `push_subscriptions`
- `game_interest_signals`

AI rule:
keep these tables user-centric, not overloaded with heavy analytics logic.

---

# 6. How Features Should Usually Be Implemented

When adding a new feature, follow this mental path:

## Step 1
Ask:
Is this feature based on raw history, current state, or presentation cache?

## Step 2
If it involves ranking, trend detection, alerts, or badges:
- compute it in the snapshot worker
- store it in `game_snapshots`

## Step 3
If it powers homepage rows, leaderboards, or recurring public sections:
- cache it in `dashboard_cache`

## Step 4
If it is a user-specific interaction:
- connect snapshot data with user tables in API or service layer

## Step 5
If it needs explanation:
- generate explanation metadata on the backend
- avoid putting explanation-only business logic in the frontend

---

# 7. What Should NOT Happen

AI agents should avoid these mistakes.

## Bad pattern: live heavy recomputation
Do not calculate full rankings inside request handlers.

Bad:
- scan large history tables per request
- recompute leaderboard logic on page load

Good:
- compute in worker
- read from snapshots/cache

---

## Bad pattern: duplicate event generation
Do not insert identical deal events every refresh cycle.

Good:
- use dedupe keys
- compare prior state
- emit only on meaningful changes

---

## Bad pattern: queue explosion
Do not insert many dirty rows for the same game if one queued row is enough.

Good:
- dedupe dirty queue
- update timestamps/reasons instead of duplicating

---

## Bad pattern: frontend owning business logic
Do not make the frontend reconstruct ranking logic from raw fields if the backend can provide explanation metadata.

Good:
- return `reason_summary`, tags, badges, and score components from the API

---

## Bad pattern: destructive schema changes by default
Do not rewrite tables unless absolutely necessary.

Good:
- additive migrations
- safe indexes
- backfill strategy if needed

---

# 8. Backend File Map Guidance

Exact file names may vary, but AI agents should expect code to be organized around these areas.

## Likely backend areas

### Jobs / workers
Look for:
- `jobs/`
- `workers/`
- `tasks/`

Common responsibilities:
- ingestion loop
- snapshot refresh
- cache rebuilds

---

### API routes / controllers
Look for:
- `api/`
- `routes/`
- `controllers/`

Common responsibilities:
- dashboard endpoint
- search/discovery endpoints
- game detail endpoints
- alerts endpoints
- leaderboard endpoints

---

### Services / business logic
Look for:
- `services/`
- `domain/`
- `lib/`

Common responsibilities:
- score calculation
- event detection
- cache builders
- alert generation

---

### Database / schema / models
Look for:
- `models/`
- `db/`
- `migrations/`
- `schema/`

Common responsibilities:
- table definitions
- query helpers
- migrations
- indexes

---

# 9. Frontend File Map Guidance

Exact file names may vary, but AI agents should expect a structure like this.

## Likely frontend areas

### Pages / route entries
Look for:
- `pages/`
- `app/`
- `routes/`

Examples:
- homepage
- discovery page
- game details page
- leaderboard pages

---

### Components
Look for:
- `components/`
- `ui/`

Expected reusable pieces:
- deal cards
- leaderboard rows
- charts
- filter controls
- badges
- alert controls

---

### Data fetching / API hooks
Look for:
- `hooks/`
- `lib/api/`
- `queries/`

Expected responsibilities:
- dashboard fetch
- deals search
- game detail fetch
- alert actions

---

### SEO / metadata utilities
Look for:
- metadata builders
- OG image helpers
- canonical URL helpers
- structured data generators

AI rule:
for public discovery pages, SEO behavior is part of the feature, not an afterthought.

---

# 10. Product Surfaces That Matter Most

When prioritizing implementation quality, these surfaces matter most.

## Homepage
Critical because it is:
- first impression
- discovery surface
- freshness signal

Desired traits:
- fast
- scanable
- cached
- visually clear

---

## Deal discovery page
Critical because it is:
- high intent
- filter-heavy
- likely SEO landing destination

Desired traits:
- stable filters
- good sort behavior
- strong card design

---

## Game details page
Critical because it converts interest into action.

Desired traits:
- current price
- historical chart
- player trend chart
- reason to buy now
- alert/watchlist actions

---

## Leaderboard pages
Critical because they scale traffic.

Examples:
- trending deals
- top deals today
- biggest price drops
- historical lows
- most played deals

Desired traits:
- SEO-friendly
- easy to generate
- derived from snapshots/cache

---

# 11. Ranking Systems and Explainability

NEWWORLD will likely have multiple ranking systems.

Examples:
- deal score
- trending score
- recommendation score
- worth buying score
- heat score

AI rule:
every ranking system should have:

- a version number
- explanation metadata
- tunable components
- deterministic logic

Suggested payload patterns:
- `score`
- `score_version`
- `reason_summary`
- `components`
- `tags`

This improves:
- debugging
- UI clarity
- A/B testing
- future tuning

---

# 12. Alerts and Notifications Mental Model

Alerts should be driven by meaningful state changes.

Examples:
- new sale
- price target hit
- discount target hit
- historical low reached
- player spike worth surfacing

AI rule:
alerts should be based on stable events, not raw ingestion noise.

Good alert design:
- deduplicated
- timestamped
- reasoned
- linked to a game page
- safe to mark read/unread

---

# 13. SEO System Map

Public growth will likely come from SEO.

That means AI agents should treat these as first-class pages:

- top deals today
- trending deals
- new historical lows
- genre deal pages
- price-tier deal pages
- upcoming releases
- recently updated pages

AI rule:
for public discovery pages, implement:

- canonical URLs
- titles
- meta descriptions
- Open Graph metadata
- internal links
- structured data where appropriate

---

# 14. Scaling Assumptions

The architecture is intended to scale by:

- append-only history
- per-game snapshot materialization
- cached public sections
- dirty-queue-based partial recomputation

Rough growth stages:
- 10k games: current important milestone
- 25k games: should still work with modest tuning
- 50k games: may need more operational tuning
- 100k+ games: likely needs fanout, partitioning, or replicas

AI rule:
prefer changes that preserve this scaling path.

---

# 15. Safe Migration Guidance for AI Agents

Before adding migrations, ask:

1. Is this additive?
2. Does it preserve existing behavior?
3. Does it need a backfill?
4. Does it need a new index?
5. Can it be deployed without downtime?

Good migration patterns:
- add nullable columns first
- backfill separately if needed
- add indexes safely
- switch reads after backfill
- remove old behavior later only if necessary

---

# 16. Safe Prompting Guidance for AI Agents

When Codex or another AI agent is working in this repo, it should follow this order:

1. inspect repository structure
2. explain approach
3. list files to change
4. confirm migrations
5. implement backend changes
6. implement worker changes
7. implement frontend changes
8. add tests
9. provide validation and rollout steps

AI rule:
never jump straight to speculative large refactors.

Prefer:
**minimal, high-confidence changes over large speculative rewrites.**

---

# 17. Repo Notes to Fill In Later

As the codebase evolves, keep this section updated with actual paths.

Examples to fill in:
- ingestion worker file:
- snapshot worker file:
- dashboard cache builder:
- leaderboard route files:
- game detail page file:
- deals search page file:
- migrations directory:
- shared score utilities:
- alert generation logic:
- SEO metadata utilities:

This turns the file into a living AI map instead of a one-time note.

---

# 18. Recommended AI Working Style for This Repo

When working on NEWWORLD, AI agents should behave like a senior technical lead.

That means:
- inspect first
- preserve architecture
- improve reliability
- prefer derived-state patterns
- avoid frontend business logic drift
- avoid queue duplication
- keep SEO in mind for public pages
- expose explanation metadata
- think about the next scale step without overengineering

---

# 19. Final Rule

If a requested change conflicts with the current architecture:

1. explain the conflict
2. propose the safest alternative
3. implement the safest high-confidence version

The goal is not just to make code changes.

The goal is to make NEWWORLD more reliable, scalable, understandable, and easier to extend.
