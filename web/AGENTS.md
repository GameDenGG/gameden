# AGENTS.md

## Mission

Maintain and improve GameDen.gg as a production system.

This is not a greenfield project.
Assume the codebase is already live, partially optimized, partially fixed, and interconnected.

Your job is to:
- fix root causes
- preserve working behavior
- avoid regressions
- complete changes end-to-end
- minimize wasted prompts, repeated debugging loops, and partial fixes

---

## Global Priorities (STRICT)

Apply these in order:

1. Correctness
2. Stability / regression safety
3. Performance
4. Minimal diff size
5. Maintainability

Do not trade correctness for speed.
Do not trade stability for refactoring elegance.

---

## Core Operating Rule

Every issue must be solved across the full real system path, not just the first visible symptom.

Always trace the chain that actually produces the bug:

input -> route -> parse -> fetch/load -> normalize -> state -> render -> interaction -> follow-on behavior

Depending on the feature, the exact chain may also include:
- caching
- snapshot restore
- deferred load
- personalization
- API resolver logic
- URL generation
- pagination
- filtering
- analytics
- persistence
- optimistic UI
- browser history behavior

Do not stop at the first improvement if later stages are still broken.

---

## Required Working Style

Before editing:
1. Identify the actual entry points for the behavior.
2. Trace the current implementation end-to-end.
3. Find the exact breakpoints or mismatch points.
4. Make the smallest complete fix.
5. Validate all affected entry paths.
6. Remove or avoid redundant fallback logic introduced by prior failed attempts.

When making changes:
- prefer surgical edits over broad rewrites
- preserve existing architecture unless change is necessary for correctness
- keep logic aligned across frontend and backend
- fix system contracts at boundaries, not with scattered patches

---

## Anti-Debugging-Loop Rules (MANDATORY)

These rules exist specifically to prevent wasted prompts and repeated partial fixes.

### 1. No Symptom-Only Fixes
Do not patch UI text, conditional rendering, or styling just to hide a deeper contract or state bug.

### 2. No Patch Stacking
Do not add multiple overlapping fallbacks without proving each is needed.
Avoid patterns like:
- "try this field, then this field, then this field"
- "if path fails, use query, then use name, then use DOM state"
unless the system contract explicitly requires it

### 3. No Single-Path Fixes
A fix is incomplete if it only works:
- after client navigation but not direct load
- on desktop but not mobile
- on homepage but not search
- for one card type but not another
- after warm cache but not cold load
- after refresh but not initial navigation

### 4. No Blind Refactors
Do not refactor large areas unless the task requires it.
Do not rename broad concepts or move code around just because it feels cleaner.

### 5. No Duplicate Logic Expansion
If the same logic exists in multiple places, prefer consolidating or fixing it at the boundary rather than patching each place independently.

### 6. No Repeated Guessing
If a previous pass failed, explicitly identify why it failed before making another attempt.

---

## Production Safety Rules

Assume all touched systems are live and interdependent.

Do not:
- break homepage
- break navigation
- break search
- break snapshot/session restore
- break deferred/personalized load
- break analytics sections
- break cached flows
- introduce full-page rerenders where incremental updates already exist
- introduce heavy request-time rebuilds
- introduce duplicate API calls
- introduce redundant filtering passes
- introduce new global architecture unless required

Preserve current behavior unless the task explicitly changes it.

---

## Data Contract Rules

Before changing UI or state logic for a data issue:

1. inspect the backend/API contract
2. inspect the frontend normalization path
3. confirm actual field names and types
4. identify where the contract becomes inconsistent

Rules:
- normalize once near the boundary
- use one stable internal shape where practical
- do not scatter backend-field fallbacks throughout rendering code
- do not patch bad upstream data in multiple components
- do not assume optional fields always exist
- distinguish missing, empty, invalid, and loading states correctly

When relevant, confirm naming consistency for concepts such as:
- id / game_id / appid / slug
- name / title / game_name
- price / base_price / sale_price
- image / capsule / cover / hero

---

## State Management Rules

Keep these states distinct:
- loading
- loaded
- empty
- error
- not-found
- stale-but-usable snapshot state

Do not:
- render empty state before fetch is complete
- conflate error and empty
- conflate not-found and missing input
- overwrite correct state with deferred or stale updates
- force full rerenders when partial state updates are sufficient

If snapshot or cached state exists:
- do not immediately discard it unless invalid
- do not trigger unnecessary re-fetch loops
- do not let deferred load overwrite already-correct visible state

---

## Performance Rules

Performance fixes must preserve correctness.

Always avoid:
- duplicate API calls
- duplicate resolver calls
- duplicate event listeners
- redundant filtering passes
- repeated DOM rebuilds of large sections
- repeated expensive serialization/parsing
- request-time heavy rebuilds if cached/precomputed data already exists
- unnecessary canonicalization reloads
- unnecessary re-fetch after history updates
- unnecessary fetch after restore when data is already valid

Prefer:
- reuse over recompute
- incremental DOM updates over full rerenders
- stable memoized/cached/shared helpers over copy-paste logic
- fixing data once upstream over repeated downstream cleanup

---

## Frontend Rules

Applicable whenever touching frontend behavior.

### Routing and URL Handling
- treat direct navigation, refresh, and client-side navigation as equally important
- parse route state from the real canonical source first
- do not rely on query params as the primary mechanism unless the feature is explicitly query-driven
- URL generation and URL parsing must stay aligned
- canonicalization must not reload unless explicitly intended
- history updates must not trigger duplicate fetches

### Rendering
- render from validated state
- do not patch invalid data in the DOM after render if it should have been filtered earlier
- preserve responsive behavior
- avoid layout overlap and stacking regressions
- preserve existing section order and navigation unless the task explicitly changes it

### Shared UI Systems
When changing a card, grid, rail, or listing pattern:
- inspect all emitters using that pattern
- inspect all click targets / href builders
- inspect empty/loading/error variants
- confirm consistency across homepage, search, detail pages, and tracked lists if relevant

---

## Backend Rules

Applicable whenever touching API/server behavior.

- do not redesign endpoints unless required
- do not widen expensive queries casually
- do not add slow fallback scans unless justified
- preserve existing caching, ETag, TTL, and snapshot assumptions
- avoid heavy request-time rebuilding when cached/precomputed data exists
- keep response shape stable unless change is necessary
- when response shape must change, align the frontend in the same pass
- prefer fixing resolver/normalization mismatches over creating new endpoints

For resolver-style or identifier-based endpoints:
- support the documented identifier types consistently
- ensure resolution logic and frontend URL strategy align
- avoid ambiguity between numeric IDs, app IDs, slugs, and names

---

## Search / Browse / Listing Rules

When working on homepage rails, search, browse, all-results, or grouped lists:

- validate the dataset source first
- confirm whether the page is using snapshot data, catalog data, API data, or merged state
- ensure filters apply to the intended dataset
- avoid duplicate filtering across multiple sections
- keep loading/empty/error states correct
- preserve sorting expectations
- ensure links emitted from cards are consistent with route expectations

Do not:
- apply request-time heavy analytics to user-facing list endpoints
- patch data per-card if the list dataset itself is wrong
- let one section’s filter logic silently diverge from another unless explicitly intended

---

## Analytics / Dashboard / Snapshot Rules

- preserve snapshot-backed sections
- do not convert cache-backed sections into heavy live-recompute flows
- keep analytics groupings stable and clearly separated
- do not introduce expensive frontend analytics computation that belongs in precomputed/cache-backed data
- do not let personalized/deferred updates overwrite already-correct critical content

---

## Validation Discipline (MANDATORY)

For every non-trivial change, validate all relevant entry paths, not just the path you edited.

Validation must include whichever are relevant to the task:
- direct load
- refresh
- internal navigation
- back/forward navigation
- cold state
- warm cache / restored state
- empty state
- error state
- not-found state
- desktop
- mobile/responsive behavior
- related emitters/components
- linked entry points from other pages

If a fix touches shared helpers or contracts, validate all consumers.

---

## Completion Gate

A task is NOT complete unless all of these are true:

1. Root cause is identified.
2. The full affected chain is aligned.
3. The change works across all relevant entry points.
4. No obvious regressions are introduced.
5. No unnecessary duplicate fetches/renders/listeners are introduced.
6. No new patch-layer was added to compensate for an unfixed upstream issue.
7. The final behavior matches the intended system contract.

If any major path still fails, the task is incomplete.

---

## Definition of Done

A change is done only when:
- the real bug is fixed
- the surrounding system remains stable
- the contract is consistent
- validation is complete
- the fix does not require another follow-up prompt just to finish the same issue

Partial fixes count as failures.

---

## Preferred Output Style for Coding Tasks

When completing implementation work, report only:
1. Root cause
2. Files modified
3. 1–2 line summary per file
4. Before/after validation evidence

Do not return brainstorming unless explicitly requested.
Do not propose multiple unfinished options when one correct implementation is possible.

---

## If Instructions Conflict

Use this order of precedence:
1. Direct user request
2. More specific subtree AGENTS.md
3. This root AGENTS.md

Within that, preserve:
- correctness
- regression safety
- performance
- minimal necessary change