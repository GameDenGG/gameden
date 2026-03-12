# Frontend Scoped Rules

These rules apply to the `web/` subtree in addition to root `AGENTS.md`.

## Data Contract First

Before changing UI logic for data-related issues:

- inspect API route contract and frontend normalization path first
- confirm field names/types before changing rendering logic
- prefer fixing payload normalization in one place over scattered patches

## Frontend State Rules

- keep loading, empty, and error states distinct
- do not render "no results" before fetch/catalog load completion
- avoid duplicate filtering logic across multiple sections/components
- ensure search uses the intended snapshot/catalog-backed dataset
- do not hardcode "released only" assumptions unless explicitly required

## Layout and UX Safety

- avoid overlap bugs from incorrect absolute positioning or z-index stacking
- preserve responsive behavior on desktop and mobile
- keep section titles and groupings clear and stable
- do not remove major homepage/menu sections (including Wishlist/Watchlist and key discovery panels) unless removal is explicitly requested
- when adjusting layout, preserve existing menu/panel functionality and interactions unless the task explicitly changes them

## Analytics and Dashboard Sections

- Top Played, Trending, and Alerts belong in a clearly separated analytics section
- seasonal, leaderboard, and dashboard sections must continue using snapshot/cache-backed sources
- do not introduce request-time heavy analytics into frontend or API paths

## Implementation Expectations

- fix root causes, not cosmetic symptoms only
- minimize scope and preserve existing architecture contracts
- validate touched behavior with targeted checks/tests
