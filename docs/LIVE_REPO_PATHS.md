# NEWWORLD Live Repo Paths

This document is the live source of truth for actual file paths in this repository.

## Requested Quick Paths
- ingestion worker:
  - `jobs/run_price_ingestion_loop.py`
  - `jobs/ingest_prices.py`
  - `main.py` (`track_all_games`)
- snapshot worker:
  - `jobs/refresh_snapshots.py`
- migrations:
  - `setup_database.py` (idempotent additive schema/index updates)
- API routes:
  - `api/server.py`
- dashboard cache builder:
  - `jobs/refresh_snapshots.py` (`rebuild_dashboard_cache`)
- homepage page:
  - `web/index.html`
- discovery page:
  - `web/all-results.html`
- game page:
  - `web/game-detail.html`
  - `web/game-detail.js`
- leaderboard pages:
  - `web/all-results.html` (view-based leaderboard rendering)
  - `api/server.py` (`/leaderboards/{board_type}`)

## 1. Repository Root Overview
- `main.py` — backend ingestion orchestration entry logic
- `api/` — FastAPI server, routes, cache/rate-limit utilities, metrics helpers
- `web/` — frontend pages/assets (static HTML/CSS/JS)
- `database/` — ORM models, sessions/engines, dirty queue helper
- `jobs/` — long-running workers and job entrypoints
- `scraper/` — Steam API fetch clients
- `services/` — push notification integration
- `scripts/` — diagnostics and scale validation scripts
- `docs/` — architecture and AI guidance docs

## 2. Core Workers
- Price ingestion worker entrypoint:
  - `jobs/run_price_ingestion_loop.py`
  - `jobs/ingest_prices.py`
  - `main.py` (`track_all_games`)
- Snapshot refresh worker entrypoint:
  - `jobs/refresh_snapshots.py`
- Catalog sync / metadata sync job:
  - `ingest_top_games.py`
- Queue processing utilities:
  - `database/dirty_games.py`
  - `jobs/refresh_snapshots.py` (`claim_dirty_batch`, retry/backoff helpers)
- Shared worker helpers:
  - `jobs/refresh_price_aggregates.py`

## 3. Data Pipeline Logic
- Steam API client / fetch logic:
  - `scraper/steam_scraper.py`
- Price normalization logic:
  - `scraper/steam_scraper.py` (`cents_to_dollars`, result normalization)
- Player count ingestion logic:
  - `scraper/steam_players.py`
  - `main.py` (write path to `game_player_history`)
- Dirty game enqueue logic:
  - `database/dirty_games.py`
  - `main.py`
  - `ingest_top_games.py`
- Snapshot build logic:
  - `jobs/refresh_snapshots.py`
- Score calculation utilities:
  - `jobs/refresh_snapshots.py` (deal, popularity, recommended, momentum, worth-buying, heat)
- Deal event detection logic:
  - `jobs/refresh_snapshots.py`
  - `main.py` (player spike during ingestion)
- Alert generation logic:
  - `jobs/refresh_snapshots.py`
  - `main.py`
- Dashboard cache build logic:
  - `jobs/refresh_snapshots.py` (`rebuild_dashboard_cache`)

## 4. Database Layer
- Schema definitions / ORM models:
  - `database/models.py`
- SQL query helpers / repositories:
  - `database/dirty_games.py`
- Migration directory:
  - No dedicated migration framework directory currently; schema updates are managed in:
  - `setup_database.py`
- Seed scripts:
  - `seed_games.py`
  - `ingest_top_games.py`
- Database connection / session management:
  - `database/__init__.py`

## 5. Core Tables and Primary Read/Write Files
- `games`:
  - `database/models.py`
  - `main.py`
  - `ingest_top_games.py`
  - `api/server.py`
- `game_prices`:
  - `database/models.py`
  - `main.py`
  - `jobs/refresh_snapshots.py`
  - `api/server.py`
- `latest_game_prices`:
  - `database/models.py`
  - `setup_database.py` (bootstrap SQL)
  - `jobs/refresh_snapshots.py` (incremental aggregate refresh)
- `game_player_history`:
  - `database/models.py`
  - `main.py`
  - `jobs/refresh_snapshots.py`
  - `api/server.py`
- `game_snapshots`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `api/server.py`
- `dashboard_cache`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `api/server.py`
- `dirty_games`:
  - `database/models.py`
  - `database/dirty_games.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
- `deal_events`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
  - `api/server.py`
- `wishlist_items`:
  - `database/models.py`
  - `api/server.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
- `deal_watchlists`:
  - `database/models.py`
  - `api/server.py`
  - `jobs/refresh_snapshots.py`
- `user_alerts`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
  - `api/server.py`
- `push_subscriptions`:
  - `database/models.py`
  - `api/server.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
- `game_interest_signals`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `api/server.py`
- `job_status`:
  - `database/models.py`
  - `jobs/refresh_snapshots.py`
  - `main.py`
  - `api/server.py`

## 6. API Surface
- API app entrypoint:
  - `api/server.py`
- Dashboard API routes:
  - `api/server.py` (`/dashboard/home`)
- Search / discovery routes:
  - `api/server.py` (`/search`, `/games/released`, `/deals/search`)
- Game detail routes:
  - `api/server.py` (`/games/{game_id}`, related detail helpers)
- Price history routes:
  - `api/server.py` (`/games/{game_id}/history`, `/games/{game_id}/price-history`)
- Player history routes:
  - `api/server.py` (`/games/{game_id}/player-history`)
- Leaderboard routes:
  - `api/server.py` (`/leaderboards/{board_type}`)
- Alerts routes:
  - `api/server.py` (`/alerts/*`)
- Watchlist routes:
  - `api/server.py` (`/wishlist/*`, `/deal-watchlists/*`)
- Notifications routes:
  - `api/server.py` (`/notifications/*`)
- Health / metrics routes:
  - `api/server.py` (`/health`, `/metrics`)
- Shared serializers / response schemas:
  - Inline helpers in `api/server.py`

## 7. Frontend App Structure
- App root / pages root:
  - `web/`
- Homepage file:
  - `web/index.html`
- Deal discovery page:
  - `web/all-results.html`
- Game details page:
  - `web/game-detail.html`
  - `web/game-detail.js`
  - `web/game-detail.css`
- Leaderboard pages:
  - route-level views currently rendered via `web/all-results.html` + query params
- Upcoming releases page:
  - surfaced in `web/index.html` and `web/all-results.html` filters/views
- Alerts / inbox page:
  - no dedicated standalone page yet (alerts rendered in homepage panels)
- Wishlist / watchlist page:
  - no dedicated standalone page yet (controls/panels integrated in homepage UI)
- Shared layout/components:
  - currently embedded in `web/index.html` script/styles (no separate component directory)
- Charts:
  - `web/game-detail.js` (Chart.js for price chart)
- Frontend fetch layer/state:
  - currently embedded in `web/index.html` and `web/game-detail.js`

## 8. SEO and Growth Files
- Metadata / canonical / OG / structured data helpers:
  - TODO (no dedicated SEO utility module yet)
- Static generation / server rendering utilities:
  - TODO (current frontend is static HTML + runtime fetch)
- Leaderboard SEO page helpers:
  - TODO (leaderboard view routing is currently query-param based)

## 9. Testing
- Backend tests root:
  - `tests/`
- Frontend tests root:
  - TODO
- Worker tests:
  - TODO (no dedicated worker test module yet)
- API tests:
  - TODO (no dedicated API test module yet)
- Snapshot / ranking tests:
  - `tests/test_scoring_engine.py`
- Queue / dirty game tests:
  - TODO
- End-to-end tests:
  - TODO

## 10. Deployment and Infrastructure
- Environment variable definitions:
  - `config.py`
  - `.env` (local)
- Local development startup files:
  - `main.py`
  - `jobs/run_price_ingestion_loop.py`
  - `jobs/refresh_snapshots.py`
- Docker / container config:
  - TODO
- Process supervisor / worker runtime config:
  - TODO
- CI / build pipelines:
  - TODO
- Production deployment manifests:
  - TODO
- Monitoring / logging config:
  - `logger_config.py`
  - `api/metrics.py`

## 11. Current Canonical Flows
- Ingestion flow:
  1. `jobs/run_price_ingestion_loop.py`
  2. `jobs/ingest_prices.py`
  3. `main.py` (`track_all_games`)
- Snapshot refresh flow:
  1. `jobs/refresh_snapshots.py` (`run_worker_forever` / `run_once`)
  2. `jobs/refresh_snapshots.py` (`refresh_snapshots_once`)
  3. `jobs/refresh_snapshots.py` (`rebuild_dashboard_cache`)

## 12. Related Canonical Docs
- `docs/AI_SYSTEM_MAP.md`
- `docs/dirty_queue_lifecycle.md`
- `docs/NEWWORLD_MASTER_FILE.md`
- `docs/NEWWORLD_QUICK_START_CONTEXT.md`
