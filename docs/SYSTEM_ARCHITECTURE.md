# NEWWORLD Steam Deals Platform
## System Architecture

---

# 1. High-Level System Diagram

```text
Steam Store API / Steam Player Data
                ↓
      Price Ingestion Scheduler
                ↓
            game_prices
                ↓
        latest_game_prices
                ↓
         Snapshot Refresh Worker
                ↓
           game_snapshots
                ↓
     ┌──────── dashboard_cache ────────┐
     ↓                                  ↓
Frontend APIs                    Leaderboards / Search / Discovery
```

---

# 2. Core Workers

## Price Ingestion Worker
Command:

```bash
python -m jobs.run_price_ingestion_loop
```

Responsibilities:
- Fetch Steam pricing
- Fetch discount data
- Fetch player counts
- Save historical rows into `game_prices`
- Save player rows into `game_player_history`
- Mark affected games as dirty

## Snapshot Refresh Worker
Command:

```bash
python -m jobs.refresh_snapshots
```

Responsibilities:
- Read dirty games
- Refresh `latest_game_prices`
- Refresh `game_snapshots`
- Detect deal events
- Compute deal score / trending score / recommendation score
- Rebuild `dashboard_cache`

---

# 3. Database Tables

## Core Catalog
- `games`
- `game_prices`
- `latest_game_prices`
- `game_snapshots`
- `dashboard_cache`

## Pipeline / Reliability
- `dirty_games`
- `job_status`

## Deal Intelligence
- `deal_events`

## Player Analytics
- `game_player_history`

## User Engagement
- `wishlist_items`
- `deal_watchlists`
- `user_alerts`
- `push_subscriptions`
- `game_interest_signals`

---

# 4. Data Flow

## Ingestion Flow
```text
Steam API
  ↓
run_price_ingestion_loop
  ↓
game_prices
  ↓
dirty_games
```

## Refresh Flow
```text
dirty_games
  ↓
refresh_snapshots
  ↓
latest_game_prices
  ↓
game_snapshots
  ↓
dashboard_cache(home)
```

## Alert Flow
```text
price change / snapshot refresh
  ↓
deal_events created
  ↓
user_alerts created
  ↓
push notifications sent
```

---

# 5. API Surface

## Dashboard / Home
- `GET /dashboard/home`

## Search / Discovery
- `GET /search`
- `GET /games/released`
- `GET /deals/search`

## History APIs
- `GET /games/{game_id}/history`
- `GET /games/{game_id}/price-history`
- `GET /games/{game_id}/player-history`

## Leaderboards
- `GET /leaderboards/top-deals-today`
- `GET /leaderboards/historical-lows`
- `GET /leaderboards/biggest-price-drops`
- `GET /leaderboards/most-played-deals`
- `GET /leaderboards/trending-deals`

## Alerts / Notifications
- `POST /notifications/subscribe`
- `POST /notifications/unsubscribe`
- `GET /alerts/{user_id}`
- `GET /alerts/unread/{user_id}`
- `POST /alerts/read`

## Watchlists
- `POST /wishlist/add`
- `POST /wishlist/remove`
- `GET /wishlist/{user_id}`
- `POST /deal-watchlists/add`
- `POST /deal-watchlists/remove`
- `GET /deal-watchlists/{user_id}`

## Ops
- `GET /health`
- `GET /metrics`

---

# 6. Scoring Systems

## Deal Score
Combines:
- discount percent
- closeness to historical low
- review quality
- player activity
- player momentum

## Trending Score
Combines:
- discount percent
- current players
- player growth / spikes

## Recommendation Score
Combines:
- deal score
- popularity score
- wishlist/watchlist counts
- interaction signals
- historical low bonus

---

# 7. Homepage Sections Powered by Backend

- Deal Ranked
- Biggest Deals
- Historical Lows
- Top Reviewed
- Top Played
- Upcoming
- Recommended Deals
- New Deals Since Last Visit
- Recently Updated
- Trending Deals
- Biggest Price Drops
- New Historical Lows

---

# 8. Reliability / Monitoring

## Health Sources
- `job_status`
- `dashboard_cache`
- `dirty_games`
- `/metrics`

## Key Metrics
- `/dashboard/home` p95 latency
- `/search` p95 latency
- `/games/released` p95 latency
- minutes since snapshot success
- dirty queue size
- cache hit rates

---

# 9. Runtime Setup

Run these in separate terminals:

```bash
python -m jobs.run_price_ingestion_loop
python -m jobs.refresh_snapshots
```

Optional:
- API server
- frontend dev server

---

# 10. Production Deployment Shape

Recommended services:
- API service
- ingestion worker
- snapshot worker
- Postgres (Neon)

Recommended future additions:
- background scheduler / supervisor
- read replica
- async ingestion fanout
- SEO rendered pages
