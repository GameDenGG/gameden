
# NEWWORLD Steam Deals Platform
## Backend Quick Start + Architecture Guide

---

# Overview

NEWWORLD is a Steam deal discovery platform that tracks:

- Game prices
- Discounts
- Player counts
- Price history
- Deal events
- Trending deals
- Historical lows
- Wishlist alerts
- Push notifications

The system continuously ingests Steam data and builds cached dashboards for fast APIs.

---

# High-Level Architecture

```
Steam API
   ↓
Price Ingestion Worker
   ↓
game_prices
   ↓
latest_game_prices
   ↓
Snapshot Worker
   ↓
game_snapshots
   ↓
dashboard_cache
   ↓
Frontend APIs
```

Workers run continuously and update the database with new price and player data.

---

# Core Backend Components

## 1. Price Ingestion Scheduler

Collects:

- game price
- original price
- discount percent
- player count
- metadata

Worker:

```
python -m jobs.run_price_ingestion_loop
```

Runs every few minutes and inserts rows into:

```
game_prices
```

---

## 2. Snapshot Worker

Processes raw data and creates summarized game records.

Run:

```
python -m jobs.refresh_snapshots
```

Responsibilities:

- update game_snapshots
- detect deal events
- compute trending scores
- rebuild dashboard cache

---

# Database Tables

## Core Tables

games

game_prices

latest_game_prices

game_snapshots

dashboard_cache

---

## Deal Detection

deal_events

Tracks:

NEW_SALE

PRICE_DROP

HISTORICAL_LOW

PLAYER_SPIKE

---

## Analytics Tables

game_player_history

Stores historical player counts.

---

## User Engagement

wishlist_items

user_alerts

deal_watchlists

push_subscriptions

---

# Price History System

API:

```
GET /games/{game_id}/price-history
```

Returns:

- historical price timeline
- historical low
- sale events

---

# Player Activity System

API:

```
GET /games/{game_id}/player-history
```

Returns:

- player timeline
- 7-day statistics
- peak players

---

# Deal Detection Engine

Automatic events:

- NEW_SALE
- PRICE_DROP
- HISTORICAL_LOW
- PLAYER_SPIKE

These power homepage sections:

🔥 Trending Deals

📉 Biggest Price Drops

🏆 New Historical Lows

---

# Global Deal Leaderboards

Endpoints:

```
/leaderboards/top-deals-today
/leaderboards/historical-lows
/leaderboards/biggest-price-drops
/leaderboards/most-played-deals
/leaderboards/trending-deals
```

These pages drive organic traffic.

---

# Deal Discovery Engine

Search API:

```
GET /deals/search
```

Filters:

- discount percent
- price
- review score
- player count
- genre
- release year

Sorting:

- trending
- biggest discount
- most players
- lowest price

---

# Wishlist + Price Targets

Users can configure:

- alert when price <= target
- alert when discount >= target

Table:

deal_watchlists

Alert types:

PRICE_TARGET_HIT

DISCOUNT_TARGET_HIT

Alerts stored in:

user_alerts

---

# Push Notifications

Users subscribe via:

```
POST /notifications/subscribe
```

Triggers:

- NEW_SALE
- PRICE_DROP
- HISTORICAL_LOW
- PLAYER_SPIKE
- PRICE_TARGET_HIT

Delivered through browser push using VAPID keys.

---

# Running the System

Start ingestion worker:

```
python -m jobs.run_price_ingestion_loop
```

Start snapshot worker:

```
python -m jobs.refresh_snapshots
```

Run both workers simultaneously.

---

# Health Check Queries

Price ingestion:

```
SELECT COUNT(*) FROM game_prices;
```

Latest prices:

```
SELECT COUNT(*) FROM latest_game_prices;
```

Snapshots:

```
SELECT COUNT(*) FROM game_snapshots
WHERE latest_price IS NOT NULL;
```

Cache status:

```
SELECT cache_key, updated_at
FROM dashboard_cache
WHERE cache_key='home';
```

---

# System Pipeline

```
Steam API
   ↓
Price ingestion scheduler
   ↓
game_prices
   ↓
latest_game_prices
   ↓
snapshot worker
   ↓
game_snapshots
   ↓
dashboard_cache
   ↓
API endpoints
   ↓
Frontend
```

---

# Next Development Priorities

1. Frontend deal explorer UI

Filters for:

- discount
- price
- player count
- reviews

---

2. Interactive price charts

Display:

- price history
- sale markers
- historical lows

---

3. Player activity graphs

Show:

- 24h players
- 7d players
- spikes during sales

---

4. SEO leaderboard pages

Examples:

- Top Steam Deals Today
- Best RPG Deals
- Most Played Steam Deals

---

5. Production deployment

Deploy workers with:

Docker

Railway

Fly.io

AWS

Workers required:

price_ingestion_worker

snapshot_worker

---

# System Status

The backend infrastructure is feature-complete for a Steam deal tracking platform.

Remaining work focuses on:

- frontend UX
- SEO pages
- deployment
