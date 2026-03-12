# NEWWORLD System Context

This document gives engineers and AI agents a fast mental model of the NEWWORLD platform.

---

## What NEWWORLD Is

NEWWORLD is a Steam deals and discovery platform.

It tracks:

- game prices
- discounts
- player counts
- price history
- deal events
- historical lows
- trending games
- alerts
- leaderboard pages

Goal: help users discover which Steam games are worth buying right now.

---

## Core Architecture

Steam API → ingestion worker → game_prices → latest_game_prices → snapshot worker → game_snapshots → dashboard_cache → API → frontend

Key rule:

Raw data first → derived intelligence later.

---

## Data Layers

### Raw History

- game_prices
- game_player_history

Used for history and charts.

### Derived State

- latest_game_prices
- game_snapshots

Stores scores, trends, historical lows.

### Presentation Cache

- dashboard_cache

Used for homepage and discovery pages.

---

## Workers

### Ingestion

python -m jobs.run_price_ingestion_loop

Writes raw price and player data.

### Snapshot

python -m jobs.refresh_snapshots

Builds derived state and caches.

---

## Key Engineering Principles

1. Heavy logic runs in workers.
2. APIs read snapshots.
3. Frontend displays signals.
4. History tables append-only.
5. Workers idempotent.

---

## Scaling Targets

- 10k games current
- 25k games medium
- 50k games large
- 100k+ future