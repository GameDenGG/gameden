# NEWWORLD Data Pipeline Diagram

## 1. Purpose

This document provides a visual explanation of how data moves through the NEWWORLD system.

Use it when debugging:

- ingestion
- dirty queue behavior
- snapshot refresh
- dashboard cache
- API data flow

---

## 2. High-Level Pipeline Diagram

```text
Steam Store / Steam API
        |
        v
jobs.run_price_ingestion_loop
        |
        v
game_prices
game_player_history
latest_game_prices
        |
        v
dirty_games
        |
        v
jobs.refresh_snapshots
        |
        v
game_snapshots
dashboard_cache
        |
        v
FastAPI endpoints
        |
        v
Frontend / dashboards
```

---

## 3. Data Ownership by Stage

- Ingestion worker owns raw fact collection and dirty queue marking.
- Snapshot worker owns derived-state recomputation and cache rebuilds.
- API layer should read snapshots/cache, not recompute heavy history logic.

---

## 4. Debug Routing Guide

- Price/player updates missing: inspect ingestion logs and `game_prices`, `game_player_history`.
- Queue backlog growing: inspect `dirty_games` volume and snapshot worker status.
- Dashboard stale: inspect `game_snapshots`, `dashboard_cache`, then snapshot worker cycle logs.
- API returning slow/stale lists: confirm endpoint is reading cache/snapshots instead of raw history scans.
