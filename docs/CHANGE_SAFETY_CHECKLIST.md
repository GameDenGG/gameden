# NEWWORLD Change Safety Checklist

Use this checklist before modifying critical systems.

---

## Critical Paths

Changes touching these require extra care:

- ingestion worker
- snapshot worker
- dirty queue
- game_prices
- latest_game_prices
- game_snapshots
- dashboard_cache
- deal_events
- alerts
- leaderboard APIs
- homepage cache

---

## Pre‑Change Questions

1. What user behavior depends on this?
2. Which worker owns this logic?
3. Does it preserve pipeline architecture?
4. Could it create duplicate events?
5. Could it slow API queries?
6. Does it require a migration?
7. Does it require a backfill?
8. What is the rollback plan?

---

## Must‑Not‑Break Invariants

Pipeline:

- ingestion → history tables
- workers → snapshots
- APIs → snapshots/cache

Queue:

- dirty queue deduplicated
- workers idempotent

Performance:

- no heavy history scans in APIs
- leaderboards read snapshots/cache

---

## Required Plan for Critical Changes

Implementation plan must include:

- files changed
- tables affected
- migrations
- validation steps
- rollback plan

---

## Final Rule

Optimize for:

- correctness
- rollback safety
- reliability

Not speed of coding.