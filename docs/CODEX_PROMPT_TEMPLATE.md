# NEWWORLD Codex Task Template

Use this template for implementation requests to Codex.

## Copy-Paste Prompt

```text
Follow all applicable AGENTS.md / AGENTS.override.md instructions first.

Before making changes, do this in order:

1. Read and follow every applicable AGENTS.md / AGENTS.override.md from repo root to the working directory.
2. Read:
   - docs/AI_SYSTEM_BOOTSTRAP.md
   - docs/ARCHITECTURE_DECISIONS.md
   - docs/OPERATIONS_RUNBOOK.md
3. If this task touches ingestion, queues, snapshots, analytics flow, caching, API data flow, or frontend data wiring, also read:
   - docs/DATA_PIPELINE_DIAGRAM.md
4. Inspect these files before making schema/pipeline assumptions:
   - database/models.py
   - database/dirty_games.py
   - jobs/run_price_ingestion_loop.py
   - jobs/refresh_snapshots.py

After reading, briefly summarize:
- architecture constraints to preserve
- dirty queue semantics
- intended frontend/API data path
- whether this task affects ingestion, snapshots, or API contracts

Then implement the requested changes directly.

Preserve:
- append-only history tables
- dirty queue semantics
- snapshot-based APIs
- idempotent setup scripts
- environment-based DB config

Never:
- bypass dirty_games
- run heavy history scans in APIs
- aggressively parallelize Steam ingestion without controls
- hardcode production DB URLs

Implementation requirements:
- fix root causes, not cosmetic symptoms
- preserve snapshot/cache-backed architecture
- normalize payload mismatches in one place when needed
- keep loading/empty/error states accurate

Validation requirements:
- run relevant lint/tests/type checks if configured
- verify touched endpoints remain snapshot/cache-backed
- verify no new API parsing/runtime regressions

Required output format:
1. Repository analysis
2. Files to modify
3. Implementation plan
4. Code changes
5. Validation steps
6. Expected outcome

Task:
[PASTE TASK HERE]
```

## Notes

- AGENTS files are the durable source of repository operating rules.
- Reuse this template to reduce prompt drift across tasks.
