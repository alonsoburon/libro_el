---
title: "Backfill Strategies"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Backfill Strategies

> **One-liner:** Reloading 6 months of data without breaking prod -- how to backfill safely alongside live pipelines.

## The Problem
- Something went wrong upstream (schema change, bad deploy, data corruption) and you need to reload a historical range
- A naive backfill -- "just rerun everything" -- competes with live scheduled runs for source connections, destination quota, and orchestrator capacity
- Large backfills on the source during business hours violate every rule in [[06-operating-the-pipeline/0607-source-system-etiquette|0607]]

## Backfill Types

### Date-Range Backfill
- Reload a specific date range -- last 3 months, last fiscal quarter, a single bad week
- Uses partition swap (see [[02-full-replace-patterns/0203-partition-swap|0203]]) or rolling window replace (see [[02-full-replace-patterns/0206-rolling-window-replace|0206]])
- The date range is explicit; everything outside it is untouched

### Full Table Backfill
- Reload the entire table from scratch -- when corruption is too widespread for a scoped fix
- Uses full replace (see [[04-load-strategies/0401-full-replace|0401]])
- Resets all state: incremental cursors, DLT pipeline state, schema versions
- After a full backfill, the next scheduled incremental run picks up from the new baseline

### Selective Backfill
- Reload specific records by primary key -- a handful of corrupted orders, not the entire table
- Rarer; requires the extraction layer to support PK-based filtering
- Usually easier to just do a date-range backfill that covers the affected period

## Execution Strategy

### Isolation from Live Pipelines
- Backfills should not block or delay scheduled runs
- Run backfills in a separate job / schedule from live pipelines
- If your orchestrator supports run priority or queue separation, use it

### Chunking
- Break large backfills into date-range chunks (one month at a time, one week at a time)
- Each chunk is independently retriable -- if chunk 3 of 6 fails, retry only chunk 3
- Chunk size trades off between per-chunk overhead and blast radius on failure

### Safe Hours
- Large backfills go in the safe window (see [[06-operating-the-pipeline/0607-source-system-etiquette|0607]])
- If the backfill is too large for one safe window, span it across multiple nights with chunking

### Staging Persistence
- For multi-chunk backfills, staging tables may intentionally persist between chunks
- Don't clean up staging until the full backfill is validated (see [[06-operating-the-pipeline/0603-cost-monitoring|0603]] for the storage cost tradeoff)

## State Reset

- After a full backfill, clear incremental state (cursor position, pipeline state) so the next scheduled run doesn't skip the data you just reloaded
- Some loaders manage this automatically when you run with `full_refresh: true`; others require explicit state cleanup
- If state cleanup is a manual step, document it -- a backfill that reloads the data but leaves the old cursor in place will re-skip the same rows on the next incremental run

## Anti-Pattern

> [!danger] Don't backfill during business hours on a live source
> - A 6-month backfill is a sustained full scan. Running it at 2pm on a Tuesday will get your access revoked.

> [!danger] Don't backfill without clearing incremental state
> - Reloading the data while the cursor still points to the old high-water mark means the next incremental run will skip everything between the cursor and the new data. Clear the state or force a full refresh.

## Related Patterns
- [[02-full-replace-patterns/0203-partition-swap|0203-partition-swap]] -- the mechanism for date-range backfills in partitioned tables
- [[02-full-replace-patterns/0206-rolling-window-replace|0206-rolling-window-replace]] -- rolling window as a scoped backfill strategy
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- full table backfill
- [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]] -- safe hours and source protection
- [[06-operating-the-pipeline/0615-recovery-from-corruption|0615-recovery-from-corruption]] -- backfill as a recovery mechanism after corruption

## Notes
- **Author prompt -- start_date/full_refresh**: You built `start_date` and `full_refresh: true` as runtime config overrides in the Dagster launchpad. How often do you actually use these? Is it a weekly thing or a "few times a year" thing?
- **Author prompt -- state after backfill**: After running `_dlt_cleanup`, the next run is always a full refresh because DLT state is gone. Has that ever caused a surprise -- someone running cleanup without realizing the next scheduled run would be a full load on everything?
- **Author prompt -- multi-night backfills**: Have you ever had a backfill so large it couldn't finish in one safe-hours window? How did you chunk it?
- **Author prompt -- backfill + live conflict**: Has a backfill ever interfered with a scheduled incremental run -- same table, same time, competing for the same destination?
