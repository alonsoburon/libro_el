---
title: "Backfill Strategies"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-29
---

# Backfill Strategies

> **One-liner:** Reloading 6 months of data without breaking prod -- how to backfill safely alongside live pipelines.

## The Problem

Something went wrong upstream -- a schema change, a bad deploy, a data corruption that drifted for weeks before anyone noticed -- and now you need to reload a historical range. The naive response is "just rerun everything," but a backfill that treats the source like a normal extraction competes with live scheduled runs for source connections, destination quota, and orchestrator capacity. If it runs unchunked during business hours, it violates every rule in [[06-operating-the-pipeline/0607-source-system-etiquette|0607]].

Backfills aren't rare. If you're running hundreds of tables with clients who routinely correct old records, delete and re-enter documents, or run maintenance scripts that touch historical data, backfills are a weekly operation. A `start_date` override or a `full_refresh: true` flag should be tools you reach for without hesitation -- the pipeline that can't backfill safely is the one that drifts furthest from its source.

We had a client with a massive table on a very slow on-prem database -- too large to extract in a single overnight window. We loaded two years of data per night, chunked by date range, and it took four nights to complete. The table has been a constant headache since: every backfill is a multi-night operation, and any interruption on night three means deciding whether to restart from scratch or resume from the interrupted chunk.

## Backfill Types

### Date-Range Backfill

The most common type: reload a specific date range -- last three months, last fiscal quarter, a single bad week -- using partition swap ([[02-full-replace-patterns/0202-partition-swap|0202]]) or rolling window replace ([[02-full-replace-patterns/0205-rolling-window-replace|0205]]). Everything outside the range stays untouched. Scope the range slightly wider than the known corruption -- the blast radius of a bad deploy is rarely as precise as the deploy timestamp suggests.

### Full Table Backfill

Reload the entire table from scratch when corruption is too widespread to scope, when the table is small enough that scoping isn't worth the effort, or when incremental state has drifted so far that a full reset is simpler than diagnosing the gap. Uses full replace ([[04-load-strategies/0401-full-replace|0401]]), which resets the destination data, any incremental cursors, pipeline state, and schema versions. After it completes, the next scheduled incremental run picks up from the new baseline.

### Selective Backfill

Reload specific records by primary key -- a handful of corrupted orders, not the entire table. Requires the extraction layer to support PK-based filtering (`WHERE id IN (:ids)`). In practice this is rare: unless you have a short list of known bad PKs and a table large enough that reloading even a date range is expensive, a date-range backfill is simpler and catches records you didn't know were affected.

## Execution Strategy

### Isolation from Live Pipelines

Backfills should never block or delay scheduled runs. Run them as separate jobs in your orchestrator, with their own schedule (or manual trigger) and their own concurrency limits. If your orchestrator supports run priority or queue separation, give scheduled runs higher priority so they proceed even when a backfill is in progress -- the backfill can pause between chunks while the scheduled run completes, then resume.

We learned this when a backfill and a scheduled incremental run hit the same table at the same time -- both slowed down, both errored, and fixing it meant stopping the backfill, waiting for the scheduled run to finish, and restarting from the interrupted chunk.

### Chunking

Break large backfills into date-range chunks -- one month, one week, or whatever granularity matches the source's partition structure. Each chunk is independently retriable: if chunk 3 of 6 fails, retry only chunk 3. Chunk size trades off per-chunk overhead (connection setup, query parsing, destination writes) against blast radius on failure -- smaller chunks lose less work when something goes wrong, larger chunks reduce overhead.

### Safe Hours

Large backfills belong in the safe-hours window from [[06-operating-the-pipeline/0607-source-system-etiquette|0607]]. If the backfill is too large for one window, span it across multiple nights with chunking. Track which chunks completed explicitly -- a simple table or config file with chunk boundaries and completion status -- so that a failure on night three doesn't force a restart from night one.

### Staging Persistence

For multi-chunk backfills, staging tables may intentionally persist between chunks so consumers see either the old data or the fully backfilled data, never a half-finished state. Don't clean up staging until the full backfill is validated -- the storage cost of a few extra days is negligible compared to restarting a multi-night backfill because you dropped staging prematurely (see [[06-operating-the-pipeline/0603-cost-monitoring|0603]]).

## State Reset

After a full backfill, the incremental state -- cursor position, high-water mark, schema version -- must match the data you just loaded. If the cursor still points to its old position, the next incremental run skips everything between that cursor and the most recent data, leaving an invisible gap. Some pipelines wipe state automatically on a full refresh; others require explicit cleanup (clearing a cursor table, deleting state files, resetting partition metadata). If state cleanup is a manual step, document it prominently -- a backfill that reloads the data but leaves the old cursor in place is worse than no backfill, because the pipeline reports success while silently skipping rows.

The risk compounds when pipeline state lives in a separate store. After clearing that state, the next scheduled run starts from scratch -- effectively a full refresh of every table, not just the one you backfilled. Engineers who don't expect this find out the hard way. This is one of the strongest arguments for stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]): the next scheduled run re-reads its normal trailing window regardless of any backfill, there's no state to reset, and the failure mode of "reload data but forget to fix the cursor" doesn't exist. It's also far simpler to reason about -- "the pipeline always grabs the last N days" requires no mental model of cursor state, cleanup procedures, or post-backfill sequencing.

## Backfill as Routine

If your clients actively manage their own source data -- correcting historical records, deleting and re-entering documents, running maintenance scripts on old rows -- backfills are part of the regular operating rhythm, and the pipeline needs to support them without ceremony. Two runtime overrides cover most cases:

**`start_date` / `end_date`** -- override the extraction's date boundaries to re-extract a specific range without pulling everything forward to today. Without an `end_date`, a backfill starting three months back also re-extracts all data between then and now -- wasting source load and destination writes on data that's already correct.

Date-range backfills can also clean up hard deletes and orphaned rows within the window if you filter on a stable business date (`order_date`, `invoice_date`) rather than `updated_at`, then swap the destination's partitions for that range with the fresh data ([[02-full-replace-patterns/0202-partition-swap|0202]]). The partition swap fully replaces the slice, so anything that existed in the destination but no longer exists in the source disappears. The business date is the right filter because it's immutable -- an order placed on March 5 always has `order_date = 2026-03-05` regardless of when it was last updated -- which keeps partition boundaries stable and guarantees you capture every row in the range, not just recently changed ones.

**`full_refresh`** -- ignore all incremental state and reload the entire table using full replace ([[04-load-strategies/0401-full-replace|0401]]) instead of a merge. A merge only updates and inserts, so rows hard-deleted at the source survive in the destination indefinitely; a full replace wipes the slate. Useful when the table is small enough that scoping isn't worth the effort, when the incremental state is corrupt, or when you suspect hard deletes have drifted the destination.

Both should be launchable from your orchestrator's UI without modifying code or config files. If a backfill requires editing a config and redeploying, you'll avoid doing it until the problem is too large to ignore. Some orchestrators go further -- Dagster's partition-based backfill UI lets you select a date range, kick off the backfill, and track per-partition status from the same interface that shows your scheduled runs (see [[08-appendix/0805-orchestrators|0805]]).

## Tradeoffs

| Pro | Con |
|---|---|
| Resets accumulated drift and restores source-destination parity | Large backfills compete with live pipelines for source and destination resources |
| Chunked backfills are independently retriable -- partial failures don't restart from scratch | Multi-night backfills require chunk-tracking state and are fragile over long durations |
| Date-range scoping limits blast radius to the affected period | Scoping too narrowly may miss corrupted rows at the edges |
| Full table backfill resets all state to a known-good baseline | Resets incremental cursor -- next run after backfill may be heavier than expected |
| Routine backfill capability reduces time-to-fix for upstream problems | Absorbing upstream messiness via frequent backfills can mask problems that should be fixed at the source |

## Anti-Patterns

> [!danger] Don't run unchunked backfills during business hours on a live source
> A 6-month backfill as a single sustained scan at 2pm on a Tuesday will get your access revoked. Chunked backfills with indexed reads can coexist with business-hours traffic if the source can handle it, but an unchunked backfill on a production OLTP during peak hours is how you lose source access.

> [!danger] Don't forget the state reset on cursor-based pipelines
> Reloading the data while the cursor still points to the old high-water mark means the next incremental run skips everything between the cursor and the new data. Clear the state or force a full refresh. Stateless window extraction avoids this entirely -- there's no state to forget.

> [!danger] Don't let backfills and scheduled runs compete for the same resources
> A backfill that blocks a scheduled run isn't fixing the pipeline -- it's degrading it. Isolate backfills in separate jobs with lower priority, and design the chunking so a backfill can yield to a scheduled run between chunks.

## Related Patterns

- [[02-full-replace-patterns/0202-partition-swap|0202-partition-swap]] -- the mechanism for date-range backfills in partitioned tables
- [[02-full-replace-patterns/0205-rolling-window-replace|0205-rolling-window-replace]] -- rolling window as a scoped backfill strategy
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- full table backfill
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- no state to reset after backfill
- [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]] -- safe hours and source protection
- [[06-operating-the-pipeline/0608-tiered-freshness|0608-tiered-freshness]] -- cold-tier full replace is a scheduled backfill by another name
- [[06-operating-the-pipeline/0615-recovery-from-corruption|0615-recovery-from-corruption]] -- backfill as a recovery mechanism after corruption
