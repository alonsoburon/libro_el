---
title: "Stateless Window Extraction"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-12
updated: 2026-03-13
---

# Stateless Window Extraction

> **One-liner:** Extract a fixed trailing window on every run. No cursor, no state between runs. This is how we run most of our incremental tables.

See [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] for when `updated_at` lies, how to validate it, and when to run a periodic full replace.

---

## How It Works

Skip the cursor entirely. Every run extracts a fixed trailing window regardless of what happened last run.

```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days';
```

No state to manage, no cursor to advance, no orchestrator metadata. Run it twice and get the same result. A failed run leaves nothing behind -- the next run picks up from the same window.

A window measured in days absorbs any clock skew between source and extractor. No buffer needed.

---

## Why We Default to This

A cursor-based pipeline can fail in ways that are hard to debug: partial loads that advance the cursor, destination rebuilds that reset the high-water mark, orchestrator metadata that gets out of sync. All of these produce permanent gaps that are invisible until someone notices the counts are off.

A stateless window can't have any of these problems. There's no state to corrupt. Re-run it, get the same result. Retry after failure -- just run again. Backfill a date range -- change the window bounds. Two runs overlap -- upsert handles it. Every property we want from a pipeline (stateless, idempotent, safe to retry, safe to parallelize) comes for free.

The tradeoff: you always process the full window even when almost nothing changed. For small-to-moderate tables with indexed `updated_at`, that cost is almost always less than the engineering cost of managing cursor state across thousands of tables.

> [!tip] Match the window to your correction lag
> How far back can a correction or late-arriving row realistically land? If support can reopen a 3-day-old order, cover at least 4 days. If the source team runs 2-week backfills, cover that. Query cost comes second.

---

## When a Cursor Earns Its Overhead

Don't use a stateless window when:

- You're running hourly or more on large tables -- a 7-day window running 24 times a day reprocesses those 7 days 24 times. The MERGE cost multiplies directly.
- The table is big enough that even an indexed `updated_at` scan on the window is expensive on the source.
- Mutation rate is high and concentrated in recent rows -- a cursor extracts only the delta, which might be 0.1% of the window.

If you're running daily or less, or the table is small-to-moderate, the stateless window wins on simplicity every time.

---

## Window Size x Run Frequency

This is the knob that matters. A 7-day window running daily costs X. The same window running hourly costs 24X -- both in source query cost and destination load cost (see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for the cost anatomy).

Size the window for correctness (it must cover your correction lag). Then set run frequency for cost. If the cost is too high, the answer is usually to run less often -- not to shrink the window below what correctness requires.

## Align Windows to Partition Boundaries

If the destination is partitioned by date, align the window to complete days. A 7-day window that spans 8 calendar days touches 8 partitions instead of 7. See [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for why partition alignment matters in columnar engines.

## Multiple Windows

For tables where freshness matters AND corrections land late:

- Narrow window (1 day) running hourly -- gives sub-hour latency for recent changes
- Wide window (30 days) running nightly -- catches retroactive edits and slow-arriving rows
- Periodic full replace -- catches everything outside both windows

Three tiers, no cursor state anywhere, each tier sized independently.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> The source query is cheap (indexed `updated_at` scan). The load cost is where window size and run frequency multiply -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] and [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]]. MySQL `DATETIME` second-level precision is a non-issue with a window measured in days.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Cheap on both sides. The source query is the same indexed scan. Load cost scales with batch size, not table size -- high-frequency runs are viable here. See [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for the upsert mechanics.

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- when `updated_at` lies, validation checklist, periodic full replace
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- stateful alternative; earns its overhead on large, high-frequency tables
- [[03-incremental-patterns/0309-late-arriving-data|0309-late-arriving-data]] -- sizing the overlap for late arrivals
- [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]] -- when the trigger fires on UPDATE only and INSERT rows are invisible
- [[02-full-replace-patterns/0205-scoped-full-replace|0205-scoped-full-replace]] -- combining a full-replace zone with a stateless window layer
