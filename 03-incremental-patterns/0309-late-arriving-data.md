---
title: "Late-Arriving Data"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Late-Arriving Data

> **One-liner:** A row's timestamp predates the extraction window. It was modified retroactively, arrived late from a batch job, or was inserted by a slow-committing transaction.

---

## The Problem

A row lands in the source with an `updated_at` or `created_at` that's already behind your cursor or outside your window. The extraction ran at 10:00, picked up everything through 09:59, and advanced the cursor. At 10:05, a batch job inserts a row with `updated_at = 08:30`. That row is now permanently behind the cursor and will never be extracted.

This happens through more mechanisms than just "slow transactions":

- **Retroactive corrections.** Support reopens a 3-day-old order and changes the shipping address. The `updated_at` fires with today's date -- fine, the cursor catches it. But in some systems, the correction sets `updated_at` to the original order date, not the correction date, meaning the row changes while the timestamp doesn't move forward.
- **Batch imports.** An overnight job loads yesterday's POS transactions with `created_at = yesterday`. If your cursor already passed yesterday, those rows are invisible.
- **ERP period closes.** Accounting closes March and runs adjustments. The adjustments land with dates in March, but the close happens in April. A daily cursor in April never looks back at March.
- **Slow-committing transactions.** A long-running transaction inserts a row at 09:50 but doesn't commit until 10:10. The `updated_at` is 09:50 (when the INSERT happened), but the row wasn't visible until 10:10 (when the COMMIT happened). If the extraction ran at 10:00, it couldn't see the row -- and the cursor already advanced past 09:50.
- **Async replication lag.** The source is a read replica that's 30 seconds behind the primary. Your extraction reads from the replica, but the cursor advances based on wall-clock time. Rows committed on the primary in those 30 seconds are invisible until the next run -- if the cursor has already moved past them.

In every case, the row's timestamp says it should have been extracted already, but it wasn't visible when the extraction ran.

---

## How Far Back Can It Land?

The overlap window must cover the worst-case late arrival, and that depends entirely on the source system's behavior:

| Source behavior | Typical lag | Example |
|---|---|---|
| Slow-committing transactions | Seconds to minutes | Long-running INSERT that commits after the extraction |
| Async replication | Seconds to minutes | Read replica behind the primary |
| Batch imports | Hours | Overnight POS load with yesterday's timestamps |
| Retroactive corrections | Days | Support editing a week-old order |
| ERP period closes | Days to weeks | Accounting adjustments backdated to the closed period |
| Cross-system reconciliation | Weeks | Finance reconciling invoices from the previous month |

> [!warning] Don't guess -- measure
> Query the source for rows where `updated_at` predates `created_at` or where `updated_at` is significantly older than the row's actual arrival. Transaction logs, audit tables, or a comparison between `updated_at` and `_extracted_at` over a few weeks will reveal the real distribution. Size the overlap to cover the 99th percentile, not the average.

---

## Overlap Window Sizing

The overlap extends the extraction window backward from the cursor or window start:

```sql
-- source: transactional
-- Cursor-based with overlap
SELECT *
FROM orders
WHERE updated_at >= :last_run - INTERVAL '2 days';
```

```sql
-- source: transactional
-- Stateless window with built-in overlap
SELECT *
FROM orders
WHERE updated_at >= CURRENT_DATE - INTERVAL '9 days';
-- 7 days of intended window + 2 days of overlap
```

The overlap is a correctness parameter, not a performance parameter. Size it for the worst-case late arrival, then evaluate the cost. If the cost is too high, the answer is to shorten the run frequency (run less often, so the overlap is a smaller fraction of total work) or accept the blind spot and let the periodic full replace catch it.

The [[03-incremental-patterns/0303-stateless-window-extraction|0303]] pattern has overlap built in by design -- a 7-day window already covers 7 days of late arrivals, with no overlap parameter to configure and no cursor to worry about. This is one of the strongest arguments for defaulting to stateless windows: the window size itself is the overlap, and the late-arriving data problem largely disappears. The only case it doesn't cover is rows that land with timestamps older than the window, which requires either a wider window or the periodic full replace. The [[03-incremental-patterns/0302-cursor-based-extraction|0302]] pattern needs the overlap added explicitly to the boundary condition.

How large can a window get? We run a 90-day stateless window on a client's transactions because their back-office team routinely edits orders weeks after the fact, backdates corrections, and re-opens closed periods without notice. A 7-day window missed data constantly; 30 days still wasn't enough. At 90 days the source query is heavier, but the table is indexed on `updated_at` and the alternative -- constant reconciliation and manual fixes -- was more expensive in engineering time.

---

## Oracle EBS PRUNE_DAYS

Oracle BI Applications (OBIA) formalized this pattern as `PRUNE_DAYS` -- a configurable parameter that subtracts N days from the high-water mark on every extraction. The parameter exists because Oracle EBS has long-running concurrent programs (batch jobs) that can take hours to complete, inserting rows with timestamps from when the program started, not when it committed. The concept generalizes beyond Oracle: any system where the gap between "when the row's timestamp says it was created" and "when the row became visible" can be large needs an equivalent parameter.

---

## Cost of Overscanning

A wider overlap re-extracts more rows that haven't changed, increasing both source query cost and destination load cost (see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for the load side). The tradeoff is correctness vs. cost, framed by [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]]: an hours-long overlap adds negligible cost, a days-long overlap is moderate depending on mutation rate, and a weeks-long overlap starts approaching a full replace -- at which point a scoped full replace ([[02-full-replace-patterns/0204-scoped-full-replace|0204]]) may be simpler than a cursor with a massive overlap.

---

## Explaining This to Stakeholders

Late-arriving data is one of the hardest pipeline problems to explain to non-technical stakeholders because the failure is invisible: the data looks correct, the pipeline reports success, and the counts are close enough that nobody notices the missing rows until a reconciliation or audit.

**What stakeholders need to understand:**

"When we extract data incrementally, we ask the source: 'give me everything that changed since the last time I asked.' But some changes arrive with timestamps in the past -- a correction from last week, a batch import with yesterday's dates, an adjustment from a period close. Our pipeline already asked for that time range and moved on. Those rows are invisible until the next full reload."

**The three questions they'll ask:**

1. **"Can't you just get everything?"** Yes -- that's a full replace. It's the most correct approach but the slowest and most expensive. We do it periodically as a safety net. The incremental extraction runs between full replaces to keep the data fresh.

2. **"How much data are we missing?"** Depends on the table and the source system. For well-behaved transactional tables, almost nothing -- seconds of lag at most. For tables fed by batch jobs or ERP period closes, the gap can be days. We size the overlap window to cover the worst case we've measured, and the periodic full replace catches anything beyond that.

3. **"Why can't the data just be right?"** Because "right" has a cost. A 7-day overlap window on a table with 100 million rows re-extracts 7 days of data on every run to catch the rare late arrival. A 30-day overlap re-extracts 30 days. At some point, the cost of absolute correctness exceeds the cost of the occasional missing row. The overlap window is where we draw that line, and the full replace is the safety net behind it.

> [!tip] Frame it as a tradeoff, not a limitation
> Stakeholders respond better to "we chose a 7-day safety margin that catches 99% of late arrivals, with a weekly full reload as a backstop" than to "our pipeline might miss some rows." Both are true, but the first version communicates a deliberate engineering decision. See [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] for how to formalize these guarantees into measurable SLAs.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> The source-side extraction cost scales with the overlap (wider window = more rows scanned on an indexed `updated_at`). The destination-side cost depends on how many partitions the overlap touches -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] and [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for partition rewrite behavior per engine.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Both sides scale with batch size, not table size, so wider overlaps are cheap. A 7-day overlap on a table with 1,000 changes per day re-extracts ~7,000 rows per run -- negligible for a transactional upsert.

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- the periodic full replace as the ultimate safety net for anything the overlap misses
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- boundary handling buffer is the same mechanism at small scale
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- the stateless window has overlap built in by design
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]] -- the tradeoff between correctness and cost that drives overlap sizing
- [[02-full-replace-patterns/0204-scoped-full-replace|0204-scoped-full-replace]] -- when the overlap grows large enough that a scoped full replace is simpler
