---
title: "Point-in-Time from Events"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-30
updated: 2026-03-30
---

# Point-in-Time from Events

> **One-liner:** Reconstruct past state from event tables, not snapshots. Events are cheaper to store and replay than periodic copies of the full state.

## The Problem

A consumer asks "what was the inventory level on March 5?" or "what was the order status at 2pm last Tuesday?" If you only have the current state -- the latest version of each row from a full replace or a dedup view -- the answer is gone, overwritten by subsequent updates. The destination reflects right now, not any point in the past.

Two mechanisms preserve history. An append-and-materialize log ([[04-load-strategies/0404-append-and-materialize|0404]]) accumulates extracted versions over time -- each extraction appends rows tagged with `_extracted_at`, and prior versions survive alongside current ones until compaction. Event tables take a different approach -- `inventory_movements`, append-only `events` -- each row is something that happened, and the full history is in the log itself. Any point-in-time state is computable by replaying events up to the target date, without storing a single snapshot.

In practice, the most common use case is inventory auditing. A client wants to reconcile their physical stock count against what the system said the stock was on the count date. That's a point-in-time reconstruction: sum all movements up to the count date, compare against the physical count, and the difference tells you whether the system or the warehouse is wrong.

## Movements to Photos

`inventory_movements` records every stock change: +50 received, -3 sold, -1 adjustment, -10 transferred out. Current stock is the running sum of all movements per SKU per warehouse. Stock as of any date is the same sum, filtered to movements before that date.

```sql
-- destination: columnar
-- Current inventory, reconstructed from movements
SELECT
    sku_id,
    warehouse_id,
    SUM(quantity) AS on_hand
FROM inventory_movements
GROUP BY sku_id, warehouse_id;
```

```sql
-- destination: columnar
-- Inventory as of March 5, reconstructed from movements
SELECT
    sku_id,
    warehouse_id,
    SUM(quantity) AS on_hand
FROM inventory_movements
WHERE created_at <= '2026-03-05'
GROUP BY sku_id, warehouse_id;
```

The two queries are identical except for the WHERE clause. Any point-in-time snapshot is computable from the event log by moving the date boundary -- this is why [[07-serving-the-destination/0701-dont-pre-aggregate|0701]] insists on landing the movements: the detail produces any aggregate, but the aggregate can't reproduce the detail.

For high-frequency point-in-time queries -- a dashboard showing stock levels at close-of-business for each day of the month -- replaying the full movement history on every query gets expensive fast. A materialized table built from movements avoids the rescan: a scheduled query ([[07-serving-the-destination/0703-pre-built-views|0703]]) runs after each extraction, replays movements up to each date, and writes the result.

The trap is materializing the full grid. 200 warehouses, 100,000 SKUs, 365 days -- that's 7.3 billion rows for a single year, most of them zeros because a given SKU doesn't move every day in every warehouse. Materialize sparse: only SKU/warehouse/date combinations where a movement actually occurred. A consumer who needs "stock on March 5 for SKU X in warehouse Y" gets the answer from the most recent materialized row on or before that date, not from a row for every day.

Even sparse, the table grows with activity volume over time. Tiered granularity keeps it manageable: daily materialization for the current month, monthly rollups for anything older. You can also scope by warehouse type -- sales warehouses that move thousands of SKUs daily need daily granularity, while a low-activity storage warehouse that sees a handful of movements per week is fine at monthly resolution. The goal is to pre-compute the queries consumers actually run, not every possible combination of dimensions and dates.

## Status History from Append Logs

Not every table has a natural event log. `orders` doesn't have a changelog -- it's a mutable table that gets updated in place. But if `orders` is loaded via append-and-materialize ([[04-load-strategies/0404-append-and-materialize|0404]]), the log table has every extracted version of each order: order 123 with `status = pending` from Monday's extraction, order 123 with `status = shipped` from Wednesday's. The extraction log becomes an implicit event trail, with one "event" per extraction run.

Status at a point in time is the latest extracted version before the target timestamp:

```sql
-- destination: columnar
-- Order status as of March 5 at 2pm
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM orders_log
    WHERE _extracted_at <= '2026-03-05 14:00:00'
)
WHERE rn = 1;
```

The granularity is limited to extraction frequency. If you extract daily, you can reconstruct state at daily resolution -- you know what the order looked like at each extraction, not at every moment between them. For most analytical use cases this is sufficient; for audit-level granularity, the source system's own changelog is the authoritative record.

> [!warning] Compaction destroys this history
> [[04-load-strategies/0404-append-and-materialize|0404]] recommends compacting the log -- trimming old extractions or collapsing to latest-only -- to keep the dedup view fast and storage bounded. That compaction deletes the version history this section depends on. If consumers need point-in-time reconstruction from the append log, the compaction retention window must be longer than their lookback requirement. A 90-day lookback needs at least 90 days of log retention, which means 90 days of extraction overlap sitting in storage. That's a real cost on a large table -- decide upfront whether the log is a temporary buffer or a historical record, because it can't cheaply be both.

## When Events Aren't Enough

Not all state changes produce events. A `customers` table updated in-place with no changelog has no event trail -- the previous state is gone the moment the row is overwritten. `products` has the same problem: a price change replaces the old price, and unless someone stored the before-and-after, the history is lost.

For tables where point-in-time matters but no event log exists:

**Append-and-materialize with history retention ([[04-load-strategies/0404-append-and-materialize|0404]]).** Skip compaction (or compact less frequently) and the append log becomes an explicit version history. Each extraction appends the current state of changed rows, and prior versions accumulate. Storage grows with extraction frequency, but the history is queryable -- point-in-time state is the latest extracted version before the target date.

**Append-and-materialize log ([[04-load-strategies/0404-append-and-materialize|0404]]).** The extraction log provides event-like history as a side effect of the load strategy -- cheaper than full snapshots because each extraction appends only the changed rows, not the entire table. The tradeoff: the history exists only at extraction granularity, and compacting the log destroys it. Once you compact to latest-only, the prior versions are gone. If consumers depend on point-in-time queries against the log, the compaction retention window must be longer than their lookback requirement -- and they need to know that compaction is happening so they don't build a process that assumes the history is permanent.

**SCD Type 2 (Slowly Changing Dimension).** When point-in-time queries are a first-class requirement -- not an occasional audit but something dashboards and reports depend on daily -- an SCD2 structure makes the history explicit in the schema itself. Each row gets `valid_from` and `valid_to` columns, and a query for "what did this customer look like on March 5?" becomes a range filter instead of a window function over an extraction log:

```sql
-- destination: columnar
-- Customer record as of March 5
SELECT *
FROM customers_scd2
WHERE customer_id = 42
  AND valid_from <= '2026-03-05'
  AND (valid_to > '2026-03-05' OR valid_to IS NULL);
```

Building the SCD2 table is a downstream transformation, not a conforming operation -- the pipeline lands the current state or the append log, and a scheduled job compares consecutive extractions to detect changes and maintain the `valid_from`/`valid_to` bookkeeping. The mechanics are well-documented elsewhere; what matters for this pattern is that SCD2 gives you point-in-time queries that are cheap to run (a range filter that benefits from partitioning and clustering -- [[07-serving-the-destination/0702-partitioning-for-consumers|0702]]), explicit in their semantics (no ambiguity about what `_extracted_at` means versus when the change actually happened), and immune to compaction -- the history is the table, not a side effect of a retention window.

The cost is maintaining the SCD2 pipeline itself. Every extraction needs to be diffed against the previous state to detect what changed, close out old rows, and open new ones. For a `customers` table with 100K rows that changes slowly (hence the name), this is trivial. For an `orders` table with millions of rows and high mutation rates, the daily diff becomes expensive. SCD2 earns its place on tables where the change rate is low relative to the table size and the point-in-time queries are frequent -- dimension tables like `customers`, `products`, `warehouses`. For high-mutation fact tables, the append log or snapshot approaches are usually cheaper to maintain.

## Storage: Events vs Snapshots

| Approach | Storage grows with | Point-in-time granularity | Completeness |
|---|---|---|---|
| Event log (`inventory_movements`) | Activity volume | Per-event (every change) | Only as complete as the event source |
| Snapshot append (`_snapshot_at`) | Snapshot frequency x table size | Per-snapshot (daily, hourly) | Always complete -- it's a full copy |
| Append-and-materialize log | Extraction frequency x change volume | Per-extraction | Only changes captured by the extraction |
| SCD Type 2 | Change volume (one row per change per key) | Per-extraction (when the diff detected it) | Only changes captured between consecutive extractions |

Low-mutation tables store far less with events than with snapshots -- 10 changes per day adds 10 rows, while a daily snapshot adds the entire table. High-mutation tables may store more with events. The break-even depends on the mutation rate relative to the table size.

Tiered retention applies to all approaches: keep daily granularity for the recent window, compress older data to monthly, drop anything beyond the retention requirement.

## Completeness

Replay is only as accurate as the event log, and event logs have gaps. The domain model's `inventory_movements` table has a soft rule: "every stock change creates a movement." But bulk import scripts that update `inventory` directly without logging a movement violate this silently ([[00-front-matter/0002-domain-model|0002]]). The reconstructed snapshot from movements will differ from the actual `inventory` table, and the difference is the sum of all unlogged changes.

We had a client whose `inventory` table and the reconstructed-from-movements inventory diverged by hundreds of units on certain SKUs. The client refused to believe our data was correct -- their expectation was that movements and inventory should always match. We had to pull both from the source, show the same discrepancy in the source system itself, and demonstrate that the gap came from bulk operations that bypassed the movement log. The pipeline was cloning faithfully; the source was inconsistent.

The periodic full replace of the `inventory` table catches the drift -- it reflects the source's current state, including unlogged changes. The event-based reconstruction doesn't. When both exist in the destination, consumers should understand which one to trust: the `inventory` table for current state (it's what the source says right now), the movement log for historical reconstruction (it's what the source recorded happening). When they disagree, the source has unlogged changes -- that's a source data quality problem, not a pipeline problem.

## Anti-Patterns

> [!danger] Don't assume every table has an event trail
> `customers`, `products`, dimension tables are overwritten in place with no changelog. Point-in-time reconstruction requires either snapshots or an append-and-materialize log. Choose the load strategy before the consumer asks for the history -- retrofitting history onto a table that was loaded with full replace from day one means there's nothing to reconstruct.

> [!danger] Don't replay without knowing the completeness boundary
> If the event log has gaps (bulk operations that bypass it, the soft rule from the domain model), the reconstructed state is wrong. Document which event sources are incomplete and surface the discrepancy rather than hiding it.

> [!danger] Don't compact the append log without considering point-in-time consumers
> Compacting the log to latest-only destroys version history. If consumers depend on point-in-time queries against the log, the compaction retention window must be longer than their lookback requirement.

## Related Patterns

- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- append log as version history when compaction is deferred
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- extraction log as an implicit event trail
- [[07-serving-the-destination/0701-dont-pre-aggregate|0701-dont-pre-aggregate]] -- land movements, build photos downstream
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- materialized tables for pre-computed daily running totals
- [[02-full-replace-patterns/0207-activity-driven-extraction|0207-activity-driven-extraction]] -- `inventory_movements` as the activity signal
