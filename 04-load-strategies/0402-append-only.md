---
title: "Append-Only Load"
aliases: []
tags:
  - pattern/load-strategy
  - chapter/part-4
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Append-Only Load

> **One-liner:** Source is immutable -- rows are inserted, never updated or deleted. Append to the destination with pure INSERT, no MERGE needed.

---

## The Problem

`events`, `inventory_movements`, and clickstream tables only grow. Rows are inserted once and never modified or deleted. A MERGE on every load -- matching on a key, checking for existence, deciding between INSERT and UPDATE -- is unnecessary work when the source guarantees that every extracted row is new.

The append-only load skips all of that: extract the new rows, INSERT them into the destination, done. No key matching, no partition rewriting, no update logic.

---

## The Pattern

The extraction side uses a sequential ID cursor ([[03-incremental-patterns/0305-sequential-id-cursor|0305]]) or a `created_at` timestamp cursor ([[03-incremental-patterns/0302-cursor-based-extraction|0302]]) to scope the new rows:

```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id > :last_id;
```

The load side is a pure INSERT:

```sql
-- destination: columnar
INSERT INTO events
SELECT * FROM _stg_events;
```

No `ON CONFLICT`, no `MERGE`, no `MATCHED` / `NOT MATCHED` logic. The destination table grows monotonically, just like the source.

---

## Why This Is the Cheapest Load

In columnar engines, `MERGE` reads the existing table to find matches, then rewrites the affected partitions with the merged result -- even when every row in the batch is new. On a table with 500M rows and 50K new rows per run, the MERGE still scans the destination side of the join to confirm that none of the 50K exist. That scan is the cost floor of any MERGE operation, regardless of how many rows actually match.

A pure APPEND writes the new rows into a new partition (or appends to the current one) without reading anything that already exists. BigQuery `INSERT` jobs write to new storage blocks without touching existing partitions. Snowflake appends to micro-partitions. The cost scales with the batch size, not the table size -- 50K rows cost the same whether the destination has 1M rows or 500M.

> [!tip] Verify the source is actually immutable before committing to this pattern
> "Events are never updated" is a soft rule ([[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]]) unless the schema enforces it. If someone eventually runs an UPDATE on `events` -- a bulk correction, an admin fix, a backfill that modifies existing rows -- the append-only load will miss the change entirely. Check with the source team, and keep the periodic full replace from [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] as a safety net.

---

## When "Append-Only" Produces Duplicates

Three scenarios where a pure APPEND loads the same row twice:

**Pipeline retry.** The extraction succeeded and the load partially completed, but the cursor didn't advance because the run was marked as failed. The retry re-extracts the same batch, and the rows that already loaded appear again.

**Overlap buffer.** [[03-incremental-patterns/0305-sequential-id-cursor|0305]] recommends a small overlap (`event_id >= :last_id - 100`) to absorb out-of-order sequence commits. The overlap region is extracted on every run by design.

**Upstream replay.** The source system replays events -- a Kafka consumer rewinds, an API returns the same batch on retry, a file is redelivered. The rows are identical to ones already loaded, but the extraction can't tell.

### Handling Duplicates

Two approaches, depending on the destination engine:

**Transactional destinations** -- reject at load time:

```sql
-- destination: transactional
INSERT INTO events (event_id, event_type, event_date, payload)
SELECT event_id, event_type, event_date, payload
FROM _stg_events
ON CONFLICT (event_id) DO NOTHING;
```

`ON CONFLICT DO NOTHING` silently drops duplicates that already exist in the destination. The primary key does the deduplication, and the load cost is negligible because the rejected rows don't generate writes.

**Columnar destinations** -- deduplicate at read time:

```sql
-- destination: columnar
-- View that exposes only the latest version of each row
CREATE OR REPLACE VIEW events_current AS
SELECT *
FROM events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _extracted_at DESC) = 1;
```

BigQuery and Snowflake don't enforce primary keys, so duplicates land in the table and the deduplication happens downstream through a view or materialized table. This is the foundation of the [[04-load-strategies/0404-append-and-materialize|0404]] pattern -- the difference is that 0404 applies it to mutable data (every version of a row), while here the duplicates are accidental copies of the same immutable row.

> [!warning] Don't MERGE just to deduplicate immutable data
> Switching from APPEND to MERGE because "duplicates might happen" throws away the cost advantage of append-only loading. Handle duplicates at the edges -- `ON CONFLICT DO NOTHING` on transactional destinations, a dedup view on columnar -- and keep the load path cheap.

---

## The Fragility of Append-Only

The cost advantage of this pattern depends entirely on the source being immutable -- and that assumption is fragile. The moment someone runs an UPDATE on `events`, or a backfill modifies existing rows, or a correction script touches historical data, the append-only contract is broken and every row that changed sits silently wrong in the destination.

The recovery path is expensive. You either switch to [[04-load-strategies/0403-merge-upsert|0403]] (which rewrites partitions on every load), add a dedup-and-reconcile layer from [[04-load-strategies/0404-append-and-materialize|0404]], or run a full replace from [[04-load-strategies/0401-full-replace|0401]] to reset the destination. What was the cheapest load pattern in the book becomes one of the most expensive the moment the assumption breaks, because the pipeline has no mechanism to detect or correct the mutation -- it just keeps appending new rows while the old ones stay wrong.

Before choosing this pattern, ask how confident you are that the source will stay immutable -- not today, but across schema changes, team turnover, and the admin script someone will write at 2am during an incident. If the answer is "pretty confident but not certain," a periodic full replace via [[04-load-strategies/0401-full-replace|0401]] is the safety net, and its cadence should reflect how much damage a silent mutation would cause before the next reload.

---

## Partitioning by Date

Append-only tables are a natural fit for date-based partitioning: `events` partitioned by `event_date`, `inventory_movements` by `movement_date`. Each run's new rows land in the partition corresponding to their date, and old partitions are never touched.

This alignment gives you three operational advantages:

- **Backfill** is a partition replace: re-extract a date range, load into the corresponding partitions using [[04-load-strategies/0401-full-replace|0401]], done. The rest of the table is untouched.
- **Retention** is a partition drop: `ALTER TABLE events DROP PARTITION '2024-01-01'` removes a day of history without scanning or rewriting anything.
- **Cost control** in columnar engines: queries that filter on `event_date` scan only the relevant partitions. Without partition pruning, a query over yesterday's events scans the entire table.

> [!warning] Late-arriving events land in past partitions
> An event with `event_date = 2026-03-10` arriving on `2026-03-14` lands in the March 10 partition. If that partition was already "closed" by a retention policy or a downstream process that assumed it was complete, the late arrival is either lost or creates an inconsistency. See [[03-incremental-patterns/0309-late-arriving-data|0309]] for overlap sizing that absorbs this.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> Pure APPEND is the cheapest load operation available -- no partition rewriting, no key matching. BigQuery charges for bytes written on INSERT, not bytes scanned. Snowflake appends to micro-partitions without compaction cost at load time. Dedup view adds read cost only when queried, not on every load.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> `INSERT ... ON CONFLICT DO NOTHING` handles duplicates at load time with minimal overhead. The primary key index absorbs the conflict check. For high-volume append tables (`events` with millions of rows per day), ensure the destination is partitioned and that `autovacuum` keeps up with the insert rate.

---

## Related Patterns

- [[03-incremental-patterns/0305-sequential-id-cursor|0305-sequential-id-cursor]] -- the extraction cursor that feeds this load pattern
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- cursor on `created_at` as an alternative extraction mechanism
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- for backfilling partitions on append-only tables
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- when the source is mutable and append-only doesn't apply
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- append every version of mutable data and deduplicate downstream
- [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106-hard-rules-soft-rules]] -- "this table is append-only" is a soft rule until the schema enforces it
