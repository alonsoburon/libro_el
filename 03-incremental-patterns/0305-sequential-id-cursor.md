---
title: "Sequential ID Cursor"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-10
updated: 2026-03-13
---

# Sequential ID Cursor

> **One-liner:** No `updated_at` anywhere, but the PK is monotonically increasing. `WHERE id > :last_id` detects inserts only -- updates are invisible by design.

---

## The Problem

`events` has no `updated_at` and no `created_at`. `inventory_movements` doesn't either. What they do have is an auto-incrementing primary key that grows with every insert. That's enough to build a cursor on -- with an explicit tradeoff.

---

## The Pattern

```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id > :last_id;
```

After a confirmed successful load, set `:last_id` to the `MAX(event_id)` from the extracted batch. On the next run, pick up where you left off.

---

## The Tradeoff You Accept

This cursor detects inserts only. An existing row that gets modified will never be re-extracted. You accept this when:

- The table is append-only in practice -- `events` and `inventory_movements` in our domain model are designed this way
- Updates are rare enough that the periodic full replace catches them

Before committing to this pattern, check the table's actual behavior against what the source team claims. "Events are never updated" is likely a soft rule ([[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]]). If nothing in the schema enforces immutability, someone will eventually run an UPDATE on it -- a bulk correction, a backfill, an admin fix. Your pipeline won't notice.

---

## Gap Safety

Sequences produce gaps all the time -- rolled-back transactions, failed inserts, reserved-but-unused IDs. Gaps are harmless here: `WHERE id > :last_id` skips the gap and picks up the next real row. No false positives, no missed rows.

The dangerous case is the opposite: a row inserted with an ID *lower* than `:last_id`. This happens with:

- Manually set IDs (bulk imports that override the sequence)
- Sequences with `CACHE` in multi-session environments -- IDs are allocated in blocks and committed out of order
- Restored backups that reset the sequence counter

> [!warning] Out-of-order inserts are permanent misses
> A row with `id = 500` inserted after the cursor has passed `id = 600` will never be extracted. The periodic full replace is the only safety net.

If you suspect out-of-order inserts are happening (multi-session `CACHE` is the usual cause), add a small overlap buffer the same way [[03-incremental-patterns/0302-cursor-based-extraction|0302]] handles clock skew:

```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id >= :last_id - 100;
```

The overlap re-extracts, at a minimum, the last 100 IDs on every run. The upsert handles duplicates. Size the buffer to your worst observed out-of-order gap -- 100 covers most `CACHE` configurations.

Hard deletes are invisible too, same as with any cursor -- see [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]].

---

## Composite Keys

When the primary key is a composite (`order_id + line_num`, `warehouse_id + sku`), there's no natural ordering to build a cursor on. This pattern doesn't apply. See [[03-incremental-patterns/0304-cursor-from-another-table|0304-cursor-from-another-table]] for borrowing a timestamp from a related table, or [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] when no timestamp is available anywhere in the relationship.

---

## By Corridor

> [!example]- Transactional -> Columnar (e.g. any source -> BigQuery)
> For truly append-only sources, the extraction is a simple indexed range scan. The load can use pure APPEND instead of MERGE -- see [[04-load-strategies/0402-append-only|0402-append-only]]. If the table turns out to have occasional updates (the soft rule breaks), a periodic full replace catches them.

> [!example]- Transactional -> Transactional (e.g. any source -> PostgreSQL)
> Same indexed range scan on the source. The load strategy depends on whether the source is truly immutable -- see [[04-load-strategies/0402-append-only|0402-append-only]] for append-only and [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for upsert.

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- when a timestamp IS available, prefer it
- [[04-load-strategies/0402-append-only|0402-append-only]] -- when the source is guaranteed immutable, the load strategy simplifies further
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- hard deletes are invisible to any cursor
- [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]] -- when you need inserts AND updates but only have a cursor for inserts
- [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106-hard-rules-soft-rules]] -- "this table is append-only" is a soft rule until the schema enforces it
