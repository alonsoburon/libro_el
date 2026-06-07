---
title: "Create vs Update Separation"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Create vs Update Separation

> **One-liner:** When the trigger fires on UPDATE only and INSERT rows have `updated_at = NULL`, you need two extraction paths.

---

## The Problem

[[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] documents the failure mode: a trigger maintains `updated_at` on UPDATE but not on INSERT, leaving new rows with `updated_at = NULL`. A cursor on `updated_at >= :last_run` catches every modification to existing rows while every new row is permanently invisible.

This happens in `orders` when the trigger was added after the table existed and only wired to the UPDATE event -- a common pattern in legacy systems where the trigger was built for auditing, not extraction. The result is two populations in the same table: rows that have been updated at least once (visible to the cursor) and rows that were inserted but never touched again (invisible).

```sql
-- source: transactional
SELECT order_id, created_at, updated_at
FROM orders
ORDER BY order_id DESC
LIMIT 5;
```

| order_id | created_at | updated_at |
|---|---|---|
| 1005 | 2026-03-14 09:30:00 | NULL |
| 1004 | 2026-03-14 08:15:00 | NULL |
| 1003 | 2026-03-13 16:00:00 | 2026-03-14 10:00:00 |
| 1002 | 2026-03-12 11:00:00 | NULL |
| 1001 | 2026-03-10 09:00:00 | 2026-03-13 14:30:00 |

Orders 1005, 1004, and 1002 were inserted but never updated -- `updated_at` is NULL and the cursor will never see them.

---

## Detection

Before building any workaround, confirm the problem exists:

```sql
-- source: transactional
SELECT
  COUNT(*) AS total_rows,
  COUNT(updated_at) AS rows_with_updated_at,
  COUNT(*) - COUNT(updated_at) AS rows_without_updated_at
FROM orders;
```

| total_rows | rows_with_updated_at | rows_without_updated_at |
|---|---|---|
| 84,230 | 61,507 | 22,723 |

If `rows_without_updated_at` is significant, the trigger is UPDATE-only. A second check confirms the pattern -- recently created rows should have NULLs:

```sql
-- source: transactional
SELECT COUNT(*) AS recent_nulls
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
  AND updated_at IS NULL;
```

A high count here means the problem is ongoing, not a historical artifact from before the trigger was added.

> [!tip] Check the trigger definition directly
> In PostgreSQL, `\dS orders` or `SELECT * FROM information_schema.triggers WHERE event_object_table = 'orders'` shows exactly which events fire the trigger. In MySQL, `SHOW TRIGGERS LIKE 'orders'` does the same. Take some time to check before debugging.

---

## Strategy 1: COALESCE

The simplest single-query approach -- fall back to `created_at` when `updated_at` is NULL:

```sql
-- source: transactional
SELECT *
FROM orders
WHERE COALESCE(updated_at, created_at) >= :last_run;
```

This works when `created_at` is reliably populated on INSERT (it usually is -- application frameworks and ORMs set it by default). The query captures both populations: updated rows through `updated_at`, and never-updated rows through `created_at`.

This works when `created_at` is reliably populated on INSERT -- application frameworks and ORMs set it by default. The query captures both populations: updated rows through `updated_at`, and never-updated rows through `created_at`.

**Index usage.** `COALESCE(updated_at, created_at)` wraps the columns in a function, which prevents the optimizer from using indexes on either column directly. PostgreSQL supports a functional index on `COALESCE(updated_at, created_at)` that resolves this -- create it if the table is large enough that the full scan matters. MySQL and SQL Server don't support functional indexes in the same way, so the query planner may fall back to a full scan.

> [!warning] COALESCE fails when `created_at` is also NULL
> If the table has rows where both `updated_at` and `created_at` are NULL, `COALESCE` returns NULL and those rows vanish from every cursor-based extraction. Check `SELECT COUNT(*) FROM orders WHERE updated_at IS NULL AND created_at IS NULL` before relying on this approach.

---

## Strategy 2: Dual Cursor

Two separate queries, each optimized for its own population:

**Inserts** -- cursor on `created_at` (or `id > :last_id` if `created_at` is unavailable):

```sql
-- source: transactional
SELECT *
FROM orders
WHERE created_at >= :last_run_created;
```

**Updates** -- cursor on `updated_at`:

```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= :last_run_updated;
```

UNION the results and load as one batch. Each query uses its own index cleanly -- `created_at` for the insert cursor, `updated_at` for the update cursor -- with no function wrapping and no optimizer guesswork.

The alternative of combining them into a single `WHERE updated_at >= :last_run OR created_at >= :last_run` looks simpler but behaves worse: the OR forces the optimizer to choose between a full scan and a bitmap OR of two index scans, and the plan it picks varies by engine, table size, and statistics freshness. Two queries with a UNION is predictable across engines.

**Cursor management.** Two cursors means two pieces of state to track and advance. If your orchestrator supports per-table metadata, storing both is straightforward. Otherwise, a dedicated state table works:

```sql
-- destination: transactional (state table)
SELECT last_run_updated, last_run_created
FROM _pipeline_state
WHERE table_name = 'orders';
```

**Overlap between the two sets.** A row inserted at 09:00 and updated at 10:30 appears in both queries if `last_run_created` is before 09:00 and `last_run_updated` is before 10:30. The upsert in the destination handles the duplicate -- the second version (the update) overwrites the first (the insert), which is the correct outcome.

> [!tip] Use `id > :last_id` for the insert cursor when `created_at` doesn't exist
> This is [[03-incremental-patterns/0305-sequential-id-cursor|0305]] applied to half the table. The same gap safety rules apply -- sequences with CACHE can produce out-of-order IDs, and a small overlap buffer absorbs them.

---

## Strategy 3: Fix the Source

Add an `AFTER INSERT` trigger that populates `updated_at` with the current timestamp on every INSERT:

```sql
-- source: transactional (PostgreSQL)
CREATE OR REPLACE TRIGGER set_updated_at_on_insert
BEFORE INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
```

Then backfill existing NULLs:

```sql
-- source: transactional
UPDATE orders
SET updated_at = created_at
WHERE updated_at IS NULL;
```

After the backfill and trigger are in place, the standard [[03-incremental-patterns/0302-cursor-based-extraction|0302]] cursor works for both inserts and updates -- no dual cursor, no COALESCE, no workarounds.

This is the cleanest outcome but requires three things: access to the source database, cooperation from the source team, and confidence that the trigger won't interfere with existing application logic. In practice, adding a trigger to a production table owned by another team is a conversation that can take weeks or never happen. Strategies 1 and 2 exist because Strategy 3 often isn't available.

> [!warning] Backfill carefully
> `UPDATE orders SET updated_at = created_at WHERE updated_at IS NULL` on a 50M-row table with 20M NULLs is a heavy write. Run it in batches during off-hours and coordinate with the source team so their monitoring doesn't flag the spike as an incident. The trigger should go live before the backfill starts -- otherwise, rows inserted between the backfill and trigger activation will still have NULLs.

---

## Choosing a Strategy

| Situation | Strategy |
|---|---|
| `created_at` is reliable, table is small-to-medium | COALESCE -- simplest, one query, one cursor |
| `created_at` is reliable, table is large, no functional index | Dual cursor -- each query hits its own index cleanly |
| `created_at` is NULL or unreliable | Dual cursor with `id > :last_id` for the insert side |
| You have source access and team cooperation | Fix the source -- eliminates the problem permanently |

In all cases, the periodic full replace from [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] catches anything the workaround misses -- rows where both timestamps are NULL, bulk imports that bypassed both triggers, sequences that created gaps the insert cursor didn't cover.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> The dual cursor produces two result sets that get UNIONed before loading. The duplicate rows from the overlap between insert and update cursors are handled by the destination's MERGE -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]]. The COALESCE approach benefits from a functional index on the source side; without one, the extraction query is a full scan on every run.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Both cursors should be cheap indexed range scans on the source. The destination upsert (`ON CONFLICT ... DO UPDATE`) absorbs overlap duplicates naturally -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]].

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- the "trigger fires on UPDATE only" failure mode that leads here
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the standard cursor that works once the source is fixed
- [[03-incremental-patterns/0305-sequential-id-cursor|0305-sequential-id-cursor]] -- the insert-only cursor used in the dual-cursor approach when `created_at` doesn't exist
- [[03-incremental-patterns/0309-late-arriving-data|0309-late-arriving-data]] -- overlap buffer on both cursors absorbs the same class of timing problems
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- handles the duplicate rows from dual-cursor overlap
