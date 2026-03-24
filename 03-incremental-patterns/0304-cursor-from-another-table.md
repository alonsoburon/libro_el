---
title: "Cursor from Another Table"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-06
updated: 2026-03-13
---

# Cursor from Another Table

> **One-liner:** When a detail table has no `updated_at`, borrow the header's timestamp to scope the extraction.

See [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] for the shared `updated_at` reliability concerns.

---

## The Problem

Some detail tables like `order_lines` and `invoice_lines` carry no timestamp of their own -- the only `updated_at` lives on the header.

Re-extracting all lines on every run works until the table crosses a few million rows and your source DBA starts asking questions. You need to scope.

---

## The Pattern

Use the header's cursor to figure out which detail rows to pull. Two ways to write it -- which one is better depends on your source engine and how the query planner handles it.

**Subquery filter:**

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
WHERE ol.order_id IN (
  SELECT o.order_id
  FROM orders o
  WHERE o.updated_at >= :last_run
);
```

Works well when the subquery returns a small set of IDs. Most transactional engines turn this into a semi-join and it's fast.

**Direct join:**

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
JOIN orders o ON ol.order_id = o.order_id
WHERE o.updated_at >= :last_run;
```

Simpler to read. Can be faster when both tables are indexed on the join key. Check your EXPLAIN -- some planners pull unnecessary header columns into the execution plan even with `SELECT ol.*`.

Both pull all lines for every order that changed. Some of those lines didn't actually change. That's fine -- the upsert handles duplicates, and you have no way to know which specific lines changed anyway.

---

## Cascading Joins

One hop is straightforward. Two hops gets expensive. Three hops -- stop and reconsider.

```sql
-- source: transactional
SELECT sl.*
FROM shipment_lines sl
JOIN shipments s ON sl.shipment_id = s.shipment_id
JOIN orders o ON s.order_id = o.order_id
WHERE o.updated_at >= :last_run;
```

Each join multiplies the row count and the assumptions. You're trusting two foreign key relationships and two intermediate tables to be correct and up to date. At three hops, the scoped full replace in [[02-full-replace-patterns/0205-scoped-full-replace|0205-scoped-full-replace]] is almost certainly simpler, cheaper, and more reliable.

---

## The False Economy of Re-extracting All Lines

Teams often feel guilty about pulling all lines for changed orders. Don't.

If an order has 5 lines on average and 200 orders changed since the last run, you're extracting 1,000 rows. The upsert handles the unchanged ones. The source barely notices.

This stops being fine when the combination of line count per header and header change rate produces batches large enough to stress the source or the destination MERGE. But that threshold is relative -- a wholesale distributor with 100+ lines per document also generates proportionally more data everywhere else, which means their infrastructure budget already accounts for heavier workloads. The absolute cost goes up; the cost relative to the operation stays similar.

Until re-extraction is actually causing problems -- slow runs, source contention, MERGE cost spikes -- it's the right default. Simple, correct, and the overhead scales with the business.

---

## When the Header Cursor Lies Too

Everything above assumes that when a detail row changes, the header's `updated_at` fires. Two categories of failure break this assumption, and they require different responses.

### The header doesn't know the line changed

`invoice_lines.status` changes from `approved` to `disputed` -- the invoice header's `updated_at` never fires. An admin script reprices 10,000 order lines without touching the header. In SAP B1, the header `UpdateDate` is a DATE field with no time component, though with a stateless window measured in days ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) this particular issue is absorbed.

The common thread: the line mutated, the header didn't, and the cursor is blind to it.

### The line disappears entirely

`invoice_lines` get hard-deleted independently of the header -- not just via cascade. In SAP B1, removing a single line triggers a delete+reinsert of ALL surviving lines with new `LineNum` values. No tombstone, no change log entry. The cursor has nothing to detect because the row is gone and the header may not have registered the event. See [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]].

When this happens, you have two good options:

1. **Accept the blind spot.** The periodic full replace catches everything the cursor misses. If your SLA tolerates the lag, this is the cheapest approach and the one we use most often.

2. **Split by document lifecycle.** Extract all open documents from the source (they're mutable, re-extract everything), combine with only the recently modified closed documents (they're frozen, cursor is reliable). This gives you full coverage of the mutable set without re-extracting the entire table -- but the combination logic is nontrivial, especially when documents transition between open and closed between runs, or when lines get hard-deleted from open documents. [[03-incremental-patterns/0307-open-closed-documents|0307-open-closed-documents]] covers the full pattern.

For detail tables where even these approaches aren't enough, see [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]].

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> The join runs on the source, so extraction cost is a source-side index scan. Wide detail tables (many columns per line) amplify the load cost even for moderate row counts -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] and [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]].

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Cheap on both sides. Extraction is the same index scan. The composite key (`order_id, line_num`) must be indexed on the destination for the upsert to perform -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]].

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- `updated_at` reliability and validation
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the header-side cursor this pattern depends on
- [[03-incremental-patterns/0307-open-closed-documents|0307-open-closed-documents]] -- using document lifecycle to scope detail extraction
- [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] -- when the header cursor can't cover detail changes at all
- [[02-full-replace-patterns/0205-scoped-full-replace|0205-scoped-full-replace]] -- when cascading joins get deep enough that scoped full replace is simpler
