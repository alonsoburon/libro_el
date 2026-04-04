---
title: "Hard Delete Detection"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Hard Delete Detection

> **One-liner:** The row was there yesterday, today it's gone. A cursor never sees a deleted row -- you need a separate mechanism.

---

## The Problem

Every extraction pattern in this chapter -- cursors, stateless windows, borrowed timestamps -- detects rows that changed. A hard delete leaves nothing behind to detect. The row is gone from the source, still present in the destination, and every cursor-based run confirms zero about its absence. The count drifts silently.

---

## When the Source Cooperates

Two mechanisms make delete detection trivial:

**Soft-delete columns** (`is_active`, `deleted_at`) -- the source marks the row as deleted instead of removing it. The normal cursor captures the flag change like any other update. This is the clean solution, but it's rarer than you'd expect. Many transactional systems -- especially ERPs -- hard-delete without ceremony.

**Tombstone tables** -- the source writes a record to a separate `deletes` or `audit_log` table when a row is removed. Extract from both tables: the main table for current state, the tombstone table for delete events. Common in CDC-adjacent systems, rare in application databases.

When neither exists -- and for most tables in most sources, neither does -- you need a detection mechanism that works from the outside.

---

## When It Doesn't

### Full ID Comparison

Extract the full set of IDs from the source. Extract the full set of IDs from the destination. Compare them -- either in the orchestrator, or by landing the source IDs into a staging table in the destination and running the diff there.

```sql
-- destination (after landing source IDs into staging)
SELECT d.invoice_id
FROM invoices d
LEFT JOIN _stg_invoice_ids s ON d.invoice_id = s.invoice_id
WHERE s.invoice_id IS NULL;
```

The rows returned exist in the destination but not in the source -- candidates for deletion.

The source-side cost is the expensive part: a full `SELECT id FROM table` on a large transactional table hits every row. Schedule it outside business hours. The destination side is cheap -- columnar engines scan a single key column efficiently regardless of table size.

For small-to-medium tables, this is the simplest and most reliable approach. For large tables, use count reconciliation as a cheaper first pass.

### Count Reconciliation

Compare `COUNT(*)` between source and destination. If the counts match, no deletes happened (or inserts and deletes balanced out -- rare but possible). If they diverge, something changed.

```sql
-- source: transactional
SELECT COUNT(*) FROM invoices;

-- destination: columnar
SELECT COUNT(*) FROM invoices;
```

This detects drift but doesn't identify which rows. Two useful responses:

- **Trigger a full replace** when counts diverge -- simple, correct, and often the cheapest response for moderate tables
- **Trigger a full ID comparison** -- when a full replace is too expensive, use the count mismatch as a signal to run the heavier detection

Run the source count immediately after the load completes -- the closer in time the two counts are, the less chance of a concurrent insert or delete skewing the comparison. The direction of the mismatch tells you something:

- **Destination > source:** deletes happened at the source since the last full sync
- **Destination < source:** inserts landed at the source that the extraction missed (cursor gap, late-arriving data, simple delay between extraction and loading)

For partitioned tables, compare counts per partition (`GROUP BY date_partition`) to narrow the scope before running a full ID comparison on only the divergent partitions.

> [!tip] Count reconciliation as a gate
> Run `COUNT(*)` on every incremental extraction as a cheap health check. It adds seconds to the run and catches drift early -- before it accumulates into a reconciliation problem. See [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]].

---

## Propagation

Once you've identified deleted IDs, the question is what to do with them in the destination. This is a load concern -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for the mechanics.

Three options, in order of preference:

1. **Soft-delete in destination.** Set `_is_deleted = true` and `_deleted_at = CURRENT_TIMESTAMP`. The row stays queryable, downstream consumers can filter on the flag, and the delete is reversible if the source was wrong. If downstream consumers are technical (analysts, dbt models), this is the default.

2. **Hard-delete in destination.** `DELETE FROM destination WHERE id IN (...)`. Matches the source exactly. Simpler for non-technical consumers who don't understand why "deleted" rows still appear in their reports.

3. **Move to a `_deleted` table.** `INSERT INTO invoices_deleted SELECT *, CURRENT_TIMESTAMP AS _deleted_at FROM invoices WHERE id IN (...); DELETE FROM invoices WHERE id IN (...)`. Only when governance or audit requirements demand a record of what was deleted and when. Adds operational complexity.

---

## `invoices` / `invoice_lines`

The domain model case: open `invoices` get hard-deleted regularly. `invoice_lines` get hard-deleted independently of their header -- not just via cascade. This creates two detection scopes:

- **Header deletes:** compare `invoice_id` sets between source and destination. The open/closed split from [[03-incremental-patterns/0307-open-closed-documents|0307]] helps -- the open-side full extract naturally reveals missing headers.
- **Line deletes:** for each header that still exists, compare `line_num` sets. A header that hasn't changed can still have lines removed underneath it -- the header cursor from [[03-incremental-patterns/0304-cursor-from-another-table|0304]] is blind to this.

In SAP B1, removing a single `invoice_line` triggers a delete+reinsert of ALL surviving lines with new `LineNum` values. The old line numbers are gone, the new ones look like fresh inserts. A full ID comparison catches this while a cursor never will.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> The source-side `SELECT id` is the bottleneck -- a full table scan on a transactional engine. The destination-side comparison is cheap (single-column scan). Land source IDs into a staging table and run the diff in the destination to avoid pulling large ID sets through the orchestrator. For propagation, soft-delete is a metadata update on the destination -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]].

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Both sides are cheap for ID extraction if the primary key is indexed (it always is). The comparison can run in either system. `DELETE FROM destination WHERE id IN (...)` is a natural fit here -- transactional engines handle point deletes efficiently.

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- periodic full replace as the ultimate safety net for undetected deletes
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the cursor that can't see deletes
- [[03-incremental-patterns/0304-cursor-from-another-table|0304-cursor-from-another-table]] -- blind to detail-level deletes when the header doesn't change
- [[03-incremental-patterns/0307-open-closed-documents|0307-open-closed-documents]] -- open documents are the ones most likely to get hard-deleted
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- count and hash reconciliation as ongoing health checks
