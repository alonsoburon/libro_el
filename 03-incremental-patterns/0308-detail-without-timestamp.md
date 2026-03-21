---
title: "Detail Without Timestamp"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Detail Without Timestamp

> **One-liner:** `order_lines` and `invoice_lines` have no `updated_at`. They depend on the header for change detection -- but what if the detail changes without the header changing?

See [[03-incremental-patterns/0304-cursor-from-another-table|0304]] for the simpler case where the header cursor is sufficient. This pattern covers what happens when the detail mutates independently of the header.

---

## The Problem

[[03-incremental-patterns/0304-cursor-from-another-table|0304]] extracts detail rows by joining to the header's `updated_at`, which only works when every detail change also touches the header. When it doesn't:

- `invoice_lines.status` changes from `approved` to `disputed` -- the header's `updated_at` never fires
- An admin script reprices 10,000 `order_lines` without touching the header
- A line gets hard-deleted and the header doesn't register the event (see [[03-incremental-patterns/0306-hard-delete-detection|0306]])

The header cursor is blind to all of these because the signal it depends on never fired.

---

## The Default: [[03-incremental-patterns/0304-cursor-from-another-table|0304]]

When independent detail mutations are rare, the [[03-incremental-patterns/0304-cursor-from-another-table|0304]] approach is still the right default -- just with the explicit acknowledgment that it only catches detail changes that coincide with header changes, and the periodic full replace catches the rest.

The strategies below apply when that blind spot is too wide.

---

## Strategy 1: Computed Column Signals

Some transactional systems maintain computed columns on the header that change when detail rows mutate -- `PaidToDate`, `DocTotal`, `GrossProfitPercent` in SAP B1, for example. These columns are recalculated by the engine whenever a line is added, removed, or modified, even if `updated_at` doesn't fire.

If such a column exists, use it as a change signal on the header: compare the current value against the last extracted value, and re-extract all detail lines for headers where it differs.

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
WHERE ol.order_id IN (
  SELECT o.order_id
  FROM orders o
  WHERE o.doc_total != :last_known_doc_total
     OR o.updated_at >= :last_run
);
```

This turns a header-level computed column into an indirect change detection signal for the detail table, without hashing anything yourself. The limitation is that it only detects changes that affect the computed column -- a line status change that doesn't alter the total remains invisible.

> [!tip] Audit the computed columns before trusting them
> Verify which detail-level changes actually trigger a recalculation. In SAP B1, `DocTotal` changes when quantities or prices change, but `PaidToDate` only changes on payment linkage. Match the column to the mutations you care about.

---

## Strategy 2: Hash-Based Change Detection

Hash every detail row at the source, compare against stored hashes in the destination, and only extract rows where the hash differs.

```sql
-- source: transactional
SELECT ol.*,
       MD5(CONCAT(ol.order_id, ol.line_num, ol.quantity, ol.unit_price, ol.status)) AS _row_hash
FROM order_lines ol;
```

```sql
-- destination: columnar
-- Compare against stored hashes
SELECT s._row_hash, d._row_hash, s.order_id, s.line_num
FROM _stg_source_hashes s
LEFT JOIN order_lines d ON s.order_id = d.order_id AND s.line_num = d.line_num
WHERE s._row_hash != d._row_hash
   OR d._row_hash IS NULL;
```

This detects every change at the row level -- mutations, inserts, even columns that changed without the header knowing -- but requires extracting and hashing every row from the source on every run. For a detail table with millions of rows, that's a full scan just to compute hashes, and the extraction cost approaches a full replace.

Hash all columns -- the goal is to detect any change, and deciding which columns "matter" is a business decision that breaks the conforming boundary ([[01-foundations-and-archetypes/0102-what-is-conforming|0102]]). If a column changed at the source, the destination should reflect it.

See [[02-full-replace-patterns/0209-hash-based-change-detection|0209]] for the full hash-based pattern, including how to store and compare hashes efficiently.

---

## Strategy 3: Accept the Blind Spot

Some detail changes are invisible to every cursor-based approach, and the periodic full replace from [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] is the only thing that catches them. If the SLA tolerates the lag between the mutation and the next full replace, this is the cheapest approach -- and the one we use most often.

How often do independent detail mutations happen, and how long can the destination be wrong?

| Mutation frequency | Full replace cadence | Verdict |
|---|---|---|
| Rare (admin fixes, one-off corrections) | Weekly | Accept the blind spot |
| Occasional (line-level status changes) | Daily | Probably fine -- evaluate per table |
| Frequent (line repricing, bulk updates) | Any | Need Strategy 1 or 2 |

This maps naturally to the tiered freshness model from [[06-operating-the-pipeline/0607-tiered-freshness|0607]]: the incremental layer handles what the cursor can see, and a slower full replace layer catches everything else -- including detail mutations the cursor missed.

---

## Independent Detail Mutations

`invoice_lines.status` can change independently of `invoices.status` -- a line marked `disputed` while the header is still `open`, or a line `approved` while other lines on the same invoice are not. In some systems this is a soft rule violation ([[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]]), in others the detail lifecycle is independent by design. Either way, the extraction problem is the same: the header cursor doesn't see it.

Since the header cursor misses these changes entirely, two responses are worth considering:

**Apply the open/closed split from [[03-incremental-patterns/0307-open-closed-documents|0307]] independently to the detail table.** If `invoice_lines` has its own status field with a meaningful lifecycle (open/closed, active/disputed), treat the detail table as its own document with its own split. Re-extract all "open" lines (where `status` is still mutable), cursor-only for "closed" lines. This adds complexity but gives full coverage of detail-level mutations without hashing.

**Accept the lag and let the full replace correct it.** If detail-level status changes don't affect downstream consumers until the invoice itself closes, the lag is invisible to the business.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> Hash-based detection requires landing hashes into a staging table in the destination for comparison -- the cost is a source-side full scan plus a staging load. If that cost approaches a full replace, the full replace is simpler. See [[04-load-strategies/0403-merge-upsert|0403]] for load cost.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Hash comparison can run as a cross-database query if both systems are accessible, or via staging tables. Strategy 1 (re-extract all details for changed headers) is the simplest default here -- see [[04-load-strategies/0403-merge-upsert|0403]] for the upsert mechanics.

---

## Related Patterns

- [[03-incremental-patterns/0304-cursor-from-another-table|0304-cursor-from-another-table]] -- the simpler case where header cursor is sufficient
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- detail rows can also be hard-deleted independently
- [[03-incremental-patterns/0307-open-closed-documents|0307-open-closed-documents]] -- document lifecycle split applied to headers; can also apply to detail tables independently
- [[02-full-replace-patterns/0209-hash-based-change-detection|0209-hash-based-change-detection]] -- the full hash-based pattern
- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- periodic full replace as the safety net
