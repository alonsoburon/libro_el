---
title: "Duplicate Detection"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-29
---

# Duplicate Detection

> **One-liner:** Duplicates already landed. How to find them, quantify the damage, and deduplicate without losing data.

## The Problem

Duplicates in the destination are a symptom, not a root cause -- they indicate a load strategy mismatch, a failed retry that double-wrote, or an append that should have been a merge. If you followed the patterns in this book (merge with the correct key, full replace where possible, append-and-materialize with a dedup view), duplicates should be rare. But when they happen, the damage is disproportionate: consumers don't notice until aggregations are wrong -- revenue doubled, counts inflated, joins producing unexpected fan-out -- and once they catch it, your data's credibility takes a hit that's hard to recover from. One episode of duplicates, even if you fix it in an hour, can make consumers question every number you produce for months.

Checking for duplicates is fast -- a `GROUP BY pk HAVING COUNT(*) > 1` takes seconds. Run it before anything else. If the table is clean, the problem is downstream: most "duplicate" reports turn out to be bad JOINs on the consumer's side (a one-to-many fanout they didn't expect, a missing GROUP BY). But verify your side first -- it's cheaper than asking for their query.

## How Duplicates Arrive

| Cause                         | Mechanism                                                                                                                                                                                                                                                               |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Append without dedup handling | Append-only done right ([[04-load-strategies/0402-append-only\|0402]]) handles edge cases with `ON CONFLICT DO NOTHING` or a dedup view. Raw INSERT with no conflict handling and no dedup layer produces duplicates from retries, overlap buffers, or upstream replays |
| Merge key too specific        | The merge key includes a column that changes between extractions (e.g., `_extracted_at`, a hash that incorporates load metadata), so the merge never matches existing rows and every re-extraction INSERTs instead of UPDATing                                          |
| NOLOCK page **desync**        | SQL Server NOLOCK reads can return the same row twice if a page split moves it mid-scan -- duplicates arrive in a single extraction, before the load strategy even runs                                                                                                 |

> [!warning] Cross-partition duplicates
> Partitioning the destination by `updated_at` or another mutable date makes cross-partition duplicates likely: a row lands in the March partition, gets updated in April, and the next extraction writes the updated version to the April partition while the March copy persists. Partitioning by an immutable business date (`order_date`, `invoice_date`) prevents the row from scattering across partitions -- every re-extraction targets the same partition, which is cheaper and correctly scoped. But **partitioning alone doesn't deduplicate**: columnar engines don't enforce uniqueness, so you still need your load strategy (merge with the correct key, or a dedup view from [[04-load-strategies/0404-append-and-materialize|0404]]) to handle duplicates within the partition.

## Detection

### Row Count Comparison

The simplest signal: compare `COUNT(*)` between source and destination. If the destination has more rows, you either have duplicates or you're missing hard-delete detection. Run hard-delete detection first ([[0306-hard-delete-detection|0306]]) -- if after cleaning up deleted rows the destination still has more rows than the source, the excess can only be duplicate PKs (columnar engines don't enforce uniqueness constraints).

Run `COUNT(*)` on the source and on the destination separately, then compare in your orchestrator or manually -- these are different engines, so there's no single query that spans both. If the destination has more rows after hard-delete cleanup, the excess can only be duplicate PKs (columnar engines don't enforce uniqueness). This ties directly to [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]] -- if reconciliation is already running on a schedule, it surfaces the count mismatch before anyone downstream notices.

### By Primary Key

The definitive test. Group by PK, count > 1 = duplicate.

```sql
-- destination: columnar
SELECT id, COUNT(*) AS dupes
FROM orders
GROUP BY id
HAVING COUNT(*) > 1;
```

If you're particularly worried about duplicates and you have overhead to spare, add this at the end of your pipeline, after loading finishes.
### By Content Hash

When there's no natural PK, hash the columns that identify the entity and group by hash -- count > 1 means multiple rows for the same entity. Fix the key definition ([[05-conforming-playbook/0502-synthetic-keys|0502]]) so it uses only the columns that define identity (revise your synthetic keys, maybe?), then deduplicate.

### Narrowing the Root Cause

Once you've found duplicates, `_extracted_at` or `_batch_id` from [[05-conforming-playbook/0501-metadata-column-injection|0501]] narrow down which load introduced them. "All duplicates share `_batch_id = 47`" points to a specific run and limits where to look.

## Deduplication

### Dedup in Place

Keep one row per PK, delete the rest. A `MERGE` against the deduplicated version of itself preserves table permissions, policies, and metadata that a `CREATE OR REPLACE` would wipe:

```sql
-- destination: columnar (BigQuery)
MERGE INTO orders AS tgt
USING (
    SELECT * EXCEPT(_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _extracted_at DESC) AS _rn
        FROM orders
    )
    WHERE _rn = 1
) AS deduped
ON tgt.id = deduped.id
WHEN MATCHED THEN
    UPDATE SET tgt._extracted_at = deduped._extracted_at
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
```

Expensive on large tables -- it rewrites every partition -- but should be a one-off. Fix the pipeline first so duplicates stop arriving, then clean up the destination.

### Dedup via Rebuild

Re-extract the table with a full replace ([[04-load-strategies/0401-full-replace|0401]]) or rebuild from staging. Cleaner than in-place dedup because it resets to a known-good state with no residual risk of missed duplicates. Prefer this when the duplication is widespread or when the table is small enough that a full reload is cheap.

### Dedup View

Leave the base table as-is and create a view that deduplicates:

```sql
-- destination: columnar
CREATE VIEW orders AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY _extracted_at DESC
    ) AS _rn
    FROM orders_raw
) WHERE _rn = 1;
```

Fast to deploy, no DML, no data loss risk. If you rename the base table to `orders_raw` and create the view as `orders`, downstream queries don't need to change -- this is the same mechanism that [[04-load-strategies/0404-append-and-materialize|0404]] uses permanently. As a temporary fix it buys you time to investigate the root cause while consumers see clean data immediately.

> [!tip] If you're reaching for the dedup view often, consider switching to append-and-materialize permanently
> The dedup view is the core of [[04-load-strategies/0404-append-and-materialize|0404]]. Append-and-materialize removes the duplicate problem structurally -- every extraction appends, the view always deduplicates -- and it's cheaper than merge in columnar engines because a pure INSERT never rewrites existing partitions. The dedup cost is paid at read time, not at load time, and only for the rows the consumer actually queries.

## Anti-Patterns

> [!danger] Don't deduplicate without understanding the root cause
> Deduplication fixes the symptom. If you don't fix the load strategy that produced the duplicates, they'll come back on the next run. Find the cause first, fix the pipeline, then clean up the data.

> [!danger] Don't assume "duplicates" means your pipeline is broken
> Run the `GROUP BY pk HAVING COUNT(*) > 1` check first -- it takes seconds. If the table is clean, the problem is downstream.

## Related Patterns

- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- full replace as a dedup-via-rebuild strategy
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- merge prevents duplicates when the PK is correct
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- structural dedup via view, eliminates duplicate risk permanently
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_batch_id` identifies which load introduced duplicates
- [[05-conforming-playbook/0502-synthetic-keys|0502-synthetic-keys]] -- content hashing for dedup when no natural PK exists
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- row count mismatch is often the first signal of duplication
