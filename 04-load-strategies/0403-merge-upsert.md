---
title: "Merge / Upsert"
aliases: []
tags:
  - pattern/load-strategy
  - chapter/part-4
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Merge / Upsert

> **One-liner:** Match on a key, update if exists, insert if new. The workhorse of incremental loading -- and the most expensive operation in columnar engines.

---

## The Problem

The extraction side ([[03-incremental-patterns/0302-cursor-based-extraction|0302]], [[03-incremental-patterns/0303-stateless-window-extraction|0303]]) produces a batch of changed rows. The destination already has prior versions of some of those rows. The load needs to reconcile: insert the new ones, update the existing ones, and leave everything else untouched.

---

## MERGE Across Engines

The syntax varies, the semantics are the same -- match on a key, decide between INSERT and UPDATE:

```sql
-- destination: columnar (BigQuery / Snowflake)
MERGE INTO orders AS tgt
USING _stg_orders AS src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.status = src.status,
    tgt.total = src.total,
    tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, status, total, created_at, updated_at)
  VALUES (src.order_id, src.status, src.total, src.created_at, src.updated_at);
```

```sql
-- destination: transactional (PostgreSQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON CONFLICT (order_id)
DO UPDATE SET
  status = EXCLUDED.status,
  total = EXCLUDED.total,
  updated_at = EXCLUDED.updated_at;
```

```sql
-- destination: transactional (MySQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  total = VALUES(total),
  updated_at = VALUES(updated_at);
```

All three produce the same result: rows that existed get overwritten, rows that didn't get inserted.

---

## Cost Anatomy

In transactional engines, MERGE cost scales with the batch size -- the engine looks up each incoming row by primary key (index seek), decides INSERT or UPDATE, and writes the result. A 10K-row batch against a 50M-row table does 10K index lookups and 10K writes. Cheap.

In columnar engines, the cost structure is fundamentally different. BigQuery's MERGE reads the **entire destination table** (or at minimum every partition that the batch touches) to find matches, then rewrites those partitions with the merged result. A 10K-row batch that touches 30 date partitions rewrites all 30 partitions in full -- even if 9,990 of the 10K rows land in a single partition. The read + rewrite cost dominates, and it scales with table size and partition spread, not batch size.

> [!warning] BigQuery MERGE partition cost
> Every DML statement in BigQuery rewrites every partition it touches -- not just the affected rows within each partition. If your batch contains rows spread across 30 dates, that's 30 full partition rewrites. Keep load batches aligned to as few partitions as possible. See [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for per-engine DML behavior.

Snowflake rewrites affected micro-partitions, which is more granular than BigQuery's date-partition model but still means a MERGE touching scattered micro-partitions across the table is significantly more expensive than one touching a contiguous range.

---

## Key Selection

The MERGE key determines how the destination identifies "the same row." Two options:

**Natural key** -- a column that uniquely identifies the entity at the source: `order_id`, `invoice_id`, `customer_id`. This is the default and the simplest choice when the source has a single-column primary key. Compound natural keys (`order_id + line_num`) work too but make the ON clause larger.

**Surrogate key** -- a hash or synthetic key generated during extraction (see [[05-conforming-playbook/0502-synthetic-keys|0502]]). Necessary when the source has no stable primary key, when the natural key is compound and unwieldy, or when multiple sources feed the same destination table and keys can collide.

> [!danger] Non-unique keys compound duplicates
> If the MERGE key matches more than one row in the destination, the behavior is engine-dependent and always bad. BigQuery raises an error when multiple destination rows match a single source row. PostgreSQL's `ON CONFLICT` requires the conflict target to be a unique index -- non-unique columns can't be used. Snowflake silently updates all matching rows, which means a single source row can overwrite multiple destination rows. Ensure the MERGE key is unique in the destination, or duplicates will compound on every run -- see [[06-operating-the-pipeline/0612-duplicate-detection|0612]].

---

## Full Row Replace vs. Partial Update

The ECL philosophy is to clone the source exactly -- `DO UPDATE SET (all columns)` is the simplest approach and matches that goal. Every MERGE overwrites the entire row with the source's current state, which means the destination always reflects the source regardless of which columns changed.

```sql
-- destination: transactional (PostgreSQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON CONFLICT (order_id)
DO UPDATE SET
  status = EXCLUDED.status,
  total = EXCLUDED.total,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at;
```

Partial updates -- `DO UPDATE SET status = EXCLUDED.status` while leaving other columns untouched -- earn their complexity only when partial column loading ([[02-full-replace-patterns/0210-partial-column-loading|0210]]) forces them. If you're extracting all columns, update all columns. Deciding which columns "matter" is a business decision that breaks the conforming boundary ([[01-foundations-and-archetypes/0102-what-is-conforming|0102]]).

---

## Delete-Insert as a MERGE Alternative

An alternative to a true MERGE is delete-insert: delete all destination rows that match the incoming batch's keys, then insert the full batch. The result is identical (destination ends up with the source's current state for every key in the batch), but the execution plan avoids the columnar MERGE cost on engines where DELETE + INSERT is cheaper than a single MERGE statement.

```sql
-- destination: columnar
-- Delete-insert pattern
DELETE FROM orders
WHERE order_id IN (SELECT order_id FROM _stg_orders);

INSERT INTO orders
SELECT * FROM _stg_orders;
```

On BigQuery, this still rewrites the affected partitions twice (once for DELETE, once for INSERT), so the cost advantage over MERGE depends on how many partitions are touched and whether the batch is pre-deduplicated. On Snowflake, the two operations inside a transaction can be cheaper than a MERGE because Snowflake's MERGE has additional overhead for the MATCHED/NOT MATCHED evaluation.

---

## MERGE and Schema Evolution

A new column appears in the source. What happens to the MERGE?

**Column-explicit MERGE** (listing columns in the INSERT and UPDATE clauses) silently ignores the new column -- the MERGE succeeds, but the new column's data is dropped on every load. The destination never gets it, and nothing alerts you to the gap.

**`SELECT *` extraction + dynamic MERGE** (building the MERGE statement from the staging table's schema at runtime) fails with a column mismatch if the staging table has a column that doesn't exist in the destination. The error is loud, which is better than silent data loss, but it breaks the pipeline.

Neither outcome is good. Schema evolution needs handling **before** the MERGE executes:

1. **Detect** -- compare the staging table's schema against the destination's schema before running the MERGE. New columns, dropped columns, and type changes are all detectable at this point.
2. **Decide** -- a schema policy determines the response. Two modes are compatible with ECL:

| Entity | `evolve` | `freeze` |
|---|---|---|
| New table | Create it | Raise error |
| New column | Add it via `ALTER TABLE` | Raise error |
| Type change | Widen if compatible | Raise error |

Some loaders offer `discard_row` and `discard_value` modes that drop data silently when the schema doesn't match. These are transformation decisions -- deciding what data to keep based on schema fit -- and they break the conforming boundary ([[01-foundations-and-archetypes/0102-what-is-conforming|0102]]). If the source sent it, the destination should have it. Either accept the change or reject the load; don't silently drop data.

3. **Apply** -- if the policy is `evolve`, add the column to the destination (`ALTER TABLE ADD COLUMN`) before the MERGE runs. If it's `freeze`, the pipeline stops and alerts.

The recommended production default is `evolve` for new columns and `freeze` for type changes -- new nullable columns appearing in the destination are harmless (downstream queries that don't reference them are unaffected), while type changes that silently widen a column can break downstream logic. See [[06-operating-the-pipeline/0608-data-contracts|0608]] for formalizing schema policies into enforceable contracts, and [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for how each engine handles `ALTER TABLE ADD COLUMN`.

> [!warning] Column-explicit MERGE is a silent schema freeze on the entire table
> If your MERGE statement lists columns explicitly and you don't have a detection step before it, the destination schema is frozen at whatever columns existed when the MERGE was written. New source columns are silently dropped on every load, type changes are never propagated, and the destination drifts further from the source with every schema change. Either build the MERGE dynamically from the staging schema, or add a schema comparison step that catches drift before the MERGE executes.

---

## Staging Deduplication

The extraction batch can contain duplicates: the overlap buffer from [[03-incremental-patterns/0302-cursor-based-extraction|0302]], the dual cursor overlap from [[03-incremental-patterns/0310-create-vs-update-separation|0310]], or simply a source that returns the same row twice within the extraction window.

If the staging table contains two rows with the same MERGE key, the behavior is engine-dependent:

- **BigQuery** raises a runtime error: "UPDATE/MERGE must match at most one source row for each target row"
- **Snowflake** processes both rows non-deterministically -- one wins, but which one is undefined
- **PostgreSQL** `ON CONFLICT` processes rows in order, so the last one wins -- but "in order" depends on the staging query's sort

Deduplicate the staging table before the MERGE to avoid all three problems:

```sql
-- destination: columnar
-- Keep the latest version of each key in staging
CREATE OR REPLACE TABLE _stg_orders_deduped AS
SELECT *
FROM _stg_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;
```

Some loaders deduplicate staging automatically when a primary key is defined on the resource. If yours doesn't, or if you're building the pipeline yourself, add this step explicitly.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> MERGE is the most expensive DML operation in columnar engines. The cost scales with the number of partitions touched, not the batch size. Minimize partition spread in each batch, consider delete-insert as an alternative, and evaluate whether [[04-load-strategies/0404-append-and-materialize|0404]] (append + dedup view) is cheaper for tables with low mutation rates relative to their size.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> `INSERT ... ON CONFLICT` is cheap -- each row is an index lookup + point write. Cost scales linearly with batch size. The primary key index handles conflict detection efficiently. For large batches (100K+ rows), load into a staging table first and run the `INSERT ... ON CONFLICT ... SELECT FROM staging` as a single statement rather than row-by-row inserts.

---

## Related Patterns

- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- produces the batch that feeds the MERGE
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- another extraction pattern that produces batches for MERGE
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- when the table is small enough that MERGE complexity isn't worth it
- [[04-load-strategies/0402-append-only|0402-append-only]] -- when the source is immutable and MERGE isn't needed
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- avoid MERGE entirely by appending every version and deduplicating downstream
- [[04-load-strategies/0405-hybrid-append-merge|0405-hybrid-append-merge]] -- append for history, MERGE for current state
- [[05-conforming-playbook/0502-synthetic-keys|0502-synthetic-keys]] -- when the source has no stable primary key for the MERGE
- [[06-operating-the-pipeline/0608-data-contracts|0608-data-contracts]] -- schema policies that gate the MERGE on schema compatibility
- [[06-operating-the-pipeline/0612-duplicate-detection|0612-duplicate-detection]] -- when non-unique keys cause duplicates to compound
