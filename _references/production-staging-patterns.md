---
title: "Production Staging Patterns -- Lessons from 6500 Tables"
type: reference
tags:
  - reference/production
  - reference/staging
relevant_chapters:
  - 0204-staging-swap
  - 0403-merge-upsert
  - 0406-reliable-loads
  - 0611-partial-failure-recovery
---

# Staging Patterns from Production

Source: Warp production system (35+ clients, 6500+ tables, BigQuery destination).

## The Two-Dataset Pattern

Every incremental load goes through a staging dataset before reaching the final destination. The staging dataset is a sibling of the destination with a `_staging` suffix:

```
project.client_dataset           -- final destination
project.client_dataset_staging   -- ephemeral staging area
```

### Why Not Load Directly?

Loading directly to the destination with `MERGE` or `DELETE+INSERT` requires the loader to know the merge logic. When using tools like dlt that abstract the load step, you lose control over:
- Partition pruning (dlt's merge scans the entire table)
- Deduplication strategy (ROW_NUMBER vs QUALIFY)
- Transaction boundaries (BEGIN/COMMIT around DELETE+INSERT)

By loading to staging first with `write_disposition="replace"`, the loader's job is simple: dump data into a clean table. The merge is a separate SQL step with full control.

### Lifecycle

```
1. Pre-cleanup: ensure staging dataset exists, delete stale staging table
2. Load:        tool loads data → staging table created with fresh data
3. Merge:       custom SQL reads staging, writes to destination
4. Cleanup:     delete staging table
```

If the merge fails, staging data survives for debugging. If the load fails, staging is empty or absent -- no corruption of destination.

## Race Conditions in Staging

### Parallel Asset Creation (409 Already Exists)

When multiple assets (tables) load simultaneously, they all try to create the same staging dataset. Without `exists_ok=True` (or equivalent), the second creator gets a 409 conflict.

**Fix:** Pre-create the dataset with `exists_ok=True` before every load. Idempotent, safe for parallel execution.

### Schema Cache vs Table Deletion (404 Not Found)

Some loaders cache schema metadata in the destination (e.g., dlt stores schema hashes in `_dlt_version`). If the staging table is deleted after merge but the schema cache survives, the next load skips table creation (cache hit) and tries to write to a nonexistent table.

**Symptoms:**
```
404 Not Found: Table project:dataset_staging.table was not found
```

**Root cause:** Schema cache says table exists → loader skips DDL → TRUNCATE or INSERT fails on missing table.

**Fixes (in order of preference):**
1. Use the loader's built-in refresh mechanism (e.g., dlt's `refresh="drop_resources"`)
2. Delete the schema cache before each load (targeted, per-pipeline)
3. Delete the entire schema cache table (dangerous for concurrent loads)

### Pending Packages from Interrupted Loads

If a load fails mid-flight (process kill, network timeout, Windows WinError 6), the loader may leave "pending packages" -- partially written load artifacts. The next run picks them up and tries to apply them to tables that may no longer exist.

**Fix:** Clear pending packages at pipeline creation time, before starting a new load.

## The Staging-Swap Pattern (Full Replace)

For full refreshes, staging doubles as a validation gate:

```sql
-- 1. Load all data to staging
-- 2. Validate: staging row count must be >= 1% of destination
--    (protects against partial loads replacing a full table)
-- 3. DROP destination
-- 4. CREATE destination AS SELECT * FROM staging (CTAS)
-- 5. DROP staging
```

The 1% threshold catches the common failure mode: the source returned 50 rows instead of 500,000 due to a connection timeout, and you're about to replace the entire table with garbage.

## The DELETE+INSERT Pattern (Incremental Merge)

```sql
BEGIN TRANSACTION;

-- Delete rows that appear in staging (they have new versions)
DELETE FROM destination AS d
WHERE EXISTS (
    SELECT 1 FROM staging s
    WHERE d.pk = s.pk
);

-- Insert new versions, deduped
INSERT INTO destination (col1, col2, ...)
SELECT col1, col2, ...
FROM staging
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _dlt_id) = 1;

COMMIT TRANSACTION;
```

### Why Not MERGE?

BigQuery `MERGE` with `WHEN MATCHED THEN DELETE` + `WHEN NOT MATCHED THEN INSERT` is functionally equivalent but:
- Scans the entire destination (no partition pruning on the DELETE)
- Can't use QUALIFY for dedup (syntax limitation)
- Harder to reason about in transactional context

### Detail Tables with Group Columns

For header-detail tables (orders/order_lines), the DELETE should match on the group column (DocEntry), not the full PK (DocEntry + LineNum). This ensures that when a header appears in staging, ALL its detail lines are replaced -- detecting lines deleted in the source.

```sql
DELETE FROM order_lines AS d
WHERE EXISTS (
    SELECT 1 FROM order_lines_staging s
    WHERE d.order_id = s.order_id  -- group column, not full PK
);
```

## Schema Evolution at Load Time

When staging has columns that don't exist in the destination, `ALTER TABLE ADD COLUMN` before the merge:

```sql
ALTER TABLE destination ADD COLUMN new_col FLOAT64;
```

BigQuery-specific gotcha: `SchemaField.field_type` returns legacy names (`FLOAT`, `INTEGER`, `BOOLEAN`) but `ALTER TABLE` requires standard SQL types (`FLOAT64`, `INT64`, `BOOL`).
