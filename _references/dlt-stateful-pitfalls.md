---
title: "dlt -- Stateful Pipeline Pitfalls & Workarounds"
source: dlt source code investigation (v0.5.x)
type: reference
tags:
  - reference/dlt
  - reference/production
relevant_chapters:
  - 0406-reliable-loads
  - 0611-partial-failure-recovery
  - 0109-idempotency
---

# dlt Stateful Pipeline Pitfalls

Source: Direct investigation of dlt source code to resolve production 404 errors.

## The Schema Hash Cache

dlt tracks schema versions in a `_dlt_version` table at the destination. On every `pipeline.run()`:

1. Compute schema hash from current table definitions
2. Query `_dlt_version` for matching hash
3. **If hash found → skip all DDL** (no CREATE TABLE, no ALTER TABLE)
4. If hash not found → execute DDL, insert new hash

This is an optimization: if the schema hasn't changed, skip the expensive DDL step.

### The Problem

If something external deletes the actual table but `_dlt_version` retains the hash, dlt sees the hash, skips DDL, and tries to TRUNCATE a nonexistent table:

```
update_stored_schema()  → hash found → skip CREATE TABLE
initialize_storage(truncate_tables=[...]) → TRUNCATE → 404 Not Found
```

This happens when:
- A custom merge step deletes the staging table after use
- A cleanup job removes tables but not `_dlt_version`
- A failed run leaves `_dlt_version` but the table was rolled back

### The Internal Mechanism (code paths)

```
dlt/load/utils.py:188          → update_stored_schema()
dlt/destinations/job_client_impl.py:306  → get_stored_schema_by_hash()
  ├─ hash found → return (skip DDL)
  └─ hash not found → _execute_schema_update_sql() → CREATE TABLE
dlt/load/utils.py:196          → initialize_storage(truncate_tables=...)
dlt/destinations/job_client_impl.py:296  → sql_client.truncate_tables()
  └─ TRUNCATE on nonexistent table → 404
```

## refresh Modes

dlt offers three refresh modes via `pipeline.run(refresh=...)`:

| Mode | Tables Affected | Schema Cache | Use Case |
|------|----------------|-------------|----------|
| `drop_resources` | Only current resource's tables | Deleted (per-pipeline) | **Safest for ECL** |
| `drop_sources` | ALL tables in source | Deleted (all) | Full reset |
| `drop_data` | Same as drop_resources | Preserved | Truncate only |

### How `drop_resources` Works Internally

1. **Extract phase:** `refresh_source()` identifies tables from `source.schema`
2. Returns `{"dropped_tables": [table_info_dicts]}`
3. **Load phase:** `init_client()` receives `drop_tables` list
4. Calls `job_client.drop_tables(*tables, delete_schema=True)`
5. `drop_tables()` does:
   ```sql
   DROP TABLE IF EXISTS staging_table;
   DELETE FROM _dlt_version WHERE schema_name = 'pipeline_name';
   ```
6. `update_stored_schema()` → hash not found → CREATE TABLE

### Concurrency Safety

`_delete_schema_in_storage()` deletes by `schema_name`:
```sql
DELETE FROM _dlt_version WHERE schema_name = %s
```

Each pipeline has a unique name → concurrent pipelines writing to the same dataset don't interfere.

### Side Effect: `_staging_staging`

With `replace_strategy="truncate-and-insert"`, `refresh="drop_resources"` triggers the `WithStagingDataset` block because `drop_table_names` is non-empty (`dlt/load/utils.py:142`):

```python
if staging_tables or drop_table_names:  # drop_table_names from refresh
    with job_client.with_staging_dataset():
        _init_dataset_and_update_schema(...)
```

This creates a `{dataset}_staging_staging` dataset containing only `_dlt_version`. It's cosmetic -- no data flows through it. Clean up periodically.

## replace_strategy Comparison

| Strategy | How It Works | Pros | Cons |
|----------|-------------|------|------|
| `truncate-and-insert` | TRUNCATE existing table, INSERT new data | No extra dataset | Requires table to exist (404 if deleted) |
| `staging-optimized` | Load to `{dataset}_staging`, move to final | Handles missing tables | Creates `_staging` suffix (doubles with manual staging) |

For pipelines that manage their own staging datasets, `truncate-and-insert` is the right choice -- but requires `refresh="drop_resources"` to handle the schema cache.

## Pending Packages

If `pipeline.run()` fails between extract and load, dlt leaves "pending packages" in the pipeline directory. The next run picks them up and tries to load them -- to tables that may no longer exist.

**Prevention:**
```python
pipeline.drop_pending_packages()
```

Call this after creating the pipeline, before starting a new extraction.

## State Tables Summary

| Table | Purpose | Managed by Pipeline | Safe to Delete |
|-------|---------|--------------------|----|
| `_dlt_version` | Schema hash cache | Yes (refresh cleans it) | Yes, per schema_name |
| `_dlt_loads` | Load history | No (not used by ECL pipelines) | Yes |
| `_dlt_pipeline_state` | Incremental state, cursors | No (not used by stateless pipelines) | Yes |

For stateless ECL pipelines, all three can be periodically deleted without consequence. The `refresh` mechanism handles `_dlt_version` per-run. The other two accumulate harmlessly and can be cleaned up by a scheduled job.
