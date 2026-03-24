---
title: "Pipeline Concurrency -- Parallel Loads, Shared Resources, Race Conditions"
type: reference
tags:
  - reference/production
  - reference/operations
relevant_chapters:
  - 0606-scheduling-and-dependencies
  - 0406-reliable-loads
  - 0612-partial-failure-recovery
---

# Pipeline Concurrency Patterns

Source: Warp production running 50-200 tables per client, 2-10 concurrent extractions.

## The Concurrency Model

Each table is an independent unit of work (a Dagster asset). Tables extract in parallel, limited by a concurrency pool. The pool size is configurable per client (default: 2).

```
Concurrency pool = 2:

  t=0  ─── [orders]     [products]
  t=1  ─── [orders]     [order_lines]  (products finished, next table starts)
  t=2  ─── [invoices]   [order_lines]
```

### Why Limit Concurrency?

1. **Source connections:** Each extraction holds a DB connection. 50 concurrent = 50 connections.
2. **Source load:** Parallel table scans on the same server compete for I/O.
3. **Destination API limits:** BigQuery has per-project rate limits for load jobs.
4. **Memory:** Each extraction buffers data in-memory before writing to parquet.

### Tuning by Client

| Client Profile | Recommended Concurrency |
|---------------|------------------------|
| Small DB, fast server | 4-6 |
| Large DB, shared server | 2 |
| SAP HANA (column store, fast reads) | 4 |
| MSSQL on client laptop | 1 |

## Shared Resources and Race Conditions

### Staging Dataset (Shared)

All tables for a client share one staging dataset. Each table gets its own staging TABLE within it. This means:

- **Safe:** Two tables loading simultaneously to `staging.orders` and `staging.products` -- different tables, no conflict
- **Unsafe:** Two runs of the SAME table loading to `staging.orders` -- same table, data corruption

The pipeline prevents same-table concurrency via unique pipeline names and `shutil.rmtree` detection (locked files = duplicate execution).

### _dlt_version (Shared)

The schema cache table `_dlt_version` is shared across all pipelines in a dataset. Each pipeline writes its own entries (identified by `schema_name`). This is safe as long as:

1. Each pipeline has a unique `schema_name` (guaranteed by unique pipeline names)
2. Cleanup operations target specific `schema_name` values (not the entire table)
3. Nobody `DROP TABLE`s `_dlt_version` while another pipeline is mid-load

### BigQuery Dataset Creation (Race)

Multiple pipelines starting simultaneously all try to `CREATE DATASET IF NOT EXISTS`. BigQuery handles this with `exists_ok=True` -- all calls succeed, no 409.

Without `exists_ok`, the second pipeline to execute gets:
```
409 Already Exists: Dataset already exists
```

## Run Cancellation

### Docker (Reliable)

Each run executes in its own container. Cancellation = `docker stop`:
1. Docker sends SIGTERM to container
2. Loader drains current load jobs (graceful shutdown)
3. After `stop_timeout` (300s), Docker sends SIGKILL

### Windows (Less Reliable)

Runs are subprocesses of the Dagster daemon. Cancellation sends a signal to the subprocess. The loader may or may not handle it gracefully depending on which step it's in:

- **Extract:** Signal interrupts the DB query -- connection drops, partial data lost
- **Normalize:** Signal interrupts file writing -- partial parquet files
- **Load:** Signal can trigger graceful drain (loader-specific)

In all cases, the next run cleans up via `rmtree` + `drop_pending_packages`.

## Duplicate Execution Detection

If the same table runs twice simultaneously (scheduler bug, manual trigger), both try to access the same pipeline directory. On the second execution:

```python
if os.path.exists(pipeline_dir):
    shutil.rmtree(pipeline_dir)  # Fails if files are locked by first run
```

The `rmtree` failure (locked files) is the detection mechanism. The second run fails with a filesystem error, which is the correct behavior -- better to fail loudly than corrupt data.

## Dependencies Between Tables

Header-detail tables have implicit dependencies:
- `orders` should load before `order_lines` (for reconciliation)
- But they CAN load in parallel (merge handles the ordering)

In practice, most pipelines don't enforce ordering between tables. The merge is idempotent -- if `order_lines` loads before `orders`, the data is still correct. The reconciliation check might show a temporary discrepancy, but the next run resolves it.

Exception: tables that reference each other via JOIN in the merge (e.g., detail with cursor-from-header). These tables should be in the same extraction batch but don't need strict ordering.
