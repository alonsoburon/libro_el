---
title: "The Health Table"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-23
updated: 2026-03-23
---

# The Health Table

> **One-liner:** One row per table per run, raw measurements only -- everything else is a query on top.

## The Problem

The four layers from [[06-operating-the-pipeline/0601-monitoring-observability|0601]] tell you *what* to watch. This pattern is the *how*: a single append-only table that captures raw measurements from every pipeline run, giving you a queryable history of everything your orchestrator doesn't track natively. Without it, monitoring lives in scattered logs, orchestrator UIs, and tribal knowledge -- none of which you can `SELECT` from at 7 AM when something is wrong.

## The Pattern

Not every column is equally important. The schema below is ordered by criticality, and the last group is optional depending on how much storage cost you're willing to absorb.

```sql
-- destination: bigquery
-- One row per table per pipeline run. Append-only.
CREATE TABLE health.runs (
  -- == Identity (always needed) ==
  extracted_at             TIMESTAMP,
  client                   STRING,
  table_id                 STRING, -- Make sure its not only table name, but identifier in case you query 2 tables of the same name from different sources.
  run_id                   STRING, -- hopefully links back to orchestrator run

  -- == Critical (the metrics you check every morning) ==
  status                   STRING, -- SUCCESS, FAILED, WARNING
  error_message            STRING, -- raw error on failure, NULL on success
  source_rows              INT64,  -- counted at the source before extraction starts
  destination_rows         INT64,  -- counted at the destination after load completes
  rows_extracted           INT64,  -- rows returned by the extraction query

  -- == Important (phase timing -- where the time goes) ==
  extraction_seconds       FLOAT64,
  normalization_seconds    FLOAT64,
  load_seconds             FLOAT64,
  extraction_strategy      STRING, -- full_replace, incremental, window, etc.

  -- == Nice to have (valuable for debugging, but may be costly at scale) ==
  bytes_extracted          INT64,  -- raw data volume from source
  query_used               STRING, -- the actual extraction query executed
  schema_json              STRING  -- column names + types snapshot, JSON
);
```

> [!warning] Watch the storage cost of the last three columns
> `query_used` and `schema_json` are STRING columns that grow with query complexity and table width. At thousands of tables running 3x daily, the row count adds up fast -- and if each `query_used` averages 2KB, that column alone is 14GB/year before compression. Worth it for debugging, but if cost is really tight, consider storing them in a separate detail table keyed by `run_id` and only joining when you need them. `bytes_extracted` is cheap (INT64) and nearly free to keep.

The guiding principle is **store raw measurements, derive the rest at query time.** Discrepancy percentage, per-row extraction time, average row size, throughput, and total duration are all computable from the columns above and don't need their own storage. A view or a dashboard query handles them.

### Critical Columns

`status` and `error_message` tell you what failed and why without leaving the health table. Without `error_message`, "12 tables failed overnight" sends you digging through orchestrator logs, job UIs, and possibly multiple systems to find out why each one broke. With it, you can triage severity from a single query -- a connection timeout is different from a schema mismatch, and you want to know which you're dealing with before you start investigating. The subtler case is `status = 'SUCCESS'` with `rows_extracted = 0` -- normal when an incremental cursor is caught up, alarming when the source table was silently dropped or permissions changed. [[06-operating-the-pipeline/0610-extraction-status-gates|0610]] covers how to gate the load on extraction status so these two scenarios don't look identical in your health table.

`source_rows` is counted at the source before extraction starts -- a snapshot of the total at the moment you begin pulling. `destination_rows` is counted at the destination after the load finishes. `rows_extracted` is the number of rows the extraction query actually returned.

The per-run reconciliation check depends on the strategy. On a **full replace**, `rows_extracted` should equal `destination_rows` -- you pulled N rows, loaded N rows, the destination should have N rows. If it doesn't, the load lost or duplicated data. `source_rows` may differ slightly from `rows_extracted` because the source can receive writes between the count and the extraction -- transit-time noise, not data loss, typically under 0.1% on a busy table. Set your alert thresholds above this floor to avoid false positives on every run.

On an **incremental**, the per-run check is less direct -- `rows_extracted` is a window of change, not the full table, so it won't match `destination_rows`. Instead, track `source_rows` vs `destination_rows` across runs: if the totals drift apart over time, the incremental is accumulating missed rows or undetected deletes, and a full replace is overdue. See [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]] for thresholds and recovery.

### Important Columns

The timing breakdown stays as three separate columns -- `extraction_seconds`, `normalization_seconds`, `load_seconds` -- because a single `total_seconds` hides whether the bottleneck is the source query, the conforming step, or the destination load. When a pipeline that used to take 5 minutes starts taking 40, you need to know which phase is degrading without digging into logs. The total is trivially computable from the parts; the parts are not recoverable from the total.

`extraction_strategy` records whether this run was `full_replace`, `incremental`, `window`, or something else. The same table can run different strategies on different schedules -- a nightly full replace for purity, intraday incremental for freshness (see [[06-operating-the-pipeline/0608-tiered-freshness|0608]]). Without this column, 50k `rows_extracted` is ambiguous: perfectly normal on a full replace, possibly alarming on an incremental that usually returns 2k.

### Nice-to-Have Columns

`bytes_extracted` is cheap to store and catches a failure mode that row counts miss entirely: rows getting wider. If `rows_extracted` stays flat but `bytes_extracted` climbs, the source table is gaining columns or existing text columns are growing -- both of which affect extraction time, network transfer, and destination storage cost. Per-row size (`bytes_extracted / rows_extracted`) and throughput (`bytes_extracted / extraction_seconds`) are both derivable.

`query_used` stores the actual extraction query, which implicitly records the cursor value, window boundaries, and any filters applied. When an incremental returns 0 rows, the query tells you whether the cursor was already caught up or stuck. When a full replace suddenly takes 10x longer, the query tells you if someone added a WHERE clause that forced a full scan at source. It's the single most useful debugging column -- and the most expensive to store.

`schema_json` is a JSON snapshot of the column names and types seen during this run. Comparing it to the previous run's snapshot detects schema drift without building a separate fingerprinting system. The policies for what to do when drift is detected -- evolve (accept the change) or freeze (reject the load) -- belong in [[06-operating-the-pipeline/0609-data-contracts|0609]]. Silently discarding columns that don't match is a transformation decision, not a conforming one -- if the source sent it, the destination should have it (see [[04-load-strategies/0403-merge-upsert|0403]]).

### Derived Metrics

None of these need their own column. Build them as a view or compute them in your dashboard:

```sql
-- destination: bigquery
-- View on top of the health table for common derived metrics.
CREATE VIEW health.runs_derived AS
SELECT
  *,
  extraction_seconds + normalization_seconds + load_seconds
    AS total_seconds,
  -- Per-run check: did everything extracted actually land?
  -- Meaningful on full_replace; less useful on incremental.
  SAFE_DIVIDE(rows_extracted - destination_rows, rows_extracted) * 100
    AS load_loss_pct,
  -- Drift check: are source and destination totals diverging?
  -- Track over time for incremental tables.
  SAFE_DIVIDE(source_rows - destination_rows, source_rows) * 100
    AS drift_pct,
  SAFE_DIVIDE(rows_extracted, extraction_seconds)
    AS rows_per_second,
  SAFE_DIVIDE(bytes_extracted, rows_extracted)
    AS avg_row_bytes,
  SAFE_DIVIDE(bytes_extracted, extraction_seconds)
    AS throughput_bytes_per_sec
FROM health.runs;
```

> [!tip] `rows_per_second` is the early warning for source degradation
> On incremental tables, this metric should be roughly stable across runs. If it drops by half, the source query is getting slower per row -- possibly because the cursor column lost its index, or because the table's physical layout changed. A drop in `rows_per_second` with stable `rows_extracted` points at the source; stable `rows_per_second` with a spike in `rows_extracted` points at a data event.

### Staleness Report

Once the health table exists, staleness is a `MAX(extracted_at)` grouped by table -- the query is straightforward enough that [[06-operating-the-pipeline/0604-sla-management|0604]] covers it in full alongside the SLA thresholds that give the number meaning.

## Populating the Health Table

The schema is the easy part; the discipline is harder. Every run -- successful or not -- must append a row. A missing row in the health table is indistinguishable from "the pipeline didn't run" when you're triaging at 7 AM, and that ambiguity is worse than a recorded failure.

```sql
-- destination: bigquery
-- Append one row per table per run. Always, even on failure.
INSERT INTO health.runs (
  extracted_at, client, table_id, run_id,
  status, error_message,
  source_rows, rows_extracted, destination_rows,
  extraction_seconds, normalization_seconds, load_seconds,
  extraction_strategy
) VALUES (
  CURRENT_TIMESTAMP(), @client, @table_id, @run_id,
  @status, @error_message,
  @source_rows, @rows_extracted, @destination_rows,
  @extraction_seconds, @normalization_seconds, @load_seconds,
  @extraction_strategy
);
```

On failure, `rows_extracted` and `destination_rows` will likely be NULL -- that's expected. The row still captures `status = 'FAILED'`, the error message, and whatever timing was available before the failure point. NULL in `destination_rows` on a FAILED row means the load never completed, which is meaningfully different from zero (the load ran but produced nothing). Both are worth recording and both tell you something different during triage.

The timing columns require wrapping each phase in a timer -- most orchestrator SDKs and pipeline frameworks provide hook points (before/after extraction, before/after load) where you can capture deltas. If yours doesn't, a context manager or simple stopwatch around each phase is enough. Sub-second precision doesn't matter here; the value comes from tracking trends across runs, not from any single measurement.

> [!tip] Counting source rows without punishing the source
> `SELECT COUNT(*)` on a 50M-row transactional table can lock pages and spike CPU on the source. For drift detection, an approximate count from the database's statistics catalog is often good enough -- `pg_stat_user_tables.n_live_tup` in PostgreSQL, `information_schema.TABLES.TABLE_ROWS` in MySQL. You're watching for 10%+ swings, not exact matches. If the approximation is too stale (PostgreSQL's stats depend on autovacuum frequency), schedule a periodic exact count during off-hours and use the approximate count for intraday runs.

The health INSERT itself can fail -- destination timeout, permission issue, quota exhaustion -- and silently leave a gap in your monitoring. Wrap it in its own error handler with a fallback to local logging (a JSON file, a stderr line, anything durable), so you at least know the health write failed even if the row didn't land. Discovering that your monitoring table has a 3-day gap because the health destination was unreachable is a particularly frustrating way to learn you had no visibility during an incident.

## Where Your Orchestrator Fits

### Generating Metadata on Load

The ideal place to capture health metrics is inside the pipeline run itself -- as a side effect of extraction and load, not in a separate job that queries the destination afterward. If your orchestrator lets you attach custom metadata to each table after a run (row counts, extraction duration, schema fingerprint), that metadata becomes queryable and historically tracked without building a separate system.

This is worth prioritizing when evaluating orchestrators for ECL workloads (see [[08-appendix/0805-orchestrators|Appendix: Orchestrators]]). Dagster's custom asset metadata, for example, lets you record these numbers directly on the asset and graph them from the UI -- the health table columns above get populated as a side effect of the pipeline run rather than requiring a post-hoc collection step. The less infrastructure you build outside the orchestrator, the less you maintain.

When your orchestrator doesn't support rich metadata attachment -- which is the more common case -- the health table INSERT becomes an explicit final step in each pipeline run: a wrapper function that captures metrics and writes the row after the load completes (or fails). This works fine and is what most teams end up building. The key is placing the INSERT in a `finally` block or equivalent, so it fires regardless of whether the run succeeded, and giving it its own error handling so a health write failure doesn't mask the original pipeline error.

### Single Orchestrator

Every orchestrator tracks run status, duration, and dependency graphs natively -- the built-in UI covers Run Health almost entirely, and duplicating that visibility in the health table wastes both storage and effort.

The gap is Data Health. Your orchestrator knows the pipeline ran for 4 minutes and succeeded, but it has no idea that `orders` returned 12k rows instead of the usual 450k, or that the source schema lost a column between yesterday and today, or that `destination_rows` doesn't match `rows_extracted`. These are the metrics that justify the health table. Build it to fill the gaps your orchestrator leaves -- row counts, reconciliation deltas, schema fingerprints, phase timing if the orchestrator only gives you total duration -- and skip what's already there.

Source Health and Load Health slot in the same way: if the orchestrator already provides retry counts and error classification, use those natively and don't duplicate them. If it only gives you pass/fail with no structured error metadata, the health table's `status`, `error_message`, and phase timing columns cover the essentials. The principle is complementary, not redundant -- one system of record per metric, and the health table picks up everything the orchestrator drops.

### Orchestrator-per-Client (Orch. Cluster)

When each client runs its own orchestrator instance, no single UI gives you the full picture. "How many tables failed last night?" requires opening N dashboards, one per client, and mentally aggregating the results -- which nobody actually does consistently at 6 AM.

The health table solves this by becoming the unified monitoring layer above the individual orchestrators. Every instance appends to the same destination table after each run, partitioned by the `client` column, and the morning routine works against a single query across all clients rather than N separate UIs. Staleness reports, reconciliation checks, and cost rollups all aggregate naturally because they share a schema.

This setup also unlocks cross-client comparison, which individual orchestrator dashboards can never provide. If `orders` extraction takes 3 minutes for client A but 25 minutes for client B on the same schema version, the health table surfaces that in a single query -- and the cause is usually environmental (client B's source database is underpowered, or their `orders` table is 10x larger, or their network path to the extraction server adds latency). Same for schema drift: if client B's source adds a column that client A doesn't have, `schema_json` catches the divergence immediately, which matters when both clients are supposed to be running the same ERP version.

> [!warning] The central health table is now a dependency
> If the health destination is unreachable, every pipeline run across all clients loses its monitoring write. A local fallback -- writing the health row to a staging table in each client's own destination, then syncing centrally on a schedule -- mitigates this at the cost of slight staleness in the unified view. At minimum, the health INSERT should log to stderr on failure so the orchestrator's native run output still captures what happened, even if the health table doesn't.

## Tradeoffs

| Pro                                                                | Con                                                                        |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Catches silent failures that pass/fail misses                      | Storage cost grows linearly with table count and run frequency             |
| Health table provides a single queryable history                   | Requires discipline to populate on every run, including failures           |
| Raw measurements let you derive new metrics without schema changes | STRING columns (`query_used`, `schema_json`) can dominate storage at scale |
| Works across orchestrators in a cluster setup                      | Adds write latency to every pipeline run (one INSERT per table per run)    |
