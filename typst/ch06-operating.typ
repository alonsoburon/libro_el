#import "theme.typ": gruvbox, ecl-theme, ecl-tip, ecl-warning, ecl-danger, ecl-info
#show: ecl-theme
= Monitoring and Observability
<monitoring-and-observability>
#quote(block: true)[
#strong[One-liner:] Row counts tell you the pipeline ran. They don't tell you it ran #emph[well];, or that the data it produced is worth trusting.
]

== The Problem
<the-problem>
Most pipelines start with a single check: did it succeed? That binary signal covers maybe 40% of what can go wrong. A pipeline can succeed while producing garbage -- a query timed out and returned partial results, a full replace that used to take 3 minutes now takes 45 because the table grew 10x, the source schema changed and the loader silently dropped columns, or half the batch loaded while the other half timed out, leaving the destination with rows from two different points in time. Every one of these scenarios reports SUCCESS. Every one of them delivers broken data to consumers.

Without structured #strong[observability];, you discover these problems when a stakeholder asks why the dashboard is wrong -- often days after the data actually broke. By that point the blast radius is wide: downstream models have consumed the bad data, reports have been sent, and the person asking is already frustrated. The monitoring pattern in this chapter is about catching those failures before anyone else does, ideally within minutes of the pipeline run that caused them.

The key insight is that you need to track more than pass/fail, but you also need to resist the urge to track everything. Every metric you record has a storage cost and a cognitive cost -- someone has to look at it, and if the dashboard has 40 numbers, nobody looks at any of them carefully. The goal is a small set of raw measurements that cover the important failure modes, from which you can derive everything else.

== Four Layers of Pipeline Observability
<four-layers-of-pipeline-observability>
Observability breaks into four layers, each covering a different failure mode. You don't need all of them on day one -- Run Health and Data Health cover the critical cases, and the other two earn their place as your pipeline count grows.

=== 1. Run Health
<run-health>
The basics: did the pipeline run, did it succeed, and how long did it take? Every orchestrator tracks this natively -- run status, duration, dependency graphs -- so there's rarely anything to build here. What the orchestrator gives you for free is already enough.

The one thing worth adding is trend tracking on duration. A 3-minute job that creeps to 30 minutes is a signal even when it still succeeds, because it tells you the table is growing or the source is degrading before either becomes an emergency. We had a table silently grow enough that its extraction started overlapping with the next scheduled run, causing 3 PM crashes for weeks before we charted duration and saw it had been climbing steadily for months -- the fix was moving heavy tables to a less frequent schedule (0608), but the signal was in the health table long before the failure. Without duration trends, you discover these problems when jobs start timing out, which is too late to fix gracefully.

Retry counts are worth recording if your pipeline retries on transient failures. A job that succeeds on the third retry every day is not healthy -- it's masking an unstable connection or a source system under load.

=== 2. Data Health
<data-health>
This is where monitoring earns its keep. Run Health tells you the pipeline executed; Data Health tells you what the pipeline produced.

#strong[Row counts] are the single most useful metric. Track three numbers: `source_rows` (counted at the source before extraction), `rows_extracted` (returned by the extraction query), and `destination_rows` (counted at the destination after load). Each pair tells you something different. On a full replace, `rows_extracted` should equal `destination_rows` -- you pulled N rows and loaded them, so the destination should have N. If it doesn't, something was lost or duplicated during the load. `source_rows` vs `destination_rows` over time is a drift indicator for incremental tables -- if the totals diverge across runs, you're accumulating missed rows or orphaned deletes. A 50% drop in any of the three is a signal worth investigating, but row counts have a blind spot: they measure volume, not composition. We had a client whose `invoices` table hard-deleted draft invoices regularly while new ones replaced them at roughly the same rate -- the count stayed stable, but the destination accumulated stale drafts the source had already removed. Only a daily PK comparison (0614) caught the problem, because row counts told us the right #emph[number] of rows existed without revealing they were the wrong rows.

For incremental tables specifically, `rows_extracted` over time is revealing. It shows big moments of change -- month-end closes, batch corrections, seasonal spikes -- where you may want to widen your extraction window or shift the schedule to avoid overlapping with the source system's heaviest period.

#ecl-warning("Alert on row count spikes")[If an incremental that usually returns 2k `rows_extracted` suddenly returns 50k, the source had a large batch operation -- month-end close, bulk import, data migration. That spike means there may be more rows changed than your window caught. Consider triggering a full replace that night to reset state and catch anything the incremental missed.]

#strong[Freshness] is the other critical data health metric: when was this table last successfully loaded? The health table records `extracted_at` on every run (complementing the per-row `_extracted_at` from 0501, which tags individual records rather than pipeline runs), so staleness is a simple aggregation -- 0604 covers the query and the SLA thresholds that give the number meaning.

#strong[Schema fingerprints and null rates] are worth tracking here as changes between runs, but enforcement -- what to do when they change -- belongs in 0609.

=== 3. Source Health
<source-health>
Source health metrics are less about your pipeline and more about the system you're extracting from. Query duration at the source, isolated from load performance, tells you whether the source database is degrading or whether your extraction query needs tuning. Timeout frequency -- queries that hit the threshold even when they eventually return on retry -- reveals instability before it becomes a failure.

Source system load impact is worth tracking for a less obvious reason: it's a sales tool. If you can demonstrate that your extraction uses less than 1% of the source database's capacity, you can sell the pipeline as a lightweight, non-invasive solution to more technical stakeholders who are nervous about letting you query their production system. See 0607 for the full treatment.

=== 4. Load Health
<load-health>
Load #strong[cost] generally matters more than load duration. Duration tends to be stable for a given table size and load strategy -- it's predictable and boring. Cost is the variable that shifts under your feet: a MERGE on BigQuery at 100k rows costs differently than at 10M, DML pricing changes without warning, and switching from full replace to incremental changes the operation type entirely. Tracking `load_seconds` is still useful for spotting bottlenecks, but if you had to pick one dimension to watch on the load side, it's cost -- and 0603 covers how to capture and attribute it.

The destination row count after load closes the loop on reconciliation. On a full replace, `destination_rows` should match `rows_extracted` -- if it doesn't, rows were lost or duplicated during the load. On an incremental, tracking `source_rows` vs `destination_rows` over time reveals whether the totals are drifting apart across runs, which is the signal that your incremental is accumulating missed rows or undetected deletes. See 0614 for the full treatment.

== The Morning Routine
<the-morning-routine>
Before diving into implementation, it's worth naming what you're actually looking at when you open the dashboard. The sequence matters -- it's a triage, not a survey.

#ecl-tip("Four numbers you check first")[(1) How many tables failed overnight. (2) Which tables are stale beyond their SLA. (3) Any row count anomalies -- spikes, drops, or reconciliation deltas above threshold. (4) Cost per day. Everything else is drill-down from one of these four.]

In a single-orchestrator setup, the orchestrator's native UI covers items 1 and 2 well enough. Items 3 and 4 come from the health table and the cost monitoring layer from 0603. In a multi-orchestrator setup, the health table is the only place where all four numbers converge -- which is why it exists.

== The Pattern
<the-pattern>
\// TODO: Convert mermaid diagram to Typst or embed as SVG

The pattern is straightforward: after every pipeline run, append a row to a health table. One row per table per run, with the raw measurements needed to answer the four morning questions. Everything else -- dashboards, alerts, SLA reports -- is a query on top of this table. 0602 covers the schema, the column-by-column rationale, and how to populate it.

== Anti-Patterns
<anti-patterns>
#ecl-warning("Don't confuse monitoring with alerting")[Monitoring is the dashboard you look at; alerting is the pager that wakes you up. They share data, but the threshold for "worth recording" is much lower than "worth paging someone." Record everything in the health table. Alert on a carefully tuned subset. See 0605 for how to calibrate the boundary.]

#ecl-danger("Don't track everything equally")[Per-row metrics on a 100M-row table are storage, not observability. The health table is one row per table per run -- aggregate metrics only. If you need row-level diagnostics, run them ad hoc against the source or destination, not as part of every pipeline run.]

#ecl-warning("Don't build a custom monitoring stack")[You don't need one if you're running a single orchestrator with 50 tables -- the orchestrator's native run history, duration tracking, and status page are probably enough. The health table pattern earns its complexity at scale -- hundreds of tables, multiple pipelines, or a multi-orchestrator cluster where no single UI gives you the full picture. Build monitoring infrastructure in proportion to the monitoring problem you actually have.]

== What Comes Next
<what-comes-next>
0602 covers the health table implementation -- the schema, column rationale, derived metrics, and how to populate it reliably. From there, 0603 extends it with cost attribution, 0604 builds freshness SLAs on the staleness data, and 0605 draws the line between what's worth recording and what's worth paging someone about.

// ---

= The Health Table
<the-health-table>
#quote(block: true)[
#strong[One-liner:] One row per table per run, raw measurements only -- everything else is a query on top.
]

== The Problem
<the-problem-1>
The four layers from 0601 tell you #emph[what] to watch. This pattern is the #emph[how];: a single append-only table that captures raw measurements from every pipeline run, giving you a queryable history of everything your orchestrator doesn't track natively. Without it, monitoring lives in scattered logs, orchestrator UIs, and tribal knowledge -- none of which you can `SELECT` from at 7 AM when something is wrong.

== The Pattern
<the-pattern-1>
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

#ecl-warning("Watch storage cost on STRING columns")[`query_used` and `schema_json` are STRING columns that grow with query complexity and table width. At thousands of tables running 3x daily, the row count adds up fast -- and if each `query_used` averages 2KB, that column alone is 14GB/year before compression. Worth it for debugging, but if cost is really tight, consider storing them in a separate detail table keyed by `run_id` and only joining when you need them. `bytes_extracted` is cheap (INT64) and nearly free to keep.]

The guiding principle is #strong[store raw measurements, derive the rest at query time.] Discrepancy percentage, per-row extraction time, average row size, throughput, and total duration are all computable from the columns above and don't need their own storage. A view or a dashboard query handles them.

=== Critical Columns
<critical-columns>
`status` and `error_message` tell you what failed and why without leaving the health table. Without `error_message`, "12 tables failed overnight" sends you digging through orchestrator logs, job UIs, and possibly multiple systems to find out why each one broke. With it, you can triage severity from a single query -- a connection timeout is different from a schema mismatch, and you want to know which you're dealing with before you start investigating. The subtler case is `status = 'SUCCESS'` with `rows_extracted = 0` -- normal when an incremental cursor is caught up, alarming when the source table was silently dropped or permissions changed. 0610 covers how to gate the load on extraction status so these two scenarios don't look identical in your health table.

`source_rows` is counted at the source before extraction starts -- a snapshot of the total at the moment you begin pulling. `destination_rows` is counted at the destination after the load finishes. `rows_extracted` is the number of rows the extraction query actually returned.

The per-run reconciliation check depends on the strategy. On a #strong[full replace];, `rows_extracted` should equal `destination_rows` -- you pulled N rows, loaded N rows, the destination should have N rows. If it doesn't, the load lost or duplicated data. `source_rows` may differ slightly from `rows_extracted` because the source can receive writes between the count and the extraction -- transit-time noise, not data loss, typically under 0.1% on a busy table. Set your alert thresholds above this floor to avoid false positives on every run.

On an #strong[incremental];, the per-run check is less direct -- `rows_extracted` is a window of change, not the full table, so it won't match `destination_rows`. Instead, track `source_rows` vs `destination_rows` across runs: if the totals drift apart over time, the incremental is accumulating missed rows or undetected deletes, and a full replace is overdue. See 0614 for thresholds and recovery.

=== Important Columns
<important-columns>
The timing breakdown stays as three separate columns -- `extraction_seconds`, `normalization_seconds`, `load_seconds` -- because a single `total_seconds` hides whether the bottleneck is the source query, the conforming step, or the destination load. When a pipeline that used to take 5 minutes starts taking 40, you need to know which phase is degrading without digging into logs. The total is trivially computable from the parts; the parts are not recoverable from the total.

`extraction_strategy` records whether this run was `full_replace`, `incremental`, `window`, or something else. The same table can run different strategies on different schedules -- a nightly full replace for purity, intraday incremental for freshness (see 0608). Without this column, 50k `rows_extracted` is ambiguous: perfectly normal on a full replace, possibly alarming on an incremental that usually returns 2k.

=== Nice-to-Have Columns
<nice-to-have-columns>
`bytes_extracted` is cheap to store and catches a failure mode that row counts miss entirely: rows getting wider. If `rows_extracted` stays flat but `bytes_extracted` climbs, the source table is gaining columns or existing text columns are growing -- both of which affect extraction time, network transfer, and destination storage cost. Per-row size (`bytes_extracted / rows_extracted`) and throughput (`bytes_extracted / extraction_seconds`) are both derivable.

`query_used` stores the actual extraction query, which implicitly records the cursor value, window boundaries, and any filters applied. When an incremental returns 0 rows, the query tells you whether the cursor was already caught up or stuck. When a full replace suddenly takes 10x longer, the query tells you if someone added a WHERE clause that forced a full scan at source. It's the single most useful debugging column -- and the most expensive to store.

`schema_json` is a JSON snapshot of the column names and types seen during this run. Comparing it to the previous run's snapshot detects schema drift without building a separate fingerprinting system. The policies for what to do when drift is detected -- evolve (accept the change) or freeze (reject the load) -- belong in 0609. Silently discarding columns that don't match is a transformation decision, not a conforming one -- if the source sent it, the destination should have it (see 0403).

=== Derived Metrics
<derived-metrics>
None of these need their own column. Build them as a view or compute them in your dashboard:

```sql
-- destination: bigquery
-- View on top of the health table for common derived metrics.
CREATE VIEW health.runs_derived AS
SELECT
  ,
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

#ecl-tip("Early warning for source degradation")[On incremental tables, `rows_per_second` should be roughly stable across runs. If it drops by half, the source query is getting slower per row -- possibly because the cursor column lost its index, or because the table's physical layout changed. A drop in `rows_per_second` with stable `rows_extracted` points at the source; stable `rows_per_second` with a spike in `rows_extracted` points at a data event.]

=== Staleness Report
<staleness-report>
Once the health table exists, staleness is a `MAX(extracted_at)` grouped by table -- the query is straightforward enough that 0604 covers it in full alongside the SLA thresholds that give the number meaning.

== Populating the Health Table
<populating-the-health-table>
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

#ecl-warning("Count source rows without punishing the source")[`SELECT COUNT(\*)` on a 50M-row transactional table can lock pages and spike CPU on the source. For drift detection, an approximate count from the database's statistics catalog is often good enough -- `pg_stat_user_tables.n_live_tup` in PostgreSQL, `information_schema.TABLES.TABLE_ROWS` in MySQL. You're watching for 10%+ swings, not exact matches. If the approximation is too stale (PostgreSQL's stats depend on autovacuum frequency), schedule a periodic exact count during off-hours and use the approximate count for intraday runs.]

The health INSERT itself can fail -- destination timeout, permission issue, quota exhaustion -- and silently leave a gap in your monitoring. Wrap it in its own error handler with a fallback to local logging (a JSON file, a stderr line, anything durable), so you at least know the health write failed even if the row didn't land. Discovering that your monitoring table has a 3-day gap because the health destination was unreachable is a particularly frustrating way to learn you had no visibility during an incident.

== Where Your Orchestrator Fits
<where-your-orchestrator-fits>
=== Generating Metadata on Load
<generating-metadata-on-load>
The ideal place to capture health metrics is inside the pipeline run itself -- as a side effect of extraction and load, not in a separate job that queries the destination afterward. If your orchestrator lets you attach custom metadata to each table after a run (row counts, extraction duration, schema fingerprint), that metadata becomes queryable and historically tracked without building a separate system.

This is worth prioritizing when evaluating orchestrators for ECL workloads (see Appendix: Orchestrators). Dagster's custom asset metadata, for example, lets you record these numbers directly on the asset and graph them from the UI -- the health table columns above get populated as a side effect of the pipeline run rather than requiring a post-hoc collection step. The less infrastructure you build outside the orchestrator, the less you maintain.

When your orchestrator doesn't support rich metadata attachment -- which is the more common case -- the health table INSERT becomes an explicit final step in each pipeline run: a wrapper function that captures metrics and writes the row after the load completes (or fails). This works fine and is what most teams end up building. The key is placing the INSERT in a `finally` block or equivalent, so it fires regardless of whether the run succeeded, and giving it its own error handling so a health write failure doesn't mask the original pipeline error.

=== Single Orchestrator
<single-orchestrator>
Every orchestrator tracks run status, duration, and dependency graphs natively -- the built-in UI covers Run Health almost entirely, and duplicating that visibility in the health table wastes both storage and effort.

The gap is Data Health. Your orchestrator knows the pipeline ran for 4 minutes and succeeded, but it has no idea that `orders` returned 12k rows instead of the usual 450k, or that the source schema lost a column between yesterday and today, or that `destination_rows` doesn't match `rows_extracted`. These are the metrics that justify the health table. Build it to fill the gaps your orchestrator leaves -- row counts, reconciliation deltas, schema fingerprints, phase timing if the orchestrator only gives you total duration -- and skip what's already there.

Source Health and Load Health slot in the same way: if the orchestrator already provides retry counts and error classification, use those natively and don't duplicate them. If it only gives you pass/fail with no structured error metadata, the health table's `status`, `error_message`, and phase timing columns cover the essentials. The principle is complementary, not redundant -- one system of record per metric, and the health table picks up everything the orchestrator drops.

=== Orchestrator-per-Client (Orch. Cluster)
<orchestrator-per-client-orch.-cluster>
When each client runs its own orchestrator instance, no single UI gives you the full picture. "How many tables failed last night?" requires opening N dashboards, one per client, and mentally aggregating the results -- which nobody actually does consistently at 6 AM.

The health table solves this by becoming the unified monitoring layer above the individual orchestrators. Every instance appends to the same destination table after each run, partitioned by the `client` column, and the morning routine works against a single query across all clients rather than N separate UIs. Staleness reports, reconciliation checks, and cost rollups all aggregate naturally because they share a schema.

This setup also unlocks cross-client comparison, which individual orchestrator dashboards can never provide. If `orders` extraction takes 3 minutes for client A but 25 minutes for client B on the same schema version, the health table surfaces that in a single query -- and the cause is usually environmental (client B's source database is underpowered, or their `orders` table is 10x larger, or their network path to the extraction server adds latency). Same for schema drift: if client B's source adds a column that client A doesn't have, `schema_json` catches the divergence immediately, which matters when both clients are supposed to be running the same ERP version.

#ecl-warning("Central health table is a dependency")[If the health destination is unreachable, every pipeline run across all clients loses its monitoring write. A local fallback -- writing the health row to a staging table in each client's own destination, then syncing centrally on a schedule -- mitigates this at the cost of slight staleness in the unified view. At minimum, the health INSERT should log to stderr on failure so the orchestrator's native run output still captures what happened, even if the health table doesn't.]

== Tradeoffs
<tradeoffs>
#figure(
  align(center)[#table(
    columns: (47.14%, 52.86%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Catches silent failures that pass/fail misses], [Storage cost grows linearly with table count and run frequency],
    [Health table provides a single queryable history], [Requires discipline to populate on every run, including failures],
    [Raw measurements let you derive new metrics without schema changes], [STRING columns (`query_used`, `schema_json`) can dominate storage at scale],
    [Works across orchestrators in a cluster setup], [Adds write latency to every pipeline run (one INSERT per table per run)],
  )]
  , kind: table
  )

// ---

= Cost Monitoring
<cost-monitoring>
#quote(block: true)[
#strong[One-liner:] Per-table, per-query, per-consumer -- know where the money goes before the invoice arrives.
]

== The Problem
<the-problem-2>
Cloud data warehouses bill by bytes scanned, slots consumed, or storage volume -- and the bill arrives after the damage is done. A single bad pattern can dominate the monthly invoice: an unpartitioned scan that reads the entire table on every query, a MERGE on a table that should be a full replace, or a staging dataset nobody cleaned up accumulating for months. You won't know which one it was until you can attribute cost to individual tables and operations.

Without per-table cost attribution, "costs went up 40%" is a mystery that sends you guessing. With it, you can point at the exact table and the exact operation that caused the spike -- and decide whether to fix the pattern, reduce the schedule frequency, or accept the cost because the freshness justifies it.

== The Pattern
<the-pattern-2>
Cost monitoring extends the health table from 0602. The health table captures `extraction_seconds`, `load_seconds`, and `bytes_extracted` per run -- time and volume metrics that tell you where the pipeline spends effort. But destination-side cost (bytes scanned, slots consumed, DML pricing) lives in the destination's own audit logs, and the connection between the two is the job label or run ID you attach to each load operation.

=== What to Track
<what-to-track>
#strong[Compute costs] are the volatile dimension. Track bytes scanned per load operation (the MERGE, the DELETE+INSERT, the partition swap), and if your engine charges by slots or query-seconds, track those too. The same data supports three useful aggregations: cost per run (anomaly detection), cost per table (pattern decisions), and cost per schedule (budgeting). MERGE deserves special attention because it's the single most expensive load operation in columnar engines -- a MERGE-heavy pipeline loading hundreds of tables #strong[can cost an order of magnitude more] than the same tables loaded via partition swap or append-and-materialize, and the difference only shows up in the bill, not in run duration.

#strong[Storage costs] are predictable but sneaky at scale. Append logs grow with every run, and without compaction that growth is unbounded (0404 covers compaction). Staging tables that outlive their load job are dead weight, though in practice orphaned staging is more of a schema hygiene issue than a cost problem -- at \~\$0.02/GB/month in BigQuery, a few hundred GB of staging is annoying but not alarming. The compute cost of accidentally querying unpartitioned staging is usually worse than storing it.

#strong[Extraction costs] are easy to forget because querying your own PostgreSQL is free. But some sources meter reads: API rate limits, licensed query slots (SAP HANA, some SaaS platforms), or egress charges from cloud-hosted sources. Also sometimes extracting over VPNs incurs in bandwidth costs. Overlapping extraction windows in stateless patterns (0303) re-extract the same rows deliberately -- the overlap is correct, but its cost in time and source-side load should be visible and known

=== Cost Attribution
<cost-attribution>
The gap between "the pipeline costs \$X/month" and "table Y's MERGE costs \$X/month" is a join key: pipeline metadata (table name, run ID, schedule) matched against the destination's query audit log (bytes scanned, cost, duration). Without that join, you're stuck with aggregates that don't point anywhere useful.

Most columnar engines expose per-query cost through their information schema. BigQuery's `INFORMATION_SCHEMA.JOBS` tracks bytes processed, slot-milliseconds, and the destination table for every DML operation. Snowflake's `QUERY_HISTORY` provides similar detail. The per-table cost report is a straightforward aggregation:

```sql
-- destination: bigquery
-- Top 20 most expensive destination tables, last 30 days.
-- BigQuery on-demand: $6.25/TB scanned. Adjust for your pricing model.
SELECT
  destination_table.table_id AS table_id,
  COUNT(*) AS load_ops,
  ROUND(SUM(total_bytes_processed) / POW(1024, 3), 2) AS gb_scanned,
  ROUND(SUM(total_bytes_processed) / POW(1024, 4) * 6.25, 2) AS est_cost_usd
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
GROUP BY table_id
ORDER BY est_cost_usd DESC
LIMIT 20;
```

For richer attribution -- by schedule, by client, by run ID -- tag each load query with job labels through your orchestrator or pipeline wrapper. These labels appear in the audit log and make the join trivial. If your orchestrator doesn't support job labels natively, a query comment (`-- table=orders run_id=20260323-001`) is a fallback that's parseable with `REGEXP_EXTRACT`.

=== The Drilldown Workflow
<the-drilldown-workflow>
Track at maximum granularity -- per table, per run, per operation -- but check at low frequency. A weekly or monthly glance at the aggregate total is enough. If the aggregate doesn't trigger your curiosity, the individual tables are fine. Drill down when the total spikes, not as a daily ritual.

The practical workflow is top-down: aggregate cost in a dashboard or a weekly scheduled query, filter by table when the number looks off, check whether the pattern changed, the table grew, or the schedule frequency increased. A report showing the top-10 most expensive tables gives you enough signal for most months. #strong[Cost monitoring should cost less attention than it saves money.]

#ecl-warning("Test pattern changes at small scale")[If you're switching a table's load strategy to reduce cost -- MERGE to append-and-materialize, full replace to incremental -- try it on a small table first, then a large one, then roll it out broadly. Measure the actual cost difference before committing to the migration. The estimated savings from theory and the actual savings in production are rarely the same number.]

== The Expensive Patterns
<the-expensive-patterns>
#figure(
  align(center)[#table(
    columns: (13.57%, 23.62%, 62.81%),
    align: (auto,auto,auto,),
    table.header([Pattern], [Why it's expensive], [Mitigation],),
    table.hline(),
    [MERGE on large tables], [Full scan of both source and destination sides], [Partition-scoped merge, or switch to 0404],
    [Unpartitioned full scan], [Every query reads the entire table], [Partition by date, enforce `require_partition_filter` (0704)],
    [Staging cleanup missed], [Orphaned staging datasets accumulate storage], [Scheduled cleanup job, weekly or after each run],
    [Append log without compaction], [Storage grows linearly with schedule frequency], [Periodic compaction to latest-only (0404)],
  )]
  , kind: table
  )

== Tradeoffs
<tradeoffs-1>
#figure(
  align(center)[#table(
    columns: (48.32%, 51.68%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Per-table attribution turns "costs went up" into an actionable lead], [Requires tagging every load query with metadata (job labels, query comments)],
    [Aggregated view catches spikes without daily effort], [Destination audit logs have retention limits -- store summaries for history],
    [Identifies which patterns to optimize first], [Cost optimization can lead to premature complexity if it drives pattern choice],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-1>
#ecl-danger("Don't let cost drive pattern selection")[Switching from full replace to incremental to \"save money\" introduces complexity that costs engineering time and creates failure modes (see 0108). The cheaper pipeline is the one that breaks less, not the one with the lowest bytes-scanned number. Pick the pattern that's correct first, then optimize its cost within that pattern.]

#ecl-warning("Don't optimize without measuring")[\"MERGE is expensive\" is true in general, but _how_ expensive depends on table size, partition layout, and update volume. A MERGE on a 10k-row lookup table costs fractions of a cent -- switching it to append-and-materialize for cost reasons adds complexity with no real savings. Measure the actual cost per table before deciding anything is worth changing.]

== What Comes Next
<what-comes-next-1>
Cost is one input to the freshness decision. 0604 defines #emph[when] data must be fresh; 0608 uses cost as one factor in deciding which tables earn high-frequency schedules and which ones run daily.

// ---

= SLA Management
<sla-management>
#quote(block: true)[
#strong[One-liner:] "The data must be fresh by 8am" -- how to define, measure, and enforce freshness commitments.
]

== The Problem
<the-problem-3>
Stakeholders care about one thing: is the data fresh when they need it? Without an explicit SLA, freshness expectations are implicit -- discovered only when violated, usually via an angry email or an angry call from your boss. A pipeline that finishes at 8:15 AM is fine until someone builds a report that refreshes at 8:00 AM, and now you have an SLA you didn't know about.

We had a client who we #emph[told] -- but didn't write down -- that data updated once daily. They built automated collection emails that fired before midday, but most of their customers had already paid by then. The emails were going out with stale receivables data, and the client blamed the pipeline for the embarrassment. The fix wasn't technical -- it was documenting the SLA in the contract so both sides agreed on what "once daily" actually meant: data reflects the previous night's extraction, available by 9 AM, not refreshed throughout the day. #strong[Everything that isn't written down can be reinterpreted against you.] Document the SLA.

== Defining an SLA
<defining-an-sla>
=== What an SLA Contains
<what-an-sla-contains>
An SLA for a data pipeline is four numbers and a signature:

#figure(
  align(center)[#table(
    columns: (25%, 75%),
    align: (auto,auto,),
    table.header([Component], [Example],),
    table.hline(),
    [#strong[Table or group];], [`orders`, `order_lines`, `invoices` -- the receivables group],
    [#strong[Freshness target];], [Data reflects source state as of no more than 24 hours ago],
    [#strong[Deadline];], [Available in the destination by 09:00 UTC-3],
    [#strong[Measurement point];], [Last successful load timestamp in the health table, not run start],
  )]
  , kind: table
  )

The measurement point matters. A run that starts at 7 AM but fails and retries until 8:45 AM doesn't meet a 9 AM SLA -- it barely makes it, and the next slow day it won't. Measure from `MAX(extracted_at) WHERE status = 'SUCCESS'` in the health table (0602), not from when the orchestrator kicked off the job.

=== SLA Tiers
<sla-tiers>
Not every table deserves the same freshness. `metrics_daily` refreshed once a day has a different SLA than `orders` refreshed every 15 minutes or a balance sheet refreshed monthly. Group tables by consumer urgency, not by source system -- the tables that most often need more than daily are sales data (especially during Black Friday or seasonal peaks), receivables (for end-of-month collection runs), and inventory stock levels (for in-store availability decisions). Everything else is usually fine at daily.

Daily is the best default. It handles the vast majority of use cases, and the contract should say so explicitly: no more than one scheduled update per day, data reflects the previous night's extraction. When you increase frequency for specific tables -- an extra midday refresh for receivables, intraday incremental for sales -- make it clear in writing that the increased cadence is outside the base SLA and can be adjusted at any time. This matters because ad-hoc refreshes have a way of becoming expected commitments: you give a consumer an extra midday refresh as a favor, they build a process around it, and now you have an SLA you never agreed to. Give consumers `_extracted_at` in their reports (0501) so they always know how fresh the data actually is, rather than assuming.

#ecl-warning("On-demand refreshes replace high-frequency schedules")[If a consumer needs fresh data once or twice a day at unpredictable times, an on-demand refresh button that triggers the pipeline is often better than scheduling loads every 30 minutes \"just in case.\" One triggered run costs far less than 48 idle runs per day, and the consumer gets exactly-when-needed freshness instead of at-most-30-minutes-stale. On-demand #emph[can] be part of the SLA ("consumer may trigger up to N refreshes per day"), but keep it bounded -- without a cap, a trigger-happy user can spam refreshes and compete with scheduled runs for source connections and orchestrator slots. Document the limit, enforce it with a cooldown or queue, and monitor trigger frequency in the health table.]

== Measuring Freshness
<measuring-freshness>
Staleness is the gap between now and the last successful load. The health table gives you this with a single query:

```sql
-- destination: bigquery
-- Freshness report: staleness per table against declared SLA thresholds.
WITH last_success AS (
  SELECT
    table_id,
    MAX(extracted_at) AS last_load,
    TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(), MAX(extracted_at), HOUR
    ) AS staleness_hours
  FROM health.runs
  WHERE status = 'SUCCESS'
  GROUP BY table_id
)
SELECT
  ls.table_id,
  ls.last_load,
  ls.staleness_hours,
  sla.freshness_hours,
  CASE
    WHEN ls.staleness_hours > sla.freshness_hours THEN 'BREACH'
    WHEN ls.staleness_hours > sla.freshness_hours * 0.8 THEN 'WARNING'
    ELSE 'OK'
  END AS sla_status
FROM last_success ls
JOIN health.sla_config sla USING (table_id)
ORDER BY sla_status DESC, staleness_hours DESC;
```

The `sla_config` table is a simple lookup: one row per table or table group, with the `freshness_hours` threshold from the SLA. Hard-code it, load it from a config API, or manage it in a spreadsheet -- the mechanism doesn't matter as long as the thresholds are explicit and queryable rather than living in someone's head.

This query is the second item in the morning routine from 0601: after checking failures, check which tables are stale beyond their SLA.

== What Erodes SLAs
<what-erodes-slas>
#strong[Upstream delays] are the most common cause and the hardest to control. ERP systems run their own batch jobs -- posting runs, period closes, nightly aggregation -- and those jobs determine when your source data is ready to extract. The ERP itself is rarely the problem; it's the people operating it. When a client has a technical team that runs ad-hoc processes or overloads the database during the window they designated to you, you're the one who gets blamed for stale data. #strong[Build buffer into the SLA] for exactly this -- if the source is ready by 7 AM on a good day, don't promise 7:30 AM.

#strong[Extraction duration creep] turns a comfortable SLA into a tight one over months. The health table's `extraction_seconds` column (0602) catches this trend before it becomes a breach -- a 3-minute extraction that silently creeps to 25 minutes eats into your buffer without anyone noticing until the SLA breaks.

```
Example line graph, X axis is time (last 30 days), Y axis is max staleness (measured as distance from last successful timestamp to SLA)

Have a static line on Y axis representing max tolerated staleness (24 hours) and a line that grows past it.

Something LIKE that, think about a table that updates once daily starting at 8 to end at 9, with SLA at 930. and it exceeds it, maybe Y axis should be different.
```

#strong[Stale joins at consumption] are the subtler freshness problem. `orders` and `order_lines` can extract and load independently -- there's no dependency between them at load time. But if only one of the two refreshes on a given run, consumers joining them will see orphan records: order lines pointing at a non-existent order header, or a refreshed header missing today's new lines. The SLA for header-detail pairs should cover both tables on the same schedule, not because the pipeline requires it, but because the consumer's query does (0606).

#strong[Backfills that steal capacity] from scheduled runs are a less obvious risk. A 6-month backfill running alongside production extractions competes for source connections, orchestrator slots, and destination DML quota (0611).

== SLA Breach Response
<sla-breach-response>
#figure(
  align(center)[#table(
    columns: (11.86%, 19.21%, 68.93%),
    align: (auto,auto,auto,),
    table.header([Severity], [Trigger], [Action],),
    table.hline(),
    [#strong[Warning];], [Staleness \> 80% of SLA window], [Increase priority of next scheduled run; investigate if it's a trend],
    [#strong[Breach];], [Staleness \> SLA window], [Alert via 0605, investigate root cause, notify consumers],
    [#strong[Sustained breach];], [Multiple consecutive violations], [Escalate -- the schedule, the pattern, or the SLA itself needs to change],
  )]
  , kind: table
  )

A single breach is an incident. Sustained breaches mean the SLA is wrong -- either the pipeline can't deliver what was promised, or the consumer's actual needs have shifted. Renegotiate the SLA rather than patching around it with increasingly fragile workarounds.

== Tradeoffs
<tradeoffs-2>
#figure(
  align(center)[#table(
    columns: (47.97%, 52.03%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Explicit SLAs set expectations before they're violated], [Requires upfront agreement with stakeholders],
    [Staleness query catches breaches before consumers notice], [Only measures load completion, not data correctness],
    [Tiered SLAs avoid over-engineering low-priority tables], [More tiers means more schedules and more monitoring surface],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-2>
#ecl-danger("Don't promise SLAs you can't control")[If your pipeline depends on a source system batch job that finishes \"sometime between 5 AM and 7 AM,\" your SLA cannot be 7:30 AM. Build buffer or set the SLA at 9 AM and be honest about it. A missed SLA erodes trust in the pipeline and in you -- a conservative SLA that's always met builds more credibility than an aggressive one that breaks monthly.]

#ecl-warning("Don't confuse desire with willingness to pay")[We had a client who wanted 15-minute maximum delay on their invoicing data. They weren't willing to pay the increased BigQuery bill, and their source had terrible metadata, hard deletes, and no reliable cursor -- making high-frequency extraction expensive to build and expensive to run. After scoping the effort and cost, they realized all they actually needed was one extra on-demand refresh per day. The Head of Sales wanted fresh numbers on his dashboard mid-morning, and a refresh button that triggered the pipeline solved the problem at a fraction of the cost and complexity. Ask what decision the freshness enables before engineering the SLA around it.]

== What Comes Next
<what-comes-next-2>
0605 covers the mechanics of turning SLA breaches into alerts -- the thresholds defined here are the input, and 0605 decides who gets paged, how, and at what severity.

// ---

= Alerting and Notifications
<alerting-and-notifications>
#quote(block: true)[
#strong[One-liner:] Schema drift, row count drops, partial failures -- calibrate severity so not everything is an incident.
]

== The Problem
<the-problem-4>
Pipelines fail silently. Zero rows extracted successfully, schema changed upstream, row counts drifting apart between source and destination -- all of these can happen while the orchestrator reports SUCCESS. The monitoring layer from 0601 and the health table from 0602 capture these signals; this pattern is about deciding which of them deserve to wake someone up.

The calibration problem has two failure modes. 1. Too many alerts -- every run sends a notification, every minor discrepancy triggers a warning -- produces alert fatigue, and alert fatigue produces ignored alerts, and ignored alerts produce missed failures. 2. Too few alerts -- only page on total outages -- means silent data loss accumulates for days before anyone notices. \
The goal is a narrow band between the two: alert on conditions that require human attention, monitor everything else on the dashboard. Your pipelines should be loud, so that you can rest comfortably when there is silence.

== Severity Calibration
<severity-calibration>
Not every failure is equally urgent, and not every table is equally important. A load failure on `orders` during month-end close is a different severity than a stale `item_groups` lookup table on a Saturday. Calibrate on two axes: what broke and how much the table matters.

#figure(
  align(center)[#table(
    columns: (6.42%, 31.55%, 62.03%),
    align: (auto,auto,auto,),
    table.header([Severity], [Condition], [Example],),
    table.hline(),
    [#strong[Critical];], [Destination data lost or significantly diverged from source], [Table empty after load, row count dropped 80%, source/destination totals diverged beyond recovery],
    [#strong[Error];], [Load failed, destination stale, SLA breach], [Permission denied, query timeout, staleness exceeds SLA from 0604],
    [#strong[Warning];], [Anomaly detected but data is present and current], [Row count drop \> threshold, schema drift (new columns), extraction duration 3x historical average],
    [#strong[Info];], [Nothing wrong], [Successful run, counts in range, no drift. Log it, dashboard it, never notify],
  )]
  , kind: table
  )

Table importance is the second axis. Sales and receivables tables failing during end-of-month is critical; a dimension lookup table being 2 hours stale is a warning at most. Classify tables into importance tiers and let the combination of condition severity and table importance determine the alert routing -- a WARNING on a critical table might route the same as an ERROR on a low-priority one.

== What to Alert On
<what-to-alert-on>
The rule: alert on things that need human attention before the next morning's monitoring review. At scale -- thousands of tables -- you can't afford to alert on every condition the pipeline doesn't handle automatically, because there are too many tables where a failure simply doesn't matter overnight. A warehouse dimension table that gets a new row every six months doesn't need to page anyone when it fails on a Tuesday; it'll still be there in the morning. The filter is urgency, not just "unhandled."

If the pipeline already has a pattern that resolves the condition -- retry logic, automatic schema evolution, reconciliation with auto-recovery -- the alert is redundant. Monitor it, log it, but don't page on it. And if the pipeline #emph[doesn't] handle it but the table can wait, that's a dashboard item, not a notification.

=== Always Alert
<always-alert>
These are conditions where waiting until morning costs you something real -- data loss that compounds, costs that keep burning, or downstream consumers already seeing wrong results. Even here, table importance matters: a load failure on `orders` during month-end close is a page, the same failure on a warehouse lookup table is a line on tomorrow's dashboard.

#strong[Data didn't arrive and it matters now] -- load failure (quota exceeded, permission revoked, timeout) or extraction error on a table that was healthy yesterday. The distinction between "load rejected" and "source query failed" matters for triage but not for urgency -- either way, the destination is stale and nothing will fix it automatically. The health table's `status = 'FAILED'` with `error_message` gives you the starting point. Don't confuse extraction errors with "returned 0 rows," which can be normal for quiet incrementals (0610).

#strong[SLA breach on a table with consumers waiting] -- staleness exceeds the threshold defined in 0604, and duration is trending in the same direction. A breach means someone downstream is already affected or about to be; check whether it's duration creep, an upstream delay, or a schedule that needs adjustment. Duration anomalies that haven't breached an SLA yet are an early warning -- worth surfacing as a warning, not a page, unless the trajectory makes the breach inevitable.

#strong[Partial failure across a dependency group] -- some tables loaded, others didn't, and the successful ones depend on the failed ones or vice versa. This is particularly dangerous because the overall run may report partial success and fly under the radar (0612). Isolated failures on independent tables can wait for morning; failures that leave the destination in an inconsistent state can't.

#strong[Cost spike] -- daily compute cost exceeds threshold (0603). A runaway MERGE or an unpartitioned scan keeps burning money every run until someone intervenes, so this is one of the few conditions where urgency is about the pipeline itself rather than the data.

=== Alert Only When Unhandled
<alert-only-when-unhandled>
These conditions may or may not need attention depending on two filters: whether the pipeline has automatic recovery, and whether the table's importance justifies a notification over a dashboard entry.

#strong[Row count deviation] -- if the table uses hard-delete detection (0306) or reconciliation with auto-recovery, the pipeline handles it. Alert when the discrepancy exceeds the threshold #emph[and] no automatic pattern resolves it (0614). On low-importance tables, even an unhandled deviation can wait for the morning review.

#strong[Schema drift] is nuanced. New columns with an `evolve` policy are accepted automatically -- log them, don't alert. Dropped columns deserve an alert even with `evolve`, because a missing column can break downstream queries silently and an `evolve` policy should reject column removal anyway. Type changes depend on direction: widening (INT → BIGINT) is usually safe; narrowing or type-class changes (INT → VARCHAR) are probably a problem. See 0609 for the policy framework.

=== Never Alert
<never-alert>
#strong[Successful runs.] Log them, put them on the dashboard, never send a notification. If you get a "success" message for every table on every run, you'll have hundreds of Slack messages per day and you'll stop reading any of them.

#strong[Zero rows on an incremental] -- quiet periods are normal. The cursor is caught up or the source had no changes. This is a data health metric in the health table, not an alert condition.

#strong[Minor reconciliation discrepancies] within the configured threshold -- a 0.05% drift on a busy table is likely to be fixed next run, don't alert but keep it in mind in your dashboard.

#strong[Failures on tables that can wait] -- a warehouse dimension table that gets a new row every six months, a lookup table with no downstream SLA, a staging table for a report that runs weekly. These are real failures that need fixing, but they're morning-coffee problems, not pager problems. The dashboard and health table surface them; a notification adds nothing but noise.

== Alert Channels
<alert-channels>
Route by severity, not by table. Critical alerts go to the pager or a DM -- something that demands immediate attention. Warnings go to a Slack channel where they're visible but not intrusive. Info stays on the dashboard where it's available on demand but never pushes a notification.

Your orchestrator's alerting layer handles the routing -- configure severity-based rules, not per-table rules. If you find yourself managing per-table routing for more than a handful of exceptions, the severity classification isn't doing its job.

Every alert should tell the responder what to do next -- or at least where to look. "Row count anomaly on `events`" is not actionable; the person reading it doesn't know if the anomaly is a 5% dip or a 90% drop, whether it's expected (month-end spike subsiding) or a real problem, or who should investigate. Include the metric value, the threshold it crossed, and a pointer to the relevant health table query or dashboard view. An alert that doesn't guide triage is just noise with a timestamp.

#ecl-warning("Pre-filter before you fix")[When multiple tables fail overnight, resist the urge to investigate all of them at once. Filter to critical failures first, fix those, then work down to warnings. A critical failure on `orders` that blocks month-end reporting matters more than a warning on `products` with a new column. If you try to process every alert in arrival order, the important ones get buried and you burn your morning on problems that could have waited.]

== Tradeoffs
<tradeoffs-3>
#figure(
  align(center)[#table(
    columns: (37.82%, 62.18%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Severity tiers prevent alert fatigue], [Requires upfront classification of tables and conditions],
    ["Alert only when unhandled" reduces noise], [Under-alerting is a real risk if the automatic recovery pattern has a bug],
    [Channel routing keeps critical alerts visible], [Warning thresholds need periodic tuning as tables grow and patterns change],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-3>
#ecl-danger("Don't use one severity for everything")[Schema drift on a lookup table and a total load failure on `orders` are not the same event. If everything is "Error," nothing is -- the on-call engineer can't prioritize and will eventually stop responding to any of them.]

#ecl-warning("Don't alert without an escalation path")[A warning that persists for 3 consecutive days is no longer a warning -- it's either a real problem being ignored or a miscalibrated threshold. Build automatic severity promotion: warning → error after N consecutive violations. If a threshold triggers daily and nobody investigates, the threshold is wrong, not the data.]

== What Comes Next
<what-comes-next-3>
0606 covers the scheduling layer that determines when pipelines run and in what order -- the timing decisions that directly affect whether SLAs from 0604 are achievable and which alert conditions fire.

// ---

= Scheduling and Dependencies
<scheduling-and-dependencies>
#quote(block: true)[
#strong[One-liner:] Most tables are independent. For the ones that aren't, group them so they update together -- but don't enforce strict ordering unless you have a real reason.
]

== The Problem
<the-problem-5>
With a handful of tables, scheduling is simple: run everything on a cron, wait for it to finish, done. At hundreds or thousands of tables, three questions dominate your scheduling decisions -- how often each table should update, how many extractions your source and infrastructure can handle at once, and which tables need to land in the same window. Get any of these wrong and the consequences are immediate: SLA breaches because heavy tables crowd out critical ones, angry DBAs because you're hammering their production system during business hours, or a pipeline that takes six hours because someone chained 200 independent tables into a single sequence years ago and nobody questioned it.

== The Pattern
<the-pattern-3>
=== How Often: Schedule Frequency
<how-often-schedule-frequency>
Every table needs a schedule, and the schedule should reflect how the table is consumed, not how often the source changes. A `customers` table that changes ten times a day but feeds a weekly report doesn't need hourly extraction -- once a day is fine. An `orders` table that feeds a real-time dashboard needs to update as frequently as your source and infrastructure can sustain. Watch for schedule pile-ups as tables grow: an extraction that used to finish in 10 minutes may creep to 40 and start overlapping with the next scheduled run, silently turning two clean windows into one messy one.

0608 covers the framework for assigning freshness tiers. The scheduling implication is straightforward: group tables by the freshness their consumers need, not by their source system or their size.

Most teams evolve through a predictable sequence:

+ #strong[Single cron] -- everything runs together. Simple but slow, works when you have few tables and falls apart when it takes longer than \~4 hrs or you need to update within the day.
+ #strong[Weight-based groups] -- tables split by size or duration, distributed across time slots. Better throughput, but the groupings don't map to anything the business cares about.
+ #strong[Consumer-driven groups] -- tables grouped by the downstream report or dashboard that consumes them, scheduled to meet that consumer's freshness target. If the sales report goes live at 8 AM, its tables update at 6:30. If the warehouse team doesn't check inventory until noon, those tables can run later and spread the source load across a wider window.

The third stage is where you want to end up, but each stage is the right answer at a certain scale -- don't over-engineer a consumer-driven architecture when you have three dashboards.

#ecl-warning("Group by consumer, not by source")[Early designs tend to group tables by source connection -- \"all SAP tables run at midnight.\" That works until the finance team needs invoice data at 7 AM while the warehouse team doesn't check inventory until noon. Grouping by consumer lets you schedule tighter windows for the tables that matter most and spread the rest across off-peak hours.]

=== How Many: Concurrency and Source Load
<how-many-concurrency-and-source-load>
Every concurrent extraction consumes RAM and CPU on your pipeline infrastructure #emph[and] an open connection plus query load on the source. Getting the concurrency level wrong hurts in both directions: too few concurrent extractions and your pipeline takes hours longer than it should, too many and you overload the source system or exhaust your own memory.

Start conservative -- 3 to 5 concurrent extractions per source for a typical transactional database. The beefiest production setups might run up to 8 tables concurrently against a strong source, but mostly during off-peak hours when the source has headroom. Monitor source response times and pipeline memory, and increase the limit only when you have evidence that both sides can handle it.

The mechanism is your orchestrator's concurrency controls -- run queues, tag-based limits, or pool-based workers. The limit itself comes from knowing your environment: what the source can tolerate and what your infrastructure can sustain.

#ecl-tip("Set concurrency per source, not per pipeline")[Concurrency limits should be set per source system, not per schedule. If three schedules each run 5 extractions against the same database, that's 15 concurrent queries -- the source doesn't care that they came from different schedules. However, always keep in mind your orchestrator's limit as a general maximum of available operations.]

#ecl-warning("Lock contention on SQL Server")[Some databases handle concurrent reads worse than others. SQL Server in particular can lock tables during long reads, blocking the source application's writes. The usual workaround is `WITH (NOLOCK)`, which avoids locks but introduces dirty reads -- rows mid-transaction, partially updated, or about to be rolled back. I've seen dirty reads lead to erroneous business decisions when an in-flight transaction appeared as committed data in the destination. Schedule heavy SQL Server extractions for off-peak hours rather than reaching for `NOLOCK`, and if you must use it, document the risk so downstream consumers know what they're looking at. Please de-duplicate on source, since batched loads with `NOLOCK` can repeat records even when having enforced primary keys.]

=== When: Safe Hours
<when-safe-hours>
Large extractions during business hours can slow down or even lock the source system (see 0607). Gate heavy extractions behind a safe-hours window -- typically off-peak, like 19:00 to 06:00 -- with a row-count or size threshold that determines which tables qualify as "heavy." Tables below the threshold run during business hours on their normal schedule; tables above it get deferred to the safe window automatically.

A threshold around 100,000 rows is a reasonable starting point, set proactively before an incident forces the decision. The exact number depends on the source -- a well-provisioned cloud database tolerates larger reads during business hours than an on-prem ERP running on aging hardware.

#ecl-warning("Safe hours are per source")[If three pipelines each respect their own safe-hours window against the same source, they might all stack into the same off-peak slot. Coordinate safe hours at the source level: one window, one concurrency limit, shared across every pipeline that touches that source.]

=== Which Together: Grouping Related Tables
<which-together-grouping-related-tables>
Most tables in a pipeline are independent -- `customers` and `events` share no relationship that affects extraction, and there's no reason they need to land at the same time. The few that #emph[are] related -- header-detail pairs like `orders`/`order_lines` or `invoices`/`invoice_lines` -- should land in the same schedule window so the destination doesn't show today's headers with yesterday's lines.

Within that window, arrival order shouldn't matter. Make sure no table depends on the other's data being present at load time so that joins work regardless of which side finished first. What matters is that both sides reflect roughly the same point in time, which co-scheduling achieves naturally without any dependency graph.

Lookup tables like `customers` and `products` ideally land before `orders` so a consumer querying right after the load sees consistent references, but if `products` is 30 minutes stale while `orders` is fresh, the join still works -- the data is slightly behind, not broken. Express this as a preferred ordering in your orchestrator if it supports it, but don't block `orders` on `products` completing unless you want slower loads.

The only time you need strict ordering is when one extraction's #emph[input] depends on another extraction's #emph[output] -- which is uncommon in ECL because each table is extracted independently from the source. If you do have this case, express it as a real dependency in the orchestrator's DAG, but confirm you actually need it before building the graph.

=== DAG vs.~Schedule Groups
<dag-vs.-schedule-groups>
For the vast majority of table relationships, co-scheduling is enough: put related tables on the same cron, let them run concurrently within the window, done. No dependency graph, no ordering logic.

Reserve DAG-based dependencies for actual extraction-feeds-extraction cases or for coordinating with downstream transformations that must wait for a group of tables to complete. Building a 200-node extraction DAG when 190 of those nodes are independent is complexity that buys nothing -- and a fragile DAG where one table's failure cascades into blocking dozens of unrelated tables is worse than no DAG at all.

If your orchestrator can't group tables into a single schedule that runs them concurrently, that's a serious limitation -- grouping related tables for parallel extraction within a window is a basic scheduling requirement, and working around it with cron offsets (`orders` at 6:00, `order_lines` at 6:15) is fragile enough that it should push you toward a better orchestrator rather than deeper into workarounds.

== Tradeoffs
<tradeoffs-4>
#figure(
  align(center)[#table(
    columns: (42.24%, 57.76%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Schedule groups keep related tables coherent without strict ordering], [Consumers may briefly see one side of a relationship fresher than the other within the window],
    [Consumer-driven grouping aligns freshness with business needs], [Tables needed by multiple consumers may run on multiple schedules, increasing source load],
    [Conservative concurrency limits protect the source], [Lower concurrency means longer total pipeline duration],
    [Safe-hours gating prevents source impact during business hours], [Heavy tables only update during the off-peak window, which may not meet freshness SLAs],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-4>
#ecl-warning("Don't serialize everything")[Running 200 tables sequentially \"just to be safe\" because \"it's simpler\" turns a 30-minute pipeline into a 6-hour one. Most tables are independent and can run concurrently within your concurrency limits -- group the few that are related, and only add explicit dependencies when one extraction actually needs another's output.]

#ecl-danger("Don't model FKs as extraction dependencies")[Source tables have foreign keys; that doesn't mean your extraction needs to respect their ordering. The destination's landing layer doesn't enforce FKs, and joins work regardless of which side arrived first. Treating every FK as a hard dependency turns simple co-scheduling into a fragile DAG that blocks unrelated tables on each other.]

#ecl-warning("Don't use sleep as a dependency")[\"Wait 10 minutes for orders to finish\" is a guess that breaks the first time extraction duration changes. Use schedule groups or the orchestrator's native dependency graph.]

#ecl-danger("Don't assume your limit is theirs")[Your orchestrator might allow 20 parallel tasks, but the on-prem database you're extracting from might buckle under 8. The constraint is always the weakest link -- your infrastructure #emph[or] the source, whichever gives first. Test against the actual source before increasing limits.]

== Related Patterns
<related-patterns>
- 0604-sla-management -- schedule design directly affects whether SLAs are achievable
- 0607-source-system-etiquette -- concurrency limits and safe hours protect the source
- 0608-tiered-freshness -- different freshness tiers drive different schedule groups
- 0308-detail-without-timestamp -- header-detail extraction strategy when the detail table has no cursor

== What Comes Next
<what-comes-next-4>
0607 covers the source side of the equation in depth -- what your pipeline does to the database it reads from, and how to keep your access when extracting thousands of tables on a schedule.

// ---

= Source System Etiquette
<source-system-etiquette>
#quote(block: true)[
#strong[One-liner:] Your pipeline is a guest on someone else's production database. Act like it.
]

== The Problem
<the-problem-6>
READ-ONLY access doesn't mean zero impact. A full table scan on a 50-million-row table locks pages, consumes I/O, and competes with the application for CPU and memory -- and the DBA watching the monitoring dashboard doesn't care that your query is a harmless SELECT. Their job is to keep the application fast for the users who generate revenue; your extraction is a background process that, from their perspective, exists only to slow things down. If you're careless about when and how you extract, you'll lose access -- and if you're unlucky, you'll bring the database down on your way out.

I had a client whose IT team didn't mention they ran full database backups between 5 and 6 AM. A load failed overnight, and the automatic retry kicked in at 5:30 AM -- right on top of the backup window. The database went down. It was back up within the hour, but the conversation about revoking our access lasted a week. The mistake wasn't the retry logic; it was not knowing the source's maintenance windows.

== Know Your Source
<know-your-source>
The sensitivity of a source system determines how carefully you need to tread. Before writing the first extraction query, understand what you're connecting to:

#strong[Production OLTP] -- a live transactional database serving the application's users. Every query competes with their transactions. Full scans lock pages, long reads block writes on some engines (see the SQL Server warning in 0606), and a bad retry at the wrong time can cascade into an outage. Treat these with maximum care: off-peak scheduling, conservative concurrency, explicit timeouts.

#strong[Read replica] -- lower sensitivity, but not zero. Replicas share storage I/O with the primary or lag behind it on the same hardware. A full scan on a replica can saturate disk throughput, increase replication lag, and degrade the primary indirectly. Treat replicas with the same patterns, just wider tolerances -- more concurrent queries, wider safe-hours windows.

#strong[Vendor-controlled ERP] -- systems like SAP where the vendor owns the schema and you have no leverage to change it. You can't add indexes, you can't create views, and the timestamp columns your incremental queries need were designed for the application's audit trail, not for your `WHERE` clause. Tread carefully and accept that some extractions will be slower than you'd like.

== The Pattern
<the-pattern-4>
=== Check Your Cursor Columns
<check-your-cursor-columns>
`updated_at`, `UpdateDate`, `CreateDate` -- the columns your incremental queries filter on exist for the application, not for your extraction. Check whether they're indexed before assuming your `WHERE updated_at > :cursor` will be fast. If they're not indexed, you're forcing a full table scan every run, and the DBA will notice before you do.

Ask the DBA to add an index. This is more achievable than it sounds -- adding an index on a timestamp column is a low-risk change that benefits anyone querying by date, and technical stakeholders on the source side often stand to gain from it too. We've had clients proactively add indexes after noticing our scans were slow, before we even asked. It's a soft rule -- officially read-only, but the performance improvement is large enough that most DBAs will cooperate.

If they can't add an index -- vendor-controlled schemas sometimes make this difficult or unsupported -- schedule those extractions for off-peak hours and accept that the scan will be heavier than ideal (see 0606, safe hours).

=== Respect Business Hours
<respect-business-hours>
Whether extraction load during business hours is a problem depends entirely on the database and the client. Some clients proactively ask for intraday updates and are willing to absorb the source load. Others will escalate immediately if they see any query from your pipeline during working hours. This is a conversation for the SLA stage (see 0604) -- agree on what hours are acceptable before the pipeline goes live, not after the first complaint.

As a baseline: small incremental pulls during business hours are usually fine on a healthy source, because the query filters on a recent cursor and touches a small number of rows. Full table scans and backfills are a different story -- they read the entire table and should be gated behind a safe-hours window. Enforce this automatically for very weak or very massive databases by deferring tables above a row-count threshold to the off-peak window (see 0606). Sources in the middle ground -- decent hardware, moderate table sizes -- generally don't need the gate.

#ecl-warning("Know the source's maintenance windows")[Backup jobs, index rebuilds, integrity checks -- these run during off-peak hours too, which means your \"safe\" extraction window may overlap with the source's heaviest internal workload. Ask the DBA for their maintenance schedule and avoid stacking your largest extractions on top of their backup window.]

=== Limit Concurrency
<limit-concurrency>
Multiple parallel extractions against the same source multiply the load. Cap concurrent connections per source system -- not per pipeline or per schedule, because the source doesn't care which schedule spawned the query. 3 to 5 concurrent extractions is a reasonable starting point for a typical transactional database; tune based on the DBA's feedback and the source's monitoring (see 0606 for the full treatment of parallelism tradeoffs).

=== Set Timeouts
<set-timeouts>
Set query timeouts explicitly. A query that runs for hours without a timeout is holding a connection, consuming source resources, and probably blocking something. When a query times out, fail the table explicitly (see 0610) -- don't retry immediately, because the condition that caused the timeout is likely still present.

Timeout thresholds depend on whether you're reading in batches. For unbatched reads, keep timeouts tight: a few minutes for regular tables, longer for known large ones. For batched reads (see below), individual batch timeouts can be shorter since each batch is small, while the overall extraction can run for hours.

=== Batched Reads for Massive Tables
<batched-reads-for-massive-tables>
For tables too large to extract in a single query within a reasonable time, SQLAlchemy's `yield_per()` with `stream_results=True` lets you read in batches using a server-side cursor. Each batch is small and fast -- 100,000 rows is a solid default -- even if the full read takes hours. This keeps your pipeline's memory flat (you're never holding millions of rows at once, just the current batch) and reduces the per-query impact on the source.

The tradeoff: you hold an open connection and server-side cursor for the entire duration, so the source is occupied for longer even though the per-second load is lighter. Schedule batched reads for off-peak hours, and make sure the source's connection pool can accommodate a long-lived session alongside normal application traffic.

```sql
-- Batched read: 100k rows at a time, server-side cursor
-- source: transactional
-- engine: sqlalchemy (pseudocode)

with engine.connect() as conn:
    result = conn.execution_options(
        stream_results=True
    ).execute(
        text("SELECT * FROM orders")
    )
    for batch in result.yield_per(100_000):
        load_batch(batch)
```

== What You Can and Can't Do
<what-you-can-and-cant-do>
#strong[Never];: triggers, stored procedures, temp tables, or writes of any kind on someone else's production database. You are a reader.

#strong[Schema modifications] -- officially off-limits without DBA approval, but adding an index is worth asking for. It's a low-risk change with high payoff, and framing it as a performance improvement that benefits the application (not just your pipeline) makes the conversation easier.

#strong[Views] -- useful when downstream needs a subset of data that would be expensive to reconstruct from base tables. The recommended approach: build the query in your destination first, validate it works, then send the "translated" query to the DBA and ask them to create the view on the source. This keeps the DBA in control of their schema while giving you a stable, optimized read target.

== Building Trust with the DBA
<building-trust-with-the-dba>
The relationship with the source team determines how much access you keep and how much flexibility you get. A DBA who trusts your pipeline will add indexes, extend your safe hours, and warn you before maintenance windows. A DBA who doesn't trust you will restrict your hours, throttle your connections, and eventually revoke access.

#strong[Share your schedule] -- what you extract, when, how often, how much data. No surprises.

#strong[Report your own impact] -- query duration, rows scanned, connection time. If you can show the source team that your extraction uses a small fraction of their database's capacity, you've answered the question before they ask it. The source health metrics from 0601 give you the numbers.

#strong[Own your incidents] -- when your query causes a slowdown, acknowledge it and fix the schedule before they have to ask. Nothing destroys trust faster than a DBA discovering your pipeline caused an issue and you didn't notice or didn't say anything.

== Anti-Patterns
<anti-patterns-5>
#ecl-danger("Read replicas still matter")[Read replicas share storage or lag behind the primary on the same hardware. A full scan on a replica can saturate disk I/O, increase replication lag, and affect the primary indirectly. Treat replicas with the same patterns, just wider tolerances.]

#ecl-warning("Don't retry extractions blindly")[A retry that hits the source during a backup window, a peak traffic period, or while the condition that caused the failure is still present makes things worse, not better. Retry logic should respect safe hours and back off on repeated failures rather than hammering the source immediately.]

#ecl-danger("Don't assume the DBA knows you exist")[If nobody on the source team knows your pipeline connects to their database, the first time they find out will be during an incident -- which is the worst possible time to introduce yourself. Establish the relationship before you go live.]

== Related Patterns
<related-patterns-1>
- 0601-monitoring-observability -- source health metrics measure your impact
- 0606-scheduling-and-dependencies -- safe hours, concurrency limits, and parallelism tradeoffs
- 0610-extraction-status-gates -- timeout handling and explicit failure
- 0201-full-scan-strategies -- full scans are the highest-impact extraction pattern
- 0604-sla-management -- business hours and freshness expectations are agreed during SLA stage

== What Comes Next
<what-comes-next-5>
0608 covers how to assign different update frequencies to different tables based on consumer needs -- the framework that determines which tables run during business hours, which wait for the off-peak window, and which only need a daily refresh.

// ---

= Tiered Freshness
<tiered-freshness>
#quote(block: true)[
#strong[One-liner:] Not every row needs the same refresh cadence -- partition your pipeline into hot, warm, and cold tiers so the tables that matter most get attention first.
]

== The Problem
<the-problem-7>
The naive approach is one schedule for everything: all tables, same cadence, same extraction method. It works when you have a dozen tables and a daily overnight window. It stops working when some of those tables need to be fresh within the hour while others haven't changed in months -- because now you're either over-refreshing cold data (wasting compute, money and source load) or under-refreshing hot data (delivering stale results to the consumers).

The subtler version of this problem is not refreshing everything at the same #emph[frequency] but with the same #emph[method];. We had an `orders` table that ran a full replace of the entire year's data many times a day. The frequency was right -- the table needed intraday updates -- but full-replacing twelve months of data every run was not. The DBA noticed before we did. The fix wasn't changing the schedule; it was splitting the table's extraction into tiers: recent data incrementally and often, historical data fully but rarely.

== The Tiers
<the-tiers>
The model is three zones, each with its own cadence and extraction method. The boundaries between them depend on the table, the source system, and the consumer's SLA -- the names are universal, the numbers are not.

=== Hot (Intraday)
<hot-intraday>
Tables or partitions with actively changing data: today's `orders`, open `invoices`, recent `events`. Refreshed multiple times per day via incremental extraction when neccesary (0302). The actual interval depends on the table's volume, source capacity, and consumer SLA -- a 500-row lookup table can refresh every few minutes while a 50M-row fact table might only sustain hourly.

The hot tier tolerates impurity. Slight gaps from late-arriving data or cursor lag aren't catastrophic here because the warm tier catches them on the next pass. This is where you accept a tradeoff: the data is fresh but might not be perfectly pure, and that's fine because purity comes later.

=== Warm (Daily)
<warm-daily>
Current month or current quarter -- data that still receives occasional updates but not at high frequency. Refreshed daily, often overnight when the source is under less load. The extraction method is either a full replace of the warm window (0205) or incremental with a wider lag.

This tier takes advantage of harder business boundaries. A closed month in an ERP is unlikely to change (though "unlikely" is not "impossible" -- see the soft rules in 0106). The warm tier's job is to re-read recent history with enough depth to catch what the hot tier missed: late cursor updates, backdated transactions, documents that changed without updating their `updated_at`. Here purity is a lot more important, and you should expect your destination to be exactly equal to source 99% of the time after loading.

=== Cold (Weekly / On-Demand)
<cold-weekly-on-demand>
Historical data: prior years, closed fiscal periods, archived partitions. Refreshed on a slow cadence -- weekly, monthly, or only on demand for backfills and corrections. Full replace is the right method here because the volume is bounded and the frequency is low enough that the cost is negligible.

The cold tier is where 0108 plays out most directly: cold data trades freshness for purity. A weekly full replace of last year's data resets accumulated drift from the hot and warm tiers -- any row that was missed by a cursor, any late update that arrived outside the warm window, gets picked up here. The cold tier is your cleanup pass.

=== The Lag Window
<the-lag-window>
The warm tier's extraction window needs to overlap with the hot tier's territory -- otherwise changes that happen between the last hot run and the warm run's cutoff fall through the gap. This overlap is the lag window: how far back the warm tier reads beyond its own boundary.

The right lag depends on how reliably the source system updates its cursors. For well-organized systems where every modification touches `updated_at`, 7 days of lag is enough -- especially when the cold tier runs weekly and catches anything the warm tier missed. For messier systems where documents get modified without updating any cursor (common in ERPs where back-office edits bypass the application layer), 30 days is safer. The decision is empirical: start at 7, watch for rows that appear in the cold tier's full replace but were never picked up by warm, and widen the window if it happens regularly.

The same logic applies between cold and warm. The cold tier's full replace naturally covers everything, so it doesn't need a lag window -- it reads the entire historical range. That's what makes it the safety net.

== Assigning Tables to Tiers
<assigning-tables-to-tiers>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Signal], [Tier],),
    table.hline(),
    [Has active writes in the last hour], [Hot],
    [Has writes in the last 7 days but not the last hour], [Warm],
    [No writes in \> 7 days], [Cold],
    [Append-only, partitioned by date], [Hot for today's partition, cold for everything else],
    [Open documents (`invoices` with status = draft)], [Hot regardless of write frequency],
  )]
  , kind: table
  )

Tier assignment can be static (configured per table in your orchestrator) or dynamic (based on recent activity signal from 0207). Static is simpler and covers most cases -- you know which tables are transactional and which are archival. Dynamic earns its complexity when you have hundreds of tables and can't manually classify each one, or when the same table's activity profile shifts seasonally.

Most pipelines don't need all three tiers from day one. About two-thirds of tables in a typical pipeline are lookups and dimensions that full-replace daily and never need anything faster. Incrementalizing everything you can is tempting but generates more errors than it saves time -- or money. The simpler approach is to maximize full replace and reserve incremental for the cases that actually demand it. The tier system matters most for the remaining third.

Being in the hot tier doesn't automatically mean incremental. A `products` table with 10k rows that needs intraday freshness can full-replace every run without anyone noticing -- the volume is trivial, the extraction takes seconds, and you avoid maintaining cursor state entirely. The same applies to tables on a low-enough frequency: if you're only refreshing twice a day, a full replace of even a moderately large table might be cheaper than the complexity of tracking what changed. Incremental earns its place when the table is too large to full-replace at the cadence you need -- `events` growing by millions of rows per day, `orders` with years of history. For everything else, full replace at whatever frequency the consumer requires is simpler, purer, and usually fast enough.

== Month-End and Seasonal Shifts
<month-end-and-seasonal-shifts>
ERP systems behave differently at month-end and period close. Whether that affects your tiered schedule depends on who consumes the data and why.

If the extracted data drives quick decision-making -- collections teams chasing receivables before month-end, sales managers tracking targets -- consumers will ask for #emph[more] frequency. Promoting tables to the hot tier during the last week of the month gives them fresher data when the stakes are highest.

If the extracted data feeds a historical analysis engine -- a data warehouse that produces reports after the period closes -- consumers will often ask for the #emph[opposite];: reduce extraction frequency during month-end to avoid competing with the ERP's own close process for database resources. The source system is already under pressure from period-end batch jobs, and your pipeline hammering it with intraday reads doesn't help anyone.

For pipelines that run overnight only, month-end rarely changes the schedule. The overnight window already avoids the daytime contention, and the warm tier's daily refresh picks up whatever happened during the close.

== Schedule Configuration
<schedule-configuration>
Each tier maps to a separate schedule or schedule group in your orchestrator:

- #strong[Hot];: frequent cron, interval driven by table volume and source tolerance
- #strong[Warm];: daily cron, typically overnight
- #strong[Cold];: weekly or monthly cron, or triggered manually for backfills

A table can move between tiers as business cycles shift. Month-end promotes some tables to hot; fiscal year rollover pushes last year's data from warm to cold; seasonal patterns (Black Friday, harvest season, enrollment periods) can temporarily increase the hot tier's population. If your orchestrator supports dynamic schedule assignment, encode these transitions as rules rather than manual changes.

#ecl-warning("Don't mix tiers on the same cron")[This anti-pattern applies when you have tables at different cadences. If some tables need intraday freshness but share a cron with everything else, the hot tables wait in line behind cold tables that didn't need refreshing. Separate the schedules when you have tables that genuinely need different cadences.]

#ecl-danger("Same frequency, wrong method")[Refreshing a table many times a day is fine. Full-replacing a year's worth of data many times a day is not. If a table needs intraday freshness, the hot tier should extract only the recent window incrementally -- not reload the entire history on every run. The frequency is a schedule concern; the method is a pattern concern. Getting one right and the other wrong is how you end up on the phone with the DBA.]

== Tradeoffs
<tradeoffs-5>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Hot data gets to consumers faster without over-refreshing cold data], [Three schedules to configure and monitor instead of one],
    [Cold-tier full replace acts as a purity checkpoint, resetting drift], [Lag window tuning is empirical -- too short misses rows, too long wastes reads],
    [Tables can shift tiers as business needs change], [Dynamic tier assignment adds orchestrator complexity],
    [Cost scales with actual freshness needs, not with table count], [Month-end and seasonal shifts require manual or rule-based tier promotions],
  )]
  , kind: table
  )

== Related Patterns
<related-patterns-2>
- 0604-sla-management -- SLA tiers define freshness requirements; tiered freshness implements them
- 0606-scheduling-and-dependencies -- schedule structure and safe hours
- 0603-cost-monitoring -- tiered freshness is partly a cost optimization strategy
- 0205-rolling-window-replace -- the warm tier often uses rolling window
- 0302-cursor-based-extraction -- the hot tier uses cursor-based incremental
- 0108-purity-vs-freshness -- the fundamental tradeoff that tiered freshness navigates

// ---

= Data Contracts
<data-contracts>
#quote(block: true)[
#strong[One-liner:] Schema drift, row counts, null rates, freshness -- what to enforce at the boundary between source and destination.
]

== The Problem
<the-problem-8>
Source schemas change without notice. A column gets renamed, a type changes from INT to VARCHAR, a new column appears when someone activates an ERP module, an old one disappears after a migration. The source team doesn't know your pipeline exists -- they won't tell you before they deploy a schema migration, and they shouldn't have to. The boundary between their system and yours is your responsibility to defend.

Without a contract, drift propagates silently into the destination -- a dropped column becomes NULLs in downstream queries, a type change produces casting errors that surface three layers deep in a dashboard nobody connects back to the source, and a 90% row count drop looks like a quiet day until someone notices the month-end report is missing most of its data. By then the blast radius is wide and the root cause is buried. A data contract makes these boundaries explicit and checkable.

== What a Data Contract Covers
<what-a-data-contract-covers>
=== Schema Contract
<schema-contract>
The schema contract defines the expected column names and types -- the fingerprint from 0601. It answers three questions when the schema changes:

#strong[New columns] -- accept or reject? The policy is either evolve (add the column to the destination) or freeze (fail the load). Evolve is the right default for almost every table. Source schemas grow -- ERPs add columns when modules are activated, applications add fields as features ship. Freezing a schema that legitimately evolves means a manual intervention every time the source team deploys, which is maintenance you don't want and they won't coordinate with you on. Evolve means one less thing to manage, and downstream consumers shouldn't be doing `SELECT *` against your destination anyway -- an added column doesn't break anything for them unless they wrote their queries wrong.

#strong[Dropped columns] -- no decision gets made without the source system. A column disappearing could be a deliberate removal, a migration gone wrong, or a temporary rollback. Set up tolerances: if the column was created yesterday and disappeared today, it was probably a rollback and you can let it go. If a column that's been there for months vanishes, fail the load and investigate. The tolerance depends on how the downstream uses the column -- a critical join key disappearing is different from an unused description field being cleaned up.

#strong[Type changes] -- fail, cast, or warn. See \#Type Mapping below for how to handle the mapping itself.

=== Volume Contract
<volume-contract>
The volume contract defines the expected row count range per extraction, derived from recent history. A table that normally extracts 450k rows and today extracts 12k likely has a problem -- even if the pipeline reports SUCCESS. The contract surfaces this before the data reaches consumers.

The threshold should come from observed baselines, not assumptions. A simple approach: track the rolling average and standard deviation of row counts over the last 30 runs, and alert when the current run falls outside 2-3 standard deviations. For tables with predictable seasonality (month-end spikes on `invoices`, weekend dips on `orders`), factor the day-of-week or day-of-month into the baseline.

This feeds directly into 0610 for inline enforcement -- block the load when the volume looks wrong, rather than discovering the problem downstream.

=== Null Contract
<null-contract>
The null contract defines expected null rates on key columns. A cursor column like `updated_at` should never be NULL -- if it is, your incremental extraction is blind to those rows. A description column being 40% NULL is probably normal. The contract distinguishes between the two.

The purpose is to protect your pipeline's ability to do its job. A null rate spike on `updated_at` disrupts your extraction; a null rate spike on `customer_name` is the source's problem and downstream's concern. Anything that disrupts your ability to extract and load accurately is alertable. Everything else passes through as-is.

=== Freshness Contract
<freshness-contract>
The freshness contract is the SLA from 0604 expressed as a checkable rule: maximum acceptable staleness per table, measured from the health table's last successful load timestamp. This is the simplest contract to define and the most visible when violated -- a stale table is the one that generates the "why hasn't the dashboard updated" email.

== Enforcement Points
<enforcement-points>
=== Pre-Load (Gate)
<pre-load-gate>
Check schema, row count, and null rates after extraction but before loading. If the contract is violated, block the load and alert (0605). This is the extraction status gate from 0610 extended with richer checks.

Pre-load gates are the strongest enforcement point because they prevent bad data from reaching the destination. The cost is that a false positive blocks a load that was actually fine -- which is why baselining matters. A gate based on assumptions ("this column should never be NULL") fires on the first run and trains you to ignore it.

=== Post-Load (Validation)
<post-load-validation>
Run checks after the load completes: destination row count vs source, schema matches expected, null rates within bounds. Your orchestrator's post-load check primitives are built for this -- ideally run them as part of the load job so the check and the data it validates stay in sync. At scale, though, the overhead of inline checks on every table may not fit in the schedule window (see \#The Cost of Checking), and running validation on a separate, less frequent cadence becomes the practical tradeoff: you lose immediate detection but keep the pipeline on time.

Post-load validation catches problems that pre-load gates can't see: rows that were lost during the load itself, type coercions that silently truncated values, partition misalignment that put data in the wrong place. The tradeoff is that by the time you detect the problem, the bad data is already in the destination -- you're limiting blast radius rather than preventing damage.

=== Continuous (Monitoring)
<continuous-monitoring>
Schema fingerprint comparison on every run, volume trend tracking over time. This feeds the observability layer from 0601 and catches slow drift that no single-run check would flag: a table whose row count grows 2% less than expected every week, a column whose null rate creeps from 0.1% to 5% over a quarter.

=== The Cost of Checking
<the-cost-of-checking>
Every contract check adds overhead to every run. A schema fingerprint comparison, a row count validation, a null rate scan -- each one might take 10 or 15 seconds on its own, barely noticeable on a single table. Multiply that by 1,000 tables and you've added over 4 hours of load time to your pipeline. The contracts that felt free at 20 tables become a bottleneck at scale.

Contract coverage is a budgeting decision. Not every table needs every check. A critical `orders` table might deserve schema + volume + null rate validation on every run. A 200-row lookup table probably doesn't need anything beyond the run health your orchestrator already provides. Allocate checks where the blast radius of a silent failure justifies the overhead, and leave the rest to the monitoring layer where the cost is amortized across a dashboard glance, not multiplied across every load.

== Schema Evolution Policies
<schema-evolution-policies>
#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Policy], [Behavior], [When to use],),
    table.hline(),
    [#strong[Evolve];], [Accept new columns, add them to destination], [Default for most tables -- source schemas grow],
    [#strong[Freeze];], [Reject any schema change, fail the load], [Critical tables where downstream depends on exact schema],
  )]
  , kind: table
  )

These are the only two valid policies in an ECL context. Some loaders offer `discard_row` and `discard_value` modes that silently drop data when the schema doesn't match -- these are transformation decisions, not conforming ones. If the source sent it, the destination should have it. Either accept the change or reject the load; don't silently drop data. See 0403 for the full reasoning.

== Column Naming as a Contract
<column-naming-as-a-contract>
Your column naming convention -- whether you preserve source names verbatim or normalize to `snake_case` -- is itself a schema contract, and one of the hardest to change after the fact. Changing the convention on a running pipeline means reloading every table and updating every downstream query that references the old names -- a full migration.

The problem gets sharper when you're running multiple pipelines or migrating between systems. A pipeline that loads with source-native names (`@ORDER_VIEW`, `CustomerID`, `línea_factura`) and another that normalizes to `snake_case` produce incompatible destinations. If you plan on running meta-pipelines that handle hundreds of sources, document exactly how you normalize column names and make the convention configurable at two levels: per destination (because consumers expect consistency within the dataset they're querying) and per table (because migrating a source sometimes means fixing individual tables that arrived with a different convention).

This also means you need a documented answer for the edge cases: how do you handle a column named `@ORDER_VIEW` with emoji? A column with spaces? A reserved word? These aren't hypothetical -- ERP systems and legacy databases produce all of them. Your naming contract should handle the full range, not just the clean cases.

== Type Mapping
<type-mapping>
Type mismatches between source and destination are universal and varied enough that hand-coding each one is a losing strategy. The corridor determines the severity: transactional-to-transactional pairs usually have close type mappings, while transactional-to-columnar pairs (SQL Server to BigQuery, SAP HANA to Snowflake) produce a steady stream of precision loss, overflow risk, and silent truncation.

Numeric precision is the most dangerous category. SQL Server's `DECIMAL(38,12)` mapped to BigQuery's `NUMERIC(29,9)` silently loses precision on values that fit the source but overflow the destination. Financial data with high-precision decimals is exactly the data where this matters most and where the bug is hardest to catch -- the numbers look reasonable until someone reconciles and finds a two-cent discrepancy across a million rows.

The practical approach is to rely on a type-mapping library (SQLAlchemy, your loader's built-in adapters) and override only when you know a specific mapping is wrong for your data. Don't spend time building a comprehensive type-mapping system from scratch -- the libraries have already solved the common cases, and the edge cases are specific enough that a generic solution wouldn't help.

#ecl-warning("Unusual source-destination pairs")[If you're extracting from a source where no well-tested adapter exists -- a niche ERP, a legacy database with non-standard types, a SaaS API that returns ambiguous JSON types -- you may have no alternative to manual type mapping. Document every mapping decision, test with real data (not just the schema), and watch for silent truncation on the first few runs.]

== Anti-Patterns
<anti-patterns-6>
#ecl-danger("Don't enforce unbaselined contracts")[A contract based on assumptions (\"this column should never be NULL\") will fire false positives on the first run. Baseline the actual data first: run a profiling pass, measure real null rates and row counts, then set thresholds from observed behavior. A contract that cries wolf on day one trains everyone to ignore it by day three.]

#ecl-warning("Don't freeze evolving schemas")[`products` gains a new attribute column every quarter. Freezing its schema means a load failure every quarter and a manual intervention to update the contract. Use evolve for tables with expected growth; freeze only for tables with stable, critical schemas where a column change would genuinely break something important downstream.]

#ecl-danger("Don't silently discard columns")[Silently dropping new or unexpected columns that don't match your schema breaks the conforming boundary. Wide ERP tables with hundreds of columns are tempting candidates for discard, but the right answer is evolve (accept the column) or 0209 (explicitly declare which columns you extract and document why). Discarding is implicit partial column loading with no documentation -- the worst version of both.]

== Tradeoffs
<tradeoffs-6>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Schema drift caught before it reaches consumers], [Every check adds per-table overhead that compounds at scale],
    [Volume anomalies surfaced immediately, not days later], [False positives on poorly baselined contracts erode trust],
    [Explicit evolve/freeze policy eliminates ambiguity on schema changes], [Evolve means downstream must handle new columns; freeze means manual intervention on legitimate changes],
    [Type mapping libraries handle the common cases transparently], [Edge cases on unusual source-destination pairs still require manual work],
  )]
  , kind: table
  )

== Related Patterns
<related-patterns-3>
- 0601-monitoring-observability -- schema fingerprinting and null rate tracking feed contracts
- 0605-alerting-and-notifications -- contract violations trigger alerts
- 0610-extraction-status-gates -- pre-load gate is the inline enforcement mechanism
- 0604-sla-management -- freshness contract is the SLA expressed as a checkable rule
- 0614-reconciliation-patterns -- volume contract enforcement post-load
- 0403-merge-upsert -- schema evolution policy reasoning (why discard modes break conforming)
- 0209-partial-column-loading -- the explicit alternative to silent column discarding

// ---

= Extraction Status Gates
<extraction-status-gates>
#quote(block: true)[
#strong[One-liner:] 0 rows returned successfully is not the same as a silent failure. Gate the load on extraction status before advancing the cursor.
]

== The Problem
<the-problem-9>
An extraction that returns 0 rows and reports SUCCESS could mean two things: the table genuinely had no changes since the last run, or the source was down, the query timed out silently, or the connection returned an empty result set instead of an error. Without a gate, these two scenarios are indistinguishable -- and the pipeline treats them identically, loading nothing and advancing the cursor past data it never read. For incremental tables, that gap is permanent. For full replace tables, it's worse: the destination gets truncated and replaced with nothing.

This happens more often with APIs than with direct SQL connections, but SQL sources aren't immune. We had a client whose upstream team gave us a "database clone" that periodically truncated its tables before reloading them. If our extraction hit the window between truncate and reload, we'd read 0 rows from a table that should have had hundreds of thousands -- and our full replace would dutifully wipe the destination clean. It happened more than once before we gated it.

== Gate Mechanics
<gate-mechanics>
The gate sits between extraction and load. After the extraction query returns, before any data reaches the destination, evaluate whether the result is plausible. The evaluation is per-table -- if 3 out of 200 tables return suspect results, block those 3 and let the other 197 proceed. Gating per-run (blocking everything because one table looks wrong) risks your SLA on every other table, and if the blocked table is a heavy one that can only run overnight, you've lost an entire day of data for tables that were fine.

=== What Triggers the Gate
<what-triggers-the-gate>
#strong[Zero rows from a table that normally returns data.] The most common trigger. A table that extracted 450k rows yesterday and 0 today deserves scrutiny. A table that routinely returns 0 rows on weekends does not -- the gate needs to know the difference.

#strong[Row count outside the expected range.] Full replace tables should stay within a percentage of their previous row count. A `customers` table that had 50k rows yesterday and has 50,200 today is normal growth; the same table at 5k rows means something upstream went wrong. The threshold depends on the table's volatility -- a `pending_payments` table can legitimately drop by 80% when a batch of payments clears, while `products` is very unlikely to lose half its rows overnight (Also, have they heard of soft deletes?).

#strong[Extraction metadata anomalies.] Query duration of 0ms on a table that normally takes 30 seconds, or bytes transferred far below the expected range. These can signal a connection that returned immediately without actually querying.

=== What the Gate Does
<what-the-gate-does>
When the gate fires:

+ #strong[Blocks the load] -- the extracted data (or lack of it) does not reach the destination. For full replace tables, the destination retains its current data untouched. #strong[For incremental tables, the decision is less clear-cut] -- you may still be getting #emph[some] new data, and a partial update is better than no update at all. Whether to block or load what you got is a case-by-case call based on how wrong the row count looks and how much damage a partial load would cause downstream.
+ #strong[Triggers an alert] (0605) with the extraction metadata: expected row count, actual row count, query duration, and which table.
+ #strong[Logs the event] in the health table (0602) so the pattern is visible over time -- a table that gates every Monday morning points to a weekend maintenance window nobody told you about.

=== Cursor Safety and Stateless Windows
<cursor-safety-and-stateless-windows>
If you're using stateless window extraction (0303), cursor advancement is already a non-issue -- the next run re-reads the same window regardless. The gate still matters for preventing a bad load, but the recovery is automatic: you have the width of your lag window for the upstream problem to be resolved before data actually falls out of scope. The alert fires on day one; upstream has until the lag window closes to fix it.

For cursor-based extraction, a stuck cursor can become a problem if the window between the cursor and "now" grows large enough that re-extraction becomes expensive. A wide enough lag window (0608) mitigates this -- the warm tier's daily pass catches what the hot tier missed, and the cold tier's full replace resets everything. Stateless windows avoid this problem entirely, which is one more reason they've become my preferred approach for most incremental extraction.

== Full Replace Gates
<full-replace-gates>
The stakes for full replace tables are higher than for incremental. An incremental extraction that reads 0 rows leaves a gap in the destination; a full replace that reads 0 rows #emph[empties the destination];. The extraction returned nothing, the pipeline replaced the table with nothing, and now consumers are querying an empty table that had 50k rows an hour ago.

Full replace gates check that the extracted row count is within an expected percentage of the previous load's row count. The percentage depends on the table: a `products` dimension that grows by 1% per month should gate on anything below 90-95% of the last load. A `pending_payments` table that legitimately fluctuates as payments clear needs a wider band. The very few tables that can legitimately approach zero (cleared queues, seasonal staging tables) should be explicitly exempted with documentation explaining why -- otherwise the next engineer on call will second-guess the exemption and re-enable the gate.

== Baselines
<baselines>
The gate's accuracy depends entirely on knowing what "normal" looks like for each table. The baseline is a range, not a point -- flag when outside the range, not when different from last run.

A rolling window of the last 30 runs gives you a reasonable baseline for most tables. Track the min, max, and average row count per table, and gate when the current extraction falls below the historical minimum by a configurable margin. For tables with predictable seasonality -- month-end spikes on `invoices`, weekend dips on `orders` -- factor the day-of-week or day-of-month into the baseline so the gate doesn't fire every Saturday.

#ecl-warning("Start loose, tighten over time")[A gate that's too tight fires false positives and trains you to ignore it -- the exact same failure mode as over-alerting (0605). Start with a generous threshold (block only on 0 rows or \>90% drop), observe for a month, then tighten based on the table's actual variance.]

== Validating Against Source
<validating-against-source>
When the gate fires, the first question is whether the source actually has the data you expected. A `COUNT(*)` against the source during business hours confirms whether the extraction was wrong (source has data, extraction missed it) or the source is genuinely empty (upstream problem). This validation is manual and delayed -- the gate fires at 3 AM, someone investigates at 9 AM, and the destination sits stale in the meantime. The SLA clock runs during that gap.

If the source confirms the data is there, the extraction failed silently -- re-run it. The truncate-then-reload pattern (source temporarily empty as part of its own load cycle) is a common culprit, and the `COUNT(*)` during business hours distinguishes it from a genuine problem.

If the source is genuinely empty, you have a harder decision with no universal answer:

#strong[Hold the gate] -- the destination keeps its previous data, stale but complete. Consumers see yesterday's numbers, which are wrong but usable. The cost is that you become a silent buffer for upstream's problem: nobody feels the pain, nobody escalates, and the issue can persist for days before anyone outside your team notices.

#strong[Load what you got] -- the destination reflects reality, empty or broken as it is. Consumers see the damage immediately, which hurts but also makes the problem visible to the people who can fix it. A downstream report showing zero revenue generates an escalation in hours; a stale report showing yesterday's revenue generates nothing.

Neither option is always right. Full replace tables almost always deserve a hold -- the destination wipeout is too destructive to let through. Incremental tables with partial data lean toward loading what you got, since some fresh data is better than none and the gap is bounded. For everything in between, the decision depends on the table, the consumer, and how much pain you're willing to absorb on upstream's behalf.

Whichever you choose, make the decision explicit: log it in the health table, include it in the alert, and document the policy per table. A gate that silently holds data without anyone knowing it held is a judgment call that nobody can audit -- and the next engineer on call will make a different judgment if they don't know yours. \#\# Tradeoffs

#figure(
  align(center)[#table(
    columns: (47.09%, 52.91%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Prevents silent data loss from empty or truncated extractions], [Adds per-table overhead (baseline tracking, threshold evaluation)],
    [Per-table gating protects SLA on unaffected tables], [Threshold tuning is empirical -- too tight fires constantly, too loose misses real failures],
    [Cursor stays safe on incremental tables, destination stays intact on full replace], [Stateless windows already mitigate cursor risk, reducing the gate's incremental value],
    [Gated events logged in health table surface recurring upstream patterns], [Volatile tables (cleared queues, seasonal) need explicit exemptions],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-7>
#ecl-danger("Gate per table, not per run")[Blocking 200 tables because 1 returned 0 rows means your entire pipeline misses its SLA. Gate individually. If your orchestrator doesn't support per-asset gating, this is worth building -- the alternative is choosing between no gate and an all-or-nothing gate that's too disruptive to enable.]

#ecl-warning("Don't gate without a baseline")[A gate that fires on \"fewer rows than I expected\" without historical data to define \"expected\" is a guess. Run the pipeline ungated for 30 days, collect baselines, then enable the gate.]

== Related Patterns
<related-patterns-4>
- 0609-data-contracts -- defines enforcement points (when to check); this pattern defines gate mechanics (what to check)
- 0605-alerting-and-notifications -- the gate triggers alerts
- 0614-reconciliation-patterns -- count reconciliation is a post-load version of the same idea
- 0406-reliable-loads -- cursor advancement gated on confirmed load success
- 0303-stateless-window-extraction -- stateless windows reduce cursor risk but still benefit from load gating

// ---

= Backfill Strategies
<backfill-strategies>
#quote(block: true)[
#strong[One-liner:] Reloading 6 months of data without breaking prod -- how to backfill safely alongside live pipelines.
]

== The Problem
<the-problem-10>
Something went wrong upstream -- a schema change, a bad deploy, a data corruption that drifted for weeks before anyone noticed -- and now you need to reload a historical range. The naive response is "just rerun everything," but a backfill that treats the source like a normal extraction competes with live scheduled runs for source connections, destination quota, and orchestrator capacity. If it runs unchunked during business hours, it violates every rule in 0607.

Backfills aren't rare. If you're running hundreds of tables with clients who routinely correct old records, delete and re-enter documents, or run maintenance scripts that touch historical data, backfills are a weekly operation. A `start_date` override or a `full_refresh: true` flag should be tools you reach for without hesitation -- the pipeline that can't backfill safely is the one that drifts furthest from its source.

We had a client with a massive table on a very slow on-prem database -- too large to extract in a single overnight window. We loaded two years of data per night, chunked by date range, and it took four nights to complete. The table has been a constant headache since: every backfill is a multi-night operation, and any interruption on night three means deciding whether to restart from scratch or resume from the interrupted chunk.

== Backfill Types
<backfill-types>
=== Date-Range Backfill
<date-range-backfill>
The most common type: reload a specific date range -- last three months, last fiscal quarter, a single bad week -- using partition swap (0202) or rolling window replace (0205). Everything outside the range stays untouched. Scope the range slightly wider than the known corruption -- the blast radius of a bad deploy is rarely as precise as the deploy timestamp suggests.

=== Full Table Backfill
<full-table-backfill>
Reload the entire table from scratch when corruption is too widespread to scope, when the table is small enough that scoping isn't worth the effort, or when incremental state has drifted so far that a full reset is simpler than diagnosing the gap. Uses full replace (0401), which resets the destination data, any incremental cursors, pipeline state, and schema versions. After it completes, the next scheduled incremental run picks up from the new baseline.

=== Selective Backfill
<selective-backfill>
Reload specific records by primary key -- a handful of corrupted orders, not the entire table. Requires the extraction layer to support PK-based filtering (`WHERE id IN (:ids)`). In practice this is rare: unless you have a short list of known bad PKs and a table large enough that reloading even a date range is expensive, a date-range backfill is simpler and catches records you didn't know were affected.

== Execution Strategy
<execution-strategy>
=== Isolation from Live Pipelines
<isolation-from-live-pipelines>
Backfills should never block or delay scheduled runs. Run them as separate jobs in your orchestrator, with their own schedule (or manual trigger) and their own concurrency limits. If your orchestrator supports run priority or queue separation, give scheduled runs higher priority so they proceed even when a backfill is in progress -- the backfill can pause between chunks while the scheduled run completes, then resume.

We learned this when a backfill and a scheduled incremental run hit the same table at the same time -- both slowed down, both errored, and fixing it meant stopping the backfill, waiting for the scheduled run to finish, and restarting from the interrupted chunk.

=== Chunking
<chunking>
Break large backfills into date-range chunks -- one month, one week, or whatever granularity matches the source's partition structure. Each chunk is independently retriable: if chunk 3 of 6 fails, retry only chunk 3. Chunk size trades off per-chunk overhead (connection setup, query parsing, destination writes) against blast radius on failure -- smaller chunks lose less work when something goes wrong, larger chunks reduce overhead.

=== Safe Hours
<safe-hours>
Large backfills belong in the safe-hours window from 0607. If the backfill is too large for one window, span it across multiple nights with chunking. Track which chunks completed explicitly -- a simple table or config file with chunk boundaries and completion status -- so that a failure on night three doesn't force a restart from night one.

=== Staging Persistence
<staging-persistence>
For multi-chunk backfills, staging tables may intentionally persist between chunks so consumers see either the old data or the fully backfilled data, never a half-finished state. Don't clean up staging until the full backfill is validated -- the storage cost of a few extra days is negligible compared to restarting a multi-night backfill because you dropped staging prematurely (see 0603).

== State Reset
<state-reset>
After a full backfill, the incremental state -- cursor position, high-water mark, schema version -- must match the data you just loaded. If the cursor still points to its old position, the next incremental run skips everything between that cursor and the most recent data, leaving an invisible gap. Some pipelines wipe state automatically on a full refresh; others require explicit cleanup (clearing a cursor table, deleting state files, resetting partition metadata). If state cleanup is a manual step, document it prominently -- a backfill that reloads the data but leaves the old cursor in place is worse than no backfill, because the pipeline reports success while silently skipping rows.

The risk compounds when pipeline state lives in a separate store. After clearing that state, the next scheduled run starts from scratch -- effectively a full refresh of every table, not just the one you backfilled. Engineers who don't expect this find out the hard way. This is one of the strongest arguments for stateless window extraction (0303): the next scheduled run re-reads its normal trailing window regardless of any backfill, there's no state to reset, and the failure mode of "reload data but forget to fix the cursor" doesn't exist. It's also far simpler to reason about -- "the pipeline always grabs the last N days" requires no mental model of cursor state, cleanup procedures, or post-backfill sequencing.

== Backfill as Routine
<backfill-as-routine>
If your clients actively manage their own source data -- correcting historical records, deleting and re-entering documents, running maintenance scripts on old rows -- backfills are part of the regular operating rhythm, and the pipeline needs to support them without ceremony. Two runtime overrides cover most cases:

#strong[`start_date` / `end_date`] -- override the extraction's date boundaries to re-extract a specific range without pulling everything forward to today. Without an `end_date`, a backfill starting three months back also re-extracts all data between then and now -- wasting source load and destination writes on data that's already correct.

Date-range backfills can also clean up hard deletes and orphaned rows within the window if you filter on a stable business date (`order_date`, `invoice_date`) rather than `updated_at`, then swap the destination's partitions for that range with the fresh data (0202). The partition swap fully replaces the slice, so anything that existed in the destination but no longer exists in the source disappears. The business date is the right filter because it's immutable -- an order placed on March 5 always has `order_date = 2026-03-05` regardless of when it was last updated -- which keeps partition boundaries stable and guarantees you capture every row in the range, not just recently changed ones.

#strong[`full_refresh`] -- ignore all incremental state and reload the entire table using full replace (0401) instead of a merge. A merge only updates and inserts, so rows hard-deleted at the source survive in the destination indefinitely; a full replace wipes the slate. Useful when the table is small enough that scoping isn't worth the effort, when the incremental state is corrupt, or when you suspect hard deletes have drifted the destination.

Both should be launchable from your orchestrator's UI without modifying code or config files. If a backfill requires editing a config and redeploying, you'll avoid doing it until the problem is too large to ignore. Some orchestrators go further -- Dagster's partition-based backfill UI lets you select a date range, kick off the backfill, and track per-partition status from the same interface that shows your scheduled runs (see 0805).

== Tradeoffs
<tradeoffs-7>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Pro], [Con],),
    table.hline(),
    [Resets accumulated drift and restores source-destination parity], [Large backfills compete with live pipelines for source and destination resources],
    [Chunked backfills are independently retriable -- partial failures don't restart from scratch], [Multi-night backfills require chunk-tracking state and are fragile over long durations],
    [Date-range scoping limits blast radius to the affected period], [Scoping too narrowly may miss corrupted rows at the edges],
    [Full table backfill resets all state to a known-good baseline], [Resets incremental cursor -- next run after backfill may be heavier than expected],
    [Routine backfill capability reduces time-to-fix for upstream problems], [Absorbing upstream messiness via frequent backfills can mask problems that should be fixed at the source],
  )]
  , kind: table
  )

== Anti-Patterns
<anti-patterns-8>
#ecl-warning("Don't run unchunked backfills live")[A 6-month backfill as a single sustained scan at 2pm on a Tuesday during business hours on a live source will get your access revoked. Chunked backfills with indexed reads can coexist with business-hours traffic if the source can handle it, but an unchunked backfill on a production OLTP during peak hours is how you lose source access.]

#ecl-danger("Don't forget the state reset")[On cursor-based pipelines, reloading the data while the cursor still points to the old high-water mark means the next incremental run skips everything between the cursor and the new data. Clear the state or force a full refresh. Stateless window extraction avoids this entirely -- there's no state to forget.]

#ecl-warning("Don't let backfills compete with schedules")[A backfill that blocks a scheduled run isn't fixing the pipeline -- it's degrading it. Isolate backfills in separate jobs with lower priority, and design the chunking so a backfill can yield to a scheduled run between chunks.]

== Related Patterns
<related-patterns-5>
- 0202-partition-swap -- the mechanism for date-range backfills in partitioned tables
- 0205-rolling-window-replace -- rolling window as a scoped backfill strategy
- 0401-full-replace -- full table backfill
- 0303-stateless-window-extraction -- no state to reset after backfill
- 0607-source-system-etiquette -- safe hours and source protection
- 0608-tiered-freshness -- cold-tier full replace is a scheduled backfill by another name
- 0615-recovery-from-corruption -- backfill as a recovery mechanism after corruption

// ---

= Partial Failure Recovery
<partial-failure-recovery>
#quote(block: true)[
#strong[One-liner:] Half the batch loaded, the other half didn't -- now what?
]

== The Problem
<the-problem-11>
A pipeline run that processes multiple tables can fail partway through: 40 tables succeed, 10 fail. Rerunning the entire job wastes time reprocessing the 40 tables that already landed correctly. Not rerunning leaves 10 tables stale, and the staleness compounds with every subsequent run that doesn't fix them. The real problem is knowing which tables failed, at which step, and whether to retry now or wait for the next scheduled run.

At scale, partial failures are daily. With hundreds of tables extracting from multiple sources, something fails every run -- a connection timeout on one source, a DML quota hit on the destination, a schema change on a table nobody warned you about. The pipeline that handles partial failures well isn't the one where nothing ever fails; it's the one where failures are visible, scoped, and retriable without disrupting the tables that succeeded.

#ecl-warning("Cursor safety and partial failures")[If your cursors advance only after a confirmed successful load (0302), partial failures don't create data gaps -- the failed tables simply get re-extracted on the next run. Stateless window extraction (0303) avoids the question entirely.]

== Failure Modes
<failure-modes>
=== Extraction Failed for Some Tables
<extraction-failed-for-some-tables>
Some tables extracted successfully, others hit a timeout, a connection error, or a source that was temporarily unavailable. The successful tables can proceed to load; the failed ones need re-extraction. Each table should have automatic retry on extraction errors -- a connection timeout on the first attempt often succeeds on the second, and waiting for the next scheduled run to discover that wastes an entire cycle. Two or three retries with a short backoff is enough; if the source is genuinely down, retrying indefinitely just adds load to a system that's already struggling (0607). Your orchestrator should track per-table status, not just per-run status -- the successful tables should proceed to load even though the run as a whole is failed.

The common causes are connection timeouts (especially on slow on-prem sources), connection pool exhaustion when too many tables extract from the same source simultaneously, and source maintenance windows that nobody told you about. A table that fails for the same reason every Monday morning is a scheduling problem, not a retry problem -- move it to a different window or investigate the source's maintenance calendar (0607).

=== Extraction Succeeded, Load Failed
<extraction-succeeded-load-failed>
The data was extracted correctly but the destination rejected it -- DML quota exceeded, permission error, schema mismatch, disk full. The extraction is valid and may still be sitting in staging; if it is, you can retry the load without re-extracting. If staging is ephemeral (cleaned up per run), the extraction has to run again.

Destination quotas are the most common cause at scale. Columnar engines like BigQuery impose daily DML limits, and a pipeline that runs hundreds of merges can exhaust the quota partway through -- the first 150 tables land fine, the remaining 50 get rejected. The fix isn't more quota (though that helps); it's knowing which tables didn't land and retrying them in the next window when the quota resets. This is also where full replace earns its keep: a `DELETE + INSERT` or partition swap avoids the DML-heavy merge path entirely, and quota limits on batch loads are generally higher than on row-level DML.

=== Load Partially Applied
<load-partially-applied>
The load started but didn't finish -- rows were written but the job died mid-stream. What happens next depends on the load strategy: full replace and partition swaps are idempotent and can be safely rerun since the incomplete load gets overwritten. Append may have produced duplicates that need deduplication (0613). A merge may be partially applied -- some rows updated, others not -- leaving the table in an inconsistent state where the same extraction's data is half-landed. See 0406 for making the load step itself resilient to interruption.

== Recovery Strategy
<recovery-strategy>
=== Per-Table Retry
<per-table-retry>
The first principle: retry only what failed, not the entire job. If 97/100 tables succeeded, rerunning all 100 wastes compute, risks introducing new failures on previously successful tables, and delays recovery. Your orchestrator should support re-running individual tables from a failed run -- if it doesn't, this is worth building, because the alternative is choosing between "rerun everything" and "wait for the next schedule." Some orchestrators support this natively -- Dagster lets you retry individual failed assets from a run's status page without touching the ones that succeeded (see 0805).

The retry should also target the right step. A table that failed at extraction needs re-extraction; a table that extracted successfully but failed at load only needs the load retried -- preferably from the data already in staging, not from a fresh extraction that hits the source again for no reason.

=== Staging as a Safety Net
<staging-as-a-safety-net>
If staging tables persist after extraction, a load failure can be retried from staging without hitting the source again. This is the faster recovery path and the one that's gentler on the source system -- the data is already extracted, you just need to land it. The tradeoff is storage cost: persistent staging means keeping a copy of every extracted table until the load confirms success (see 0603). For most tables the cost is trivial; for a few massive ones it may matter.

If staging is ephemeral, a failed load requires full re-extraction. Whether that's acceptable depends on how expensive the extraction is and how soon the data needs to land. For small tables on a healthy source, re-extraction is fast and harmless. For a 50M-row table on a slow on-prem database during business hours, you may have to wait until the next safe window (0607).

=== Per-Table Status Tracking
<per-table-status-tracking>
Track each table's lifecycle explicitly: `extracting` -\> `extracted` -\> `loading` -\> `loaded` / `failed`. On restart, tables stuck in `loading` are failed tables, not running ones -- treat them accordingly. A table that's been in `loading` for longer than its expected load duration either crashed or is hanging, and leaving it in limbo means nobody investigates.

The health table (0602) should record the outcome per table per run -- not just `success` / `failure` but which step failed and why. This is what makes per-table retry possible: without a record of where each table stopped, every retry is a guess.

== Alerting on Partial Failures
<alerting-on-partial-failures>
Any failure, no matter how small, should mark the pipeline run as failed. A run where 197 tables succeeded and 3 failed is a failed run -- not a successful run with caveats. If your orchestrator reports it as success, the 3 broken tables disappear into the noise and nobody investigates until a consumer complains. The run status should be unambiguous: if anything didn't land, the run failed.

The tension is failure fatigue. If the pipeline fails every single run because one flaky table times out on Mondays, the team learns to ignore the failure status -- and the one time 50 tables fail for a real reason, nobody notices because the alert looks the same as every other Monday. Your alerting (0605) needs to distinguish between the two: include the count of failed tables, which ones, which step failed, and whether the failure is retryable. "Run failed: 3 tables (invoices, order\_lines, products) -- extraction timeout, auto-retry exhausted" is actionable. "Run failed" with no context trains people to click dismiss.

== Anti-Patterns
<anti-patterns-9>
#ecl-danger("Don't rerun everything")[Retry only the failed tables. Rerunning the entire pipeline to fix 3 failures wastes compute, risks new failures on previously successful tables, and delays recovery.]

#ecl-warning("Don't leave tables stuck in loading")[A table in `loading` after the run process has died is a failed table. If your recovery logic doesn't detect and reset orphaned states, those tables sit in limbo indefinitely -- neither loaded nor marked for retry.]

== Related Patterns
<related-patterns-6>
- 0302-cursor-based-extraction -- cursor advances only after confirmed load; partial failures don't create gaps
- 0303-stateless-window-extraction -- no cursor state, so partial failure recovery is automatic on next run
- 0406-reliable-loads -- idempotent loads that survive interruption
- 0602-health-table -- per-table per-run outcome tracking
- 0605-alerting-and-notifications -- partial failures must alert, not just log
- 0610-extraction-status-gates -- gates prevent load on suspect extraction results
- 0613-duplicate-detection -- deduplication after a partially applied append
- 0615-recovery-from-corruption -- when partial failure leads to corrupted data

// ---

= Duplicate Detection
<duplicate-detection>
#quote(block: true)[
#strong[One-liner:] Duplicates already landed. How to find them, quantify the damage, and deduplicate without losing data.
]

== The Problem
<the-problem-12>
Duplicates in the destination are a symptom, not a root cause -- they indicate a load strategy mismatch, a failed retry that double-wrote, or an append that should have been a merge. If you followed the patterns in this book (merge with the correct key, full replace where possible, append-and-materialize with a dedup view), duplicates should be rare. But when they happen, the damage is disproportionate: consumers don't notice until aggregations are wrong -- revenue doubled, counts inflated, joins producing unexpected fan-out -- and once they catch it, your data's credibility takes a hit that's hard to recover from. One episode of duplicates, even if you fix it in an hour, can make consumers question every number you produce for months.

Checking for duplicates is fast -- a `GROUP BY pk HAVING COUNT(*) > 1` takes seconds. Run it before anything else. If the table is clean, the problem is downstream: most "duplicate" reports turn out to be bad JOINs on the consumer's side (a one-to-many fanout they didn't expect, a missing GROUP BY). But verify your side first -- it's cheaper than asking for their query.

== How Duplicates Arrive
<how-duplicates-arrive>
#figure(
  align(center)[#table(
    columns: (9.93%, 90.07%),
    align: (auto,auto,),
    table.header([Cause], [Mechanism],),
    table.hline(),
    [Append without dedup handling], [Append-only done right (0402) handles edge cases with `ON CONFLICT DO NOTHING` or a dedup view. Raw INSERT with no conflict handling and no dedup layer produces duplicates from retries, overlap buffers, or upstream replays],
    [Merge key too specific], [The merge key includes a column that changes between extractions (e.g., `_extracted_at`, a hash that incorporates load metadata), so the merge never matches existing rows and every re-extraction INSERTs instead of UPDATing],
    [NOLOCK page #strong[desync];], [SQL Server NOLOCK reads can return the same row twice if a page split moves it mid-scan -- duplicates arrive in a single extraction, before the load strategy even runs],
  )]
  , kind: table
  )

#ecl-warning("Cross-partition duplicates")[Partitioning the destination by `updated_at` or another mutable date makes cross-partition duplicates likely: a row lands in the March partition, gets updated in April, and the next extraction writes the updated version to the April partition while the March copy persists. Partitioning by an immutable business date (`order_date`, `invoice_date`) prevents the row from scattering across partitions -- every re-extraction targets the same partition, which is cheaper and correctly scoped. But #strong[partitioning alone doesn't deduplicate];: columnar engines don't enforce uniqueness, so you still need your load strategy (merge with the correct key, or a dedup view from 0404) to handle duplicates within the partition.]

== Detection
<detection>
=== Row Count Comparison
<row-count-comparison>
The simplest signal: compare `COUNT(*)` between source and destination. If the destination has more rows, you either have duplicates or you're missing hard-delete detection. Run hard-delete detection first (0306) -- if after cleaning up deleted rows the destination still has more rows than the source, the excess can only be duplicate PKs (columnar engines don't enforce uniqueness constraints).

Run `COUNT(*)` on the source and on the destination separately, then compare in your orchestrator or manually -- these are different engines, so there's no single query that spans both. If the destination has more rows after hard-delete cleanup, the excess can only be duplicate PKs (columnar engines don't enforce uniqueness). This ties directly to 0614 -- if reconciliation is already running on a schedule, it surfaces the count mismatch before anyone downstream notices.

=== By Primary Key
<by-primary-key>
The definitive test. Group by PK, count \> 1 = duplicate.

```sql
-- destination: columnar
SELECT id, COUNT(*) AS dupes
FROM orders
GROUP BY id
HAVING COUNT(*) > 1;
```

If you're particularly worried about duplicates and you have overhead to spare, add this at the end of your pipeline, after loading finishes. \#\#\# By Content Hash

When there's no natural PK, hash the columns that identify the entity and group by hash -- count \> 1 means multiple rows for the same entity. Fix the key definition (0502) so it uses only the columns that define identity (revise your synthetic keys, maybe?), then deduplicate.

=== Narrowing the Root Cause
<narrowing-the-root-cause>
Once you've found duplicates, `_extracted_at` or `_batch_id` from 0501 narrow down which load introduced them. "All duplicates share `_batch_id = 47`" points to a specific run and limits where to look.

== Deduplication
<deduplication>
=== Dedup in Place
<dedup-in-place>
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

=== Dedup via Rebuild
<dedup-via-rebuild>
Re-extract the table with a full replace (0401) or rebuild from staging. Cleaner than in-place dedup because it resets to a known-good state with no residual risk of missed duplicates. Prefer this when the duplication is widespread or when the table is small enough that a full reload is cheap.

=== Dedup View
<dedup-view>
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

Fast to deploy, no DML, no data loss risk. If you rename the base table to `orders_raw` and create the view as `orders`, downstream queries don't need to change -- this is the same mechanism that 0404 uses permanently. As a temporary fix it buys you time to investigate the root cause while consumers see clean data immediately.

#ecl-tip("Consider append-and-materialize permanently")[If you're reaching for the dedup view often, consider switching to append-and-materialize. The dedup view is the core of 0404. Append-and-materialize removes the duplicate problem structurally -- every extraction appends, the view always deduplicates -- and it's cheaper than merge in columnar engines because a pure INSERT never rewrites existing partitions. The dedup cost is paid at read time, not at load time, and only for the rows the consumer actually queries.]

== Anti-Patterns
<anti-patterns-10>
#ecl-warning("Don't deduplicate without finding the cause")[Deduplication fixes the symptom. If you don't fix the load strategy that produced the duplicates, they'll come back on the next run. Find the cause first, fix the pipeline, then clean up the data.]

#ecl-danger("Verify duplicates before blaming the pipeline")[Run the `GROUP BY pk HAVING COUNT(\*) > 1` check first -- it takes seconds. If the table is clean, the problem is downstream.]

== Related Patterns
<related-patterns-7>
- 0401-full-replace -- full replace as a dedup-via-rebuild strategy
- 0403-merge-upsert -- merge prevents duplicates when the PK is correct
- 0404-append-and-materialize -- structural dedup via view, eliminates duplicate risk permanently
- 0501-metadata-column-injection -- `_batch_id` identifies which load introduced duplicates
- 0502-synthetic-keys -- content hashing for dedup when no natural PK exists
- 0614-reconciliation-patterns -- row count mismatch is often the first signal of duplication

// ---

= Reconciliation Patterns
<reconciliation-patterns>
#quote(block: true)[
#strong[One-liner:] Source count vs destination count -- row-level, hash-level, and aggregate reconciliation.
]

== The Problem
<the-problem-13>
A load that completes without errors can still produce wrong results: missing rows, duplicate rows, stale data, wrong values. The pipeline reports success; the destination is quietly wrong. Without a verification step, discrepancies surface only when a consumer notices something off -- a report that doesn't tie out, a dashboard metric that jumped overnight, an analyst who ran the numbers twice and got different answers.

Reconciliation is the scheduled check that the destination actually reflects the source. It runs after the load, compares what arrived against what should have arrived, and alerts when the numbers don't add up. It's not a replacement for staging validation (0201) or extraction gates (0610) -- those catch failures before the load commits. Reconciliation catches what those gates didn't see: rows that fell within tolerance, drift that accumulated over multiple runs, or discrepancies that only surface after downstream queries start running.

== Reconciliation Levels
<reconciliation-levels>
=== Row Count Reconciliation (Cheapest)
<row-count-reconciliation-cheapest>
Compare `COUNT(*)` at the source against `COUNT(*)` at the destination. A count mismatch is the first and cheapest signal that something went wrong -- missing rows from a failed extraction, duplicates from a retry that double-wrote, a dropped partition that nobody noticed.

#strong[Catches:] missing rows, duplicates, dropped partitions, undetected hard deletes (destination surplus). #strong[Misses:] rows with wrong values, rows updated at the source but not reloaded.

=== Aggregate Reconciliation (Medium)
<aggregate-reconciliation-medium>
Compare key aggregates -- `SUM(amount)`, `MAX(updated_at)`, `COUNT(DISTINCT customer_id)` -- between source and destination. A row that changed from \$100 to \$0 has the same count but a different sum; row count alone misses it. Choose aggregates that are meaningful for the table: financial totals for transaction tables, max timestamps for activity tables.

The cost caveat: running `SUM(amount)` against a transactional source during business hours is expensive. Columnar destinations handle aggregation cheaply; transactional sources don't. If you're running aggregate reconciliation, run it against a read replica or during off-peak hours -- or accept that it runs less frequently than row count checks. At scale, with tables spanning dozens of clients and schemas, the variety makes it impractical to standardize meaningful aggregates across the board. Row count reconciliation runs on everything; aggregate reconciliation is reserved for critical tables where value integrity matters.

=== Hash Reconciliation (Expensive)
<hash-reconciliation-expensive>
Hash every row at source and destination and compare the hashes. Any difference at any column surfaces -- this is the nuclear option. At scale it's too expensive to run on every load; reserve it for critical tables or periodic audits, not routine runs.

== Configuring Thresholds
<configuring-thresholds>
An exact match between source and destination count is rarely achievable in practice. In-flight transactions committed after the extraction window but before the destination count is taken create natural discrepancy on live transactional sources. A tolerance threshold absorbs this noise without masking real failures.

Two asymmetric rules drive threshold configuration:

#strong[Destination has fewer rows than source] -- acceptable within a threshold, but the right threshold depends on when you extract. For off-peak extractions -- overnight, early morning -- in-flight transactions are rare and the tolerance should be tight; a deficit of more than a handful of rows is a real signal. For extractions running during business hours, the threshold needs to cover transactions committed after the extraction window closes; calibrate it from your actual discrepancy history, not a guess. 100 rows is a starting point for a busy source during business hours, but the right number varies by table and schedule.

#strong[Destination has more rows than source] -- interpretation depends on whether hard delete detection is running. If it is, a surplus means duplicates and warrants an immediate alert. If it isn't, the surplus is expected: rows deleted from the source still exist in the destination. Know which case you're in before alerting.

When a deficit above threshold surfaces, the resolution depends on the gap size. A small gap is a candidate for pk-to-pk detection (0306) to identify exactly which rows are missing without reloading the whole table. A large gap points to a structural failure -- a missed extraction window, a dropped partition, a load strategy mismatch -- and the right fix is a full reload or partition swap (0201, 0202).

== Timing Matters
<timing-matters>
Source count and destination count must be taken as close together as possible. A gap between counting the source and counting the destination lets new rows arrive at the source in between, generating a false discrepancy that looks like a pipeline failure but is just timing.

The right approach: record the source count at extraction time, before new data arrives; record the destination count after the load completes; compare in your orchestrator. The comparison cannot happen inside either database -- source and destination are different engines with no shared query context.

== Reconciliation Jobs
<reconciliation-jobs>
A per-run inline reconciliation check is ideal but expensive at scale. An alternative is a dedicated reconciliation job on a schedule -- typically once daily -- that iterates all tables, compares counts against source, and produces a summary report. Run it before business hours: discrepancies surface to operators before downstream consumers act on bad data. 07:00 is a reasonable default if your pipeline runs overnight.

The tradeoff: inline checks catch discrepancies before downstream queries run on bad data. A scheduled reconciliation job may surface an issue hours after downstream consumers have already seen it. For critical tables, inline is worth the overhead. For the long tail of lower-priority tables, a daily reconciliation job is the right balance.

Store the results in the health table (0602): table name, source count, destination count, delta, status (OK / warning / critical). Storing results historically lets you detect drift trends -- a table that's consistently 50 rows short is a different problem from one that's suddenly 50,000 rows short. If you store per-run source and destination counts as part of normal pipeline operation, the dedicated reconciliation job becomes a comparison of already-collected numbers rather than a fresh round of queries against both systems -- which makes it cheap enough to run across everything, every morning.

== By Corridor
<by-corridor>
#ecl-warning("Transactional to columnar")[Source count: `SELECT COUNT(\*) FROM schema.table` at source. Destination count: `SELECT COUNT(\*)` -- BigQuery bills 0 bytes for it (resolved from table metadata internally); Snowflake resolves it from micro-partition headers without scanning data. Both engines also expose row counts in `INFORMATION_SCHEMA`, but those update asynchronously and can lag after a recent load, making them unreliable for post-load verification.]

#ecl-info("Transactional to transactional")[Both sides support `COUNT(\*)` efficiently -- no reason not to use it.]

== Anti-Pattern
<anti-pattern>
#ecl-warning("Don't reconcile only on count")[A table with 1M rows at source and 1M rows at destination can still be wrong: 1,000 rows missing, 1,000 duplicates. Count matches, data doesn't. Use aggregate reconciliation for critical tables.]

== Related Patterns
<related-patterns-8>
- 0601-monitoring-observability -- reconciliation delta is a data health metric
- 0602-health-table -- stores per-table counts for historical comparison
- 0605-alerting-and-notifications -- threshold breaches trigger alerts
- 0609-data-contracts -- volume contracts are reconciliation expressed as a pre-load check
- 0610-extraction-status-gates -- pre-load gating is reconciliation's inline cousin
- 0613-duplicate-detection -- count mismatch is often the first signal of duplication
- 0306-hard-delete-detection -- pk-to-pk comparison for resolving small discrepancies
- 0201-full-scan-strategies -- full reload for resolving large discrepancies
- 0202-partition-swap -- partition-scoped reload for resolving discrepancies in a date range

// ---

= Recovery from Corruption
<recovery-from-corruption>
#quote(block: true)[
#strong[One-liner:] A bad deploy corrupted 3 months of data -- identifying the blast radius and rebuilding.
]

== The Problem
<the-problem-14>
Something broke and bad data has been landing for a while. A schema migration that silently changed types, a cursor that skipped a range, a load strategy that dropped a column, a conforming bug that mangled values. The pipeline reported success on every run because the failure was in the data, not in the execution -- no errors, no alerts, no signal that anything was wrong until someone downstream noticed the numbers didn't add up.

The gap between when corruption starts and when it's detected is the blast radius. A bug introduced three months ago that nobody caught until today means three months of data in the destination is suspect, every downstream model that consumed it is suspect, and every report built on those models has been wrong for three months. The recovery isn't just reloading the data -- it's scoping the damage, fixing the root cause, rebuilding what's affected, and communicating what happened so consumers can reassess decisions they made on bad data.

The worst corruptions are the ones that look plausible. A date format that flipped from D-M-Y to M-D-Y after a source ERP version upgrade produces dates that parse successfully -- January through December, day 1 through 12, nothing fails, nothing alerts. Every date-based partition, every month-end report, every time-series chart is silently wrong. You discover it when someone notices March 5th orders showing up in May, and by then the entire destination is corrupted across every table that has a date column.

== Triage: Assess the Blast Radius
<triage-assess-the-blast-radius>
=== When Did It Start?
<when-did-it-start>
`_extracted_at` from 0501 narrows the window. Filter destination rows by `_extracted_at` ranges and compare against the source to find where the data starts diverging -- the first batch where values don't match is the start of the corruption window. Cross-reference that timestamp with your deploy history and git log: a commit that shipped on the same day as the first bad batch is the likely root cause.

If `_batch_id` is populated, the scoping is even tighter -- "all rows from batch 47 onward are corrupted" is a precise statement that drives the recovery scope. Without metadata columns, you're left correlating deploy dates with destination anomalies by hand, which is slower and less certain.

=== What Tables Are Affected?
<what-tables-are-affected>
The blast radius depends on where the root cause lives. A pipeline code change that affects the conforming layer corrupts every table processed by that code path -- potentially hundreds. A source schema change affects only tables from that source. A destination-side issue (quota exhaustion, permission change) affects only tables on that destination.

Start with the narrowest plausible scope and widen if evidence points further. Checking a handful of tables from each source against their current source state is faster than assuming everything is wrong and rebuilding the world.

=== What's Downstream?
<whats-downstream>
Every downstream model, materialized view, dashboard, and report that reads from the corrupted tables is also affected. Map the lineage from corrupted tables to downstream consumers -- if the destination feeds a transformation layer that builds aggregates, those aggregates are wrong too, and they need rebuilding after the source tables are clean.

== Recovery Strategies
<recovery-strategies>
Three strategies, from broadest to most surgical. The right choice depends on how much data is affected and how precisely you can scope it.

=== Full Replace (Simplest)
<full-replace-simplest>
Reload the entire table from source using `full_refresh: true`. Resets the destination to the current source state -- every row, every column, clean baseline. Downstream models rebuild from the clean data. This is the right default when the table is small enough to reload within the schedule window, when the corruption is widespread, or when you can't precisely scope the damage.

Full replace always works for current state. The source has the correct data right now, so reloading it produces a correct destination. The caveat is historical state: if the source is transactional and rows were modified or deleted since the corruption started, the full replace reflects the source's current state, not the state at any point during the corruption window. For most tables this is exactly what you want -- the destination should match the source as of now, not as of three months ago.

=== Date-Range Rebuild
<date-range-rebuild>
Reload only the corruption window via backfill (0611). Less disruptive than full replace because data outside the window stays untouched, but it requires knowing the exact corruption range. Scope the range slightly wider than the first bad batch -- corruption boundaries are rarely as precise as a single timestamp suggests, and a few extra days of reload is cheap insurance against missing rows at the edges.

Use partition swap (0202) for the destination-side replacement so the rebuild is atomic per partition and the rest of the table stays live throughout. For tables too large to full-replace but where the corruption window is bounded, this is the sweet spot.

=== PK-to-PK Repair
<pk-to-pk-repair>
Compare primary keys between source and destination to identify exactly which rows are wrong -- missing, surplus, or mismatched values. Fix only the discrepancies: insert missing rows, delete surplus rows, update changed values. This is the same mechanism as hard delete detection (0306) and the small-gap resolution described in 0614.

Use this when the corruption is narrow -- a handful of rows in a large table, a specific set of PKs identified during triage -- and reloading an entire table or date range would be disproportionate. The tradeoff is that you need to know exactly which rows are affected, which requires either a full PK comparison against the source or a reconciliation pass that identified the discrepancies.

All three strategies may require a state reset if the table uses cursor-based extraction. A full replace or date-range rebuild that reloads the data but leaves the old cursor in place means the next incremental run skips everything between the stale high-water mark and now -- the same problem 0611 warns about. Stateless window extraction (0303) sidesteps this entirely -- the next run re-reads its normal trailing window regardless of what the rebuild did, and there's no cursor to forget about. This is one of the operational arguments for defaulting to stateless: recovery is simpler because there's less state to manage.

== Recovery Checklist
<recovery-checklist>
Regardless of which strategy you choose, the sequence is the same: confirm the fix, verify the source, rebuild, verify the result, notify.

- ☐ If consumers have already acted on corrupted data (reports sent, decisions made), notify them now -- they need to know before you start, not after
- ☐ Confirm the root cause is fixed and deployed
- ☐ Test the fix on a small range before committing to the full rebuild
- ☐ Verify source connectivity and schema haven't changed since the corruption started
- ☐ If the table uses cursor-based extraction: reset incremental state (cursor position, schema versions) so the rebuild sets a clean baseline -- not needed for stateless window extraction (0303)
- ☐ Run the rebuild (full replace, date-range backfill, or PK-to-PK repair depending on scope)
- ☐ Reconcile post-rebuild: source count vs destination count (0614)
- ☐ Notify downstream consumers that data is clean and they can rebuild dependent models

== Prevention
<prevention>
None of these prevent corruption from happening -- source schemas change, bugs ship, scripts run without warning. What they do is make corruption detectable early and recoverable fast, which limits the blast radius.

#strong[Metadata columns] (`_extracted_at`, `_batch_id`) make triage possible. Without them you can't scope the corruption to specific batches -- you're left guessing which runs introduced the bad data based on deploy dates and git blame. See 0501.

#strong[Schema contracts] catch drift before it corrupts data. A new column appearing is harmless; a column disappearing or a type changing is a signal that something upstream changed without coordination. Contracts surface these changes before the load commits, not after consumers have already consumed the result. See 0609.

#strong[Reconciliation] catches silent count and value drift between source and destination. A table that's consistently 50 rows short is a different problem from one that's suddenly 50,000 rows short, and both are problems that row-level pipeline success doesn't reveal. See 0614.

#strong[Stateless, idempotent pipelines] reduce the recovery surface. Pipeline state -- cursors, schema version tracking, checkpoint files -- is itself a corruption vector. When the state is wrong, the pipeline produces wrong output from correct source data, and the failure mode is invisible because no query failed and no error fired. The less state your pipeline carries between runs, the fewer ways it can silently break. Full replace and stateless window extraction (0303) both minimize carried state; cursor-based extraction with external state stores maximizes it.

== Anti-Patterns
<anti-patterns-11>
#ecl-warning("Don't fix forward without fixing backward")[Fixing the pipeline so future runs are correct doesn't fix the corrupted historical data already in the destination. You need both: fix the code AND rebuild the affected range. A pipeline that's producing correct data going forward while three months of bad data sits in the destination is a pipeline that's still wrong -- it's just wrong in a way that's harder to notice.]

#ecl-danger("Don't rebuild before confirming the fix")[Reloading 3 months of data only to have the same bug corrupt it again is wasted work and a wasted weekend. Confirm the fix is deployed, test it on a small range, then run the full rebuild. The checklist above puts "test on a small range" before the rebuild for exactly this reason.]

== Related Patterns
<related-patterns-9>
- 0501-metadata-column-injection -- `_batch_id` and `_extracted_at` scope the corruption to specific loads
- 0306-hard-delete-detection -- PK-to-PK comparison for surgical repair
- 0609-data-contracts -- contracts catch the drift before it becomes corruption
- 0611-backfill-strategies -- the mechanism for rebuilding a date range
- 0614-reconciliation-patterns -- post-rebuild verification and small-gap resolution
- 0612-partial-failure-recovery -- when corruption is caused by a partial failure
- 0303-stateless-window-extraction -- no state to reset after recovery

// ---
