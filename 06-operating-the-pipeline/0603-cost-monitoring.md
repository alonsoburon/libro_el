---
title: "Cost Monitoring"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-23
---

# Cost Monitoring

> **One-liner:** Per-table, per-query, per-consumer -- know where the money goes before the invoice arrives.

## The Problem

Cloud data warehouses bill by bytes scanned, slots consumed, or storage volume -- and the bill arrives after the damage is done. A single bad pattern can dominate the monthly invoice: an unpartitioned scan that reads the entire table on every query, a MERGE on a table that should be a full replace, or a staging dataset nobody cleaned up accumulating for months. You won't know which one it was until you can attribute cost to individual tables and operations.

Without per-table cost attribution, "costs went up 40%" is a mystery that sends you guessing. With it, you can point at the exact table and the exact operation that caused the spike -- and decide whether to fix the pattern, reduce the schedule frequency, or accept the cost because the freshness justifies it.

## The Pattern

Cost monitoring extends the health table from [[06-operating-the-pipeline/0602-health-table|0602]]. The health table captures `extraction_seconds`, `load_seconds`, and `bytes_extracted` per run -- time and volume metrics that tell you where the pipeline spends effort. But destination-side cost (bytes scanned, slots consumed, DML pricing) lives in the destination's own audit logs, and the connection between the two is the job label or run ID you attach to each load operation.

### What to Track

**Compute costs** are the volatile dimension. Track bytes scanned per load operation (the MERGE, the DELETE+INSERT, the partition swap), and if your engine charges by slots or query-seconds, track those too. The same data supports three useful aggregations: cost per run (anomaly detection), cost per table (pattern decisions), and cost per schedule (budgeting). MERGE deserves special attention because it's the single most expensive load operation in columnar engines -- a MERGE-heavy pipeline loading hundreds of tables **can cost an order of magnitude more** than the same tables loaded via partition swap or append-and-materialize, and the difference only shows up in the bill, not in run duration.

**Storage costs** are predictable but sneaky at scale. Append logs grow with every run, and without compaction that growth is unbounded ([[04-load-strategies/0404-append-and-materialize|0404]] covers compaction). Staging tables that outlive their load job are dead weight, though in practice orphaned staging is more of a schema hygiene issue than a cost problem -- at ~$0.02/GB/month in BigQuery, a few hundred GB of staging is annoying but not alarming. The compute cost of accidentally querying unpartitioned staging is usually worse than storing it.

**Extraction costs** are easy to forget because querying your own PostgreSQL is free. But some sources meter reads: API rate limits, licensed query slots (SAP HANA, some SaaS platforms), or egress charges from cloud-hosted sources. Also sometimes extracting over VPNs incurs in bandwidth costs. Overlapping extraction windows in stateless patterns ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) re-extract the same rows deliberately -- the overlap is correct, but its cost in time and source-side load should be visible and known

### Cost Attribution

The gap between "the pipeline costs $X/month" and "table Y's MERGE costs $X/month" is a join key: pipeline metadata (table name, run ID, schedule) matched against the destination's query audit log (bytes scanned, cost, duration). Without that join, you're stuck with aggregates that don't point anywhere useful.

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

### The Drilldown Workflow

Track at maximum granularity -- per table, per run, per operation -- but check at low frequency. A weekly or monthly glance at the aggregate total is enough. If the aggregate doesn't trigger your curiosity, the individual tables are fine. Drill down when the total spikes, not as a daily ritual.

The practical workflow is top-down: aggregate cost in a dashboard or a weekly scheduled query, filter by table when the number looks off, check whether the pattern changed, the table grew, or the schedule frequency increased. A report showing the top-10 most expensive tables gives you enough signal for most months. **Cost monitoring should cost less attention than it saves money.**

> [!tip] Test pattern changes at small scale
> If you're switching a table's load strategy to reduce cost -- MERGE to append-and-materialize, full replace to incremental -- try it on a small table first, then a large one, then roll it out broadly. Measure the actual cost difference before committing to the migration. The estimated savings from theory and the actual savings in production are rarely the same number.

## The Expensive Patterns

| Pattern                     | Why it's expensive                              | Mitigation                                                                                                                    |
| --------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| MERGE on large tables       | Full scan of both source and destination sides   | Partition-scoped merge, or switch to [[04-load-strategies/0404-append-and-materialize\|0404]]                                  |
| Unpartitioned full scan     | Every query reads the entire table              | Partition by date, enforce `require_partition_filter` ([[07-serving-the-destination/0702-partitioning-for-consumers\|0704]])        |
| Staging cleanup missed      | Orphaned staging datasets accumulate storage    | Scheduled cleanup job, weekly or after each run                                                                               |
| Append log without compaction | Storage grows linearly with schedule frequency  | Periodic compaction to latest-only ([[04-load-strategies/0404-append-and-materialize\|0404]])                                |

## Tradeoffs

| Pro                                                                      | Con                                                                           |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| Per-table attribution turns "costs went up" into an actionable lead       | Requires tagging every load query with metadata (job labels, query comments)  |
| Aggregated view catches spikes without daily effort                       | Destination audit logs have retention limits -- store summaries for history    |
| Identifies which patterns to optimize first                               | Cost optimization can lead to premature complexity if it drives pattern choice |

## Anti-Patterns

> [!danger] Don't let cost optimization drive pattern selection
> Switching from full replace to incremental to "save money" introduces complexity that costs engineering time and creates failure modes (see [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]]). The cheaper pipeline is the one that breaks less, not the one with the lowest bytes-scanned number. Pick the pattern that's correct first, then optimize its cost within that pattern.

> [!danger] Don't optimize without measuring
> "MERGE is expensive" is true in general, but *how* expensive depends on table size, partition layout, and update volume. A MERGE on a 10k-row lookup table costs fractions of a cent -- switching it to append-and-materialize for cost reasons adds complexity with no real savings. Measure the actual cost per table before deciding anything is worth changing.

## What Comes Next

Cost is one input to the freshness decision. [[06-operating-the-pipeline/0604-sla-management|0604]] defines *when* data must be fresh; [[06-operating-the-pipeline/0608-tiered-freshness|0608]] uses cost as one factor in deciding which tables earn high-frequency schedules and which ones run daily.
