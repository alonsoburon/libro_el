---
title: "Cost Optimization by Engine"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Cost Optimization by Engine

> **One-liner:** Engine-specific strategies for keeping query costs under control -- because BigQuery bills differently from Snowflake, and the optimizations don't transfer.

## The Problem

Cost optimization is engine-specific. What saves money on BigQuery (reducing bytes scanned) is irrelevant on Snowflake (which bills by warehouse time). Generic advice like "use partitions" applies everywhere, but the specifics -- what to partition on, how clustering interacts with the billing model, which operations are free and which are traps -- differ enough across engines that generic advice doesn't help with the decisions that actually move the bill.

The ECL engineer's load-time decisions have permanent cost consequences on every consumer query. A partition key chosen at table creation, a clustering configuration, a table format -- these compound across every query for the lifetime of the table. This chapter is the engine-specific reference for making those decisions correctly, and for knowing which levers to pull when the cost monitoring from [[06-operating-the-pipeline/0603-cost-monitoring|0603]] surfaces a table that's too expensive.

I once wasted $500 in a single night because of unlimited retries on a badly merged table. The retries ran all night, rescanning the table roughly 30 times a minute. By next morning the bill was already in, and the lesson was clear: set per-day cost limits on the project, and understand what each query costs before you let it retry indefinitely.

## BigQuery (Bytes Scanned)

BigQuery on-demand billing charges per byte scanned: $6.25/TB. Every query pays for the bytes it reads from the columns it touches, regardless of how many rows the result returns. The optimization target is reducing bytes scanned per query.

> [!info] Documentation
> - [Pricing](https://cloud.google.com/bigquery/pricing)
> - [Partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables)
> - [Clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables)
> - [Materialized views](https://cloud.google.com/bigquery/docs/materialized-views-intro)
> - [Reservations (slots)](https://cloud.google.com/bigquery/docs/reservations-intro)

**Partitioning + `require_partition_filter`.** Mandatory cost control for any table over a few GB. A query that filters on the partition column reads only the partitions that match; everything else is skipped at zero cost. `require_partition_filter = true` rejects queries that forget the filter, turning a potential $50 full scan into an error message. See [[07-serving-the-destination/0702-partitioning-for-consumers|0702]] for partition key selection.

**Clustering.** Reduces bytes scanned within partitions. Up to 4 columns, ordered by filtering priority -- the first column clusters most effectively. A query filtering on a clustered column reads fewer storage blocks because the engine skips blocks whose min/max range doesn't include the target value. BigQuery auto-reclusters in the background at no explicit cost.

**Column selection.** `SELECT col1, col2` scans only those two columns. `SELECT *` scans every column in the table, including that 2MB JSON blob nobody needed. Columnar storage physically separates columns -- the engine literally reads fewer bytes when you name fewer columns.

**`COUNT(*)`.** Free -- resolved from table metadata at 0 bytes scanned. Use it for row counts without cost anxiety.

**`LIMIT`.** Does NOT reduce bytes scanned. `SELECT * FROM events LIMIT 10` still scans the full table; the LIMIT only caps the result set.

**Materialized views.** BigQuery auto-refreshes materialized views and can route queries to the MV when the optimizer determines the MV can answer the query. The consumer queries the base table, but the engine reads the smaller MV instead. Effective for repetitive aggregation patterns -- dashboards that always GROUP BY the same dimensions.

**Slots vs on-demand.** Flat-rate pricing (reservations/slots) makes bytes-scanned irrelevant -- you pay for compute capacity, not data read. The optimization target shifts from "scan fewer bytes" to "avoid slot contention." Most of the advice above still helps because it reduces execution time, which frees slots for other queries.

**Per-day cost limits.** BigQuery supports custom cost controls at the project and user level -- maximum bytes billed per query and per day. Set these before your first production run, not after the first surprise bill. A runaway retry loop is bounded by the daily limit instead of running until someone notices.

## Snowflake (Warehouse Time)

Snowflake bills per second of warehouse compute. No per-byte charge -- a query that scans 1TB and one that scans 1GB cost the same if they run for the same duration on the same warehouse size. The optimization target is reducing query runtime and minimizing idle warehouse time.

> [!info] Documentation
> - [Warehouses overview](https://docs.snowflake.com/en/user-guide/warehouses-overview)
> - [Micro-partitions and clustering](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions)
> - [Persisted query results (caching)](https://docs.snowflake.com/en/user-guide/querying-persisted-results)

**Warehouse sizing.** Right-size the warehouse for the workload. A larger warehouse (XL) finishes queries faster but costs more per second; a smaller warehouse (XS) costs less but takes longer. For batch loads, an XS or S warehouse running for 10 minutes is usually cheaper than an XL running for 2 minutes -- the XL's per-second rate is higher and the minimum billing increment (60 seconds) means short bursts on a large warehouse are disproportionately expensive.

**Auto-suspend and auto-resume.** Set auto-suspend aggressively -- 60 seconds is reasonable for most workloads. An idle warehouse that stays running burns credits for nothing. Auto-resume starts the warehouse on the next query, so the only cost of aggressive suspend is a brief cold-start delay.

**Clustering.** Snowflake manages micro-partitions automatically, but declaring cluster keys helps when the natural ingestion order doesn't match how consumers filter. Reclustering costs warehouse credits, so don't cluster tables where the natural order already matches the query pattern.

**Result caching.** Identical queries within 24 hours return cached results at zero compute cost. Significant for dashboards that refresh periodically with the same query -- the first execution pays, subsequent ones are free until the underlying data changes or 24 hours pass.

**Query queuing.** Too many concurrent queries on a small warehouse queue instead of fail. Queued queries wait for a slot, which is fine for batch loads but terrible for interactive dashboards. Monitor queue times and scale the warehouse if interactive queries are consistently queuing.

## ClickHouse (Self-Hosted Compute)

ClickHouse is self-hosted (or managed via ClickHouse Cloud) -- cost is infrastructure (CPU, memory, disk), not per-query metering. The optimization target is making queries fast enough that the infrastructure you're already paying for can handle the workload.

> [!info] Documentation
> - [MergeTree engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
> - [Projections](https://clickhouse.com/docs/en/sql-reference/statements/alter/projection)

**MergeTree `ORDER BY`.** The primary cost lever. The `ORDER BY` clause in the MergeTree definition determines how data is physically sorted on disk. Queries that filter on the `ORDER BY` prefix skip entire granules (~8,192 rows) that don't match -- the ClickHouse equivalent of partition pruning. Choose the `ORDER BY` to match the most common consumer query pattern. Fixed at creation -- can't be changed without recreating the table.

**Compression.** ClickHouse compresses aggressively by default, and column types affect compression ratio. `LowCardinality(String)` for columns with a small number of distinct values (status fields, country codes, category names) replaces each value with a dictionary-encoded integer, reducing both storage and scan time. Apply it at table creation for columns with fewer than ~10,000 distinct values.

**Materialized views.** ClickHouse materialized views trigger on INSERT and pre-compute aggregations at write time -- a fundamentally different model from the others. The materialized result is always current with zero read-time overhead, which makes them ideal for dashboards that need real-time aggregations. The tradeoff is that the aggregation logic runs on every insert, adding load-time overhead.

**Projections.** Alternative physical orderings of the same data. If your table is `ORDER BY (event_date, event_type)` but some queries filter only on `user_id`, a projection ordered by `user_id` lets those queries skip granules efficiently. Multiple projections optimize multiple query patterns simultaneously, at the cost of additional storage and insert overhead.

## Redshift (Cluster Compute)

Redshift bills per node per hour (provisioned) or per RPU-second (Serverless). Provisioned clusters pay for the hardware regardless of utilization; Serverless pays per compute consumed. The optimization target depends on the model: provisioned optimizes for query efficiency (get more done on the same nodes), Serverless optimizes for query cost (reduce compute time per query).

> [!info] Documentation
> - [Sort keys](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
> - [Distribution styles](https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html)
> - [VACUUM](https://docs.aws.amazon.com/redshift/latest/dg/t_Reclaiming_storage_space202.html)
> - [Redshift Spectrum](https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html)

**Sort keys.** The equivalent of clustering -- they determine physical sort order on disk and enable block skipping on filtered queries. Compound sort keys work for range queries on a prefix of columns. Interleaved sort keys work for multi-column filters where queries might filter on any combination, at the cost of slower VACUUM. Fixed at creation -- changing requires a full table rebuild.

**Dist keys.** Control how data is distributed across nodes. When two tables are distributed on the same key (e.g., both `orders` and `order_lines` on `order_id`), JOINs between them don't need to redistribute data across the network -- co-located rows are already on the same node.

**VACUUM and ANALYZE.** After heavy DELETE or UPDATE operations, Redshift doesn't fully reclaim space automatically. Dead rows from deleted records inflate scans and waste I/O until VACUUM runs. ANALYZE updates the query planner's statistics. If your pipeline does heavy deletes (hard delete detection, merge patterns), schedule VACUUM as part of the post-load step.

**Spectrum.** Query data in S3 directly without loading it into the cluster. Useful for cold data that's too large or too infrequent to justify cluster storage. Spectrum bills per byte scanned (like BigQuery), so the optimization advice for S3-resident data is the same: partition, use Parquet, select only the columns you need.

## Cross-Engine Principles

**Partition by business date, cluster/sort by consumer filter columns.** Universal. The partition key controls which slices the engine reads; the cluster/sort key controls how efficiently it reads within those slices.

**Select only the columns you need.** Matters most on BigQuery (bytes scanned = money), still reduces I/O and speeds up queries on every engine.

**Monitor before optimizing.** Cost attribution from [[06-operating-the-pipeline/0603-cost-monitoring|0603]] tells you which tables and queries to focus on. Optimizing a table that costs $0.02/month is wasted effort.

**Set cost guardrails early.** BigQuery's per-day cost limits, Snowflake's resource monitors, Redshift's query monitoring rules -- every engine has a mechanism to cap runaway costs. Configure them before production, not after.

## Anti-Patterns

> [!danger] Don't apply BigQuery optimizations to Snowflake
> Reducing bytes scanned doesn't affect Snowflake's bill -- it's warehouse time that matters. Conversely, warehouse sizing is irrelevant on BigQuery's serverless model. Know which billing model you're optimizing for.

> [!danger] Don't optimize tables that aren't expensive
> A 10k-row lookup table costs fractions of a cent per query regardless of partitioning or clustering. Optimize what shows up in the top-10 cost report from [[06-operating-the-pipeline/0603-cost-monitoring|0603]].

> [!danger] Don't let unlimited retries run against a pay-per-scan engine
> A retry loop on BigQuery rescans the table on every attempt. 30 retries per minute on a 100GB table is 4.3TB scanned per hour -- $27/hour, $216 overnight. Set retry limits and per-day cost caps before the first production run.

## Related Patterns

- [[01-foundations-and-archetypes/0104-columnar-destinations|0104-columnar-destinations]] -- per-engine storage and DML mechanics
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- measure before optimizing
- [[07-serving-the-destination/0702-partitioning-for-consumers|0702-partitioning-for-consumers]] -- partition and cluster key selection
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- materialized views as a cost reduction tool
