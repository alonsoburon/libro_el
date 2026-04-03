---
title: "Destinations"
aliases: []
tags:
  - appendix
status: draft
created: 2026-03-06
updated: 2026-04-02
---

# Destinations

[[01-foundations-and-archetypes/0104-columnar-destinations|0104]] covers how columnar engines store, partition, and price data. [[07-serving-the-destination/0705-cost-optimization-by-engine|0705]] covers the cost levers once data is loaded. This page is the decision: which engine for which workload, and what to watch out for when running ECL pipelines against each one.

## Cost Model Comparison

| Engine | Billing model | What you optimize | Cost guardrails |
|---|---|---|---|
| BigQuery | Per TB scanned (on-demand) or slots (reservations) | Bytes scanned per query | Per-query and per-day byte limits, `require_partition_filter` |
| Snowflake | Per second of warehouse compute | Query runtime, warehouse idle time | Auto-suspend, resource monitors, warehouse sizing |
| ClickHouse | Self-hosted infrastructure (or ClickHouse Cloud RPU) | Query speed on fixed hardware | Infrastructure budget |
| Redshift | Per node per hour (provisioned) or RPU-second (Serverless) | Cluster utilization or query compute time | Query monitoring rules, WLM queues |
| PostgreSQL | Self-hosted or managed instance (RDS, Cloud SQL) | Instance size, connection count | Fixed monthly cost regardless of query volume |

---

## BigQuery

**Best for:** serverless pay-per-query, many ad-hoc consumers, Google Cloud native stacks.

**ECL strengths:**
- `require_partition_filter` is the only engine with query-cost enforcement built into the table definition -- consumers literally cannot full-scan without a partition predicate
- Copy jobs are free for same-region operations, which makes partition swap and staging swap patterns nearly zero-cost
- QUALIFY is native, so dedup views and compaction queries are clean single statements
- Per-day cost limits prevent runaway retry loops from burning through the budget overnight

**ECL weaknesses:**
- DML concurrency caps at 2 concurrent mutating statements per table, with up to 20 queued -- flood it and statements fail outright, which means parallel MERGE across many tables needs careful throttling
- Every MERGE or UPDATE rewrites entire partitions it touches, so a 10-row update across 30 dates triggers 30 full partition rewrites
- JSON columns can't load from Parquet -- use Avro or JSONL for tables with JSON fields
- 10,000 partition limit per table (4,000 per single job), which constrains daily-partitioned tables to ~27 years of history

> [!warning] Streaming insert visibility gap
> Rows inserted via the streaming buffer may be invisible to `EXPORT DATA` and table copy jobs for up to 90 minutes. If your pipeline chains a load step with an immediate copy or export, use batch load jobs (`bq load` / `LOAD DATA`) instead of streaming inserts.

The author's primary destination. BigQuery's cost model rewards the exact behavior ECL pipelines produce: partition-scoped writes, partition-filtered reads, and bulk loads over row-by-row DML.

---

## Snowflake

**Best for:** predictable budgets, multi-workload isolation, data sharing, semi-structured data.

**ECL strengths:**
- `VARIANT` handles arbitrary JSON natively with `:` path notation, no schema needed at load time
- `SWAP WITH` is atomic metadata-only swap -- staging swap completes in milliseconds regardless of table size
- Result cache returns identical queries within 24 hours at zero warehouse cost, which saves on repeated dashboard queries
- Micro-partition pruning is automatic and doesn't require explicit partition DDL

**ECL weaknesses:**
- `PRIMARY KEY` and `UNIQUE` constraints are not enforced -- they're metadata hints only, so deduplication is entirely your responsibility at the SQL level
- Grants don't survive `SWAP WITH` or `CREATE TABLE ... CLONE`, requiring `FUTURE GRANTS` on the schema or a re-grant step after every swap
- Reclustering costs warehouse credits and runs in the background; heavily mutated tables can accumulate significant reclustering charges
- No partition filter enforcement -- consumers can full-scan any table without warning, which makes cost attribution harder

Good for teams that need warehouse-level isolation between workloads: a small warehouse for ECL loads, a medium one for analyst queries, a large one for dashboard refreshes, each with its own auto-suspend and budget ceiling.

---

## ClickHouse

**Best for:** append-heavy analytical workloads, real-time dashboards, self-hosted control, extreme query speed on fixed hardware.

**ECL strengths:**
- Fastest raw INSERT throughput of any engine on this list -- bulk inserts into `MergeTree` engines are limited by disk I/O, not by the engine
- `ReplacingMergeTree` provides eventual deduplication on merge, which fits naturally with append-and-materialize patterns
- `REPLACE PARTITION` is atomic and operates at the partition level without rewriting other partitions
- Materialized views trigger on INSERT, enabling real-time pre-aggregation without a separate scheduling layer

**ECL weaknesses:**
- No ACID guarantees for mutations -- `ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE` are async, queued operations that execute during the next merge cycle
- Duplicates coexist in `ReplacingMergeTree` until the merge scheduler runs; `SELECT ... FINAL` forces read-time dedup but at a meaningful performance cost
- `ORDER BY` is fixed at table creation and cannot be changed without rebuilding the table
- Small frequent inserts cause "too many parts" errors -- batch aggressively (tens of thousands of rows minimum per insert)

ClickHouse works best as an append-only destination where you lean into the merge model rather than fighting it. If your workload is primarily appending event data and reading it through pre-built materialized views, ClickHouse is hard to beat on raw performance per dollar.

---

## Redshift

**Best for:** AWS-native shops with existing infrastructure, teams that want PostgreSQL-compatible SQL in a columnar engine.

**ECL strengths:**
- `COPY` from S3 is fast bulk load with automatic compression, and S3 is the natural staging area for AWS-based pipelines
- PostgreSQL dialect means familiar SQL for teams coming from transactional databases
- `MERGE` added in late 2023, same syntax as BigQuery/Snowflake
- Spectrum queries S3 data directly without loading, useful for cold-tier data that doesn't justify warehouse storage

**ECL weaknesses:**
- Sort keys and dist keys are fixed at table creation -- changing them requires a full table rebuild (`CREATE TABLE ... AS SELECT` + rename)
- `VACUUM` is required after heavy deletes; dead rows inflate scan time and storage until cleaned up
- Row-by-row `INSERT` is orders of magnitude slower than `COPY`, so every load path must stage through S3
- Hard limit of 1,600 columns per table, and type changes require table rebuilds

The legacy choice among columnar engines. Still viable for AWS shops already invested in the ecosystem, but BigQuery and Snowflake have moved ahead in ECL ergonomics -- particularly around DML flexibility, schema evolution, and operational overhead.

---

> [!tip] Don't overlook PostgreSQL as a destination
> For small-to-medium pipelines with fewer than ~100 tables, a PostgreSQL destination with real PK enforcement, transactional `TRUNCATE`, and cheap `INSERT ON CONFLICT` is simpler and more forgiving than any columnar engine. Constraint violations surface bugs immediately instead of silently landing duplicates. Schema changes are transactional. `TRUNCATE ... CASCADE` in a transaction gives you atomic full replace for free. The complexity tax of columnar engines only pays off when you need partition pruning, bytes-scanned billing, or warehouse-scale analytics. See [[01-foundations-and-archetypes/0107-corridors|0107]] for the Transactional -> Transactional corridor.

---

## Load Pattern Compatibility

How well each engine supports the load strategies from Part IV:

| Load strategy | BigQuery | Snowflake | ClickHouse | Redshift | PostgreSQL |
|---|---|---|---|---|---|
| Full replace ([[04-load-strategies/0401-full-replace\|0401]]) | Partition copy or `CREATE OR REPLACE` | `SWAP WITH` (metadata-only) | `EXCHANGE TABLES` | `TRUNCATE` + `COPY` in transaction | `TRUNCATE` + `INSERT` in transaction |
| Append-only ([[04-load-strategies/0402-append-only\|0402]]) | Free `INSERT` via load jobs | `COPY INTO` from stage | Native strength | `COPY` from S3 | Standard `INSERT` |
| Merge / upsert ([[04-load-strategies/0403-merge-upsert\|0403]]) | `MERGE` (rewrites partitions) | `MERGE` (warehouse time) | Not native -- use `ReplacingMergeTree` | `MERGE` or DELETE + INSERT | `INSERT ON CONFLICT` (real enforcement) |
| Append-and-materialize ([[04-load-strategies/0404-append-and-materialize\|0404]]) | `QUALIFY` dedup view, `CREATE OR REPLACE` compaction | `QUALIFY` dedup view, `CREATE TABLE ... AS` compaction | `ReplacingMergeTree` + `FINAL` | Subquery dedup view, `CREATE TABLE ... AS` compaction | Subquery dedup view, materialized view option |

---

## Decision Matrix

| Workload | Recommended | Why |
|---|---|---|
| Many ad-hoc analysts, pay-per-query | BigQuery | Cost scales with actual usage; partition filter enforcement protects the bill |
| Predictable budget, multi-team | Snowflake | Warehouse isolation, fixed compute costs, data sharing |
| Append-heavy, real-time dashboards | ClickHouse | Fastest inserts, materialized views on write |
| AWS-native, existing infrastructure | Redshift | Familiar PostgreSQL dialect, `COPY` from S3, Spectrum for cold data |
| Small team, PostgreSQL expertise | PostgreSQL | Cheapest, real constraint enforcement, transactional `TRUNCATE` |
| Mixed analytical + operational consumers | Snowflake or BigQuery + PostgreSQL | Columnar for analytics, transactional for point queries ([[04-load-strategies/0405-hybrid-append-merge|0405]]) |

> [!tip] Start with the load strategy, not the engine
> The decision matrix above is a starting point, but the more productive question is often: which load strategies does my pipeline need, and which engines support them cheaply? If every table can be fully replaced, all five engines work fine and the choice comes down to your cloud provider and team expertise. The engine choice starts to matter when you need high-concurrency MERGE, append-and-materialize with dedup views, or partition-level atomic swaps -- that's when the compatibility table above narrows the field.

---

## Related Patterns

- [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] -- Storage mechanics, partitioning, and engine behavior
- [[01-foundations-and-archetypes/0107-corridors|0107]] -- Transactional -> Columnar vs Transactional -> Transactional
- [[04-load-strategies/0405-hybrid-append-merge|0405]] -- Dual-destination pattern for mixed workloads
- [[07-serving-the-destination/0705-cost-optimization-by-engine|0705]] -- Engine-specific cost levers once data is loaded
- [[08-appendix/0801-sql-dialect-reference|0801]] -- Syntax differences across all engines
