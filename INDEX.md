---
title: "ECL Patterns"
tags:
  - index
status: outline
---

# ECL Patterns
### A practical guide to moving data between systems without losing your mind

---

## Proposed Structure

> [!info] How to use this outline
> Each item below is a planned chapter or pattern. Create files using the templates in `_templates/`. The author writes opinions and war stories, Claude Code fills in structure, SQL examples, and diagrams.

### Front Matter
- [[00-front-matter/0001-preface|0001-preface]]
- [[00-front-matter/0002-domain-model|0002-domain-model]] -- The shared fictional schema used in all SQL examples throughout the book

### Part I -- Foundations & Source Archetypes
- [[01-foundations-and-archetypes/0101-the-el-myth|0101-the-el-myth]] -- Why pure EL doesn't exist in practice
- [[01-foundations-and-archetypes/0102-what-is-conforming|0102-what-is-conforming]] -- What belongs in the C (and what doesn't)
- [[01-foundations-and-archetypes/0103-transactional-sources|0103-transactional-sources]] -- Row-oriented, mutable, ACID. The terrain: PostgreSQL, MySQL, SQL Server, SAP HANA
- [[01-foundations-and-archetypes/0104-columnar-destinations|0104-columnar-destinations]] -- Append-optimized, partitioned, cost-per-query. The terrain when you're loading into BigQuery, Snowflake, ClickHouse, Redshift
- [[01-foundations-and-archetypes/0105-the-lies-sources-tell|0105-the-lies-sources-tell]] -- Catalog of false assumptions (timestamps, PKs, deletes, schemas)
- [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106-hard-rules-soft-rules]] -- "Maximum 4" means 4 until it's 5. If the database doesn't enforce it, your pipeline can't trust it
- [[01-foundations-and-archetypes/0107-corridors|0107-corridors]] -- Transactional -> Columnar vs Transactional -> Transactional: same patterns, different trade-offs
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]] -- The fundamental tradeoff: perfectly stable clones require full replaces and low update frequency. Sometimes you sacrifice purity for immediacy
- [[01-foundations-and-archetypes/0109-idempotency|0109-idempotency]] -- Rerun = same result, or you have a bug. The property every pipeline must have; full replace gets it for free, incremental has to earn it

### Part II -- Full Replace Patterns
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- When incremental isn't worth it
- [[02-full-replace-patterns/0202-snapshot-append|0202-snapshot-append]] -- Append full periodic snapshots with `_snapshot_at`, deduplicate downstream. For external sources with no change tracking
- [[02-full-replace-patterns/0203-partition-swap|0203-partition-swap]] -- Drop + reload, atomic and idempotent
- [[02-full-replace-patterns/0204-staging-swap|0204-staging-swap]] -- Stage, validate, swap
#### The Gray Middle
- [[02-full-replace-patterns/0205-scoped-full-replace|0205-scoped-full-replace]] -- Full replace but only current + previous year. Bounded full scans for tables too big to reload entirely
- [[02-full-replace-patterns/0206-rolling-window-replace|0206-rolling-window-replace]] -- Drop and reload the last N days/months. Not incremental, not full -- just the hot zone
- [[02-full-replace-patterns/0207-sparse-table-extraction|0207-sparse-table-extraction]] -- Cross-product tables (SKU x Warehouse) where 90% of rows are zeros. Filter at extraction, but know the risks
- [[02-full-replace-patterns/0208-activity-driven-extraction|0208-activity-driven-extraction]] -- Use recent transactions to know which dimension combos are active, extract only those
- [[02-full-replace-patterns/0209-hash-based-change-detection|0209-hash-based-change-detection]] -- No `updated_at`? Hash the row, compare to last extraction. Only pull what changed
- [[02-full-replace-patterns/0210-partial-column-loading|0210-partial-column-loading]] -- When you can't or shouldn't extract all columns. How to do it explicitly and avoid the trap of consumers assuming the destination is complete

### Part III -- Incremental Extraction Patterns
- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- When `updated_at` lies, how to validate it, and when to run a periodic full replace
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- Track a high-water mark; extract only rows updated after the last run
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- Extract a fixed trailing window every run; no cursor, no state between runs
- [[03-incremental-patterns/0304-cursor-from-another-table|0304-cursor-from-another-table]] -- Borrowing a header's timestamp for detail extraction
- [[03-incremental-patterns/0305-sequential-id-cursor|0305-sequential-id-cursor]] -- No `updated_at` anywhere, but the PK is monotonic. `WHERE id > :last_id` detects inserts only
#### Complex Extraction Patterns
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- The row was there yesterday, today it's gone
- [[03-incremental-patterns/0307-open-closed-documents|0307-open-closed-documents]] -- Mutable drafts vs immutable posted documents. Split extraction by lifecycle state
- [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] -- Header-detail coupling when the detail has no signal of its own
- [[03-incremental-patterns/0309-late-arriving-data|0309-late-arriving-data]] -- Overlap windows and reprocessing for rows that land with timestamps in the past
- [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]] -- When `updated_at` doesn't fire on INSERT

### Part IV -- Load Strategies
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- Drop and reload. The simplest load strategy and the default. Partition swap, staging swap, and when to use each
- [[04-load-strategies/0402-append-only|0402-append-only]] -- Source is immutable, destination only grows. Pure INSERT, no MERGE, cheapest possible load in columnar
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- Match on a key, update if exists, insert if new. The workhorse -- and the most expensive operation in columnar engines
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- Append every version, deduplicate to current state with a view. The log is the table
- [[04-load-strategies/0405-hybrid-append-merge|0405-hybrid-append-merge]] -- Append raw to a log table, merge to a "current" table. Two destinations, one extraction
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- Checkpointing, partial failure recovery, idempotent loads. How to make the load step survive failures

### Part V -- The Conforming Playbook
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_extracted_at`, `_batch_id`, `_source_hash`
- [[05-conforming-playbook/0502-synthetic-keys|0502-synthetic-keys]] -- No PK, composite PK, unstable PK
- [[05-conforming-playbook/0503-type-casting-normalization|0503-type-casting-normalization]] -- Cross-engine type hell: booleans, decimals, and everything in between
- [[05-conforming-playbook/0504-null-handling|0504-null-handling]] -- NULL means NULL. Reflect the source, don't COALESCE
- [[05-conforming-playbook/0505-timezone-conforming|0505-timezone-conforming]] -- TZ stays TZ, naive stays naive. Don't make decisions that aren't in the data
- [[05-conforming-playbook/0506-charset-encoding|0506-charset-encoding]] -- Latin-1 source, UTF-8 destination. Let the library handle it
- [[0507-nested-data-and-json|0507-nested-data-and-json]] -- JSON columns from source: land as-is. Normalizing is transformation, not conforming

### Part VI -- Operating the Pipeline
#### Running
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- What to track beyond row counts
- [[06-operating-the-pipeline/0602-health-table|0602-health-table]] -- One row per table per run. The schema, the columns, and how to populate it
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- Per-table, per-query, per-consumer. Know where the money goes before the invoice arrives
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- "The data must be fresh by 8am." How to define, measure, and alert on freshness
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- Schema drift, row count drops, partial failures. Calibrate severity so not everything is an incident
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606-scheduling-and-dependencies]] -- `order_lines` can't land before `orders`. Orchestration patterns for DAGs with real dependencies
- [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]] -- Don't make the DBA want to kill you
- [[06-operating-the-pipeline/0608-tiered-freshness|0608-tiered-freshness]] -- Cold/warm/hot zones: weekly full for history, daily for current year, intraday incremental for freshness
#### Protecting
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- Schema drift, row counts, null rates, freshness. What to enforce and how
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- 0 rows returned successfully is not the same as a silent failure. Gate the load on extraction status
#### Recovering
- [[06-operating-the-pipeline/0611-backfill-strategies|0611-backfill-strategies]] -- Reloading 6 months without breaking prod
- [[06-operating-the-pipeline/0612-partial-failure-recovery|0612-partial-failure-recovery]] -- Half the batch loaded, the other half didn't. Now what?
- [[06-operating-the-pipeline/0613-duplicate-detection|0613-duplicate-detection]] -- Duplicates already landed. How to find them, quantify the damage, and deduplicate without losing data
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- Source count vs destination count. Row-level, hash-level, and aggregate reconciliation
- [[06-operating-the-pipeline/0615-recovery-from-corruption|0615-recovery-from-corruption]] -- A bad deploy corrupted 3 months of data. Identifying the blast radius and rebuilding

### Part VII -- Serving the Destination
- [[07-serving-the-destination/0701-dont-pre-aggregate|0701-dont-pre-aggregate]] -- Land the movements, build the photo downstream. Resist the pressure to transform at extraction
- [[07-serving-the-destination/0702-partitioning-for-consumers|0702-partitioning-for-consumers]] -- Partition landed data so downstream queries don't full-scan
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- Materialized views, scheduled queries, and pre-cooked tables for consumers who can't write efficient SQL
- [[07-serving-the-destination/0704-clustering-and-pruning|0704-clustering-and-pruning]] -- Cluster keys, partition filters, require_partition_filter. Make it physically impossible to accidentally scan 3TB
- [[07-serving-the-destination/0705-query-patterns-for-analysts|0705-query-patterns-for-analysts]] -- Cheat sheet: how to query append-only tables, how to get latest state, how to not blow up costs
- [[07-serving-the-destination/0706-cost-optimization-by-engine|0706-cost-optimization-by-engine]] -- Engine-specific strategies for keeping query costs under control
- [[07-serving-the-destination/0707-point-in-time-from-events|0707-point-in-time-from-events]] -- Reconstruct past state from event tables, not snapshots. Events are cheaper to store and replay than periodic copies of the full state
- [[07-serving-the-destination/0708-schema-naming-conventions|0708-schema-naming-conventions]] -- Table and column naming at the destination: as-is from source, snake_case, normalized? Pick a convention and apply it consistently

### Appendix
- [[08-appendix/0801-sql-dialect-reference|0801-sql-dialect-reference]] -- Syntax comparison across engines (PostgreSQL, MySQL, SQL Server, BigQuery, Snowflake, ClickHouse)
- [[08-appendix/0802-decision-flowchart|0802-decision-flowchart]] -- "I have X, use pattern Y"
- [[08-appendix/0803-glossary|0803-glossary]]
- [[08-appendix/0804-domain-model|0804-domain-model]] -- Shared fictional schema for all examples
#### Software Recommendations
- [[08-appendix/0805-orchestrators|0805-orchestrators]] -- Dagster, Airflow, Prefect: which one and why. Custom metadata, freshness policies, partition-native backfills, and the features that matter for ECL
- [[08-appendix/0806-extractors-and-loaders|0806-extractors-and-loaders]] -- dlt, Airbyte, Fivetran, custom Python: when to build vs buy
- [[08-appendix/0807-destinations|0807-destinations]] -- BigQuery, Snowflake, ClickHouse, Redshift: cost models, DML behavior, and what each does well for ECL workloads
