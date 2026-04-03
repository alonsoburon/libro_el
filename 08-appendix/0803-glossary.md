---
title: "Glossary"
aliases: []
tags:
  - appendix
status: draft
created: 2026-03-06
updated: 2026-04-02
---

# Glossary

**Append-and-materialize** -- Load strategy that appends every extraction as new rows to a log table and deduplicates to current state via a view. Avoids MERGE cost on columnar engines. See [[04-load-strategies/0404-append-and-materialize|0404]].

**Backfill** -- Reloading a historical date range or an entire table to correct accumulated drift, recover from corruption, or onboard a new table. See [[06-operating-the-pipeline/0611-backfill-strategies|0611]].

**Batch ID (`_batch_id`)** -- Metadata column that correlates all rows from the same extraction run. Used for rollback, debugging, and reconciliation. See [[05-conforming-playbook/0501-metadata-column-injection|0501]].

**Cold tier** -- Freshness tier for historical data refreshed weekly or monthly via full replace. Acts as the purity safety net. See [[06-operating-the-pipeline/0608-tiered-freshness|0608]].

**Compaction** -- Collapsing an append log to one row per key, removing all historical versions. Always collapse-to-latest (`QUALIFY ROW_NUMBER() = 1`), never trim-by-date. See [[04-load-strategies/0404-append-and-materialize|0404]].

**Conforming** -- Everything the data needs to survive the crossing between source and destination: type casting, metadata injection, null handling, charset encoding, key synthesis. If it changes business meaning, it belongs downstream. See [[01-foundations-and-archetypes/0102-what-is-conforming|0102]].

**Corridor** -- The combination of source type and destination type. Transactional -> Columnar (e.g. PostgreSQL -> BigQuery) or Transactional -> Transactional (e.g. PostgreSQL -> PostgreSQL). Same pattern, different trade-offs. See [[01-foundations-and-archetypes/0107-corridors|0107]].

**Cursor** -- A high-water mark (typically `MAX(updated_at)` or `MAX(id)`) used to extract only rows that changed since the last run. See [[03-incremental-patterns/0302-cursor-based-extraction|0302]].

**Data contract** -- Explicit, checkable rules at the boundary between source and destination: schema shape, volume range, null rates, freshness. See [[06-operating-the-pipeline/0609-data-contracts|0609]].

**Dedup view** -- A SQL view over an append log that uses `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _extracted_at DESC) = 1` to expose only the latest version of each row. See [[04-load-strategies/0404-append-and-materialize|0404]].

**ECL** -- Extract, Conform, Load. The framework this book documents. The C handles type casting, metadata injection, null handling, key synthesis -- everything the data needs to land correctly. See [[01-foundations-and-archetypes/0101-the-el-myth|0101]].

**EL** -- Extract-Load with zero transformation. The theoretical ideal that never survives contact with real systems. See [[01-foundations-and-archetypes/0101-the-el-myth|0101]].

**Evolve** -- Schema policy that accepts new columns from the source and adds them to the destination automatically. The recommended default for most tables. See [[06-operating-the-pipeline/0609-data-contracts|0609]].

**Extracted at (`_extracted_at`)** -- Metadata column recording when the pipeline pulled the row, not when the source last modified it. Foundation for dedup ordering in append-and-materialize. See [[05-conforming-playbook/0501-metadata-column-injection|0501]].

**Extraction gate** -- A check between extraction and load that blocks the load when the result looks implausible (0 rows from a table that normally has data, row count outside expected range). See [[06-operating-the-pipeline/0610-extraction-status-gates|0610]].

**Freeze** -- Schema policy that rejects any schema change and fails the load. Reserved for tables with stable, critical schemas. See [[06-operating-the-pipeline/0609-data-contracts|0609]].

**Freshness** -- How recently the destination reflects the source. The other end of the purity tradeoff. See [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]].

**Full replace** -- Drop and reload the entire table on every run. Stateless, idempotent, catches everything. The default until the table outgrows the scan window. See [[02-full-replace-patterns/0201-full-scan-strategies|0201]].

**Hard delete** -- A source row that was physically removed. Invisible to any cursor-based extraction. Requires a separate detection mechanism. See [[03-incremental-patterns/0306-hard-delete-detection|0306]].

**Hard rule** -- A constraint enforced by the database: PK, UNIQUE, NOT NULL, FK, CHECK. If the system rejects violations at write time, it's hard. See [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]].

**Health table** -- Append-only table with one row per table per pipeline run, capturing raw measurements (row counts, timing, status, schema fingerprint). See [[06-operating-the-pipeline/0602-the-health-table|0602]].

**Hot tier** -- Freshness tier for actively changing data refreshed multiple times per day via incremental extraction. See [[06-operating-the-pipeline/0608-tiered-freshness|0608]].

**Idempotent** -- A pipeline that produces the same destination state whether it runs once or ten times with the same input. Full replace gets it for free; incremental has to earn it. See [[01-foundations-and-archetypes/0109-idempotency|0109]].

**Metadata columns** -- Columns injected during extraction that don't exist in the source: `_extracted_at`, `_batch_id`, `_source_hash`. See [[05-conforming-playbook/0501-metadata-column-injection|0501]].

**Open document** -- A record that can still be modified (e.g. draft invoice, pending order). Contrast with closed document. See [[03-incremental-patterns/0307-open-closed-documents|0307]].

**Closed document** -- A record that is immutable (e.g. posted invoice). In many jurisdictions, modifying a closed invoice is illegal. See [[03-incremental-patterns/0307-open-closed-documents|0307]].

**Partition swap** -- Replace data at partition granularity without touching the rest of the table. See [[02-full-replace-patterns/0202-partition-swap|0202]].

**Purity** -- The degree to which the destination is an exact clone of the source at a given point in time. Full replace maximizes it; incremental carries purity debt. See [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]].

**QUALIFY** -- SQL clause that filters directly on window functions without a subquery. Native on BigQuery, Snowflake, ClickHouse. Not supported on PostgreSQL, MySQL, SQL Server, Redshift. See [[08-appendix/0801-sql-dialect-reference|0801]].

**Reconciliation** -- Post-load verification that the destination matches the source: row count comparison, aggregate checks, hash comparison. See [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]].

**Schema policy** -- How the pipeline responds when the source schema changes. Two valid modes in ECL: evolve (accept) or freeze (reject). See [[06-operating-the-pipeline/0609-data-contracts|0609]].

**Scoped full replace** -- Full-replace semantics applied to a declared scope (e.g. current year) while historical data outside the scope is frozen. See [[02-full-replace-patterns/0204-scoped-full-replace|0204]].

**SLA** -- Service Level Agreement. Four components: table/group, freshness target, deadline, measurement point. See [[06-operating-the-pipeline/0604-sla-management|0604]].

**Soft rule** -- A business expectation with no database enforcement. "Quantities are always positive," "only open invoices get deleted." Your pipeline must survive these being wrong. See [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]].

**Source hash (`_source_hash`)** -- Hash of all business columns at extraction time. Enables change detection without relying on `updated_at`. See [[05-conforming-playbook/0501-metadata-column-injection|0501]], [[02-full-replace-patterns/0208-hash-based-change-detection|0208]].

**Staging swap** -- Load into a staging table, validate, then atomically swap to production. Zero downtime, trivial rollback. See [[02-full-replace-patterns/0203-staging-swap|0203]].

**Stateless window** -- Extract a fixed trailing window on every run with no cursor state between runs. The default incremental approach for most tables. See [[03-incremental-patterns/0303-stateless-window-extraction|0303]].

**Synthetic key (`_source_key`)** -- A hash of immutable business columns, used as the MERGE key when the source has no stable primary key. See [[05-conforming-playbook/0502-synthetic-keys|0502]].

**Tiered freshness** -- Splitting a pipeline into hot, warm, and cold tiers so tables are refreshed at the cadence that matches their consumption, not at a uniform schedule. See [[06-operating-the-pipeline/0608-tiered-freshness|0608]].

**Warm tier** -- Freshness tier for recent data refreshed daily, typically overnight. The purity layer that catches what the hot tier missed. See [[06-operating-the-pipeline/0608-tiered-freshness|0608]].
