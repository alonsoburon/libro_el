---
title: "Extractors and Loaders"
aliases: []
tags:
  - appendix
status: draft
created: 2026-03-06
updated: 2026-04-02
---

# Extractors and Loaders

## The Spectrum

Extractor/loader tools sit on a spectrum from fully managed to fully custom. On the managed end, Fivetran handles everything -- connectors, scheduling, schema decisions, infrastructure -- and you accept whatever it decides. On the custom end, you write Python with SQLAlchemy, own every line, and maintain every failure mode. In between, Airbyte gives you managed connectors with more visibility into what they do, and dlt gives you a Python library that handles the plumbing while leaving schema control, deployment, and orchestration in your hands.

Where you belong on this spectrum depends on three things: how many sources you need to cover, how much control you need over the conforming layer ([[01-foundations-and-archetypes/0102-what-is-conforming|0102]]), and whether someone else's schema decisions are acceptable for your destination. If the answer to that last question is "no," managed tools are off the table.

## Comparison

| Tool | Type | Schema control | Incremental | Naming | Deployment | Best for |
|---|---|---|---|---|---|---|
| **Fivetran** | Fully managed | None -- Fivetran decides | Built-in cursors | Fivetran decides | SaaS | Teams without engineering capacity |
| **Airbyte** | Semi-managed | Limited -- normalization layer | Built-in per connector | Configurable | Cloud or self-hosted | SaaS sources (Salesforce, Stripe) |
| **dlt** | Python library | Full -- schema contracts, naming conventions | Cursor or stateless window | Configurable (`snake_case` default) | You deploy it | SQL sources, custom APIs, full control |
| **Custom Python** | Code | Total | You build it | You decide | You deploy it | Legacy/niche sources, extreme requirements |

---

## dlt

The library this book's author uses in production at scale. dlt handles type inference, schema evolution, staging, and destination-specific load formats while keeping you in control of extraction logic, naming, and schema policy. It sits at the sweet spot for SQL-heavy ECL workloads: enough abstraction to avoid reinventing loaders, enough control to implement every pattern in this book.

### Schema Contracts

dlt's schema contract system controls what happens when the source sends something unexpected -- a new table, a new column, or a type change on an existing column. Four modes: `evolve` (accept it), `freeze` (fail the pipeline), `discard_row` (silently drop the row), `discard_value` (silently drop the value).

The production pattern that works for ECL:

```python
schema_contract={"tables": "freeze", "columns": "evolve", "data_type": "freeze"}
```

Freeze tables so a source bug can't create junk tables in your destination. Evolve columns so new source columns land automatically. Freeze data types so a `VARCHAR` that suddenly arrives as `INT64` stops the pipeline instead of corrupting downstream queries.

> [!warning] `discard_row` and `discard_value` break the conforming boundary
> If the source sent it, the destination should have it. Silently dropping rows or values means your destination no longer mirrors the source -- you've introduced an invisible filter that nobody downstream knows about. These modes are useful for ingesting messy event streams where some rows are genuinely garbage, but for ECL workloads where the goal is a faithful clone, they violate the core contract. Stick to `evolve` and `freeze`. See [[01-foundations-and-archetypes/0102-what-is-conforming|0102]].

### Naming Conventions

dlt normalizes all identifiers through a naming convention before they reach the destination. The default is `snake_case` -- lowercased, ASCII only, special characters stripped. Other options include `duck_case` (case-sensitive Unicode), `direct` (preserve as-is), and SQL-safe variants (`sql_cs_v1`, `sql_ci_v1`).

This is a one-time decision with permanent consequences -- the same tradeoff described in [[07-serving-the-destination/0707-schema-naming-conventions|0707]]. Changing the convention after data exists is destructive: dlt re-normalizes already-normalized identifiers (it doesn't store the originals), which means every table and column name in your destination could change.

> [!danger] Silent key collisions
> Two source columns that normalize to the same identifier (`OrderID` and `order_id` both become `order_id` under `snake_case`) collide without warning. dlt doesn't detect this. One column silently overwrites the other. Audit your source schema before the first load.

### Destination Gotchas

Destination engines have format-specific limitations that dlt inherits:

- **BigQuery**: cannot load JSON columns from Parquet files -- the job fails permanently. Use JSONL or Avro for tables with JSON columns, or switch to staging via JSONL.
- **Snowflake**: `VARIANT` columns loaded from Parquet land as strings, not queryable JSON. Downstream queries need `PARSE_JSON()` to unwrap them. `PRIMARY KEY` and `UNIQUE` constraints are metadata-only and not enforced.
- **PostgreSQL**: the default `insert_values` loader generates large INSERT statements. Switching to the CSV loader (`COPY` command) is several times faster -- worth the configuration effort for any serious volume.

### Staging and Merge

For incremental loads, dlt supports a two-dataset pattern: load to a staging dataset first (`_staging` suffix), then merge into the production dataset. This keeps the loader's job simple -- dump data into a clean table -- and gives you a separate SQL step with full control over the merge logic, deduplication, and partition pruning.

Schema evolution happens via `ALTER TABLE ADD COLUMN` on the destination before the merge executes. One gotcha on BigQuery: the schema API returns legacy type names (`FLOAT`, `INTEGER`) but DDL requires standard SQL types (`FLOAT64`, `INT64`).

When multiple tables load simultaneously into the same staging dataset, parallel runs can race on dataset creation -- the second process gets a 409 conflict. Use `exists_ok=True` (or your destination's equivalent) when creating the staging dataset to make the operation idempotent.

### Stateless Operation

For maximum reliability, delete dlt's pipeline directory before every run. This prevents stale schema caches from causing 404 errors on staging tables that were cleaned up after the last merge, and stops pending packages from interrupted runs from being replayed against tables that no longer exist.

Even with a clean pipeline directory, dlt caches schema metadata in the destination itself (the `_dlt_version` table). If a staging table is deleted after merge but the destination-side cache survives, the next load skips table creation and writes to a nonexistent table. Use dlt's `refresh="drop_resources"` mechanism or delete cache entries before each load.

Combined with a stateless trailing-window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]), the pipeline has no persisted state between runs -- every execution is independent and idempotent.

### SQL Extraction

dlt uses SQLAlchemy as its universal connector for SQL sources, covering PostgreSQL, MySQL, SQL Server, and SAP HANA with a single API. No comparable alternative exists for multi-database extraction -- SQLAlchemy's dialect system means the same extraction code works against any supported engine with only a connection string change. Extraction backends include SQLAlchemy (default, universal), PyArrow (fast columnar reads), and ConnectorX (parallel reads for large tables). The backend choice affects which types are supported and how they're serialized, so test with your actual source schema before committing to one.

---

## Airbyte

Airbyte provides a catalog of managed connectors -- pre-built extractors for SaaS APIs (Salesforce, HubSpot, Stripe, Jira) and databases (PostgreSQL, MySQL). Each connector handles authentication, pagination, rate limiting, and incremental state. Available as a cloud service or self-hosted via Docker.

**Where it works well**: SaaS sources where you don't have direct database access and the API is the only option. Writing a Salesforce extractor from scratch means handling OAuth refresh, query pagination, bulk API vs REST API selection, and field-level security. Airbyte's connector does this, and when it works, it saves weeks. CDC support for PostgreSQL and MySQL is available through Debezium-backed connectors, which gives you change streams without managing Debezium infrastructure directly.

**Where it gets complicated**: Airbyte applies a normalization step after extraction -- flattening nested JSON, renaming columns, and creating sub-tables for arrays. This is a transformation step you may not want, sitting between your source and your destination without your explicit control. Connector quality varies significantly; some are maintained by Airbyte's core team, others by the community, and community connectors break on edge cases that the core team never tested. The self-hosted (OSS) version requires Docker infrastructure and has no built-in orchestration -- you schedule syncs externally or use the cloud tier, which imposes sync frequency minimums that may not match your freshness requirements.

> [!tip] Check the connector support level
> Airbyte classifies connectors as Generally Available, Beta, or Alpha. For production ECL pipelines, stick to GA connectors. Beta and Alpha connectors change their schemas across versions, which means your downstream queries break when Airbyte pushes an update.

---

## Fivetran

Fully managed, zero code, zero infrastructure. You authenticate a source, pick a destination, set a sync schedule, and Fivetran handles everything else. For teams without engineering capacity or for SaaS sources where the connector exists and works well, this is the fastest path to having data in your warehouse.

The tradeoff is control. Fivetran decides column types, naming conventions, and how to handle nested data. You can't inject metadata columns ([[05-conforming-playbook/0501-metadata-column-injection|0501]]), can't control the schema contract ([[06-operating-the-pipeline/0609-data-contracts|0609]]), and can't customize the merge strategy. What lands in your destination is what Fivetran decided, and if that decision is wrong for your use case, your only recourse is a support ticket.

Fivetran does add its own metadata columns (`_fivetran_synced`, `_fivetran_deleted`) and handles soft deletes for some connectors. These are useful but non-standard -- your downstream queries become Fivetran-aware, which creates coupling that matters if you ever migrate off the platform.

**Cost**: priced by Monthly Active Rows (MAR). Affordable for small volumes, expensive at scale -- a table that re-extracts 10 million rows monthly on a trailing window costs the same as 10 million unique rows. Sync frequency minimum is 5 minutes on the standard tier, 1 minute on business/enterprise. At scale -- hundreds or thousands of tables -- Fivetran's pricing becomes a serious constraint; the math works best when you have a few dozen high-value SaaS sources and the engineering team to maintain them doesn't exist.

---

## Custom Python + SQLAlchemy

When the source is niche enough that no connector exists -- a legacy ERP with a proprietary database, a vendor-specific API with no public documentation, a mainframe behind three layers of VPN -- you write it yourself.

SQLAlchemy is the universal connector for SQL sources. It covers PostgreSQL, MySQL, SQL Server, SAP HANA, and dozens of other databases with a unified API for connection management, query execution, and type introspection. For extraction specifically, three backends cover most needs:

- **SQLAlchemy** (universal): works everywhere, reasonable performance, handles all types.
- **PyArrow**: fast columnar reads, good for wide tables headed to columnar destinations. Doesn't handle every type (JSONB on PostgreSQL, for example).
- **ConnectorX**: parallel reads that saturate the network. Best for large tables where single-threaded extraction is the bottleneck.

> [!warning] Custom extractors accumulate
> Every custom extractor is a maintenance surface. After a year, you'll have 15 of them, each with slightly different error handling, slightly different retry logic, and slightly different assumptions about how types map. If you find yourself writing the third custom extractor, evaluate whether dlt or another library can absorb the common plumbing before the codebase becomes a collection of snowflakes.

The cost is everything else. Schema evolution, error handling, retry logic, state management, observability -- dlt and Airbyte handle these as features, and with custom code, they're your problem. You also own type mapping: deciding that a SQL Server `DATETIME2(7)` should land as `TIMESTAMP` in BigQuery (truncating nanoseconds to microseconds) is now an explicit choice you make in code, not something a library infers for you.

Worth it when no alternative exists or when the extraction logic is complex enough that a generic tool gets in the way. Most production pipelines end up with at least a few custom extractors for the sources that no tool covers.

---

## Decision Table

| Source type | Recommended | Why |
|---|---|---|
| Direct DB access, SQL sources | dlt or custom SQLAlchemy | Full control over extraction and conforming |
| SaaS APIs (Salesforce, Stripe) | Airbyte or Fivetran | Managed connectors handle auth, pagination, rate limits |
| File-based (S3, SFTP, CSV drops) | dlt or custom | Connector overhead not justified for file reads |
| Legacy/niche sources | Custom SQLAlchemy | No connector exists |
| Team without engineering capacity | Fivetran | Zero code, zero ops |

> [!tip] Mix and match
> Running dlt for your SQL sources and Fivetran for two SaaS APIs is a perfectly valid architecture. The destination doesn't care which tool loaded the data, as long as your naming convention and metadata columns are consistent across all of them.

## Related

- [[01-foundations-and-archetypes/0102-what-is-conforming|0102]] -- What belongs in the conforming layer
- [[05-conforming-playbook/0501-metadata-column-injection|0501]] -- Metadata columns that every loader should inject
- [[06-operating-the-pipeline/0609-data-contracts|0609]] -- Schema contracts and data quality gates
- [[07-serving-the-destination/0707-schema-naming-conventions|0707]] -- Naming conventions and why they're permanent
