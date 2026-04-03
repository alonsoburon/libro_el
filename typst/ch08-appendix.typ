#import "theme.typ": ecl-danger, ecl-info, ecl-theme, ecl-tip, ecl-warning, gruvbox
#show: ecl-theme
= SQL Dialect Reference
<sql-dialect-reference>
The lookup table for every operation that differs between engines. When a pattern in the book says "syntax varies by engine," it points here. Six engines are covered: PostgreSQL, MySQL, and SQL Server as sources and transactional destinations; BigQuery, Snowflake, ClickHouse, and Redshift as columnar destinations.

#strong[Quick nav]

- \#Identifier Quoting and Case Sensitivity
- \#Timestamp and Datetime Types
- \#Date and Time Functions
- MERGE
- \#Append and Materialize
- \#Table Swap
- \#Partition Operations
- \#Partition and Clustering DDL
- \#Deduplication (QUALIFY vs Subquery)
- \#Bulk Loading
- \#JSON and Semi-Structured Data
- \#Schema Evolution
- \#Source-Specific Traps
- \#Engine Quirks

// ---

== Identifier Quoting and Case Sensitivity
<identifier-quoting-and-case-sensitivity>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto, auto, auto, auto),
    table.header([Engine], [Default case], [Quote character], [Example]),
    table.hline(),
    [PostgreSQL], [Folds to lowercase], [`"double quotes"`], [`"OrderID"` preserves case],
    [MySQL],
    [Case depends on OS (Linux: sensitive, Windows: insensitive)],
    [#raw("`backticks`");],
    [#raw("`Order ID`");],
    [SQL Server], [Case-insensitive (collation-dependent)], [`[brackets]` or `"double quotes"`], [`[Order ID]`],
    [BigQuery], [Case-sensitive], [#raw("`backticks`");], [#raw("`project.dataset.table`");],
    [Snowflake], [Folds to uppercase], [`"double quotes"`], [`"order_id"` preserves lowercase],
    [ClickHouse], [Case-sensitive], [#raw("`backticks`") or `"double quotes"`], [Names preserved exactly],
    [Redshift], [Folds to lowercase], [`"double quotes"`], [Same as PostgreSQL],
  )],
  kind: table,
)

See 0707 for naming strategy.

// ---

== Timestamp and Datetime Types
<timestamp-and-datetime-types>
#figure(
  align(center)[#table(
    columns: (12.5%, 12.5%, 12.5%, 12.5%, 12.5%, 12.5%, 12.5%, 12.5%),
    align: (auto, auto, auto, auto, auto, auto, auto, auto),
    table.header([Type], [PostgreSQL], [MySQL], [SQL Server], [BigQuery], [Snowflake], [ClickHouse], [Redshift]),
    table.hline(),
    [Naive (no TZ)],
    [`TIMESTAMP`],
    [`DATETIME`],
    [`DATETIME2(n)`],
    [--],
    [`TIMESTAMP_NTZ`],
    [`DateTime`],
    [`TIMESTAMP`],
    [Aware (with TZ)],
    [`TIMESTAMPTZ`],
    [--],
    [`DATETIMEOFFSET`],
    [`TIMESTAMP`],
    [`TIMESTAMP_TZ`],
    [`DateTime64` with tz],
    [`TIMESTAMPTZ`],
    [Max precision],
    [Microseconds],
    [Microseconds],
    [100 nanoseconds],
    [Microseconds],
    [Nanoseconds],
    [Nanoseconds],
    [Microseconds],
  )],
  kind: table,
)

#ecl-warning(
  "BigQuery has no naive datetime",
)[Every `TIMESTAMP` in BigQuery is UTC. Naive timestamps from the source land as UTC -- if they were actually in `America/Santiago` or `Europe/Berlin`, every value is wrong from the moment it lands. Conform timezone info during load. See 0505.]

#ecl-warning(
  "DATETIME2 precision truncates on load",
)[SQL Server DATETIME2(7) 100-nanosecond precision truncates to microseconds on BigQuery and Redshift. Snowflake's `TIMESTAMP_NTZ(9)` and ClickHouse's `DateTime64(7)` can preserve it.]

See 0503 for the full type mapping.

// ---

== Date and Time Functions
<date-and-time-functions>
#figure(
  align(center)[#table(
    columns: (14.29%, 14.29%, 14.29%, 14.29%, 14.29%, 14.29%, 14.29%),
    align: (auto, auto, auto, auto, auto, auto, auto),
    table.header([Operation], [PostgreSQL], [MySQL], [SQL Server], [BigQuery], [Snowflake], [ClickHouse]),
    table.hline(),
    [Subtract interval],
    [`date - INTERVAL '1 day'`],
    [`DATE_SUB(d, INTERVAL 1 DAY)`],
    [`DATEADD(day, -1, d)`],
    [`DATE_SUB(d, INTERVAL 1 DAY)`],
    [`DATEADD(day, -1, d)`],
    [`d - INTERVAL 1 DAY`],
    [Add interval],
    [`date + INTERVAL '1 day'`],
    [`DATE_ADD(d, INTERVAL 1 DAY)`],
    [`DATEADD(day, 1, d)`],
    [`DATE_ADD(d, INTERVAL 1 DAY)`],
    [`DATEADD(day, 1, d)`],
    [`d + INTERVAL 1 DAY`],
    [Truncate to month],
    [`date_trunc('month', d)`],
    [`DATE_FORMAT(d, '%Y-%m-01')`],
    [`DATEFROMPARTS(YEAR(d), MONTH(d), 1)`],
    [`DATE_TRUNC(d, MONTH)`],
    [`DATE_TRUNC('month', d)`],
    [`toStartOfMonth(d)`],
    [Difference (days)],
    [`d2 - d1` (returns integer)],
    [`DATEDIFF(d2, d1)`],
    [`DATEDIFF(day, d1, d2)`],
    [`DATE_DIFF(d2, d1, DAY)`],
    [`DATEDIFF(day, d1, d2)`],
    [`dateDiff('day', d1, d2)`],
    [Extract part],
    [`EXTRACT(YEAR FROM d)`],
    [`EXTRACT(YEAR FROM d)`],
    [`DATEPART(year, d)` or `YEAR(d)`],
    [`EXTRACT(YEAR FROM d)`],
    [`EXTRACT(YEAR FROM d)` or `YEAR(d)`],
    [`toYear(d)`],
    [Current timestamp],
    [`NOW()` or `CURRENT_TIMESTAMP`],
    [`NOW()` or `CURRENT_TIMESTAMP`],
    [`GETDATE()` or `SYSDATETIME()`],
    [`CURRENT_TIMESTAMP()`],
    [`CURRENT_TIMESTAMP()`],
    [`now()`],
  )],
  kind: table,
)

#ecl-warning(
  "DATEDIFF argument order varies",
)[MySQL and BigQuery put `DATEDIFF(end, start)`. SQL Server, Snowflake, and ClickHouse put the unit first: `DATEDIFF(day, start, end)`. PostgreSQL skips the function entirely and uses subtraction. Getting the argument order wrong produces results with the wrong sign.]

// ---

== Upsert / MERGE
<upsert-merge>
#strong[BigQuery / Snowflake / SQL Server]

```sql
MERGE INTO orders AS tgt
USING _stg_orders AS src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN
  UPDATE SET
    tgt.status = src.status,
    tgt.total = src.total,
    tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, status, total, created_at, updated_at)
  VALUES (src.order_id, src.status, src.total, src.created_at, src.updated_at);
```

#strong[PostgreSQL]

```sql
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON CONFLICT (order_id)
DO UPDATE SET
  status = EXCLUDED.status,
  total = EXCLUDED.total,
  updated_at = EXCLUDED.updated_at;
```

#strong[MySQL]

```sql
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  total = VALUES(total),
  updated_at = VALUES(updated_at);
```

#strong[ClickHouse] -- no native upsert. Use `ReplacingMergeTree` with eventual dedup on merge, or append + deduplicate. See 0404.

#strong[Redshift] -- `MERGE` added in late 2023, same syntax as Snowflake/BigQuery. For older clusters or performance-sensitive loads, the classic pattern is DELETE + INSERT in a transaction.

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto, auto),
    table.header([Engine], [Duplicate key in staging]),
    table.hline(),
    [BigQuery], [Runtime error if multiple source rows match one destination row],
    [Snowflake], [Processes both rows, nondeterministic -- undefined which wins],
    [PostgreSQL], [Processes rows in insertion order, last one wins],
    [SQL Server], [Runtime error on multiple matches (same as BigQuery)],
  )],
  kind: table,
)

See 0403 for cost analysis and when to use MERGE vs alternatives.

// ---

== Append and Materialize
<append-and-materialize>
The alternative to MERGE on columnar engines: append every extraction to a log table, deduplicate with a view. Load cost drops to near-zero (pure INSERT), and the dedup cost shifts to read time.

#strong[Append to log (all engines)]

```sql
INSERT INTO orders_log
SELECT *, CURRENT_TIMESTAMP AS _extracted_at
FROM _stg_orders;
```

#strong[Dedup view -- BigQuery / Snowflake / ClickHouse]

```sql
CREATE OR REPLACE VIEW orders AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

#strong[Dedup view -- PostgreSQL / MySQL / SQL Server / Redshift]

```sql
CREATE OR REPLACE VIEW orders AS
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY _extracted_at DESC
    ) AS rn
  FROM orders_log
) sub
WHERE rn = 1;
```

#strong[Compaction -- collapse to latest-only (BigQuery / Snowflake / ClickHouse)]

```sql
CREATE OR REPLACE TABLE orders_log AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

Keeps exactly one row per key regardless of age -- safe with any extraction strategy. All version history is gone, but every current row survives. On engines without `QUALIFY`, use the subquery wrapper inside the `CREATE TABLE ... AS SELECT`.

See 0404 for the full pattern, cost tradeoffs, and retention sizing.

// ---

== Table Swap
<table-swap>
#strong[Snowflake]

```sql
ALTER TABLE stg_orders SWAP WITH orders;
```

Atomic, metadata-only. Grants do #strong[not] carry over -- they follow the table name, not the data. Re-grant after every swap, or use `FUTURE GRANTS` on the schema.

#strong[BigQuery]

```bash
# Copy job: works across datasets, free for same-region
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_orders \
  project:dataset.orders
```

```sql
-- DDL rename: same dataset only, brief unavailability window
ALTER TABLE `project.dataset.orders` RENAME TO orders_old;
ALTER TABLE `project.dataset.stg_orders` RENAME TO orders;
DROP TABLE IF EXISTS `project.dataset.orders_old`;
```

`ALTER TABLE RENAME TO` does not cross dataset boundaries. Use the copy job for cross-dataset swaps or when consumers can't tolerate unavailability.

#strong[PostgreSQL / Redshift]

```sql
BEGIN;
ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE stg_orders RENAME TO orders;
DROP TABLE orders_old;
COMMIT;
```

Atomic within the transaction. If the transaction rolls back, `orders` is untouched.

#strong[ClickHouse]

```sql
EXCHANGE TABLES stg_orders AND orders;
```

Atomic swap of both table names. The old production data moves to `stg_orders` after the swap.

See 0203 for the full pattern.

// ---

== Partition Operations
<partition-operations>
#strong[BigQuery -- partition copy]

```bash
# Near-metadata operation, orders of magnitude faster than DML
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_events$20260307 \
  project:dataset.events$20260307
```

Staging must be partitioned by the same column and type as the destination. One copy per partition, but each copy is near-free.

#strong[Snowflake / Redshift -- DELETE + INSERT in transaction]

```sql
BEGIN;
DELETE FROM events
WHERE partition_date BETWEEN :start_date AND :end_date;
INSERT INTO events SELECT * FROM stg_events;
COMMIT;
```

Delete by the declared range, not by what's in staging. If Saturday had rows last run and the source corrected them to Friday, a staging-driven delete would leave stale Saturday data in place.

#strong[ClickHouse -- REPLACE PARTITION]

```sql
ALTER TABLE events REPLACE PARTITION '2026-03-07' FROM stg_events;
ALTER TABLE events REPLACE PARTITION '2026-03-08' FROM stg_events;
```

Atomic per partition, operates at the partition level without rewriting rows.

See 0202 for the full pattern.

// ---

== Partition and Clustering DDL
<partition-and-clustering-ddl>
#strong[BigQuery]

```sql
CREATE TABLE `project.dataset.events` (
  event_id STRING,
  event_type STRING,
  event_date DATE,
  payload JSON
)
PARTITION BY event_date
CLUSTER BY event_type
OPTIONS (require_partition_filter = true);
```

Up to 4 cluster columns. `require_partition_filter` rejects queries without a partition filter -- mandatory cost protection on large tables. Limit: 10,000 partitions per table, 4,000 per job.

#strong[Snowflake]

```sql
CREATE TABLE events (
  event_id VARCHAR,
  event_type VARCHAR,
  event_date DATE,
  payload VARIANT
)
CLUSTER BY (event_date, event_type);
```

Snowflake has no traditional partitions -- micro-partitions are managed automatically. Clustering keys guide the physical layout. Snowflake auto-reclusters in the background (costs warehouse time).

#strong[ClickHouse]

```sql
CREATE TABLE events (
  event_id String,
  event_type String,
  event_date Date,
  payload String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id);
```

`ENGINE` is required. `ORDER BY` is the primary sort key and cannot be changed after creation. `PARTITION BY` supports expressions (`toYYYYMM`, `toDate`).

#strong[Redshift]

```sql
CREATE TABLE events (
  event_id VARCHAR,
  event_type VARCHAR,
  event_date DATE,
  payload SUPER
)
SORTKEY (event_date)
DISTSTYLE KEY
DISTKEY (event_id);
```

Sort keys and dist keys are fixed at creation -- changing them requires a full table rebuild. Sort key serves the role of a partition/cluster key for scan pruning. Dist key controls how data distributes across nodes for join performance.

See 0104 for storage mechanics and 0702 for key selection.

// ---

== Deduplication (QUALIFY vs Subquery)
<deduplication-qualify-vs-subquery>
#strong[BigQuery / Snowflake / ClickHouse -- QUALIFY]

```sql
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY _extracted_at DESC
) = 1;
```

#strong[PostgreSQL / MySQL / SQL Server / Redshift -- subquery wrapper]

```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY _extracted_at DESC
    ) AS rn
  FROM orders_log
) sub
WHERE rn = 1;
```

Same result, different syntax. `QUALIFY` filters directly on window functions without a subquery. Engines that don't support it need the subquery wrapper.

See 0404 for dedup views and 0613 for detection patterns.

// ---

== Bulk Loading
<bulk-loading>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto, auto, auto, auto),
    table.header([Engine], [Primary method], [Preferred format], [Key constraint]),
    table.hline(),
    [BigQuery],
    [`bq load` / `LOAD DATA` / streaming],
    [Avro (handles JSON natively)],
    [JSON columns can't load from Parquet],
    [Snowflake], [`COPY INTO` from stage], [Parquet], [`VARIANT` from Parquet lands as string, needs `PARSE_JSON`],
    [ClickHouse],
    [`INSERT INTO ... SELECT` (batch)],
    [Parquet or native format],
    [Small inserts cause too-many-parts; batch aggressively],
    [Redshift], [`COPY` from S3], [Parquet], [Row-by-row INSERT is orders of magnitude slower than COPY],
    [PostgreSQL], [`COPY` / `\copy`], [CSV or binary], [Binary is faster but not human-readable],
    [MySQL], [`LOAD DATA INFILE`], [CSV], [`LOAD DATA LOCAL INFILE` has security restrictions],
  )],
  kind: table,
)

See 0104 for format compatibility and gotchas.

// ---

== JSON and Semi-Structured Data
<json-and-semi-structured-data>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto, auto, auto, auto),
    table.header([Engine], [Native type], [Load from Parquet?], [Query syntax]),
    table.hline(),
    [BigQuery], [`JSON`], [No -- use JSONL or Avro], [`JSON_VALUE(col, '$.key')`, dot notation],
    [Snowflake], [`VARIANT`], [Lands as string, needs `PARSE_JSON`], [`col:key::type`, `:` path notation],
    [ClickHouse], [`String` (parse with functions)], [Yes (as string)], [`JSONExtractString(col, 'key')`],
    [Redshift], [`SUPER`], [Yes], [PartiQL syntax: `col.key`],
    [PostgreSQL], [`JSONB` / `JSON`], [N/A (not a bulk load format)], [`col->>'key'`, `col @> '{}'` operators],
    [MySQL], [`JSON`], [N/A], [`JSON_EXTRACT(col, '$.key')`, `col->>'$.key'`],
  )],
  kind: table,
)

See 0507 for conforming strategy.

// ---

== Schema Evolution
<schema-evolution>
#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto, auto, auto, auto, auto),
    table.header([Operation], [BigQuery], [Snowflake], [ClickHouse], [Redshift]),
    table.hline(),
    [ADD COLUMN], [Instant], [Fast (metadata)], [Metadata-only for MergeTree], [Cheap if added at end],
    [Type widening],
    [Compatible pairs (`INT64` → `NUMERIC`)],
    [VARCHAR width increase OK],
    [Some widening via `MODIFY COLUMN`],
    [Requires full table rebuild],
    [DROP COLUMN], [Destructive (breaks `SELECT *` downstream)], [Fast], [Supported], [Supported],
    [Column limit], [10,000], [No hard limit], [No hard limit], [1,600],
    [Key limitation],
    [--],
    [CLONE picks up new columns automatically],
    [`ORDER BY` key fixed at creation],
    [Sort/dist keys fixed at creation],
  )],
  kind: table,
)

See 0104 for full details and 0609 for schema policies.

// ---

== Source-Specific Traps
<source-specific-traps>
- #strong[PostgreSQL];: `TIMESTAMP` vs `TIMESTAMPTZ` confusion -- both exist, applications mix them. TOAST compression on large columns can slow extraction on wide tables.
- #strong[MySQL];: `utf8` is 3-byte UTF-8, not real UTF-8. `utf8mb4` is real UTF-8. If the source uses `utf8`, you might be getting truncated data. `DATETIME` has no timezone at all. `TINYINT(1)` is commonly used as a boolean but it's still an integer.
- #strong[SQL Server];: `WITH (NOLOCK)` avoids blocking writers during extraction but reads dirty data (rows mid-transaction). `DATETIME2(7)` nanosecond precision truncates on most destinations. Getting read access to a production SQL Server often involves procurement, security reviews, and a DBA who has 47 other priorities.
- #strong[SAP HANA];: Proprietary SQL dialect. Legally restricted access to some tables (S/4HANA). Varies by SAP module -- extraction patterns that work for B1 may not apply to S/4. If you're extracting from SAP, you already know.

See 0103 for the full terrain.

// ---

== Engine Quirks
<engine-quirks>
#strong[BigQuery] - DML concurrency: max 2 mutating statements per table concurrently, up to 20 queued. Flood it and statements fail outright - Every DML rewrites entire partitions it touches -- 10K rows across 30 dates = 30 full partition rewrites - Copy jobs are free for same-region operations - Streaming inserts: rows may be briefly invisible to `EXPORT DATA` and table copies (typically minutes, up to 90)

#strong[Snowflake] - `PRIMARY KEY` and `UNIQUE` constraints are not enforced -- they're metadata hints only. Deduplication is your problem - `VARIANT` from Parquet loads as string, not queryable JSON, until you `PARSE_JSON` - Result cache: identical queries within 24h return cached results at no warehouse cost - Grants don't survive `SWAP WITH` or `CREATE TABLE ... CLONE`

#strong[ClickHouse] - `ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE` are async -- they return immediately, actual work happens during the next merge - `ReplacingMergeTree` deduplicates on merge, not on insert. Duplicates coexist until the merge scheduler runs. `SELECT ... FINAL` forces read-time dedup at a performance cost - Small inserts cause a "too many parts" error. Batch inserts into blocks of at least tens of thousands of rows - `ENGINE` is required in every `CREATE TABLE`. `ORDER BY` is fixed at creation

#strong[Redshift] - `COPY` from S3 is the only performant bulk load. Row-by-row `INSERT` is orders of magnitude slower - `VACUUM` is required after heavy deletes -- dead rows inflate scan time and storage until cleaned up - Sort keys and dist keys are fixed at creation. Changing them requires a full table rebuild - Hard limit of 1,600 columns per table

See 0104 for full engine profiles and 0705 for cost levers.

// ---

= Decision Flowchart
<decision-flowchart>
Three decisions drive every ECL pipeline: how to extract, how to load, and how often to refresh. These flowcharts walk through each one, then map every table in the domain model to its recommended pattern combination.

== Extraction Strategy
<extraction-strategy>
\// TODO: Convert mermaid diagram to Typst or embed as SVG

The default path is the shortest: if the table fits a full scan, use full replace and stop thinking. Every branch to the right adds complexity that should be earned, not assumed.

== Load Strategy
<load-strategy>
\// TODO: Convert mermaid diagram to Typst or embed as SVG

On transactional destinations, MERGE is cheap -- use it by default. On columnar destinations, append-and-materialize avoids the per-run MERGE cost and shifts deduplication to read time or a scheduled compaction job.

== Freshness Tier
<freshness-tier>
\// TODO: Convert mermaid diagram to Typst or embed as SVG

See 0608 for the full framework.

== Domain Model Mapping
<domain-model-mapping>
Every table in the domain model mapped to its recommended extraction, load, and freshness pattern:

#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto, auto, auto, auto, auto),
    table.header([Table], [Extraction], [Load], [Freshness], [Why]),
    table.hline(),
    [`orders`],
    [Stateless window 7d (0303)],
    [Append-and-materialize (0404)],
    [Hot + warm nightly reset],
    [`updated_at` unreliable, hard deletes unlikely, high mutation rate],
    [`order_lines`],
    [Cursor from header (0304)],
    [Same as `orders`],
    [Same schedule as `orders`],
    [No own timestamp, borrows from `orders`],
    [`customers`],
    [Full replace (0201)],
    [Full replace (0401)],
    [Warm (daily)],
    [Dimension table, changes across full history, small enough to scan],
    [`products`],
    [Full replace (0201)],
    [Full replace (0401)],
    [Warm (daily)],
    [Schema mutates, full replace catches everything],
    [`invoices`],
    [Open/closed split (0307)],
    [Merge (0403)],
    [Hot for open, cold for closed],
    [Hard deletes on open invoices, closed invoices frozen],
    [`invoice_lines`],
    [Open/closed from header (0307) + detail handling (0308)],
    [Same as `invoices`],
    [Same schedule as `invoices`],
    [Independent status changes, hard deletes not just cascade],
    [`events`],
    [Sequential ID cursor (0305)],
    [Append-only (0402)],
    [Hot],
    [Append-only, partitioned by date, never updated],
    [`sessions`],
    [Sequential ID or `created_at` cursor],
    [Append-only (0402)],
    [Hot],
    [Late-arriving events need wider window (0309)],
    [`metrics_daily`],
    [Scoped full replace (0204)],
    [Partition swap (0202)],
    [Warm (daily)],
    [Pre-aggregated, overwritten daily, partition-aligned],
    [`inventory`],
    [Activity-driven (0207)],
    [Staging swap (0203)],
    [Warm (daily) + monthly full],
    [Sparse cross-product, activity-filtered extraction],
    [`inventory_movements`], [Sequential ID cursor (0305)], [Append-only (0402)], [Hot], [Append-only activity log],
  )],
  kind: table,
)

#ecl-tip(
  "This mapping is a starting point",
)[The recommended combination depends on the source system, the destination engine, and the consumer's SLA. A `customers` table with 500 rows doesn't need the same treatment as one with 5 million. Use the flowcharts to classify, then adjust based on what you learn about the source during the first few weeks of extraction.]

// ---

= Glossary
<glossary>
#strong[Append-and-materialize] -- Load strategy that appends every extraction as new rows to a log table and deduplicates to current state via a view. Avoids MERGE cost on columnar engines. See 0404.

#strong[Backfill] -- Reloading a historical date range or an entire table to correct accumulated drift, recover from corruption, or onboard a new table. See 0611.

#strong[Batch ID (`_batch_id`)] -- Metadata column that correlates all rows from the same extraction run. Used for rollback, debugging, and reconciliation. See 0501.

#strong[Cold tier] -- Freshness tier for historical data refreshed weekly or monthly via full replace. Acts as the purity safety net. See 0608.

#strong[Compaction] -- Collapsing an append log to one row per key, removing all historical versions. Always collapse-to-latest (`QUALIFY ROW_NUMBER() = 1`), never trim-by-date. See 0404.

#strong[Conforming] -- Everything the data needs to survive the crossing between source and destination: type casting, metadata injection, null handling, charset encoding, key synthesis. If it changes business meaning, it belongs downstream. See 0102.

#strong[Corridor] -- The combination of source type and destination type. Transactional -\> Columnar (e.g.~PostgreSQL -\> BigQuery) or Transactional -\> Transactional (e.g.~PostgreSQL -\> PostgreSQL). Same pattern, different trade-offs. See 0107.

#strong[Cursor] -- A high-water mark (typically `MAX(updated_at)` or `MAX(id)`) used to extract only rows that changed since the last run. See 0302.

#strong[Data contract] -- Explicit, checkable rules at the boundary between source and destination: schema shape, volume range, null rates, freshness. See 0609.

#strong[Dedup view] -- A SQL view over an append log that uses `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _extracted_at DESC) = 1` to expose only the latest version of each row. See 0404.

#strong[ECL] -- Extract, Conform, Load. The framework this book documents. The C handles type casting, metadata injection, null handling, key synthesis -- everything the data needs to land correctly. See 0101.

#strong[EL] -- Extract-Load with zero transformation. The theoretical ideal that never survives contact with real systems. See 0101.

#strong[Evolve] -- Schema policy that accepts new columns from the source and adds them to the destination automatically. The recommended default for most tables. See 0609.

#strong[Extracted at (`_extracted_at`)] -- Metadata column recording when the pipeline pulled the row, not when the source last modified it. Foundation for dedup ordering in append-and-materialize. See 0501.

#strong[Extraction gate] -- A check between extraction and load that blocks the load when the result looks implausible (0 rows from a table that normally has data, row count outside expected range). See 0610.

#strong[Freeze] -- Schema policy that rejects any schema change and fails the load. Reserved for tables with stable, critical schemas. See 0609.

#strong[Freshness] -- How recently the destination reflects the source. The other end of the purity tradeoff. See 0108.

#strong[Full replace] -- Drop and reload the entire table on every run. Stateless, idempotent, catches everything. The default until the table outgrows the scan window. See 0201.

#strong[Hard delete] -- A source row that was physically removed. Invisible to any cursor-based extraction. Requires a separate detection mechanism. See 0306.

#strong[Hard rule] -- A constraint enforced by the database: PK, UNIQUE, NOT NULL, FK, CHECK. If the system rejects violations at write time, it's hard. See 0106.

#strong[Health table] -- Append-only table with one row per table per pipeline run, capturing raw measurements (row counts, timing, status, schema fingerprint). See 0602.

#strong[Hot tier] -- Freshness tier for actively changing data refreshed multiple times per day via incremental extraction. See 0608.

#strong[Idempotent] -- A pipeline that produces the same destination state whether it runs once or ten times with the same input. Full replace gets it for free; incremental has to earn it. See 0109.

#strong[Metadata columns] -- Columns injected during extraction that don't exist in the source: `_extracted_at`, `_batch_id`, `_source_hash`. See 0501.

#strong[Open document] -- A record that can still be modified (e.g.~draft invoice, pending order). Contrast with closed document. See 0307.

#strong[Closed document] -- A record that is immutable (e.g.~posted invoice). In many jurisdictions, modifying a closed invoice is illegal. See 0307.

#strong[Partition swap] -- Replace data at partition granularity without touching the rest of the table. See 0202.

#strong[Purity] -- The degree to which the destination is an exact clone of the source at a given point in time. Full replace maximizes it; incremental carries purity debt. See 0108.

#strong[QUALIFY] -- SQL clause that filters directly on window functions without a subquery. Native on BigQuery, Snowflake, ClickHouse. Not supported on PostgreSQL, MySQL, SQL Server, Redshift. See 0801.

#strong[Reconciliation] -- Post-load verification that the destination matches the source: row count comparison, aggregate checks, hash comparison. See 0614.

#strong[Schema policy] -- How the pipeline responds when the source schema changes. Two valid modes in ECL: evolve (accept) or freeze (reject). See 0609.

#strong[Scoped full replace] -- Full-replace semantics applied to a declared scope (e.g.~current year) while historical data outside the scope is frozen. See 0204.

#strong[SLA] -- Service Level Agreement. Four components: table/group, freshness target, deadline, measurement point. See 0604.

#strong[Soft rule] -- A business expectation with no database enforcement. "Quantities are always positive," "only open invoices get deleted." Your pipeline must survive these being wrong. See 0106.

#strong[Source hash (`_source_hash`)] -- Hash of all business columns at extraction time. Enables change detection without relying on `updated_at`. See 0501, 0208.

#strong[Staging swap] -- Load into a staging table, validate, then atomically swap to production. Zero downtime, trivial rollback. See 0203.

#strong[Stateless window] -- Extract a fixed trailing window on every run with no cursor state between runs. The default incremental approach for most tables. See 0303.

#strong[Synthetic key (`_source_key`)] -- A hash of immutable business columns, used as the MERGE key when the source has no stable primary key. See 0502.

#strong[Tiered freshness] -- Splitting a pipeline into hot, warm, and cold tiers so tables are refreshed at the cadence that matches their consumption, not at a uniform schedule. See 0608.

#strong[Warm tier] -- Freshness tier for recent data refreshed daily, typically overnight. The purity layer that catches what the hot tier missed. See 0608.

// ---

= Domain Model Quick Reference
<domain-model-quick-reference>
Condensed reference for the shared fictional schema used in every SQL example. For the full description, ERD, and soft rule explanations, see 0002.

== Tables at a Glance
<tables-at-a-glance>
#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto, auto, auto, auto, auto),
    table.header([Table], [PK], [Key columns], [ECL role], [Primary patterns]),
    table.hline(),
    [`orders`],
    [`id`],
    [`customer_id`, `status`, `created_at`, `updated_at`],
    [Broken cursor showcase],
    [0301, 0303, 0310],
    [`order_lines`],
    [`id`],
    [`order_id`, `product_id`, `quantity`, `unit_price`],
    [Detail with no timestamp],
    [0304, 0308],
    [`customers`], [`id`], [`name`, `email`, `is_active`], [Soft-delete dimension], [0201, 0106],
    [`products`], [`id`], [`name`, `price`], [Schema drift case], [0201, 0105, 0209],
    [`invoices`],
    [`id`],
    [`order_id`, `status`, `total_amount`, `created_at`, `updated_at`],
    [Open/closed + hard deletes],
    [0306, 0307],
    [`invoice_lines`],
    [`id`],
    [`invoice_id`, `description`, `amount`, `status`],
    [Independent detail lifecycle],
    [0308, 0306],
    [`events`], [`event_id`], [`event_type`, `event_date`, `payload`], [Append-only, partitioned], [0305, 0402],
    [`sessions`], [(implicit)], [`session_id`, `user_id`, `start_time`], [Late-arriving data], [0309],
    [`metrics_daily`],
    [(composite)],
    [`metric_date`, `metric_name`, `value`],
    [Pre-aggregated, partition-replace],
    [0202, 0204],
    [`inventory`], [(`sku_id`, `warehouse_id`)], [`on_hand`, `on_order`], [Sparse cross-product], [0206, 0207],
    [`inventory_movements`],
    [`id`],
    [`sku_id`, `warehouse_id`, `movement_type`, `quantity`, `created_at`],
    [Activity signal, append-only],
    [0207, 0402, 0706],
  )],
  kind: table,
)

== Soft Rules
<soft-rules>
Every "always true" business rule in the domain model is a soft rule -- none have a database constraint enforcing them.

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Table], [Soft rule], [How it breaks]),
    table.hline(),
    [`orders`], ["Always has at least one line"], [UI bug creates empty order],
    [`orders`], ["Status goes `pending` -\> `confirmed` -\> `shipped`"], [Support resets manually],
    [`order_lines`], ["Quantities are always positive"], [Return entered as `-1`],
    [`invoices`], ["Only open invoices get deleted"], [Year-end cleanup script],
    [`invoice_lines`], ["Line status always matches header"], [One line disputed independently],
    [`customers`], ["Emails are unique"], [Duplicate registration, no unique index],
    [`inventory`], ["`on_hand` is always \>= 0"], [Write-off creates negative balance],
    [`inventory_movements`], ["Every stock change creates a movement"], [Bulk import bypasses movement log],
  )],
  kind: table,
)

See 0106 for why these matter and how your pipeline should handle violations.

== Relationships
<relationships>
\// TODO: Convert mermaid diagram to Typst or embed as SVG

`events`, `sessions`, and `metrics_daily` have no foreign keys into the schema above. `inventory` and `inventory_movements` connect to `products` via `sku_id` but have no `warehouses` table -- `warehouse_id` is a plain integer key.

// ---

= Orchestrators
<orchestrators>
Every pattern in the book works regardless of tooling. This page names names.

An orchestrator schedules extractions, retries failures, and tracks what happened on each run. For ECL, the relevant concerns are scheduling cadence (0606), tiered freshness (0608), backfill execution (0611), and health table population (0602). The three serious options for a Python-based stack are Dagster, Airflow, and Prefect -- each models work differently, and the model shapes what's easy.

== Feature Comparison
<feature-comparison>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto, auto, auto, auto),
    table.header([Concern], [Dagster], [Airflow 3], [Prefect 3]),
    table.hline(),
    [Pipeline unit],
    [Software-defined asset],
    [`@asset` decorator (creates a DAG per asset) or traditional DAG + tasks],
    [Flow + tasks],
    [Scheduling],
    [Schedules + Sensors],
    [Cron, data-aware triggers, asset-aware scheduling],
    [Deployment schedules, automations],
    [Freshness],
    [Freshness policies per asset, violations in UI],
    [Deadline alerts (3.1), SLA callbacks on task duration],
    [No native staleness tracking],
    [Data quality],
    [`@asset_check` inline after materialization],
    [Custom operators, external tools],
    [Artifacts + assertions],
    [Backfill],
    [Partition-based: select range in UI, per-partition retry],
    [Scheduler-managed from UI (3.0): missing/all/failed runs, DAG-scoped],
    [Parameterized reruns, no native partition concept],
    [Metadata],
    [`context.add_output_metadata({...})` per materialization],
    [XComs, asset metadata for lineage],
    [Artifacts on flow/task runs],
    [Concurrency],
    [Per-resource limits (e.g. 2 connections to source X)],
    [Pool-based (N slots per pool)],
    [Work pool limits],
    [Managed offering], [Dagster Cloud], [Astronomer, MWAA, Cloud Composer], [Prefect Cloud],
    [Task SDK], [Python], [Python, Go, Java, TypeScript (3.0 Task SDK)], [Python],
    [Learning curve],
    [Moderate -- asset model requires rethinking],
    [Low for DAG users, moderate for asset model],
    [Low -- decorators, minimal concepts],
  )],
  kind: table,
)

== Dagster
<dagster>
Dagster's core abstraction is the #strong[software-defined asset]: a function that produces a named data artifact, declared in code. For ECL, one asset maps to one destination table -- `orders`, `customers`, `events` -- and the orchestrator tracks when each was last materialized, whether it's fresh, and what metadata the last run attached to it.

- #strong[Partitioned assets] let you declare that `events` is partitioned by date, then backfill a range by selecting it in the UI. The orchestrator chunks the range into partition runs, respects concurrency limits, and tracks success per partition. Prefer monthly partitions over daily -- a yearly backfill with daily partitions spawns 365 individual runs with their own metadata and UI entries, while monthly gives you 12 with the same per-partition retry.
- #strong[Asset checks] (`@asset_check`) run inline after materialization: row count validation, null rate thresholds, schema drift detection. Maps directly to 0609 and 0610.
- #strong[Freshness policies] declare how stale an asset is allowed to be. Violations surface in the UI and trigger alerts -- the 0604 SLA expressed as a one-liner in the asset definition.
- #strong[Custom metadata per materialization] (`context.add_output_metadata({"row_count": n})`) feeds the health table (0602) as a side effect of every run, with no explicit INSERT required.
- #strong[Sensors] trigger runs from external events. We use sensors to let dashboard admins trigger an on-demand refresh of the tables behind their reports, which means the pipeline only needs to run once daily while consumers who need fresher data pull it when they actually need it -- without a high-frequency schedule running for data nobody checks until 10 AM.
- #strong[Concurrency limits per resource] cap concurrent extractions against a single source without global semaphores. At scale -- thousands of tables across dozens of sources -- this is what keeps the pipeline from overloading its own clients.

#ecl-info(
  "Stateless by default",
)[Dagster's asset model encourages stateless pipelines: each materialization reads from the source and writes to the destination with no persisted cursor between runs. Incremental cursors (0302) can live in Dagster's built-in cursor mechanism or in the destination itself, but the orchestrator doesn't force a state store. This aligns with the 0109 goal.]

#strong[Where it costs you:]

- The asset abstraction requires rethinking pipeline structure, especially coming from a DAG/task mental model. The learning curve is real and takes a few weeks.
- Smaller community and fewer pre-built connectors than Airflow's ecosystem.
- Multi-table operations (extract 5 tables from one API call, split them) need multi-asset functions, which are more awkward than single-asset definitions.
- Dagster Cloud's pricing is credit-based (per materialization), which can add up at high table counts with frequent schedules. Self-hosting on Kubernetes is the alternative but requires platform engineering.

== Airflow
<airflow>
Airflow is the most widely deployed orchestrator in the data ecosystem. Its traditional model is the #strong[DAG] -- a directed acyclic graph of tasks -- and Airflow 3.0 (April 2025) added an `@asset` decorator that brings asset-oriented thinking into the framework alongside the existing DAG model.

Airflow 3 is a substantial release: asset-aware scheduling, scheduler-managed backfills with a UI, a new Task SDK that supports Go/Java/TypeScript alongside Python, DAG versioning, event-driven scheduling, and deadline alerts in 3.1. The gap between Airflow and Dagster narrowed significantly with this release.

- Widest connector ecosystem of any orchestrator -- if a source system has an API, there's probably an Airflow provider package for it.
- Pool-based concurrency control is straightforward: define a pool with N slots, assign tasks to it, and Airflow queues the rest.
- Backfills in 3.0 are scheduler-managed and triggerable from the UI with configurable reprocessing (missing, all, or failed runs) -- a major improvement over 2.x's CLI-only `airflow dags backfill`. They're still DAG-scoped, so backfilling `orders` for March reruns the entire DAG for that range including other tables in it.
- Mature managed offerings (Astronomer, AWS MWAA, Cloud Composer) all support Airflow 3 and handle infrastructure.
- The `@asset` decorator creates a DAG per asset with asset-aware scheduling, which means you can trigger downstream work when an upstream asset updates. The model is conceptually similar to Dagster's assets but architecturally different -- each `@asset` is its own DAG, and cross-asset data passes through XComs rather than through a shared graph context.
- The team already knows it, and that matters more than any feature comparison.

#strong[Where it needs more wiring for ECL:]

- Populating the health table (0602) with structured run metrics (row counts, durations, schema hashes) still requires explicit code per task. Asset metadata in 3.0 is oriented toward lineage tracking rather than the kind of per-run operational metrics that Dagster's `add_output_metadata` captures.
- SLA miss callbacks track task duration, and 3.1's deadline alerts add proactive monitoring on schedules -- but neither directly measures data freshness as 0604 defines it. You still need your own staleness query.
- XComs improved in 3.0 but remain the primary mechanism for passing structured data between tasks, and at scale (hundreds of tables) the ergonomics for metadata like row counts and schema hashes feel heavier than Dagster's built-in approach.

#ecl-tip(
  "One DAG per source system",
)[Group tables by source system, with each table as a task within the DAG. One DAG per table creates hundreds of DAGs that overwhelm the scheduler and UI. One monolithic DAG creates a single point of failure where a stuck extraction blocks everything downstream. The per-source structure groups tables that share connection limits and scheduling cadence while keeping the blast radius of a failure scoped to one source.]

== Prefect
<prefect>
Prefect 3 (September 2024) brought the events and automation system to open source, added a transactional interface for idempotent pipelines, and significantly improved performance for distributed workloads. The API is genuinely pleasant -- `@flow` on a Python function and it's orchestrated -- and Prefect Cloud removes infrastructure concerns for small-to-medium deployments.

- Python-native API with minimal boilerplate. The gap between "script that works" and "orchestrated pipeline" is the smallest of the three tools.
- Automations (trigger actions on flow/task state changes, external events) provide flexible alerting and event-driven scheduling.
- Ephemeral infrastructure via work pools -- Prefect spins up ECS tasks or Kubernetes jobs per run and deprovisions after completion, which keeps costs low for bursty workloads.
- The transactional interface lets you group tasks into transactions with automatic rollback on failure, which helps with the idempotency goals from 0109.

#strong[Where it's limited for ECL at scale:]

- No native partition concept. Backfilling a date range means parameterizing the flow and triggering N runs manually -- the orchestrator doesn't know they form a logical unit.
- No first-class freshness tracking or per-asset metadata. The health table (0602) is entirely your responsibility.
- Flow-level concurrency from Prefect 2 was removed in 3.0, replaced by a combination of global concurrency limits, work pool limits, and work queue limits -- functional but less ergonomic.
- At scale (thousands of tables), the flow-per-table model generates UI clutter without the asset lineage graph that helps navigate large Dagster installations.

== Other Tools
<other-tools>
- #strong[Kestra] -- Event-driven, YAML-based. Good for non-Python teams and polyglot pipelines, with a visual flow editor. The tradeoff is losing Python-native advantages for data engineering.
- #strong[Mage] -- Notebook-like UI, promising for exploratory work but less mature for production ECL at scale. The UI also gets painfully slow over time, and is by far the most volatile one.
- #strong[cron + scripts] -- Acceptable for < 10 tables with no dependencies and no backfill needs. Falls apart the moment you need retries, visibility, or any coordination between jobs.

#ecl-warning(
  "Never build your own orchestrator",
)[Every team that builds a custom orchestrator eventually rebuilds 60% of Airflow, poorly. The "we just need a simple scheduler" conversation leads to a homegrown system with no UI, no backfill capability, no alerting, and a bus factor of one. Use a real orchestrator and spend the engineering time on the pipelines.]

== Author's Recommendation
<authors-recommendation>
For a new ECL project, start with #strong[Dagster]. The asset model maps 1:1 to the "one asset = one destination table" structure this book is built around, partition-based backfills are the hardest thing to build from scratch, and inline asset checks plus freshness policies implement half of Part VI as configuration. The asset graph and partition-based backfills have justified the learning curve many times over.

#strong[Airflow] is a strong choice when the team already runs it, when you need the widest connector ecosystem, or when Airflow 3's asset model and managed backfills cover your needs without Dagster's steeper learning curve. The 3.0 release closed many of the gaps that used to make the comparison one-sided -- asset-aware scheduling, UI-managed backfills, and the Task SDK are real improvements. Structure one DAG per source system, add health table inserts per task, and it works well. If you're still on Airflow 2.x, upgrading to 3 is worth the effort before considering a migration to a different tool.

#strong[Prefect] is the right pick for smaller teams (< 500 tables) that value developer velocity and don't need partition-aware backfills or per-asset freshness tracking. Prefect Cloud removes infrastructure overhead entirely, and the transactional interface aligns well with idempotency goals. Move to Dagster when backfill complexity or table count outgrows it.

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto, auto),
    table.header([Scenario], [Recommendation]),
    table.hline(),
    [New project, ECL-focused], [Dagster],
    [Existing Airflow, already on 3.x or upgrading], [Stay on Airflow 3],
    [Existing Airflow 2.x], [Upgrade to 3 before considering migration],
    [Small team, < 500 tables, no platform engineer], [Prefect Cloud],
    [Non-Python team or polyglot stack], [Kestra or Airflow 3 (Task SDK supports Go/Java/TypeScript)],
  )],
  kind: table,
)

== Related Patterns
<related-patterns>
- 0602 -- The health table that orchestrators populate
- 0604 -- Freshness policies and SLA monitoring
- 0606 -- Scheduling cadence and co-scheduling
- 0608 -- Tiered freshness by table criticality
- 0609 -- Data quality checks and schema contracts
- 0610 -- Status gates that block downstream on extraction failure
- 0611 -- Backfill execution patterns

// ---

= Extractors and Loaders
<extractors-and-loaders>
== The Spectrum
<the-spectrum>
Extractor/loader tools sit on a spectrum from fully managed to fully custom. On the managed end, Fivetran handles everything -- connectors, scheduling, schema decisions, infrastructure -- and you accept whatever it decides. On the custom end, you write Python with SQLAlchemy, own every line, and maintain every failure mode. In between, Airbyte gives you managed connectors with more visibility into what they do, and dlt gives you a Python library that handles the plumbing while leaving schema control, deployment, and orchestration in your hands.

Where you belong on this spectrum depends on how many sources you need to cover, how much control you need over the conforming layer (0102), whether someone else's schema decisions are acceptable for your destination, and price. A self-built stack running dlt on your own infrastructure with BigQuery as the destination can run thousands of tables for a few hundred dollars a month in compute and storage. The same workload on Fivetran costs an order of magnitude more because you're paying per row, per sync, per connector -- and you're paying for the engineering you didn't have to do, which is a valid tradeoff only if you genuinely don't have the engineering capacity.

== Comparison
<comparison>
#figure(
  align(center)[#table(
    columns: (14.29%, 14.29%, 14.29%, 14.29%, 14.29%, 14.29%, 14.29%),
    align: (auto, auto, auto, auto, auto, auto, auto),
    table.header([Tool], [Type], [Schema control], [Incremental], [Naming], [Deployment], [Best for]),
    table.hline(),
    [#strong[Fivetran];],
    [Fully managed],
    [None -- Fivetran decides],
    [Built-in cursors],
    [Fivetran decides],
    [SaaS],
    [Teams without engineering capacity],
    [#strong[Airbyte];],
    [Semi-managed],
    [Limited -- normalization layer],
    [Built-in per connector],
    [Configurable],
    [Cloud or self-hosted],
    [SaaS sources (Salesforce, Stripe)],
    [#strong[dlt];],
    [Python library],
    [Full -- schema contracts, naming conventions],
    [Cursor or stateless window],
    [Configurable (`snake_case` default)],
    [You deploy it],
    [SQL sources, custom APIs, full control],
    [#strong[Custom Python];],
    [Code],
    [Total],
    [You build it],
    [You decide],
    [You deploy it],
    [Legacy/niche sources, extreme requirements],
  )],
  kind: table,
)

// ---

== dlt
<dlt>
dlt is an open-source Python library for loading data into warehouses. It handles type inference, Parquet/JSONL serialization, destination-specific load jobs, and schema evolution -- the plumbing that every loader needs and nobody wants to build twice.

=== The Standard Way: sql_database and sql_table
<standard-dlt>
dlt ships with a `sql_database` source that reflects an entire database via SQLAlchemy and yields every table as a resource, and a `sql_table` function for extracting individual tables. For most teams getting started, this is the right entry point:

```python
from dlt.sources.sql_database import sql_database, sql_table

# Full database: reflect all tables, load everything
source = sql_database(
    connection_url="postgresql://user:pass@host/db",
    schema="public",
    backend="pyarrow",  # or "sqlalchemy", "pandas", "connectorx"
)

# Single table with incremental merge
orders = sql_table(
    connection_url="postgresql://user:pass@host/db",
    table="orders",
    incremental=dlt.sources.incremental("updated_at"),
    primary_key="order_id",
    write_disposition="merge",
    backend="pyarrow",
)

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="bigquery",
    dataset_name="raw_erp",
)
pipeline.run(source)
```

The `sql_database` source handles schema reflection, type mapping, and batching automatically. Four backends are available: `sqlalchemy` (default, yields Python dicts), `pyarrow` (yields Arrow tables -- significantly faster for columnar destinations), `pandas` (yields DataFrames), and `connectorx` (parallel reads for large tables).

Callbacks let you customize behavior per table without writing custom extraction code:

- #strong[`table_adapter_callback`] -- receives each reflected table and lets you modify which columns get extracted, add computed columns, or skip tables entirely. This is where you'd exclude PII columns (0209) or add metadata columns.
- #strong[`type_adapter_callback`] -- overrides SQLAlchemy type mappings. If your source has `FLOAT` columns that should land as `DECIMAL`, this is where you fix it before any data moves.
- #strong[`query_adapter_callback`] -- modifies the SELECT query before execution. Add WHERE clauses for scoped extraction (0204), change the ORDER BY for cursor alignment, or inject hints for the source query planner.

For incremental loading, dlt tracks cursor state internally via `dlt.sources.incremental()` -- it stores the last value in a `_dlt_pipeline_state` table on the destination and picks up where it left off on the next run. Incremental requires a primary key on the resource so dlt can merge correctly; without one, you're appending duplicates on every run. This works well for simple cursor-based patterns (0302) where you trust `updated_at` and want the library to manage state for you.

=== Schema Contracts
<schema-contracts>
dlt's schema contract system controls what happens when the source sends something unexpected. Four modes per entity (`tables`, `columns`, `data_type`): `evolve` (accept it), `freeze` (fail the pipeline), `discard_row` (drop the row), `discard_value` (drop the value).

```python
# Permissive: evolve everything, let the pipeline adapt
pipeline.run(source, schema_contract={"tables": "evolve", "columns": "evolve"})

# Conservative: freeze tables and types, evolve columns only
pipeline.run(source, schema_contract={"tables": "freeze", "columns": "freeze"})
```

I run permissive (`evolve`/`evolve`) in production because at scale the alternative is a constant stream of freeze-triggered failures from ERP modules being activated, schema migrations, and column additions that are all legitimate. The monitoring layer (0609) catches what matters; the pipeline keeps running.

The conservative option makes sense when you have a small number of high-value tables where a schema surprise should stop the pipeline -- freeze tables to prevent junk table creation from source bugs, freeze types so a `VARCHAR` that suddenly arrives as `INT64` doesn't silently corrupt downstream queries.

One thing to know about `data_type: "evolve"`: when a column's type changes, dlt creates a variant column alongside it -- `amount__v_text` next to the original `amount` -- so old data stays intact while new rows land in the variant. Variant columns can accumulate if the source is messy with types.

#ecl-warning(
  "Discard modes break the conforming boundary",
)[Silently dropping rows or values means your destination no longer mirrors the source -- you've introduced an invisible filter that nobody downstream knows about. For ECL workloads where the goal is a faithful clone, stick to `evolve` and `freeze`. See 0102.]

=== Naming Conventions
<naming-conventions>
dlt normalizes all identifiers through a naming convention before they reach the destination. The default is `snake_case` -- lowercased, ASCII only, special characters stripped. Other options include `duck_case` (case-sensitive Unicode), `direct` (preserve as-is), and SQL-safe variants (`sql_cs_v1`, `sql_ci_v1`).

This is a one-time decision with permanent consequences -- the same tradeoff described in 0707. Changing the convention after data exists is destructive: dlt re-normalizes already-normalized identifiers (it doesn't store the originals), which means every table and column name in your destination could change.

#ecl-warning(
  "Normalization can collide source keys",
)[dlt detects some collision types (case-sensitive convention on a case-insensitive destination, convention changes on existing tables) but does not detect collisions in the source data itself. If two dictionary keys or column names normalize to the same identifier under `snake_case`, they merge silently -- the last value wins. This is rare in SQL tables (column names are unique at the source) but common in JSON/dict sources where keys like `ProductID` and `product_id` can coexist. Audit nested or dict-based sources before the first load.]

=== Destination Gotchas
<destination-gotchas>
Destination engines have format-specific limitations that dlt inherits:

- #strong[BigQuery]: cannot load JSON columns from Parquet files -- the job fails permanently. Use JSONL or Avro for tables with JSON columns.
- #strong[Snowflake]: `VARIANT` columns loaded from Parquet land as strings, not queryable JSON. Downstream queries need `PARSE_JSON()` to unwrap them. `PRIMARY KEY` and `UNIQUE` constraints are metadata-only.
- #strong[PostgreSQL]: the default `insert_values` loader generates large INSERT statements. Switching to the CSV loader (`COPY` command) is several times faster.

=== Stateless Operation
<stateless-operation>
dlt persists pipeline state in a local directory (schema cache, pending packages, load history) and in the destination (`_dlt_version`, `_dlt_pipeline_state`). For stateless operation (0109), delete the pipeline directory before every run to prevent stale caches from causing errors on staging tables that were cleaned up after the last merge.

Even with a clean local directory, dlt caches schema metadata in the destination's `_dlt_version` table. If a staging table is deleted after merge but the destination-side cache survives, the next load can skip table creation and fail. Use dlt's `refresh="drop_resources"` mechanism or delete cache entries before each load.

Combined with a stateless trailing-window extraction (0303), the pipeline has no persisted state between runs -- every execution is independent and idempotent.

=== Going Custom
<going-custom>
At scale, we don't use `sql_database` or `sql_table` -- we use dlt as a loader only and build extraction, merge, and schema evolution ourselves. The reasons are specific to our workload (thousands of tables, custom partition-pruned merges, PyArrow batching for performance), and most teams won't need this level of control. But if you outgrow the standard `sql_table` path, here's what we replaced and why:

- #strong[Extraction]: custom `@dlt.resource` functions with manual SQL via SQLAlchemy instead of `sql_table`. We build the query ourselves (including the WHERE clause for trailing-window extraction) and yield PyArrow tables via dlt's `row_tuples_to_arrow` helper -- significantly faster than dict-based iteration for large tables.
- #strong[Merge]: custom DELETE+INSERT+QUALIFY in a BigQuery transaction instead of dlt's built-in merge. dlt's merge rewrites all touched partitions; ours prunes the DELETE to only the months that appear in the staging data, which matters when a 7-day trailing window touches rows across 2-3 partition months on a table with years of history.
- #strong[Schema evolution]: custom `ALTER TABLE ADD COLUMN` before the merge step, with a mapping for BigQuery's legacy type names (`FLOAT` → `FLOAT64`, `INTEGER` → `INT64`) that the schema API returns.
- #strong[Incremental state]: no `_dlt_pipeline_state` table. The trailing window (0303) means every run re-extracts the same N-day range regardless of what happened before -- no cursor to track, no state to corrupt.

dlt still handles the load job itself (`pipeline.run()` with `write_disposition="replace"` to staging), Parquet serialization, `_dlt_id`/`_dlt_load_id` generation, schema contracts, and naming conventions. The library earns its place even when you bypass most of its extraction and merge machinery.

#ecl-info(
  "dlt and append-and-materialize",
)[dlt's three write dispositions are `replace`, `append`, and `merge`. There's no built-in support for the append-and-materialize pattern from 0404 -- appending every extraction to a log table and deduplicating via a view. You can use `write_disposition="append"` to build the log, but the dedup view, compaction job, and materialization schedule are entirely yours to build and maintain outside of dlt. If append-and-materialize is your primary load strategy for columnar destinations, know that dlt handles the append step but everything after it -- the view, the compaction, the partition management -- is custom SQL you manage separately.]

// ---

== Airbyte
<airbyte>
Airbyte provides a catalog of managed connectors -- pre-built extractors for SaaS APIs (Salesforce, HubSpot, Stripe, Jira) and databases (PostgreSQL, MySQL). Each connector handles authentication, pagination, rate limiting, and incremental state. Available as a cloud service or self-hosted via Docker.

#strong[Where it works well];: SaaS sources where you don't have direct database access and the API is the only option. Writing a Salesforce extractor from scratch means handling OAuth refresh, query pagination, bulk API vs REST API selection, and field-level security. Airbyte's connector does this, and when it works, it saves weeks. CDC support for PostgreSQL and MySQL is available through Debezium-backed connectors, which gives you change streams without managing Debezium infrastructure directly.

#strong[Where it gets complicated];: Airbyte applies a normalization step after extraction -- flattening nested JSON, renaming columns, and creating sub-tables for arrays. This is a transformation step you may not want, sitting between your source and your destination without your explicit control. Connector quality varies significantly; some are maintained by Airbyte's core team, others by the community, and community connectors break on edge cases that the core team never tested. The self-hosted (OSS) version requires Docker infrastructure and has no built-in orchestration -- you schedule syncs externally or use the cloud tier, which imposes sync frequency minimums that may not match your freshness requirements.

#ecl-tip(
  "Check the connector support level",
)[Airbyte classifies connectors as Generally Available, Beta, or Alpha. For production ECL pipelines, stick to GA connectors. Beta and Alpha connectors change their schemas across versions, which means your downstream queries break when Airbyte pushes an update.]

// ---

== Fivetran
<fivetran>
Fully managed, zero code, zero infrastructure. You authenticate a source, pick a destination, set a sync schedule, and Fivetran handles everything else. For teams without engineering capacity or for SaaS sources where the connector exists and works well, this is the fastest path to having data in your warehouse.

The tradeoff is control. Fivetran decides column types, naming conventions, and how to handle nested data. You can't inject metadata columns (0501), can't control the schema contract (0609), and can't customize the merge strategy. What lands in your destination is what Fivetran decided, and if that decision is wrong for your use case, your only recourse is a support ticket.

Fivetran does add its own metadata columns (`_fivetran_synced`, `_fivetran_deleted`) and handles soft deletes for some connectors. These are useful but non-standard -- your downstream queries become Fivetran-aware, which creates coupling that matters if you ever migrate off the platform.

#strong[Cost];: priced by Monthly Active Rows (MAR). Affordable for small volumes, expensive at scale -- a table that re-extracts 10 million rows monthly on a trailing window costs the same as 10 million unique rows. Sync frequency minimum is 5 minutes on the standard tier, 1 minute on business/enterprise. At scale -- hundreds or thousands of tables -- Fivetran's pricing becomes a serious constraint; the math works best when you have a few dozen high-value SaaS sources and the engineering team to maintain them doesn't exist.

// ---

== Custom Python + SQLAlchemy
<custom-python-sqlalchemy>
When the source is niche enough that no connector exists -- a legacy ERP with a proprietary database, a vendor-specific API with no public documentation, a mainframe behind three layers of VPN -- you write it yourself.

SQLAlchemy is the universal connector for SQL sources. It covers PostgreSQL, MySQL, SQL Server, SAP HANA, and dozens of other databases with a unified API for connection management, query execution, and type introspection. For extraction specifically, three backends cover most needs:

- #strong[SQLAlchemy] (universal): works everywhere, reasonable performance, handles all types.
- #strong[PyArrow];: fast columnar reads, good for wide tables headed to columnar destinations. Doesn't handle every type (JSONB on PostgreSQL, for example).
- #strong[ConnectorX];: parallel reads that saturate the network. Best for large tables where single-threaded extraction is the bottleneck.

#ecl-warning(
  "Custom extractors accumulate",
)[Every custom extractor is a maintenance surface. After a year, you'll have 15 of them, each with slightly different error handling, slightly different retry logic, and slightly different assumptions about how types map. If you find yourself writing the third custom extractor, evaluate whether dlt or another library can absorb the common plumbing before the codebase becomes a collection of snowflakes.]

The cost is everything else. Schema evolution, error handling, retry logic, state management, observability -- dlt and Airbyte handle these as features, and with custom code, they're your problem. You also own type mapping: deciding that a SQL Server `DATETIME2(7)` should land as `TIMESTAMP` in BigQuery (truncating nanoseconds to microseconds) is now an explicit choice you make in code, not something a library infers for you.

Worth it when no alternative exists or when the extraction logic is complex enough that a generic tool gets in the way. Most production pipelines end up with at least a few custom extractors for the sources that no tool covers.

// ---

== Decision Table
<decision-table>
#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Source type], [Recommended], [Why]),
    table.hline(),
    [Direct DB access, SQL sources], [dlt or custom SQLAlchemy], [Full control over extraction and conforming],
    [SaaS APIs (Salesforce, Stripe)], [Airbyte or Fivetran], [Managed connectors handle auth, pagination, rate limits],
    [File-based (S3, SFTP, CSV drops)], [dlt or custom], [Connector overhead not justified for file reads],
    [Legacy/niche sources], [Custom SQLAlchemy], [No connector exists],
    [Team without engineering capacity], [Fivetran], [Zero code, zero ops],
  )],
  kind: table,
)

#ecl-tip(
  "Mix and match tools freely",
)[Running dlt for your SQL sources and Fivetran for two SaaS APIs is a perfectly valid architecture. The destination doesn't care which tool loaded the data, as long as your naming convention and metadata columns are consistent across all of them.]

== Related
<related>
- 0102 -- What belongs in the conforming layer
- 0501 -- Metadata columns that every loader should inject
- 0609 -- Schema contracts and data quality gates
- 0707 -- Naming conventions and why they're permanent

// ---

= Destinations
<destinations>
0104 covers how columnar engines store, partition, and price data. 0705 covers the cost levers once data is loaded. This page is the decision: which engine for which workload, and what to watch out for when running ECL pipelines against each one.

== Cost Model Comparison
<cost-model-comparison>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto, auto, auto, auto),
    table.header([Engine], [Billing model], [What you optimize], [Cost guardrails]),
    table.hline(),
    [BigQuery],
    [Per TB scanned (on-demand) or slots (reservations)],
    [Bytes scanned per query],
    [Per-query and per-day byte limits, `require_partition_filter`],
    [Snowflake],
    [Per second of warehouse compute],
    [Query runtime, warehouse idle time],
    [Auto-suspend, resource monitors, warehouse sizing],
    [ClickHouse],
    [Self-hosted infrastructure (or ClickHouse Cloud RPU)],
    [Query speed on fixed hardware],
    [Infrastructure budget],
    [Redshift],
    [Per node per hour (provisioned) or RPU-second (Serverless)],
    [Cluster utilization or query compute time],
    [Query monitoring rules, WLM queues],
    [PostgreSQL],
    [Self-hosted or managed instance (RDS, Cloud SQL)],
    [Instance size, connection count],
    [Fixed monthly cost regardless of query volume],
    [DuckDB / MotherDuck],
    [Free locally; MotherDuck ~\$0.15/GB scanned (higher unit price than BQ, lower total bills on moderate data)],
    [Query efficiency (local); GB scanned (MotherDuck)],
    [Per-second billing, no idle tax, Duckling size limits],
  )],
  kind: table,
)

// ---

== BigQuery
<bigquery>
#strong[Best for:] serverless pay-per-query, many ad-hoc consumers, Google Cloud native stacks. This is our primary destination -- BigQuery's cost model rewards exactly what ECL pipelines produce: partition-scoped writes, partition-filtered reads, and bulk loads over row-by-row DML.

#strong[ECL strengths:]
- `require_partition_filter` is the only engine with query-cost enforcement built into the table definition -- consumers literally cannot full-scan without a partition predicate
- Copy jobs are free for same-region operations, making partition swap and staging swap nearly zero-cost
- `QUALIFY` is native, so dedup views and compaction queries are clean single statements
- Per-day cost limits prevent runaway retry loops from burning through the budget overnight

#strong[ECL weaknesses:]
- DML concurrency caps at 2 concurrent mutating statements per table, with up to 20 queued -- flood it and statements fail outright
- Every MERGE or UPDATE rewrites entire partitions it touches, so a 10-row update across 30 dates triggers 30 full partition rewrites
- JSON columns can't load from Parquet -- use Avro or JSONL for tables with JSON fields
- 10,000 partition limit per table (4,000 per single job), constraining daily-partitioned tables to ~27 years of history
- Rows inserted via the streaming buffer may be invisible to `EXPORT DATA` and table copy jobs for up to 90 minutes -- use batch load jobs instead of streaming inserts if your pipeline chains a load with an immediate copy

// ---

== Snowflake
<snowflake>
#strong[Best for:] predictable budgets, multi-workload isolation, data sharing, semi-structured data. Good for teams that need warehouse-level isolation between workloads: a small warehouse for ECL loads, a medium one for analyst queries, a large one for dashboard refreshes, each with its own auto-suspend and budget ceiling.

#strong[ECL strengths:]
- `VARIANT` handles arbitrary JSON natively with `:` path notation, no schema needed at load time
- `SWAP WITH` is atomic metadata-only swap -- staging swap completes in milliseconds regardless of table size
- Result cache returns identical queries within 24 hours at zero warehouse cost
- Micro-partition pruning is automatic without explicit partition DDL

#strong[ECL weaknesses:]
- `PRIMARY KEY` and `UNIQUE` constraints are metadata hints only -- deduplication is entirely your responsibility
- Grants don't survive `SWAP WITH` or `CREATE TABLE ... CLONE`, requiring `FUTURE GRANTS` or a re-grant step after every swap
- Reclustering costs warehouse credits in the background; heavily mutated tables accumulate significant charges
- No partition filter enforcement -- consumers can full-scan any table without warning

// ---

== ClickHouse
<clickhouse>
#strong[Best for:] append-heavy analytical workloads, real-time dashboards, self-hosted control, extreme query speed on fixed hardware. Works best when you lean into the merge model rather than fighting it -- if your workload is primarily appending event data and reading through pre-built materialized views, ClickHouse is hard to beat on raw performance per dollar.

#strong[ECL strengths:]
- Fastest raw INSERT throughput of any engine on this list -- bulk inserts into `MergeTree` engines are limited by disk I/O, not the engine
- `ReplacingMergeTree` provides eventual deduplication on merge, fitting naturally with append-and-materialize
- `REPLACE PARTITION` is atomic and operates at the partition level without rewriting other partitions
- Materialized views trigger on INSERT, enabling real-time pre-aggregation without a separate scheduling layer

#strong[ECL weaknesses:]
- No ACID guarantees for mutations -- `ALTER TABLE ... UPDATE` and `DELETE` are async, queued for the next merge cycle
- Duplicates coexist in `ReplacingMergeTree` until the merge scheduler runs; `SELECT ... FINAL` forces read-time dedup at a performance cost
- `ORDER BY` is fixed at table creation -- changing it requires rebuilding the table
- Small frequent inserts cause "too many parts" errors -- batch aggressively (tens of thousands of rows minimum)

// ---

== Redshift
<redshift>
#strong[Best for:] AWS-native shops with existing infrastructure, teams that want PostgreSQL-compatible SQL in a columnar engine. The legacy choice -- still viable for teams already invested in AWS, but BigQuery and Snowflake have moved ahead in ECL ergonomics around DML flexibility, schema evolution, and operational overhead.

#strong[ECL strengths:]
- `COPY` from S3 is fast bulk load with automatic compression, and S3 is the natural staging area for AWS pipelines
- PostgreSQL dialect means familiar SQL for teams coming from transactional databases
- `MERGE` added in late 2023, same syntax as BigQuery/Snowflake
- Spectrum queries S3 data directly without loading, useful for cold-tier data

#strong[ECL weaknesses:]
- Sort keys and dist keys are fixed at table creation -- changing them requires a full table rebuild
- `VACUUM` is required after heavy deletes; dead rows inflate scan time until cleaned up
- Row-by-row `INSERT` is orders of magnitude slower than `COPY` -- every load path must stage through S3
- Hard limit of 1,600 columns per table, and type changes require table rebuilds

// ---

== DuckDB / MotherDuck
<duckdb-motherduck>
#strong[Best for:] small-to-medium analytical workloads, local-first development, startups that want a warehouse without the bill.

DuckDB is an embedded columnar engine that runs in-process -- no server, no cluster, no infrastructure. MotherDuck adds a cloud layer on top: managed storage, sharing, and read scaling via "Ducklings" (isolated compute instances per user). The combination gives you BigQuery-class query performance on datasets up to a few TB.

#strong[ECL strengths:]
- Reads and writes Parquet and CSV natively from S3/GCS/Azure -- no separate load job needed
- `INSERT ON CONFLICT` and `MERGE INTO` (DuckDB 1.4+) support the upsert and merge patterns from 0403
- Develop locally with the exact same SQL that runs in MotherDuck cloud -- the dev-to-prod gap is zero
- Local DuckDB is free. MotherDuck's per-GB price (~\$0.15/GB) is higher than BigQuery's on-demand rate (~\$0.006/GB), but the total bill is often lower because DuckDB's single-node engine scans less data per query -- no distributed overhead, no shuffle. The savings come from efficiency, not a cheaper unit price

#strong[ECL weaknesses:]
- Single-writer architecture -- concurrent pipeline runs writing to the same database need external coordination (one run at a time, or separate databases per table)
- No partitioning in the BigQuery/Snowflake sense. Hive-partitioned Parquet on object storage or min/max index pruning, but no `PARTITION BY` in DDL, no partition-level replace, no `require_partition_filter`
- No `QUALIFY` syntax -- dedup queries need the subquery wrapper, same as PostgreSQL and Redshift
- At multi-TB scale with many concurrent dashboard users, MotherDuck costs converge toward Snowflake territory. The cost advantage is strongest for small teams with moderate data
- Self-hosting DuckDB on a dedicated server (Hetzner, bare metal) is zero-cost-per-query for a single client, but for multi-client pipelines the single-writer constraint means one database file per client with no shared users, roles, access control, or high availability -- at that point the engineering overhead of building isolation exceeds the hosting savings

For self-hosted columnar beyond ClickHouse, #strong[StarRocks] and #strong[Apache Doris] are worth evaluating -- both are FOSS MPP databases with MySQL wire protocol, real MERGE/upsert, ACID transactions, and better write concurrency than ClickHouse. Younger ecosystems, but they solve the concurrent-write limitations that make ClickHouse awkward for mutable ECL workloads.

#ecl-tip(
  "PostgreSQL as a destination",
)[For pipelines with fewer than ~100 tables, PostgreSQL with real PK enforcement, transactional `TRUNCATE`, and cheap `INSERT ON CONFLICT` is simpler and more forgiving than any columnar engine. The complexity tax of columnar only pays off when you need partition pruning, bytes-scanned billing, or warehouse-scale analytics. See 0107.]

// ---

== Load Pattern Compatibility
<load-pattern-compatibility>
How each engine handles the load strategies from Part IV, and what each costs relative to the others. Cost is per-run relative cost for the same data volume -- not absolute pricing, which depends on your contract and usage tier.

#figure(
  align(center)[#table(
    columns: (16%, 21%, 21%, 21%, 21%),
    align: (auto, auto, auto, auto, auto),
    table.header(
      [Engine], [Full replace (0401)], [Append-only (0402)], [Merge / upsert (0403)], [Append-and-materialize (0404)]
    ),
    table.hline(),
    [#strong[BigQuery]],
    [Partition copy or `CREATE OR REPLACE`. Near-free (copy jobs cost nothing same-region)],
    [`INSERT` via load jobs. Cheapest load operation -- no partition rewrite],
    [`MERGE` rewrites every partition touched. Expensive at scale -- cost proportional to partitions, not rows],
    [`QUALIFY` dedup view + `CREATE OR REPLACE` compaction. Load is cheap (append); read cost depends on log size],
    [#strong[Snowflake]],
    [`SWAP WITH` (metadata-only, instant). Free beyond warehouse startup],
    [`COPY INTO` from stage. Fast, warehouse time only],
    [`MERGE` consumes warehouse time. Moderate -- more predictable than BigQuery's partition model],
    [`QUALIFY` dedup view + `CREATE TABLE ... AS` compaction. Warehouse time on reads and compaction],
    [#strong[ClickHouse]],
    [`EXCHANGE TABLES` (atomic). Minimal cost on self-hosted],
    [Native strength -- fastest INSERT throughput of any engine],
    [No native MERGE. `ReplacingMergeTree` deduplicates eventually on merge cycle. Cheapest if you accept eventual consistency],
    [`ReplacingMergeTree` + `FINAL` for read-time dedup. Write is free; `FINAL` adds read overhead],
    [#strong[Redshift]],
    [`TRUNCATE` + `COPY` in transaction. Fast via S3 staging],
    [`COPY` from S3. Row-by-row `INSERT` is orders of magnitude slower],
    [`MERGE` (late 2023) or DELETE + INSERT in transaction. Moderate -- cluster compute],
    [Subquery dedup view + `CREATE TABLE ... AS` compaction. No `QUALIFY`],
    [#strong[PostgreSQL]],
    [`TRUNCATE` + `INSERT` in transaction. Atomic, transactional, cheap],
    [Standard `INSERT`. Cheap at moderate volumes],
    [`INSERT ON CONFLICT` with real PK enforcement. Cheapest upsert of any engine -- index lookup per row],
    [Subquery dedup view or materialized view. Read overhead on view; `REFRESH MATERIALIZED VIEW CONCURRENTLY` for zero-downtime],
    [#strong[DuckDB]],
    [`CREATE OR REPLACE` or `TRUNCATE` + `INSERT`. Free locally],
    [`INSERT` or `COPY FROM` Parquet. Free locally],
    [`MERGE INTO` (1.4+) or `INSERT ON CONFLICT`. Free locally; MotherDuck charges per GB scanned],
    [Subquery dedup view (no `QUALIFY`) + `CREATE OR REPLACE` compaction. Free locally],
  )],
  kind: table,
)

// ---

== Decision Matrix
<decision-matrix>
#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Workload], [Recommended], [Why]),
    table.hline(),
    [Many ad-hoc analysts, pay-per-query],
    [BigQuery],
    [Cost scales with actual usage; partition filter enforcement protects the bill],
    [Predictable budget, multi-team], [Snowflake], [Warehouse isolation, fixed compute costs, data sharing],
    [Append-heavy, real-time dashboards], [ClickHouse], [Fastest inserts, materialized views on write],
    [AWS-native, existing infrastructure],
    [Redshift],
    [Familiar PostgreSQL dialect, `COPY` from S3, Spectrum for cold data],
    [Small team, PostgreSQL expertise], [PostgreSQL], [Cheapest, real constraint enforcement, transactional `TRUNCATE`],
    [Startup, small team, moderate data],
    [DuckDB / MotherDuck],
    [Lowest cost, local-first dev, no infrastructure to manage],
    [Mixed analytical + operational consumers],
    [Snowflake or BigQuery + PostgreSQL],
    [Columnar for analytics, transactional for point queries (0405)],
  )],
  kind: table,
)

#ecl-tip(
  "Start with load strategy, not engine",
)[The decision matrix above is a starting point, but the more productive question is often: which load strategies does my pipeline need, and which engines support them cheaply? If every table can be fully replaced, all five engines work fine and the choice comes down to your cloud provider and team expertise. The engine choice starts to matter when you need high-concurrency MERGE, append-and-materialize with dedup views, or partition-level atomic swaps -- that's when the compatibility table above narrows the field.]

// ---

== Related Patterns
<related-patterns-1>
- 0104 -- Storage mechanics, partitioning, and engine behavior
- 0107 -- Transactional -\> Columnar vs Transactional -\> Transactional
- 0405 -- Dual-destination pattern for mixed workloads
- 0705 -- Engine-specific cost levers once data is loaded
- 0801 -- Syntax differences across all engines

// ---
