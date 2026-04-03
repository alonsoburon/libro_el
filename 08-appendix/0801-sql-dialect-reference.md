---
title: "SQL Dialect Reference"
aliases: []
tags:
  - appendix
status: draft
created: 2026-03-06
updated: 2026-04-01
---

# SQL Dialect Reference

The lookup table for every operation that differs between engines. When a pattern in the book says "syntax varies by engine," it points here. Six engines are covered: PostgreSQL, MySQL, and SQL Server as sources and transactional destinations; BigQuery, Snowflake, ClickHouse, and Redshift as columnar destinations.

**Quick nav**

- [[#Identifier Quoting and Case Sensitivity]]
- [[#Timestamp and Datetime Types]]
- [[#Date and Time Functions]]
- [[#Upsert / MERGE]]
- [[#Append and Materialize]]
- [[#Table Swap]]
- [[#Partition Operations]]
- [[#Partition and Clustering DDL]]
- [[#Deduplication (QUALIFY vs Subquery)]]
- [[#Bulk Loading]]
- [[#JSON and Semi-Structured Data]]
- [[#Schema Evolution]]
- [[#Source-Specific Traps]]
- [[#Engine Quirks]]

---

## Identifier Quoting and Case Sensitivity

| Engine | Default case | Quote character | Example |
|---|---|---|---|
| PostgreSQL | Folds to lowercase | `"double quotes"` | `"OrderID"` preserves case |
| MySQL | Case depends on OS (Linux: sensitive, Windows: insensitive) | `` `backticks` `` | `` `Order ID` `` |
| SQL Server | Case-insensitive (collation-dependent) | `[brackets]` or `"double quotes"` | `[Order ID]` |
| BigQuery | Case-sensitive | `` `backticks` `` | `` `project.dataset.table` `` |
| Snowflake | Folds to uppercase | `"double quotes"` | `"order_id"` preserves lowercase |
| ClickHouse | Case-sensitive | `` `backticks` `` or `"double quotes"` | Names preserved exactly |
| Redshift | Folds to lowercase | `"double quotes"` | Same as PostgreSQL |

See [[07-serving-the-destination/0707-schema-naming-conventions|0707]] for naming strategy.

---

## Timestamp and Datetime Types

| Type | PostgreSQL | MySQL | SQL Server | BigQuery | Snowflake | ClickHouse | Redshift |
|---|---|---|---|---|---|---|---|
| Naive (no TZ) | `TIMESTAMP` | `DATETIME` | `DATETIME2(n)` | -- | `TIMESTAMP_NTZ` | `DateTime` | `TIMESTAMP` |
| Aware (with TZ) | `TIMESTAMPTZ` | -- | `DATETIMEOFFSET` | `TIMESTAMP` | `TIMESTAMP_TZ` | `DateTime64` with tz | `TIMESTAMPTZ` |
| Max precision | Microseconds | Microseconds | 100 nanoseconds | Microseconds | Nanoseconds | Nanoseconds | Microseconds |

> [!warning] BigQuery has no naive datetime type
> Every `TIMESTAMP` in BigQuery is UTC. Naive timestamps from the source land as UTC -- if they were actually in `America/Santiago` or `Europe/Berlin`, every value is wrong from the moment it lands. Conform timezone info during load. See [[05-conforming-playbook/0505-timezone-conforming|0505]].

> [!warning] SQL Server DATETIME2(7) truncates on most destinations
> 100-nanosecond precision truncates to microseconds on BigQuery and Redshift. Snowflake's `TIMESTAMP_NTZ(9)` and ClickHouse's `DateTime64(7)` can preserve it.

See [[05-conforming-playbook/0503-type-casting-normalization|0503]] for the full type mapping.

---

## Date and Time Functions

| Operation | PostgreSQL | MySQL | SQL Server | BigQuery | Snowflake | ClickHouse |
|---|---|---|---|---|---|---|
| Subtract interval | `date - INTERVAL '1 day'` | `DATE_SUB(d, INTERVAL 1 DAY)` | `DATEADD(day, -1, d)` | `DATE_SUB(d, INTERVAL 1 DAY)` | `DATEADD(day, -1, d)` | `d - INTERVAL 1 DAY` |
| Add interval | `date + INTERVAL '1 day'` | `DATE_ADD(d, INTERVAL 1 DAY)` | `DATEADD(day, 1, d)` | `DATE_ADD(d, INTERVAL 1 DAY)` | `DATEADD(day, 1, d)` | `d + INTERVAL 1 DAY` |
| Truncate to month | `date_trunc('month', d)` | `DATE_FORMAT(d, '%Y-%m-01')` | `DATEFROMPARTS(YEAR(d), MONTH(d), 1)` | `DATE_TRUNC(d, MONTH)` | `DATE_TRUNC('month', d)` | `toStartOfMonth(d)` |
| Difference (days) | `d2 - d1` (returns integer) | `DATEDIFF(d2, d1)` | `DATEDIFF(day, d1, d2)` | `DATE_DIFF(d2, d1, DAY)` | `DATEDIFF(day, d1, d2)` | `dateDiff('day', d1, d2)` |
| Extract part | `EXTRACT(YEAR FROM d)` | `EXTRACT(YEAR FROM d)` | `DATEPART(year, d)` or `YEAR(d)` | `EXTRACT(YEAR FROM d)` | `EXTRACT(YEAR FROM d)` or `YEAR(d)` | `toYear(d)` |
| Current timestamp | `NOW()` or `CURRENT_TIMESTAMP` | `NOW()` or `CURRENT_TIMESTAMP` | `GETDATE()` or `SYSDATETIME()` | `CURRENT_TIMESTAMP()` | `CURRENT_TIMESTAMP()` | `now()` |

> [!tip] Argument order varies
> MySQL and BigQuery put `DATEDIFF(end, start)`. SQL Server, Snowflake, and ClickHouse put the unit first: `DATEDIFF(day, start, end)`. PostgreSQL skips the function entirely and uses subtraction. Getting the argument order wrong produces results with the wrong sign.

---

## Upsert / MERGE

**BigQuery / Snowflake / SQL Server**

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

**PostgreSQL**

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

**MySQL**

```sql
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  total = VALUES(total),
  updated_at = VALUES(updated_at);
```

**ClickHouse** -- no native upsert. Use `ReplacingMergeTree` with eventual dedup on merge, or append + deduplicate. See [[04-load-strategies/0404-append-and-materialize|0404]].

**Redshift** -- `MERGE` added in late 2023, same syntax as Snowflake/BigQuery. For older clusters or performance-sensitive loads, the classic pattern is DELETE + INSERT in a transaction.

| Engine | Duplicate key in staging |
|---|---|
| BigQuery | Runtime error if multiple source rows match one destination row |
| Snowflake | Processes both rows, nondeterministic -- undefined which wins |
| PostgreSQL | Processes rows in insertion order, last one wins |
| SQL Server | Runtime error on multiple matches (same as BigQuery) |

See [[04-load-strategies/0403-merge-upsert|0403]] for cost analysis and when to use MERGE vs alternatives.

---

## Append and Materialize

The alternative to MERGE on columnar engines: append every extraction to a log table, deduplicate with a view. Load cost drops to near-zero (pure INSERT), and the dedup cost shifts to read time.

**Append to log (all engines)**

```sql
INSERT INTO orders_log
SELECT *, CURRENT_TIMESTAMP AS _extracted_at
FROM _stg_orders;
```

**Dedup view -- BigQuery / Snowflake / ClickHouse**

```sql
CREATE OR REPLACE VIEW orders AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

**Dedup view -- PostgreSQL / MySQL / SQL Server / Redshift**

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

**Compaction -- collapse to latest-only (BigQuery / Snowflake / ClickHouse)**

```sql
CREATE OR REPLACE TABLE orders_log AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

Keeps exactly one row per key regardless of age -- safe with any extraction strategy. All version history is gone, but every current row survives. On engines without `QUALIFY`, use the subquery wrapper inside the `CREATE TABLE ... AS SELECT`.

See [[04-load-strategies/0404-append-and-materialize|0404]] for the full pattern, cost tradeoffs, and retention sizing.

---

## Table Swap

**Snowflake**

```sql
ALTER TABLE stg_orders SWAP WITH orders;
```

Atomic, metadata-only. Grants do **not** carry over -- they follow the table name, not the data. Re-grant after every swap, or use `FUTURE GRANTS` on the schema.

**BigQuery**

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

**PostgreSQL / Redshift**

```sql
BEGIN;
ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE stg_orders RENAME TO orders;
DROP TABLE orders_old;
COMMIT;
```

Atomic within the transaction. If the transaction rolls back, `orders` is untouched.

**ClickHouse**

```sql
EXCHANGE TABLES stg_orders AND orders;
```

Atomic swap of both table names. The old production data moves to `stg_orders` after the swap.

See [[02-full-replace-patterns/0203-staging-swap|0203]] for the full pattern.

---

## Partition Operations

**BigQuery -- partition copy**

```bash
# Near-metadata operation, orders of magnitude faster than DML
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_events$20260307 \
  project:dataset.events$20260307
```

Staging must be partitioned by the same column and type as the destination. One copy per partition, but each copy is near-free.

**Snowflake / Redshift -- DELETE + INSERT in transaction**

```sql
BEGIN;
DELETE FROM events
WHERE partition_date BETWEEN :start_date AND :end_date;
INSERT INTO events SELECT * FROM stg_events;
COMMIT;
```

Delete by the declared range, not by what's in staging. If Saturday had rows last run and the source corrected them to Friday, a staging-driven delete would leave stale Saturday data in place.

**ClickHouse -- REPLACE PARTITION**

```sql
ALTER TABLE events REPLACE PARTITION '2026-03-07' FROM stg_events;
ALTER TABLE events REPLACE PARTITION '2026-03-08' FROM stg_events;
```

Atomic per partition, operates at the partition level without rewriting rows.

See [[02-full-replace-patterns/0202-partition-swap|0202]] for the full pattern.

---

## Partition and Clustering DDL

**BigQuery**

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

**Snowflake**

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

**ClickHouse**

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

**Redshift**

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

See [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for storage mechanics and [[07-serving-the-destination/0702-partitioning-for-consumers|0702]] for key selection.

---

## Deduplication (QUALIFY vs Subquery)

**BigQuery / Snowflake / ClickHouse -- QUALIFY**

```sql
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY _extracted_at DESC
) = 1;
```

**PostgreSQL / MySQL / SQL Server / Redshift -- subquery wrapper**

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

See [[04-load-strategies/0404-append-and-materialize|0404]] for dedup views and [[06-operating-the-pipeline/0613-duplicate-detection|0613]] for detection patterns.

---

## Bulk Loading

| Engine | Primary method | Preferred format | Key constraint |
|---|---|---|---|
| BigQuery | `bq load` / `LOAD DATA` / streaming | Avro (handles JSON natively) | JSON columns can't load from Parquet |
| Snowflake | `COPY INTO` from stage | Parquet | `VARIANT` from Parquet lands as string, needs `PARSE_JSON` |
| ClickHouse | `INSERT INTO ... SELECT` (batch) | Parquet or native format | Small inserts cause too-many-parts; batch aggressively |
| Redshift | `COPY` from S3 | Parquet | Row-by-row INSERT is orders of magnitude slower than COPY |
| PostgreSQL | `COPY` / `\copy` | CSV or binary | Binary is faster but not human-readable |
| MySQL | `LOAD DATA INFILE` | CSV | `LOAD DATA LOCAL INFILE` has security restrictions |

See [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for format compatibility and gotchas.

---

## JSON and Semi-Structured Data

| Engine | Native type | Load from Parquet? | Query syntax |
|---|---|---|---|
| BigQuery | `JSON` | No -- use JSONL or Avro | `JSON_VALUE(col, '$.key')`, dot notation |
| Snowflake | `VARIANT` | Lands as string, needs `PARSE_JSON` | `col:key::type`, `:` path notation |
| ClickHouse | `String` (parse with functions) | Yes (as string) | `JSONExtractString(col, 'key')` |
| Redshift | `SUPER` | Yes | PartiQL syntax: `col.key` |
| PostgreSQL | `JSONB` / `JSON` | N/A (not a bulk load format) | `col->>'key'`, `col @> '{}'` operators |
| MySQL | `JSON` | N/A | `JSON_EXTRACT(col, '$.key')`, `col->>'$.key'` |

See [[05-conforming-playbook/0507-nested-data-and-json|0507]] for conforming strategy.

---

## Schema Evolution

| Operation | BigQuery | Snowflake | ClickHouse | Redshift |
|---|---|---|---|---|
| ADD COLUMN | Instant | Fast (metadata) | Metadata-only for MergeTree | Cheap if added at end |
| Type widening | Compatible pairs (`INT64` → `NUMERIC`) | VARCHAR width increase OK | Some widening via `MODIFY COLUMN` | Requires full table rebuild |
| DROP COLUMN | Destructive (breaks `SELECT *` downstream) | Fast | Supported | Supported |
| Column limit | 10,000 | No hard limit | No hard limit | 1,600 |
| Key limitation | -- | CLONE picks up new columns automatically | `ORDER BY` key fixed at creation | Sort/dist keys fixed at creation |

See [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for full details and [[06-operating-the-pipeline/0609-data-contracts|0609]] for schema policies.

---

## Source-Specific Traps

- **PostgreSQL**: `TIMESTAMP` vs `TIMESTAMPTZ` confusion -- both exist, applications mix them. TOAST compression on large columns can slow extraction on wide tables.
- **MySQL**: `utf8` is 3-byte UTF-8, not real UTF-8. `utf8mb4` is real UTF-8. If the source uses `utf8`, you might be getting truncated data. `DATETIME` has no timezone at all. `TINYINT(1)` is commonly used as a boolean but it's still an integer.
- **SQL Server**: `WITH (NOLOCK)` avoids blocking writers during extraction but reads dirty data (rows mid-transaction). `DATETIME2(7)` nanosecond precision truncates on most destinations. Getting read access to a production SQL Server often involves procurement, security reviews, and a DBA who has 47 other priorities.
- **SAP HANA**: Proprietary SQL dialect. Legally restricted access to some tables (S/4HANA). Varies by SAP module -- extraction patterns that work for B1 may not apply to S/4. If you're extracting from SAP, you already know.

See [[01-foundations-and-archetypes/0103-transactional-sources|0103]] for the full terrain.

---

## Engine Quirks

**BigQuery**
- DML concurrency: max 2 mutating statements per table concurrently, up to 20 queued. Flood it and statements fail outright
- Every DML rewrites entire partitions it touches -- 10K rows across 30 dates = 30 full partition rewrites
- Copy jobs are free for same-region operations
- Streaming inserts: rows may be briefly invisible to `EXPORT DATA` and table copies (typically minutes, up to 90)

**Snowflake**
- `PRIMARY KEY` and `UNIQUE` constraints are not enforced -- they're metadata hints only. Deduplication is your problem
- `VARIANT` from Parquet loads as string, not queryable JSON, until you `PARSE_JSON`
- Result cache: identical queries within 24h return cached results at no warehouse cost
- Grants don't survive `SWAP WITH` or `CREATE TABLE ... CLONE`

**ClickHouse**
- `ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE` are async -- they return immediately, actual work happens during the next merge
- `ReplacingMergeTree` deduplicates on merge, not on insert. Duplicates coexist until the merge scheduler runs. `SELECT ... FINAL` forces read-time dedup at a performance cost
- Small inserts cause a "too many parts" error. Batch inserts into blocks of at least tens of thousands of rows
- `ENGINE` is required in every `CREATE TABLE`. `ORDER BY` is fixed at creation

**Redshift**
- `COPY` from S3 is the only performant bulk load. Row-by-row `INSERT` is orders of magnitude slower
- `VACUUM` is required after heavy deletes -- dead rows inflate scan time and storage until cleaned up
- Sort keys and dist keys are fixed at creation. Changing them requires a full table rebuild
- Hard limit of 1,600 columns per table

See [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] for full engine profiles and [[07-serving-the-destination/0705-cost-optimization-by-engine|0705]] for cost levers.
