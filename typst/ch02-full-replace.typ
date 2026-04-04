#import "theme.typ": palette, ecl-tip, ecl-warning, ecl-danger, ecl-info
= Full Scan Strategies
<full-scan-strategies>
#quote(block: true)[
#strong[One-liner:] When incremental isn't worth it -- or isn't possible -- extract everything and replace the destination completely.
]

Full scan is the simplest pipeline that exists. Extract every row, replace the destination, done. No cursor state to maintain, no missed deletes, no drift accumulation. It resets the world on every run. The engineering community has overcomplicated data pipelines by defaulting to incremental when most tables don't need it. This chapter is about when full scan is the right answer (Which hopefully is all times) -- and how to do it without killing your source database or leaving a window of empty data in production.

== When Full Scan Wins
<when-full-scan-wins>
The decision comes down to one comparison: #strong[full scan cost vs.~incremental complexity cost + drift risk];. If the scan is cheap and the table is messy, full scan wins every time.

#strong[The table is small enough.] Dimensions, configuration tables, reference data, lookup tables. What "small enough" means depends on your source database size and how frequently the table is updated -- a 500k-row table on a lightly loaded PostgreSQL replica is different from 500k rows on a production ERP mid-day. The question is whether the source can absorb the scan without impacting application performance, and whether the extraction time fits your schedule window.

#strong[No reliable cursor.] No `updated_at`, no row version, no sequence. You asked the team and they shrugged. You checked `information_schema` and found nothing useful. You can't borrow a cursor from a header table because this is a standalone table with no parent. Without a cursor, incremental extraction is impossible without hashing every row -- which has its own cost and complexity. Full scan is cleaner.

#strong[Hard deletes happen and aren't worth tracking separately.] Incoming payments, cancelled reservations, temporary staging records -- tables where rows get deleted regularly and the deletion is part of the business state you need to reflect. A full scan picks up deletions automatically because the deleted rows simply aren't there when you extract. A cursor-based incremental is blind to deletions by design and requires a separate detection mechanism. If the table is small enough, don't bother.

#strong[The source rewrites history.] Some applications correct past records in bulk. A pricing table where last quarter's prices get retroactively adjusted. An ERP where a journal entry gets reversed and reposted to a prior period. A more problematic DBA who runs UPDATE scripts directly. A cursor on `updated_at` misses rows that were corrected without bumping the timestamp. Full scan doesn't care -- you get the current state of every row, always.

#ecl-warning("Earn incremental complexity")[Full scan is the default -- don't assume incremental is needed. Incremental is a performance optimization with a cost in complexity, drift risk, and maintenance. Build full scan first. Switch to incremental only when the scan is genuinely too slow or too expensive for your schedule window. See @purity-vs-freshness.]

== The Two Shapes of Full Scan
<the-two-shapes-of-full-scan>
#strong[Full table, every run.] Extract all rows, replace the destination completely on every execution. The simplest pipeline that exists and the most reliable. No state, no checkpoints, no drift. This should be your default for any table that fits the window.

#strong[Full table, periodic + incremental between.] Run a full scan nightly or weekly to reset state, run incremental extractions intraday to get freshness. The full scan is the safety net that catches everything the incremental misses -- soft rule violations, missed timestamps, retroactive corrections. The incremental is the performance optimization that gives you sub-daily freshness without scanning the whole table every hour. See 0301 for how to design the incremental so it plays well with the periodic full reset.

== Source Load and Extraction Etiquette
<source-load-and-extraction-etiquette>
Full scans hit the source harder than incremental extractions. A few rules:

#strong[Schedule during off-peak hours.] 2am, weekends, after the monthly close finishes. Know your source system's busy hours before you set the schedule. On a production ERP, mid-morning is when 200 users are posting invoices and confirming orders. That is not when you want to be scanning `order_lines`.

#strong[Use a read replica when available.] A replica absorbs your scan without touching the primary. Replication lag is a real concern -- you might miss rows committed in the last few seconds -- but for a nightly full scan this is almost never material. Confirm lag with the DBA.

#strong[Chunk large tables.] Never pull millions of rows in a single query. Break the extraction into chunks by PK range and append each chunk before replacing the destination. Chunking reduces peak memory, avoids query timeouts, and makes failures recoverable -- if chunk 47 fails, you retry chunk 47, not the whole table.

#strong[Manual chunking] by PK range:

```sql
-- source: transactional
-- engine: postgresql
-- Chunk 1: rows 1 -- 100000
SELECT * FROM order_lines
WHERE id BETWEEN 1 AND 100000
ORDER BY id;

-- Chunk 2: rows 100001 -- 200000
SELECT * FROM order_lines
WHERE id BETWEEN 100001 AND 200000
ORDER BY id;
```

The chunk size is a tunable parameter -- start at 100k rows and adjust based on query time and memory pressure. Most orchestrators let you parameterize this per asset or per source.

Most database drivers support streaming modes that yield rows incrementally without loading the full result set into memory. SQLAlchemy's `yield_per` does this consistently across every source engine -- the same code works against PostgreSQL, MySQL, SQL Server, and SAP HANA:

```python
# orchestrator: python
# SQLAlchemy yield_per: stream results without loading full result set into memory
with engine.connect() as conn:
    result = conn.execution_options(yield_per=10_000).execute(
        text("SELECT * FROM order_lines ORDER BY id")
    )
    for chunk in result.partitions():
        stage_chunk(chunk)  # append to staging, not to final table
```

#ecl-warning("Stage all chunks before replacing")[If you chunk the extraction and write each chunk directly to the final destination table, chunk N replaces chunk N-1. You'll end up with only the last chunk in the destination. Extract all chunks to a staging area first, validate, then swap. Always.]

See @source-system-etiquette for connection limits, timeout coordination, and DBA communication.

== At the Destination: Replace Strategies
<at-the-destination-replace-strategies>
Full replace is not "DELETE everything, INSERT everything." That approach leaves a window where the table is empty, and it's more expensive than necessary on most engines.

// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Staging swap.] Load into a staging table, validate, then atomically swap staging to production. Zero downtime -- consumers query the production table and see complete data throughout. Rollback is dropping the staging table without touching prod. This is the recommended approach for any table with live consumers. See @staging-swap.

#strong[Partition-level replace.] When the table is partitioned by date and you're replacing a specific date range, drop and reload only the affected partitions. Still a full replace per partition -- you extract all rows for those dates and reload completely -- but you don't touch partitions outside the range. See @partition-swap.

#strong[Truncate + reload.] `TRUNCATE` the table and insert fresh. Simple, but it has a window where the table is empty. Acceptable for overnight runs where no dashboards or queries are running against the table. Never acceptable for tables with intraday consumers.

#ecl-warning("Use bulk loads, not row-by-row inserts")[On every columnar engine, a `LOAD DATA` job or `COPY INTO` from a file is significantly cheaper than a set of `INSERT` statements. BigQuery charges for DML operations; Snowflake burns warehouse time per statement. Load your staging data from Parquet or Avro files, not from repeated inserts. This is especially important at scale -- 10M rows via `LOAD DATA` is one job; 10M rows via `INSERT` is 10M jobs.]

== Data Quality Before the Swap
<data-quality-before-the-swap>
Never swap staging to production without validating first. The full replace pattern is powerful precisely because it resets state -- which means a bad load resets to bad state, with no prior version to fall back on.

#strong[Minimum checks before every swap:]

```sql
-- source: columnar
-- engine: bigquery
-- Run these against the staging table before swapping to production

-- 1. Table is not empty
SELECT COUNT(*) AS row_count FROM stg_order_lines;
-- Fail if row_count = 0

-- 2. Row count is within 10% of yesterday's production count
SELECT
    ABS(staging_count - prod_count) * 1.0 / prod_count AS pct_change
FROM (
    SELECT COUNT(*) AS staging_count FROM stg_order_lines
) s,
(
    SELECT COUNT(*) AS prod_count FROM order_lines
) p;
-- Fail if pct_change > 0.10

-- 3. No NULLs on required columns
SELECT COUNT(*) AS null_order_ids
FROM stg_order_lines
WHERE order_id IS NULL;
-- Fail if null_order_ids > 0
```

A full replace that lands zero rows because of a source connection failure is a production disaster. The table goes empty. Every dashboard shows nothing. Every downstream query breaks. The check `row_count > 0` is the single most important gate in a full replace pipeline.

Most orchestrators support post-load validation hooks or checks that run after the staging load and block the swap if any check fails.

See @data-contracts for formalizing these checks into reusable contracts.

== What Full Scan Doesn't Solve
<what-full-scan-doesnt-solve>
#strong[Tables too large to scan entirely.] When the full scan takes longer than your schedule window, or when the source can't handle the load at any hour, full scan isn't viable. Options: scope the scan to the current + previous period (@scoped-full-replace), or switch to a rolling window (@rolling-window-replace).

#strong[Freshness tighter than scan frequency.] If the business needs data every 15 minutes and a full scan takes 2 hours, you need incremental. Part III covers cursor-based extraction, merge patterns, and append strategies for tables that need sub-hourly freshness.

#strong[Source that can't absorb the load.] Some sources are so sensitive that even an off-hours full scan causes problems. Shared multi-tenant SaaS databases, under-resourced ERPs, systems with hard connection limits. In these cases, extract incrementally and accept the complexity cost. It's cheaper than a production incident.

== Related Patterns
- @partition-swap
- @staging-swap
- @scoped-full-replace
- @cursor-based-timestamp-extraction
- @source-system-etiquette
- @data-contracts
- @purity-vs-freshness

// ---

= Partition Swap
<partition-swap>
#quote(block: true)[
#strong[One-liner:] Replace data at partition granularity -- one partition or thirty, one extraction pass, without touching the rest of the table.
]

A full table replace is the cleanest option when the table fits the window. When it doesn't -- years of historical events, a `metrics_daily` table going back a decade -- partition swap is the next cleanest. You still extract everything in the target range in one pass, still load to staging, still validate before touching production. The only difference is the destination operation: instead of replacing the entire table, you replace only the partitions that changed.

Rows outside the target range are never touched. The rest of the table stays exactly as it was.

== When to Use It
<when-to-use-it>
- The table is partitioned by date and the data is naturally aligned to partition boundaries
- A bounded range needs reloading: yesterday's data was corrected, a backfill covers a month, an upstream pipeline redelivered a week of events
- Full table replace is too expensive -- years of history sit in partitions you have no reason to touch
- `metrics_daily`, `events`, `sessions` -- any table where each partition is a self-contained, replaceable unit

The partition boundary must be meaningful. If your `events` table has rows for `2026-03-07` scattered across multiple partitions because of a timezone mismatch, partition swap will produce incorrect results. See @timezone-conforming.

== The Mechanics
<the-mechanics>
One extraction pass covers the full target range:

```sql
-- source: transactional
-- engine: postgresql
-- Extract the complete target range in a single pass
SELECT *
FROM events
WHERE event_date BETWEEN :start_date AND :end_date;
```

Load everything to a staging table on the destination. Validate. Then replace the affected partitions -- all of them, in the same job.

// TODO: Convert mermaid diagram to Typst or embed as SVG

=== Extraction Status as the First Gate
<extraction-status-as-the-first-gate>
Before anything touches staging, the extraction must have completed successfully. A query that returns 0 rows is not an error -- it means the source had no data for that range, and that is correct information. A query that fails with a connection error, a timeout, or an exception is a different outcome entirely.

```python
# orchestrator: python
try:
    rows = extract_events(start_date, end_date)  # raises on connection error, timeout, etc.
    load_to_staging(rows)                         # 0 rows is a valid result here
except Exception as e:
    raise  # propagate -- do not proceed to partition operations
```

If the extraction raised an error, the job fails. Staging is never loaded. No partition is replaced. The data in production stays exactly as it was.

#ecl-warning("Silent failures return empty results")[The dangerous case is an extraction layer that swallows exceptions and returns an empty result set instead of raising. Check your database driver and connection wrapper -- make sure a dropped connection or a query timeout surfaces as an error, not as an empty iterator. If your extraction layer can return 0 rows on failure, you've lost the signal that makes this safe. See @extraction-status-gates.]

== Atomicity Per Engine
<atomicity-per-engine>
The extraction is always one pass. The destination-side replacement varies by engine.

=== Snowflake / Redshift
<snowflake-redshift>
Load to staging, then DELETE + INSERT in a transaction. Delete the full target range by date bounds -- not by what's in staging:

```sql
-- engine: snowflake / redshift
BEGIN;
DELETE FROM events
WHERE partition_date BETWEEN :start_date AND :end_date;
INSERT INTO events SELECT * FROM stg_events;
COMMIT;
```

Atomic: if the INSERT fails, the DELETE rolls back. Safe to retry.

The DELETE must cover the full target range, not `IN (SELECT DISTINCT partition_date FROM stg)`. If Saturday had 10 rows last run and the source corrected them to Friday, staging has no Saturday rows -- and a DELETE driven by staging would leave the old Saturday data in place. Delete by the declared range; insert whatever staging holds, including nothing for days with no activity.

=== BigQuery
<fullreplace-bigquery>
MERGE is the wrong answer here. It scans both tables in full and is the slowest, most expensive DML option BigQuery has. Real-world cases of MERGE consuming hours of slot time on large tables are documented. DELETE + INSERT has no transaction wrapper and leaves an empty-partition window between the two statements.

The right approach: load all data to a staging table partitioned by the same column as the destination, then use #strong[partition copy] per partition -- a near-metadata operation that is orders of magnitude faster than any DML:

```bash
# staging must be partitioned by the same column as destination
# then: copy each staging partition to destination
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_events$20260307 \
  project:dataset.events$20260307
```

N partition copies for N partitions, but each copy is fast. The staging load is one job. The partition copies are the loop -- and in BigQuery, copy jobs are near-free in both time and cost compared to DML.

#ecl-warning("Staging must match partition spec")[`bq cp` with a partition decorator requires the source table to be partitioned by the same column and type as the destination. Create staging with `PARTITION BY event_date` -- same as the destination -- before loading.]

=== ClickHouse
<fullreplace-clickhouse>
DELETE is an async mutation -- queued, not inline. `ALTER TABLE ... REPLACE PARTITION` is the right mechanism: it atomically swaps the source partition into the destination.

```sql
-- engine: clickhouse
-- For each partition in the target range:
ALTER TABLE events REPLACE PARTITION '2026-03-07' FROM stg_events;
ALTER TABLE events REPLACE PARTITION '2026-03-08' FROM stg_events;
```

Sequential within the job, still one orchestrator run. ClickHouse `REPLACE PARTITION` is fast -- it operates at the partition level without rewriting rows.

== Validation Before Swap
<validation-before-swap>
```sql
-- source: columnar
-- engine: bigquery
-- Run against the staging table before any partition operations

-- No NULLs on the partition key
SELECT COUNT(*) AS null_dates
FROM stg_events
WHERE event_date IS NULL;
-- Fail if null_dates > 0
-- A NULL partition key means a row can't be assigned to any partition.
-- On BigQuery, it lands in the __NULL__ partition. On Snowflake/Redshift,
-- it won't be deleted by the BETWEEN range and won't insert into the right place.
```

The partition list for replacement must come from the #strong[target date range you declared];, not from the distinct dates in staging. If you drive the replacement from `SELECT DISTINCT event_date FROM stg`, you'll skip dates that went to zero -- and those partitions will keep their old data.

For Snowflake and Redshift this means the DELETE covers `:start_date` to `:end_date` regardless of what staging contains. For BigQuery, the partition copy loop iterates the declared date range -- for dates with no staging rows, copy an empty partition or explicitly delete the destination partition.

== One Job
<one-job>
From the outside, this is a single pipeline run: one extraction, one staging load, N destination operations. The orchestrator sees one job succeed or fail -- not thirty.

When it fails, you rerun it. The extraction reruns cleanly because it's a bounded range query against the source. The staging load reruns because staging is a throwaway table -- truncate and reload. The partition operations rerun because replacing a partition with the same data produces the same result. There's no accumulated state to worry about, no half-applied changes to untangle. Rerun it and move on.

Compare that to an incremental pipeline that fails mid-run: you're left asking what got written, what didn't, whether the cursor advanced, and whether rerunning will duplicate data. With partition swap, the answer to "what do I do if it fails?" is always the same.

#strong[When staging is already valid.] For large backfills -- 30 partitions, say -- rerunning the full extraction just to retry two failed partition copies is wasteful. If staging is still intact from the previous run, retry only the failed partition operations against the existing staging data. The staging table didn't change; the per-partition operation is independent and safe to rerun in isolation. BigQuery partition copies and ClickHouse `REPLACE PARTITION` both support this cleanly.

For Snowflake and Redshift, the DELETE + INSERT is a single transaction -- it either committed or rolled back entirely. There are no partial partitions to retry. Rerun the full destination step against the existing staging table.

== Partition Alignment Is Your Responsibility
<partition-alignment-is-your-responsibility>
The engine partitions by whatever value is in the partition key column. If that value is wrong -- because of a timezone mismatch, a bulk load that used server time instead of event time, a late-arriving batch processed with today's date -- the row lands in the wrong partition and partition swap will replace the wrong thing.

Conform timezone before determining the partition key, not after. See @timezone-conforming.

Late-arriving data adds another dimension: rows for prior dates arriving today belong in their original partition, not today's. Your extraction range must account for this. If yesterday's data is still arriving today, your target range should include yesterday -- and your overlap window should be wide enough to catch stragglers. See @late-arriving-data.

== By Corridor
#ecl-warning("Transactional to Columnar")[Primary use case (e.g.~PostgreSQL → BigQuery). Columnar destinations are built for partitioned loads. One staging load + N partition operations per job. BigQuery partition copy is near-free compared to any DML option.]

#ecl-info("Transactional to Transactional")[E.g.~PostgreSQL → PostgreSQL. Transactional destinations have no columnar partition concept. Equivalent: `DELETE WHERE partition_key BETWEEN :start AND :end` then bulk INSERT from staging, inside a transaction. Less elegant but achieves the same scoped replace with the same atomicity guarantee.]

== Related Patterns
<related-patterns-1>
- @full-scan-strategies
- @staging-swap
- @late-arriving-data
- @timezone-conforming
- @columnar-destinations
- @extraction-status-gates

// ---

= Staging Swap
<staging-swap>
#quote(block: true)[
#strong[One-liner:] Load into a staging table, validate, then atomically swap to production. Zero downtime, trivial rollback.
]

== The Problem
The naive full replace is `TRUNCATE production; INSERT INTO production SELECT * FROM source`. Simple. And it leaves a window where `production` is empty -- any dashboard or query that runs between the TRUNCATE and the INSERT sees nothing. On a table with live consumers, that's an incident.

The second problem: if the load fails halfway through, you're left with a half-loaded production table and no clean way back. You can't replay the INSERT without truncating again, which means another empty window.

Staging swap eliminates both problems. Consumers see complete data throughout. Rollback is dropping the staging table without touching production.

== The Mechanics
<the-mechanics-1>
// TODO: Convert mermaid diagram to Typst or embed as SVG

Three steps:

#strong[\1. Load to staging.] Extract from source and load entirely into the staging table. Production is untouched. If the extraction or load fails at any point, nothing has happened to production.

Two conventions for where staging lives, each with real trade-offs:

#figure(
  align(center)[#table(
    columns: (18.26%, 37.39%, 44.35%),
    align: (auto,auto,auto,),
    table.header([], [Table prefix`public.stg_orders`], [Parallel schema`orders_staging.orders`],),
    table.hline(),
    [Namespace], [Pollutes production schema], [Clean separation],
    [Permissions], [Per-table grants], [Schema-level grant / revoke],
    [Cleanup], [Drop tables individually], [`DROP SCHEMA ... CASCADE`],
    [Swap complexity], [Simple -- rename within schema], [Harder -- cross-schema move or copy],
    [Snowflake `SWAP WITH`], [Works directly], [Works across schemas],
    [PostgreSQL swap], [`RENAME TO` in transaction], [`SET SCHEMA` + rename -- 3 steps],
    [BigQuery], [`bq cp` or DDL rename (within same dataset)], [`bq cp` only -- `RENAME TO` doesn't cross datasets.],
    [ClickHouse], [No difference], [No difference],
  )]
  , kind: table
  )

The parallel schema convention is worth it at scale -- permission management alone justifies it when you're running hundreds of tables. But go in with eyes open: the swap step is more involved on PostgreSQL and Redshift, and you'll need to handle it explicitly per engine.

#strong[\2. Validate.] Run checks against `stg_orders` before touching production. At minimum: row count \> 0, % change vs.~production is within threshold, required columns have no NULLs. See @data-contracts for formalizing these as reusable contracts.

#strong[\3. Swap.] Atomically replace production with staging. The mechanism varies by engine -- covered below -- but the result is the same: one moment consumers are reading the old data, the next they're reading the new data, with no empty window in between.

== The Swap Operation
<the-swap-operation>
The swap must be atomic -- consumers should never see a missing table. Each engine has its own mechanism.

=== Snowflake
<fullreplace-snowflake>
```sql
-- engine: snowflake
-- Atomic metadata-only swap -- fast regardless of table size
ALTER TABLE stg_orders SWAP WITH orders;
```

`SWAP WITH` is the cleanest option on any engine: metadata-only, instant, truly atomic. One caveat: grants defined on `orders` do #strong[not] carry over after a SWAP -- they follow the table name, not the data. If consumers have been granted access to `orders`, they now have access to the old staging table (renamed to `orders` after the swap). Re-grant after every swap, or use `FUTURE GRANTS` on the schema.

=== BigQuery
<bigquery-1>
BigQuery has no native SWAP. In BigQuery, schema = dataset, so the parallel schema convention means `orders_staging.orders` → `orders.orders`.

#strong[Table prefix convention] (`dataset.stg_orders` → `dataset.orders`):

```bash
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_orders \
  project:dataset.orders
```

Or with DDL rename (brief unavailability window between steps):

```sql
-- engine: bigquery
ALTER TABLE `project.dataset.orders` RENAME TO orders_old;
ALTER TABLE `project.dataset.stg_orders` RENAME TO orders;
DROP TABLE IF EXISTS `project.dataset.orders_old`;
```

#strong[Parallel dataset convention] (`orders_staging.orders` → `orders.orders`):

```bash
bq cp --write_disposition=WRITE_TRUNCATE \
  project:orders_staging.orders \
  project:orders.orders
```

`ALTER TABLE RENAME TO` does not cross dataset boundaries -- DDL rename is not an option with parallel datasets. The copy job works in both conventions.

#ecl-warning("BigQuery copy job performance")[Copy jobs are free (no slot consumption, no bytes-scanned charge) for same-region operations. Cross-region copies incur data transfer charges. Google's documentation explicitly notes that copy job duration "might vary significantly across different runs because the underlying storage is managed dynamically" -- there are no guarantees about speed regardless of whether source and destination are in the same dataset or different datasets. Factor this into your schedule window for large tables.]

Use the copy job for tables with live consumers. Use DDL rename (same-dataset only) when you control the maintenance window.

=== PostgreSQL / Redshift
<postgresql-redshift>
#strong[Table prefix convention] -- rename within the same schema:

```sql
-- engine: postgresql / redshift
BEGIN;
ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE stg_orders RENAME TO orders;
DROP TABLE orders_old;
COMMIT;
```

#strong[Parallel schema convention] -- move across schemas:

```sql
-- engine: postgresql / redshift
BEGIN;
ALTER TABLE orders RENAME TO orders_old;
ALTER TABLE orders_staging.orders SET SCHEMA public;  -- moves to public schema, keeps name 'orders'
DROP TABLE orders_old;
COMMIT;
```

`SET SCHEMA` moves the table without copying data -- it's a metadata operation, not a rewrite. In both cases, if the transaction rolls back, `orders` is still the original table, unchanged.

=== ClickHouse
<clickhouse-1>
```sql
-- engine: clickhouse
-- EXCHANGE TABLES is atomic -- no intermediate state
EXCHANGE TABLES stg_orders AND orders;
```

`EXCHANGE TABLES` swaps both table names atomically. After the swap, `stg_orders` contains the old production data -- useful if you want to keep the previous version for a period before dropping it.

== Validation Before Swap
<validation-before-swap-1>
Never skip validation. A staging swap that replaces production with zero rows because of a silent extraction failure is worse than a failed load -- it actively corrupts your destination and every consumer sees empty data.

```sql
-- source: columnar
-- engine: bigquery
-- Run against stg_orders before any swap operation

-- 1. Not empty
SELECT COUNT(*) AS row_count FROM stg_orders;
-- Fail if row_count = 0

-- 2. Within 10% of yesterday's production count
SELECT ABS(s.cnt - p.cnt) * 1.0 / p.cnt AS pct_change
FROM (SELECT COUNT(*) AS cnt FROM stg_orders) s,
     (SELECT COUNT(*) AS cnt FROM orders) p;
-- Fail if pct_change > 0.10

-- 3. No NULLs on merge key
SELECT COUNT(*) AS null_keys FROM stg_orders WHERE order_id IS NULL;
-- Fail if null_keys > 0
```

Most orchestrators let you wire these as post-load checks that gate the swap step. If any check fails, the job stops, staging is left intact for inspection, and production is untouched.

#ecl-tip("Keep staging around on failure")[Don't drop staging when validation fails. Leave it for debugging -- it's the evidence of what went wrong. Drop it only after the issue is resolved and the next successful run creates a fresh staging table.]

== Rollback
<rollback>
There is no rollback step. If validation fails, you abort before the swap. Production never changed. On the next run, staging is recreated from scratch -- it's a throwaway table, not a state you carry forward.

If the swap itself fails mid-operation (rare, but possible on non-atomic engines like BigQuery's DDL rename), check which table exists and which doesn't before deciding how to recover. On atomic engines (Snowflake SWAP, ClickHouse EXCHANGE, PostgreSQL transaction), a failure means the swap didn't happen -- production is still the original.

#ecl-warning("Don't reuse staging across runs")[Staging tables are throwaway. Truncate or drop and recreate on every run. A staging table left over from a prior failed run contains stale data -- if your validation only checks row count, it might pass against the wrong rows.]

== By Corridor
<by-corridor-1>
#ecl-info("Transactional to Columnar")[Primary use case for this pattern (e.g.~PostgreSQL → BigQuery). Columnar destinations have no cheap in-place UPDATE, so full replace via staging swap is the standard approach for any mutable table that fits the schedule window. On BigQuery, prefer `bq cp` over DDL rename for live tables. On Snowflake, use `SWAP WITH` and re-grant permissions after.]

#ecl-warning("Transactional to Transactional")[Equally valid (e.g.~PostgreSQL → PostgreSQL). The PostgreSQL RENAME-within-transaction approach is clean and atomic. One additional concern: foreign keys referencing the production table. If other tables have FK constraints pointing to `orders`, the rename sequence may fail or temporarily break referential integrity. Disable FK checks or use `CASCADE` options with care before swapping.]

== Related Patterns
<related-patterns-2>
- @full-scan-strategies
- @partition-swap
- @data-contracts
- @extraction-status-gates

// ---

= Scoped Full Replace
<scoped-full-replace>
#quote(block: true)[
#strong[One-liner:] Declare a scope boundary, apply full-replace semantics inside it, and explicitly freeze everything outside -- so you get idempotent reloads without scanning years of history every run.
]

== The Problem
<the-problem-1>
A full table replace is the cleanest option available. It resets state, eliminates drift, and gives you a complete, verifiable destination every run. The problem is cost. An `orders` table with five years of history might have 200 million rows. A nightly full reload takes hours and burns slot quota. At some point the cost of purity exceeds its value.

The alternative most people reach for is incremental. That trades one problem for another: cursor management, drift accumulation, delete detection -- the full weight of Part III. For tables where historical rows rarely change, that complexity is never earned.

Scoped full replace is the middle path. Define a boundary and apply full-replace semantics to everything on the right side of it. Rows to the left are frozen: loaded once via a one-time backfill, never touched again. Within the scope, the pipeline runs a complete, idempotent reload every time. Outside the scope, it owns nothing.

== The Mechanics
<the-mechanics-2>
// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Declare the scope.] `scope_start` is a parameter the pipeline receives at runtime, not a constant baked into SQL. Externalizing it lets you widen the scope for backfills without touching extraction logic.

#strong[Extract within scope.] Pull only rows where the scope field falls inside the declared window. The source query is bounded -- no full-table scan.

#strong[Replace the managed zone.] Use partition swap (@partition-swap) to replace every partition in `scope_start → today`. The frozen zone is never part of the destination operation.

== Defining the Scope
<defining-the-scope>
```sql
-- source: transactional
-- engine: postgresql
-- :scope_start injected by the orchestrator
SELECT *
FROM orders
WHERE created_at >= :scope_start;
```

Three ways to anchor `scope_start`:

#figure(
  align(center)[#table(
    columns: (8.87%, 26.6%, 64.53%),
    align: (auto,auto,auto,),
    table.header([Anchor], [Definition], [When to use],),
    table.hline(),
    [Start of last year], [`DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')`], [Accounting data with open/closed fiscal years. Year boundaries are natural partition boundaries. Window grows Jan→Dec then resets.],
    [Fixed date], [`'2025-01-01'`], [History before that date is known bad, migrated from another system, or simply not needed. Stable until you change it deliberately.],
    [Rolling offset], [Last N days], [Different pattern -- see @rolling-window-replace],
  )]
  , kind: table
  )

The calendar year anchor is particularly useful for transactional systems with formal year-close processes. Once a fiscal year is closed in the source, no document in that year should change. The year boundary is a business invariant backed by a process -- align your scope to it.

=== The Field That Defines the Scope
<the-field-that-defines-the-scope>
The scope filter doesn't always belong on `created_at`. Some ERP systems define the fiscal year through a document date field that is separate from the record's creation timestamp. In SAP Business One, `DocDate` is the field that places a document in an accounting period -- a document created on December 31 with `DocDate` set to January 5 of the next year belongs to the next year, not the current one. Filtering by `created_at` would put it in the wrong scope.

Use whichever date field your source system uses to assign records to fiscal periods. When in doubt, ask the source system owner, not the DBA.

== The Assumption You're Making
<the-assumption-youre-making>
Scoped full replace rests on one explicit bet: #strong[records created before `scope_start` will not change in ways consumers care about.]

#figure(
  align(center)[#table(
    columns: (4.48%, 2.99%, 92.54%),
    align: (auto,auto,auto,),
    table.header([Table], [Fits?], [Why],),
    table.hline(),
    [`events`], [Yes], [Append-only. Historical events are immutable by definition.],
    [`metrics_daily`], [Yes], [Old dates only change during explicit recalculations. Treat those as one-off backfills.],
    [`invoices`], [Yes], [Closed invoices are frozen. Open invoices are recent.If this soft rule is broken, there could be some legal trouble.],
    [`orders`], [Usually], [Most old orders are done. Verify with the source team whether support can reopen them.],
    [`customers`], [No], [A customer created in 2022 can update their email today. Use full scan (see @full-scan-strategies],
    [`products`], [No], [Price changes and schema mutations affect all rows regardless of age. Use full scan.],
    [`order_lines`], [Indirectly], [No reliable own timestamp. Borrow scope from `orders` via cursor from another table (see @cursor-from-another-table],
  )]
  , kind: table
  )

Dimension tables (`customers`, `products`) change across their full history. The right answer for them is a cheap full scan, not an ever-growing scope.

== Scope Maintenance
<scope-maintenance>
#strong[Widening the scope] means moving `scope_start` backwards -- including a year of history that was previously frozen. This is a one-time manual operation: run the pipeline with the new `scope_start` to reload the newly included range. Subsequent nightly runs extract from the wider window automatically.

#strong[Narrowing the scope] is dangerous. Moving `scope_start` forward freezes data that may still need correction. If those rows were corrupted or incomplete in the destination, they are now permanently frozen as-is. Only move `scope_start` forward once you're confident the data you're freezing is correct.

#ecl-warning("Don't advance the year boundary early")[Year-end corrections, late-arriving documents, and accounting adjustments routinely arrive in January and February. The fiscal year may be nominally closed, but the data isn't stable yet. A safe rule: don't advance `scope_start` past a year boundary until Q1 is well underway -- at least March or April -- and only after confirming with the source team that the prior year is closed in the system, with no pending documents or adjustments expected.]

== Validation
<validation>
Before any destination operation, verify staging is not empty and reaches the expected end of the window. Whether `scope_start` was set correctly is a parameter-level concern -- validate it in your orchestrator, not by interrogating the data boundary, since gaps near the scope edge are legitimate on low-activity days.

```sql
-- source: columnar
-- engine: bigquery
SELECT
    MAX(DATE(created_at)) AS latest_row,
    COUNT(*)              AS total_rows
FROM stg_orders;
-- Fail if total_rows = 0
-- Fail if latest_row < CURRENT_DATE - INTERVAL '1 day'
```

#ecl-warning("Document the scope boundary")[Every consumer of this table is reading data that may not reflect source state for historical rows. Put `scope_start` in your destination table metadata or documentation. "Complete from 2025-01-01 onwards" is essential information. Leaving it implicit is how you get a silent correctness bug six months later.]

== Getting Creative
<getting-creative>
Scoped full replace sets a single boundary: managed vs.~frozen. Once you see it as a zone concept, the obvious next step is multiple zones with different replacement cadences -- each tuned to how often that slice of data actually changes.

// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Cold zone] (2+ years ago): Data is almost certainly stable. Replace weekly -- one extraction pass covers the full cold range, partition swap replaces those partitions. Cost is low because the source query is bounded and runs once a week.

#strong[Warm zone] (current year including last 7 days): Daily full replace via partition swap, `scope_start → today`. The overlap with the hot zone is intentional -- the nightly warm run is the purity reset for the week. Hard deletes, retroactive corrections, and incremental drift all get wiped. Any row the intraday incremental got wrong is corrected by morning.

#strong[Hot zone] (last 7 days): Intraday incremental runs every hour or few hours, merging only changed rows. It doesn't need delete detection, no lookback window, no complexity -- because the nightly warm replace corrects everything the incremental missed. The incremental is a freshness layer, not the source of truth.

Three pipelines, one table, each running at the cadence that matches the data's volatility. The cold run is cheap and slow. The warm run is the core and the cleanup. The hot run is fast and disposable.

#ecl-warning("Tiered freshness goes further")[The building blocks are this pattern, partition swap (0202), and incremental merge (0403). The hybrid strategy is introduced in 0108. For the full architecture -- how to wire the three zones together operationally -- see @tiered-freshness.]

== By Corridor
<by-corridor-2>
#ecl-info("Transactional to Columnar")[Natural fit (e.g.~PostgreSQL → BigQuery). The frozen zone lives in historical partitions that are never touched. Partition swap handles the managed zone. Ensure `scope_start` aligns with a partition date -- splitting a partition between managed and frozen creates a partial-partition edge case on BigQuery.]

#ecl-warning("Transactional to Transactional")[Same logic, different destination operation (e.g.~PostgreSQL → PostgreSQL): `DELETE FROM orders WHERE created_at >= :scope_start` followed by bulk INSERT from staging, inside a transaction. Rows before `scope_start` are outside the DELETE range and untouched. The same scope documentation requirement applies.]

== Related Patterns
<related-patterns-3>
- @partition-swap -- execution mechanism for the managed zone
- @full-scan-strategies -- for dimension tables that don't fit a scope
- @rolling-window-replace -- rolling offset instead of calendar anchor
- @cursor-from-another-table -- scoping detail tables without their own timestamp
- @purity-vs-freshness

// ---

= Rolling Window Replace
<rolling-window-replace>
#quote(block: true)[
#strong[One-liner:] Drop and reload the last N days every run. The window moves forward with time; everything outside it is frozen.
]

== The Problem
<the-problem-2>
A full table replace is too expensive. A cursor-based incremental is unreliable or more complexity than the table deserves. But the data changes -- corrections arrive, statuses update, late rows trickle in -- and those changes cluster in a predictable recent window.

Rolling window replace exploits that clustering. Instead of scanning the full table or tracking individual row changes, it defines a fixed-width window anchored to today, does a complete full replace inside that window every run, and leaves everything older untouched. Within the window, the destination is always correct. Outside it, the data is frozen at whenever it last fell inside the window.

== Distinction from Scoped Full Replace
<distinction-from-scoped-full-replace>
Both patterns maintain a managed zone and a frozen zone. The difference is in how the boundary is defined and what the filter operates on.

0204 uses a calendar anchor -- Jan 1 of last year, or a fixed migration date. The boundary is a business date: a fiscal year, a known cutover point. The filter typically operates on `created_at` or `doc_date`. The managed zone grows over the year and resets annually.

Rolling window uses a metadata anchor -- `updated_at` or `created_at` relative to today. The window is always the same width. It advances daily. There's no natural hard boundary like a fiscal year close; N is a judgment call based on how long corrections typically take to arrive in the source.

Rolling window also freezes data more aggressively. A 30-day window freezes anything older than a month. That's a much shorter guarantee than 0204's "everything since last January." This also makes it composable into more stages -- a 7-day daily window, a 90-day weekly window, a yearly scoped replace -- each tier running at the cadence that matches its data's volatility. See @tiered-freshness.

== The Mechanics
<the-mechanics-3>
// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Extract by `updated_at`.] The filter is on the metadata field that reflects when a row last changed, not when it was created. A 3-year-old order that got its status updated yesterday is inside the 30-day window. A 3-week-old order that hasn't changed is also inside it -- you pull it again regardless, because within the window you replace everything, not just what changed.

```sql
-- source: transactional
-- engine: postgresql
SELECT *
FROM orders
WHERE updated_at >= :window_start;
```

#strong[Replace the window in the destination.] In a transactional destination, delete by PK -- not by `updated_at`. The destination's `updated_at` reflects when the row was last synced, not the current source value. A row updated today in the source still has the old `updated_at` in the destination until you replace it. Deleting by PK covers exactly what was extracted:

```sql
-- engine: postgresql
BEGIN;
DELETE FROM orders WHERE id IN (SELECT id FROM stg_orders);
INSERT INTO orders SELECT * FROM stg_orders;
COMMIT;
```

Or collapse into an upsert:

```sql
-- engine: postgresql
INSERT INTO orders
SELECT * FROM stg_orders
ON CONFLICT (id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    status      = EXCLUDED.status,
    updated_at  = EXCLUDED.updated_at;
```

In a columnar destination, the filter field mismatch creates a real cost problem; see By Corridor below.

== Choosing N
<choosing-n>
N must be wider than your source system's actual correction window. The relevant question: how long after a record is created can it still receive updates? That varies by table and by source system behavior.

- Too narrow: corrections arriving on day N+1 miss the window and are permanently invisible.
- Too wide: the extraction approaches a full scan and the pattern loses its cost advantage.

A rough starting point is 2x the maximum expected correction lag -- if corrections typically arrive within 7 days, start with 14. Then watch it. Correction windows change when source system behavior changes, and N needs to follow.

#ecl-warning("N is not set-and-forget")[A new bulk update script in the source, a change in how corrections are posted, a migration that backdates rows -- any of these can push changes outside your current window and you won't know until a reconciliation catches the drift. Review N when anything significant changes upstream. Complement with a periodic full scan (weekly or monthly) to reset accumulated drift in the frozen zone. See 0201.]

== The Assumption You're Making
<the-assumption-youre-making-1>
Every row older than N days is either immutable or stale-by-design. The frozen zone grows continuously -- a row that was last updated 31 days ago is frozen forever in a 30-day window. Unlike 0204, there's no fiscal year close or business invariant backing this up. N is purely a statistical bet on source behavior.

#ecl-warning("Document the window for consumers")[Consumers querying this table should know that data older than N days may not reflect current source state. The destination is not a complete mirror -- it's a rolling-correct-within-window, frozen-outside table. Treat this the same as 0204's scope boundary documentation.]

== Validation
<validation-1>
```sql
-- source: columnar
-- engine: bigquery
SELECT
    MAX(DATE(updated_at)) AS latest_updated,
    COUNT(*)              AS total_rows
FROM stg_orders;
-- Fail if total_rows = 0
-- Fail if latest_updated < CURRENT_DATE
```

Optionally, compare the window row count against the prior run. A large drop in row count (e.g.~>20% fewer rows than yesterday's window) likely signals a source issue, not a real change in data volume.

== By Corridor

#ecl-warning("Transactional to Columnar")[E.g.~PostgreSQL → BigQuery. Columnar destinations should partition by a stable (hopefully unchangeable) business date -- `created_at`, `doc_date`, `event_date`. Never by `updated_at`: a row that gets updated moves to a different partition on each edit, creating duplicates across partition boundaries -- deduplication requires a full table scan to resolve. This means the filter field (`updated_at`) and the partition key are misaligned. An order created two years ago that was updated yesterday lives in a two-year-old partition -- to replace it, you'd need to replace that partition too. Without scanning the whole table, you can't know which historical partitions are affected. The pattern becomes expensive and unpredictable in columnar. Prefer 0204 for columnar destinations.]

#ecl-info("Transactional to Transactional")[Natural fit (e.g.~PostgreSQL → PostgreSQL). DELETE by PK from staging, then INSERT -- or upsert with `ON CONFLICT (id) DO UPDATE`. Precise, no partition mismatch, no overshoot. The destination PK constraint is the safety net. See mechanics above for the full SQL.]

== Related Patterns
<related-patterns-4>
- @partition-swap -- execution mechanism for the columnar replacement
- @scoped-full-replace -- calendar anchor variant; harder boundary, less aggressive freezing
- @late-arriving-data -- sizing the window for late arrivals
- @cursor-based-timestamp-extraction -- cursor-based equivalent; same intuition, different mechanics

// ---

= Sparse Table Extraction
<sparse-table-extraction>
#quote(block: true)[
#strong[One-liner:] Cross-product tables where 90%+ of rows are zeros -- filter at extraction to pull only meaningful combinations, but know that "empty" is a business definition, not a data one.
]

== The Problem
<the-problem-3>
Some tables are the cartesian product of two dimensions. Every SKU against every Warehouse. Every Employee against every Benefit. Every Product against every Location. The source system pre-computes all combinations and fills in zeros where nothing is happening.

The result is a table that's technically large but informationally sparse. A retailer with 50,000 SKUs and 200 warehouses has a 10-million-row inventory table -- and in most businesses, the vast majority of those rows have `OnHand = 0` and `OnOrder = 0`. Extracting all of them is expensive, slow, and loads mostly noise into the destination.

The obvious fix is to filter: `WHERE OnHand <> 0 OR OnOrder <> 0`. Pull only the combinations with actual activity. The destination shrinks dramatically, queries are faster, and the pipeline runs in a fraction of the time.

The risk is that filtering zeros is not neutral. A zero row and a missing row look identical in the destination but mean different things in the source.

== The Filter
<the-filter>
```sql
-- source: transactional
-- engine: ansi
SELECT
    sku_id,
    warehouse_id,
    on_hand,
    on_order
FROM inventory
WHERE on_hand <> 0
   OR on_order <> 0;
```

Simple. The source still scans the full table -- the filter reduces the rows transferred, not the rows read. On a large sparse table this is still a significant win: network transfer, staging load size, and destination query cost all drop proportionally to sparsity.

== Zero vs.~Missing
<zero-vs.-missing>
This is the decision that matters. In the destination, a missing row and a filtered-out zero row look the same. Consumers have no way to distinguish them unless you tell them.

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([In the source], [In the destination (after filter)], [What a consumer sees],),
    table.hline(),
    [`on_hand = 5`], [Row present], [Active combination],
    [`on_hand = 0`], [Row absent], [???],
    [No row], [Row absent], [???],
  )]
  , kind: table
  )

The third column is the problem. If a consumer does `COALESCE(on_hand, 0)` on a JOIN, they get zero for both cases -- which may be exactly right. But if they're counting rows, or checking for row existence, or relying on the destination having the full cartesian product, the filtered data produces wrong results.

If the source table actually contains the full cartesian product of both dimensions, you can reconstruct existence data in the destination by cross-joining the two dimension tables (`skus` and `warehouses`). The sparse table becomes an enrichment on top of a complete baseline, not the source of truth for which combinations exist.

#ecl-warning("Don't filter silently")[A destination that has filtered rows looks exactly like a destination with missing data. Every consumer who queries it will eventually hit this. Document the filter explicitly -- in the table description, in a metadata table, in a comment on the asset. "This table excludes rows where on\_hand = 0 AND on\_order = 0" should be impossible to miss.]

== When It's Safe
<when-its-safe>
- Consumers only care about active combinations -- reporting on what's in stock, not on what's never been stocked
- The filter matches a real business concept ("active inventory") that consumers already think in terms of
- The dimension tables exist separately and can be used to reconstruct the full combination space if needed

== When It's Not Safe
<when-its-not-safe>
- Consumers need to distinguish zero stock from never-tracked -- "we have none" vs.~"we don't carry this here"
- Downstream aggregations count rows where a zero is a valid data point
- The source uses explicit zeros as a business signal: a zero `on_hand` with a `replenishment_blocked` flag means something different from a row that simply doesn't exist. Filtering removes that signal entirely.

== The Filter Is a Business Decision
<the-filter-is-a-business-decision>
`WHERE on_hand <> 0 OR on_order <> 0` sounds technical but it encodes a business definition of "active." Who decided that `on_order = 1` with `on_hand = 0` is worth tracking, but `on_hand = 0` and `on_order = 0` is not? Someone did. Find out if that definition matches what consumers expect.

The filter is a contract. If the business changes the definition -- "now we also want rows where `min_stock > 0`" -- the destination needs a full reload, not an incremental correction. Any row that was filtered out and then became relevant won't be caught by the next run unless you reload.

== Relation to Activity-Driven Extraction
<relation-to-activity-driven-extraction>
0207 solves a related problem differently. Sparse table extraction still scans the full source table -- it just drops most rows before loading. Activity-driven extraction avoids scanning the sparse table at all: it uses recent transaction history to determine which dimension combinations are worth pulling, then queries only those.

0207 is simpler and works for any sparse table. 0208 is more surgical -- it trades source query complexity for a much smaller extraction scope. If your sparse table is large enough that even the filtered extraction is slow, 0207 is the next step.

== Related Patterns
<related-patterns-5>
- @activity-driven-extraction -- avoid scanning the sparse table entirely
- @full-scan-strategies -- if the table is small enough, the filter isn't worth the complexity
- @data-contracts -- formalize the filter as a documented contract

// ---

= Activity-Driven Extraction
<activity-driven-extraction>
#quote(block: true)[
#strong[One-liner:] Don't scan the sparse table at all -- use recent transaction history to identify which dimension combinations are active, then extract only those rows.
]

== The Problem
<the-problem-4>
0206 reduces transfer volume by filtering zeros at extraction. The source still scans the full table -- it just drops most rows before sending them. For a 10-million-row inventory table that's 95% zeros, you're still reading 10 million rows on the source every run and discarding 9.5 million of them. On a busy production ERP at peak hours, that scan may be a problem.

Activity-driven extraction skips the scan entirely. Instead of asking the sparse table "which of your rows are non-zero?", it asks the transaction table "which dimension combinations have been active recently?" -- then pulls only those specific rows from the sparse table. The source reads a few thousand rows instead of millions.

== The Mechanics
<the-mechanics-4>
// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Step 1: get active combos from movements.]

```sql
-- source: transactional
-- engine: postgresql
SELECT DISTINCT
    sku_id,
    warehouse_id
FROM inventory_movements
WHERE created_at >= :window_start;
```

Using `inventory_movements` rather than `order_lines` matters: movements capture every stock change -- sales, manual adjustments, transfers, write-offs. `order_lines` only captures sales. A combo updated through a bulk adjustment script would be invisible to an `order_lines`-based activity filter.

#strong[Step 2: pull only those rows from the sparse table.]

```sql
-- source: transactional
-- engine: postgresql
SELECT
    i.sku_id,
    i.warehouse_id,
    i.on_hand,
    i.on_order
FROM inventory i
JOIN (
    SELECT DISTINCT sku_id, warehouse_id
    FROM inventory_movements
    WHERE created_at >= :window_start
) active USING (sku_id, warehouse_id);
```

The JOIN is preferable to an `IN` clause with tuple values -- tuple `IN` is valid PostgreSQL but not portable across all engines. The JOIN with a subquery or CTE works everywhere and the query planner handles it cleanly when `(sku_id, warehouse_id)` is indexed on both tables.

The source now reads a small slice of the inventory table via index lookups, not a full scan.

== The Assumption
<the-assumption>
Recent transactions are a reliable proxy for which inventory combinations matter. A combo that had activity in the last N days is worth tracking. A combo with no activity in that window is inactive enough to skip.

This holds for most inventory use cases. It breaks when:

- #strong[Slow movers exist.] A SKU that sells once a quarter won't appear in a 30-day transaction window. It might still have 500 units on hand. If no one queries it, that's fine. If a consumer expects complete on-hand data, it's a blind spot.
- #strong[New combos have no history.] A SKU just added to a warehouse has zero transactions. It won't appear in the active set until its first order.
- #strong[Not all systems log every change to movements.] If a bulk import script updates `inventory` directly without inserting a row into `inventory_movements`, the combo changes but the activity signal doesn't fire. This is a soft rule: "every stock change creates a movement" -- until it doesn't. See @domain-model.

== Solving Blind Spots: Tiered Windows
<solving-blind-spots-tiered-windows>
A single activity window can't cover all cases without growing large enough to approach a full scan. The solution is the same as 0608: tier the cadences.

- #strong[Daily];: short window (e.g.~30 days) -- catches everything that moved recently, fast and cheap
- #strong[Weekly];: wider window (e.g.~180 days) -- catches slow movers, more expensive but still targeted
- #strong[Monthly];: full scan via 0201 -- catches everything the activity windows missed, resets any accumulated drift

The monthly full scan is the safety net. It's expensive but infrequent. The daily and weekly runs are fast because their active sets are small. Don't try to size a single window to cover slow movers -- a 365-day window defeats the purpose of the pattern.

#ecl-warning("The full scan makes this safe")[Without a periodic full scan, blind spots accumulate silently. A combo that fell outside all activity windows still exists in the source -- it's just invisible in the destination until the next full scan corrects it. Schedule the full scan, document its cadence, and treat it as load-bearing -- not optional.]

== By Corridor
<by-corridor-3>
#ecl-info("Transactional to Columnar")[E.g.~any source → BigQuery. Columnar destinations don't enforce PKs or maintain useful indexes for point lookups. MERGE is expensive without them. Inventory tables also rarely have a natural partition key -- a stock snapshot has no obvious business date to partition by. If the filtered set is small enough after activity-driven extraction, the cleanest option is a full staging swap (0203): replace the entire destination table, which is now small. The monthly full scan runs the same way. The destination stays small because the extraction is always activity-filtered -- the staging swap cost scales with active rows, not total rows.]

#ecl-warning("Transactional to Transactional")[Natural fit (e.g.~any source → PostgreSQL). The destination has a composite PK on `(sku_id, warehouse_id)`. Load staging and upsert:
  ```sql
-- engine: postgresql
INSERT INTO inventory
SELECT * FROM stg_inventory
ON CONFLICT (sku_id, warehouse_id) DO UPDATE SET
    on_hand  = EXCLUDED.on_hand,
    on_order = EXCLUDED.on_order;
```

The index makes this fast. No full destination scan required -- the database resolves each upsert via the PK index.]

== Related Patterns
<related-patterns-6>
- @sparse-table-extraction -- simpler variant; still scans the sparse table, just filters it
- @full-scan-strategies -- periodic reset that catches every blind spot
- @staging-swap -- load mechanism for columnar destinations
- @rolling-window-replace -- activity window sizing follows the same logic as rolling window N
- @tiered-freshness -- tiered cadences applied to the activity window
- @late-arriving-data -- sizing the window for slow movers

// ---

= Hash-Based Change Detection
<hash-based-change-detection>
#quote(block: true)[
#strong[One-liner:] No `updated_at`? Hash the row, compare to the last extraction, load only what changed.
]

== The Problem
<the-problem-5>
Every incremental pattern in this book assumes the source has a cursor -- an `updated_at`, a sequence, a changelog. When that signal doesn't exist or can't be trusted (see 0105), the standard incremental approach fails silently. You either miss changes or you load everything every run.

A full replace every run is correct but expensive when only a small fraction of rows actually change. A 10-million-row products table where 50 rows change per day doesn't need 10 million destination writes nightly. Hash-based change detection threads the needle: read the full source, but write only the rows that are actually different.

This is a last resort, not a first choice. Reach for it when there is genuinely no cursor and the table is too large or the destination too expensive to justify a full replace on every run.

== The Mechanics
<the-mechanics-5>
// TODO: Convert mermaid diagram to Typst or embed as SVG

#strong[Hash every source row.] Concatenate all data columns and compute a hash. The hash is a fingerprint of the row's current state.

```sql
-- source: transactional
-- engine: postgresql
SELECT
    id,
    name,
    price,
    category,
    MD5(
        COALESCE(name::text,     '') ||
        COALESCE(price::text,    '') ||
        COALESCE(category::text, '')
    ) AS _source_hash
FROM products;
```

#strong[Compare against stored hashes.] Rows where `_source_hash` differs from the stored value have changed. Rows with no stored hash are new. Rows in the store with no corresponding source row were hard-deleted.

#strong[Load only changed rows.] Write new and changed rows to the destination. Delete rows that disappeared. Skip everything else. On a table where 1% of rows change per run, 99% of destination writes are eliminated.

== Full Source Scan: Avoidable With Scoping
<full-source-scan-avoidable-with-scoping>
The naive implementation reads every source row to compute hashes -- the same cost as a full replace at source. The win is on the destination side: fewer writes, less DML cost, smaller staging loads.

Combined with 0204, the source scan shrinks too. Scope the hash comparison to the managed zone -- rows within `scope_start → today`. Frozen history is never read or compared. You get the source-side savings of scoped replace and the destination-side savings of hash filtering in one pipeline.

== Where Hash State Lives
<where-hash-state-lives>
The hash comparison requires storing the previous hash somewhere. Two options with real trade-offs:

#strong[`_source_hash` on the destination table.] The hash travels with the row. Comparison is a JOIN between source hashes and destination hashes: rows where they differ are changed. Simple, no extra infrastructure. The problem on columnar destinations: reading the `_source_hash` column to compare costs money. On BigQuery, that's a full column scan on every run. On Snowflake, it's warehouse compute. The comparison itself has a cost.

#strong[Orchestrator state store.] Persist hashes in the orchestrator's own storage -- a key-value store, a metadata table on a cheap transactional database, or a local file. The destination is never queried for hashes. Comparison happens in the pipeline layer: source hashes vs.~stored hashes, entirely outside the destination. More infrastructure to manage, but destination query costs for the comparison step go to zero.

For large columnar destinations where every column scan has a cost, the orchestrator state store pays for itself quickly. For transactional destinations where a column scan is cheap, `_source_hash` on the destination is simpler and sufficient.

== Column Selection and NULL Handling
<column-selection-and-null-handling>
Hash only source data columns. Exclude injected metadata -- `_extracted_at`, `_batch_id`, `_source_hash` itself. If a metadata column changes but the source data doesn't, you don't want a false positive.

Column order in the concatenation must be fixed and explicit. The hash is order-sensitive: `MD5('ab')` differs from `MD5('ba')`. Define the column order in code, not dynamically from schema introspection -- schema changes can silently reorder columns and invalidate your entire hash store.

NULL handling requires care. A naïve `col1 || col2` in SQL returns NULL if any column is NULL. Use `COALESCE`:

```sql
-- source: transactional
-- engine: ansi
MD5(
    COALESCE(col1::text, '') ||
    COALESCE(col2::text, '') ||
    COALESCE(col3::text, '')
)
```

There's a subtle trap: `COALESCE(col, '')` makes NULL and empty string indistinguishable. If your source distinguishes between them (a name that was never set vs.~a name explicitly set to blank), use a separator that can't appear in the data, or encode NULLs explicitly (`COALESCE(col, '\x00')`).

== Hash Function Choice
<hash-function-choice>
MD5 is standard, available on every engine, and produces a 32-character string. Collision probability for a data pipeline is negligible -- you'd need billions of rows for even a theoretical concern. MD5 is the right default.

SHA-256 is more collision-resistant and produces 64 characters. Use it if regulatory requirements or security policy prohibit MD5, or if the table contains data where even a theoretical collision is unacceptable. The compute cost difference is minor on modern hardware.

== When to Use It
<when-to-use-it-1>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Condition], [Use hash detection?],),
    table.hline(),
    [No `updated_at` or unreliable cursor], [Yes -- it's the primary use case],
    [\< \~1% of rows change per run], [Yes -- destination write savings justify the overhead],
    [Destination DML is expensive (columnar)], [Yes -- hash filtering reduces the write cost significantly],
    [Wide table, narrow change set], [Yes -- avoid loading hundreds of columns for a handful of changed rows],
    [\> 10% of rows change per run], [No -- overhead exceeds the savings, use full replace],
    [Cheap transactional destination], [Probably not -- just upsert everything],
  )]
  , kind: table
  )

== Combining With Scoped Replace
<combining-with-scoped-replace>
Hash detection and 0204 compose cleanly. Define `scope_start`, scan only the managed zone at source, compute hashes for those rows, compare against stored hashes, load only changed rows within the scope. Frozen history is never touched.

The frozen zone's hashes don't need to be maintained -- those rows are immutable by definition. If you ever widen the scope backwards, treat the newly included historical rows as "new" (no stored hash) on the first run and load them fully.

== By Corridor
<by-corridor-4>
#ecl-warning("Transactional to Columnar")[E.g.~PostgreSQL → BigQuery. Hash comparison reduces the set of rows you need to write -- but it does not reduce the cost of writing them. On BigQuery, a MERGE that touches 10 rows still rewrites the entire partition containing those rows. On Snowflake, a MERGE still consumes warehouse time proportional to the scan, not the row count. The win from hash detection in columnar is narrowing #strong[which partitions] you touch, not the cost per partition once you do.
  The practical approach: after hash comparison, identify which partitions contain changed rows, then use partition swap (0202) to replace only those partitions via staging. You avoid the DML concurrency constraints (BigQuery's 2-concurrent MERGE limit) and replace entire partitions cleanly rather than doing in-place mutations. Reach for MERGE only when the changed rows span too many partitions to swap individually, and accept the cost explicitly.

If using `_source_hash` on the destination for comparison, reading that column on BigQuery costs bytes scanned. For large tables, storing hashes in an orchestrator state store is cheaper.]

#ecl-warning("Transactional to Transactional")[E.g.~PostgreSQL → PostgreSQL. `_source_hash` on the destination is cheap -- a column scan on a transactional DB with an index is fast. Compare via JOIN, upsert changed rows by PK with `ON CONFLICT DO UPDATE`, delete missing PKs. The destination enforces the PK, so the upsert is safe. Simpler than the columnar case.]

== Related Patterns
<related-patterns-7>
- @the-lies-sources-tell -- broken cursors that make this necessary
- @cursor-based-timestamp-extraction -- the cursor-based alternative when `updated_at` works
- @metadata-column-injection -- `_source_hash` as a standard metadata column
- @scoped-full-replace -- scope the hash comparison to avoid scanning frozen history

// ---

= Partial Column Loading
<partial-column-loading>
#quote(block: true)[
#strong[One-liner:] When you can't or won't extract all columns, do it explicitly, document what's missing and why, and accept that your destination is no longer a complete clone.
]

== The Problem
<the-problem-6>
Most pipelines extract all columns. `SELECT *` from source, load to destination -- a complete clone. Partial column loading is a deliberate departure from that: you extract a subset of columns and leave the rest behind.

Three situations justify it:

#strong[PII and restricted data.] GDPR, HIPAA, contractual data processing agreements. Some columns can't land in your analytics destination regardless of what consumers want. `national_id`, `ssn`, `raw_card_number` -- these don't belong in BigQuery, period.

#strong[BLOBs and binary columns.] PDFs, images, audio files, attachments stored in the source database. Extracting them bloats transfer size, explodes storage costs at the destination, and is useless to anyone running SQL. Leave them in the source.

#strong[Columns your destination can't represent.] A PostgreSQL `geometry` type, a SQL Server `hierarchyid`, a custom SAP compound type. Sometimes there's no clean mapping to the destination's type system. Excluding the column is preferable to a failed extraction or a corrupted value landing silently.

What doesn't justify it: filtering for "relevance." A wide table with 200 columns where analytics only uses 40 is not a reason to exclude 160. That's a transformation -- a decision about what matters -- and it belongs downstream, not at the extraction layer. Consumers don't understand the difference between "this column has nulls" and "this column was never loaded."

== The Trap
<the-trap>
The danger isn't the exclusion. It's the silence.

A consumer queries `destination.customers` looking for `national_id`. The column doesn't exist. They assume it's null in the source -- or worse, they assume the source doesn't have it. Neither is true. The column exists in the source with valid data; it just wasn't loaded.

This is how a pipeline correctness problem becomes a business trust problem. The consumer makes a decision based on a gap they didn't know existed.

A second trap: schema drift. When a source table adds a new column, `SELECT *` picks it up automatically on the next run. An explicit column list doesn't. The destination falls silently behind the source -- no error, no alert, just a growing gap between what's there and what's available.

== When to Use It
<when-to-use-it-2>
#figure(
  align(center)[#table(
    columns: (18.37%, 28.57%, 53.06%),
    align: (auto,auto,auto,),
    table.header([Reason], [Example columns], [Recoverable?],),
    table.hline(),
    [PII / legal restriction], [`ssn`, `national_id`, `raw_email`], [Yes -- with proper access controls at source],
    [Binary / attachment columns], [`attachment_blob`, `document_pdf`, `photo`], [Yes -- if consumers don't need binary data],
    [Unextractable type], [`location geometry`, `sap_custom_type`], [Sometimes -- type casting may be an option first, everything #emph[can] be a string],
    ["Irrelevant" columns], [Wide table, only 40 of 200 columns used], [No -- this is a transformation, not conforming],
  )]
  , kind: table
  )

Before excluding a column for type reasons, check @type-casting-and-normalization. A type that can't be loaded directly can often be cast to a string or numeric representation. Partial loading is the fallback when casting isn't viable.

== The Pattern
Name every column explicitly. Comment every exclusion inline with the reason.

```sql
-- source: transactional
-- engine: postgresql
SELECT
    id,
    name,
    email,
    is_active,
    created_at,
    updated_at
    -- national_id excluded: GDPR Art. 9 -- special category data, no processing basis
    -- id_photo excluded: BLOB, ~2MB per row, not used by any downstream consumer
FROM customers;
```

The comments serve two purposes: they document intent for the next engineer who touches this query, and they make the exclusion visible in code review.

At the destination, document what's missing at the table level -- not just in the pipeline code. A table description, a metadata entry, a README in the project folder. Wherever consumers go to understand the data, the exclusion needs to be there.

```sql
-- source: columnar
-- engine: bigquery
-- Destination table description (set via DDL or catalog):
-- "Partial load of source customers table. Excluded: national_id (GDPR Art. 9),
--  id_photo (BLOB). See pipeline docs for details."
```

== Schema Drift Risk
<schema-drift-risk>
Every time the source schema changes, a `SELECT *` pipeline adapts automatically. A named-column pipeline doesn't.

Add a schema diff check to your pipeline: compare the source column list against your extraction column list before each run and alert on new columns. A new column in the source is either something you should be loading (add it) or something you should be explicitly excluding (add it to the exclusion list with a comment). The only unacceptable outcome is not knowing it appeared.

```sql
-- source: transactional
-- engine: postgresql
-- Run before extraction to detect new source columns
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'customers'
  AND column_name NOT IN (
      'id', 'name', 'email', 'is_active', 'created_at', 'updated_at',
      'national_id',  -- excluded: GDPR
      'id_photo'      -- excluded: BLOB
  );
-- Non-empty result = new column appeared. Investigate before proceeding.
```

#ecl-warning("New columns in products")[The `products` table in this domain mutates -- new columns appear after deploys, making this a known risk. If you're running a partial column extraction on `products`, the schema diff check is not optional. A new `supplier_id` column that appears in the source and gets silently dropped at extraction will be invisible to every downstream consumer.]

== By Corridor
<by-corridor-5>
#ecl-info("Transactional to Columnar")[E.g.~any source → BigQuery. Columnar stores have first-class column-level descriptions in their catalog. Use them. Set the table description and annotate each present column; note which columns are absent and why. Consumers who query the information schema or use a data catalog tool will see it without needing to find the pipeline code.]

#ecl-info("Transactional to Transactional")[E.g.~any source → PostgreSQL. Same extraction SQL. At the destination, use `COMMENT ON COLUMN` or `COMMENT ON TABLE` to document the exclusions directly in the schema. It's the closest equivalent to a catalog annotation and it travels with the table.]

== Related Patterns
<related-patterns-8>
- @type-casting-and-normalization -- try casting before excluding; partial loading is the fallback
- @full-scan-strategies -- column exclusion applies regardless of how you detect changes
- @hash-based-change-detection -- hash-based detection breaks if the hashed column set doesn't match the extracted column set; align them explicitly

// ---
