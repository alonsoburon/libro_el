#import "theme.typ": palette, ecl-tip, ecl-warning, ecl-danger, ecl-info
= Full Replace Load
<full-replace-load>
#quote(block: true)[
#strong[One-liner:] Drop and reload. The simplest load strategy and the default -- stateless, idempotent, no merge logic.
]

// ---

== The Problem
The extraction patterns in Part II give you a dataset -- full table, scoped range, set of partitions -- and this page covers the destination-side mechanics: how to swap it in.

The naive TRUNCATE + INSERT leaves a window where the table is empty -- bad if anyone's querying it. Safer mechanisms exist, and the choice depends on how much downtime is acceptable and how much validation you want before committing.

// ---

== Truncate + Insert
<truncate-insert>
```sql
-- destination: transactional
TRUNCATE TABLE orders;
INSERT INTO orders SELECT * FROM stg_orders;
```

The two operations aren't atomic. Between TRUNCATE and INSERT, the table is empty -- any consumer querying `orders` sees zero rows. If the INSERT fails halfway (connection drop, disk full, timeout), you're left with a partially loaded table and no way back, because TRUNCATE is DDL and can't be rolled back.

This works when the load completes in seconds and no consumers query during the load window -- small reference tables, internal staging tables, tables loaded during a maintenance window where dashboards are offline. For anything with live consumers or a load time measured in minutes, use staging swap instead.

#ecl-warning("TRUNCATE is DDL, except PostgreSQL")[In MySQL, SQL Server, BigQuery, and Snowflake, `TRUNCATE` is a DDL statement that commits implicitly and cannot be rolled back. Wrapping `TRUNCATE; INSERT` in a `BEGIN...COMMIT` block does not make it atomic in these engines. PostgreSQL is the exception: `TRUNCATE` is transactional there, so wrapping both in a transaction gives you atomicity for free.]

// ---

== Staging Swap
<load-staging-swap>
Load into a staging table, validate, then swap to production. Consumers see complete data throughout -- the old version until the swap, the new version after.

// TODO: Convert mermaid diagram to Typst or embed as SVG

The validation step between load and swap is the key advantage over truncate + insert. If the extraction returned garbage -- zero rows from a silent failure, a schema change that dropped columns, a type mismatch that cast everything to NULL -- you catch it before it reaches production.

The swap mechanism varies by engine: Snowflake has `ALTER TABLE SWAP WITH` (atomic, metadata-only), PostgreSQL uses `ALTER TABLE RENAME` inside a transaction, BigQuery uses `bq cp` or DDL rename. See 0203 for the per-engine mechanics, including the parallel schema convention for managing staging tables at scale.

// ---

== Partition Swap
<load-partition-swap>
When the table is partitioned and you're replacing a slice -- yesterday's data, last week's events, a backfill of a specific month -- partition swap replaces only the affected partitions while leaving the rest untouched.

```sql
-- destination: snowflake / redshift
BEGIN;
DELETE FROM events
WHERE partition_date BETWEEN :start_date AND :end_date;
INSERT INTO events SELECT * FROM stg_events;
COMMIT;
```

```bash
# destination: bigquery
# Partition copy -- near-metadata operation, near-free
bq cp --write_disposition=WRITE_TRUNCATE \
  project:dataset.stg_events$20260307 \
  project:dataset.events$20260307
```

The cost advantage is proportional to the scope: replacing 7 partitions out of 3,000 touches 0.2% of the table, while a full staging swap rewrites the entire thing. See 0202 for extraction-side mechanics, per-engine atomicity guarantees, and the partition alignment pitfalls.

// ---

== DROP vs TRUNCATE vs DELETE
<drop-vs-truncate-vs-delete>
Three ways to clear destination data before loading, each with different behavior:

#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto,auto,auto,auto,auto,),
    table.header([Operation], [What it removes], [DDL or DML], [Transactional?], [Speed],),
    table.hline(),
    [`DROP TABLE`], [Schema + data], [DDL], [No (except PostgreSQL)], [Instant -- metadata only],
    [`TRUNCATE TABLE`], [All rows, keeps schema], [DDL], [No (except PostgreSQL)], [Fast -- no per-row logging],
    [`DELETE FROM table`], [All rows, keeps schema], [DML], [Yes], [Slow -- logs every row],
  )]
  , kind: table
  )

`DROP` is used in staging swap workflows: drop the old production table, rename staging into its place. The risk: if the rename fails mid-way, the table vanishes. Wrapping both in a transaction (PostgreSQL, Snowflake, Redshift) eliminates this gap.

`TRUNCATE` is used in truncate + insert workflows. It deallocates storage without logging individual row deletions, which makes it orders of magnitude faster than DELETE on large tables. A 50M-row table that takes 30 minutes to DELETE completes a TRUNCATE in under a second -- the difference is that DELETE generates WAL/redo entries for every row while TRUNCATE resets the storage allocation in one operation.

`DELETE FROM table` (without a WHERE clause) achieves the same result as TRUNCATE but with full transactional semantics -- you can roll it back. The cost is the per-row logging: on a transactional destination, the WAL/redo log grows by the size of the table. On BigQuery, a full-table DELETE rewrites every partition. Use it only when you need the rollback guarantee and the table is small enough that the logging cost is acceptable.

#ecl-tip("TRUNCATE vs DELETE in columnar engines")[BigQuery `TRUNCATE` is a metadata operation that resets the table instantly at zero cost. `DELETE FROM table` without a WHERE clause rewrites every partition and charges for bytes scanned. Snowflake `TRUNCATE` reclaims storage immediately (no Time Travel retention); `DELETE` preserves Time Travel history. Choose based on whether you need the recovery window.]

// ---

== Choosing a Mechanism
<choosing-a-mechanism>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Situation], [Mechanism],),
    table.hline(),
    [Small table, no live consumers, load takes seconds], [Truncate + Insert],
    [Any table with live consumers or load \> 30 seconds], [Staging swap],
    [Partitioned table, replacing a bounded range], [Partition swap],
    [PostgreSQL destination, need atomicity with minimal complexity], [Truncate + Insert inside a transaction (PostgreSQL-only -- TRUNCATE is transactional there)],
  )]
  , kind: table
  )

All three are idempotent -- rerunning the same extraction and load produces the same destination state regardless of how many times you run it, with no accumulated state, no cursor, and no merge logic (see 0109). The shared failure mode is loading bad data into production before catching the problem, which only staging swap prevents through its validation step.

// ---

== By Corridor
#ecl-warning("Transactional to columnar")[Staging swap is the standard for mutable tables. Partition swap for partitioned tables where only a slice needs replacing. Truncate + insert is viable for small reference tables loaded outside business hours. On BigQuery, prefer `bq cp` over DML for both staging swap and partition swap -- copy jobs are free (no slot consumption, no bytes-scanned charge) for same-region operations.]

#ecl-info("Transactional to transactional")[All three mechanisms work cleanly. PostgreSQL's transactional TRUNCATE makes truncate + insert atomic for free -- a significant advantage over columnar destinations. For staging swap, the `RENAME` approach inside a transaction is atomic and instant. One caveat: foreign keys referencing the production table will break during the rename. Disable FK checks or drop and recreate constraints as part of the swap if other tables reference the target.]

// ---

// ---

= Append-Only Load
<append-only-load>
#quote(block: true)[
#strong[One-liner:] Source is immutable -- rows are inserted, never updated or deleted. Append to the destination with pure INSERT, no MERGE needed.
]

// ---

== The Problem
<the-problem-1>
`events`, `inventory_movements`, and clickstream tables only grow. Rows are inserted once and never modified or deleted. A MERGE on every load -- matching on a key, checking for existence, deciding between INSERT and UPDATE -- is unnecessary work when the source guarantees that every extracted row is new.

The append-only load skips all of that: extract the new rows, INSERT them into the destination, done. No key matching, no partition rewriting, no update logic.

// ---

== The Pattern
The extraction side uses a sequential ID cursor (0305) or a `created_at` timestamp cursor (0302) to scope the new rows:

```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id > :last_id;
```

The load side is a pure INSERT:

```sql
-- destination: columnar
INSERT INTO events
SELECT * FROM _stg_events;
```

No `ON CONFLICT`, no `MERGE`, no `MATCHED` / `NOT MATCHED` logic. The destination table grows monotonically, just like the source.

// ---

== Why This Is the Cheapest Load
<why-this-is-the-cheapest-load>
In columnar engines, `MERGE` reads the existing table to find matches, then rewrites the affected partitions with the merged result -- even when every row in the batch is new. On a table with 500M rows and 50K new rows per run, the MERGE still scans the destination side of the join to confirm that none of the 50K exist. That scan is the cost floor of any MERGE operation, regardless of how many rows actually match.

A pure APPEND writes the new rows into a new partition (or appends to the current one) without reading anything that already exists. BigQuery `INSERT` jobs write to new storage blocks without touching existing partitions. Snowflake appends to micro-partitions. The cost scales with the batch size, not the table size -- 50K rows cost the same whether the destination has 1M rows or 500M.

#ecl-warning("Verify the source is actually immutable")[Before committing to this pattern, confirm that \"events are never updated\" is a hard rule, not a soft one (0106). Unless the schema enforces it, someone will eventually run an UPDATE on `events` -- a bulk correction, an admin fix, a backfill that modifies existing rows -- and the append-only load will miss the change entirely. Check with the source team, and keep the periodic full replace from 0301 as a safety net.]

// ---

== When "Append-Only" Produces Duplicates
<when-append-only-produces-duplicates>
Three scenarios where a pure APPEND loads the same row twice:

#strong[Pipeline retry.] The extraction succeeded and the load partially completed, but the cursor didn't advance because the run was marked as failed. The retry re-extracts the same batch, and the rows that already loaded appear again.

#strong[Overlap buffer.] 0305 recommends a small overlap (`event_id >= :last_id - 100`) to absorb out-of-order sequence commits. The overlap region is extracted on every run by design.

#strong[Upstream replay.] The source system replays events -- a Kafka consumer rewinds, an API returns the same batch on retry, a file is redelivered. The rows are identical to ones already loaded, but the extraction can't tell.

=== Handling Duplicates
<handling-duplicates>
Two approaches, depending on the destination engine:

#strong[Transactional destinations] -- reject at load time:

```sql
-- destination: transactional
INSERT INTO events (event_id, event_type, event_date, payload)
SELECT event_id, event_type, event_date, payload
FROM _stg_events
ON CONFLICT (event_id) DO NOTHING;
```

`ON CONFLICT DO NOTHING` silently drops duplicates that already exist in the destination. The primary key does the deduplication, and the load cost is negligible because the rejected rows don't generate writes.

#strong[Columnar destinations] -- deduplicate at read time:

```sql
-- destination: columnar
-- View that exposes only the latest version of each row
CREATE OR REPLACE VIEW events_current AS
SELECT *
FROM events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _extracted_at DESC) = 1;
```

BigQuery and Snowflake don't enforce primary keys, so duplicates land in the table and the deduplication happens downstream through a view or materialized table. This is the foundation of the 0404 pattern -- the difference is that 0404 applies it to mutable data (every version of a row), while here the duplicates are accidental copies of the same immutable row.

#ecl-warning("Don't MERGE to deduplicate immutable data")[Switching from APPEND to MERGE because \"duplicates might happen\" throws away the cost advantage of append-only loading. Handle duplicates at the edges -- `ON CONFLICT DO NOTHING` on transactional destinations, a dedup view on columnar -- and keep the load path cheap.]

// ---

== The Fragility of Append-Only
<the-fragility-of-append-only>
The cost advantage of this pattern depends entirely on the source being immutable -- and that assumption is fragile. The moment someone runs an UPDATE on `events`, or a backfill modifies existing rows, or a correction script touches historical data, the append-only contract is broken and every row that changed sits silently wrong in the destination.

The recovery path is expensive. You either switch to 0403 (which rewrites partitions on every load), add a dedup-and-reconcile layer from 0404, or run a full replace from 0401 to reset the destination. What was the cheapest load pattern in the book becomes one of the most expensive the moment the assumption breaks, because the pipeline has no mechanism to detect or correct the mutation -- it just keeps appending new rows while the old ones stay wrong.

Before choosing this pattern, ask how confident you are that the source will stay immutable -- not today, but across schema changes, team turnover, and the admin script someone will write at 2am during an incident. If the answer is "pretty confident but not certain," a periodic full replace via 0401 is the safety net, and its cadence should reflect how much damage a silent mutation would cause before the next reload.

// ---

== Partitioning by Date
<partitioning-by-date>
Append-only tables are a natural fit for date-based partitioning: `events` partitioned by `event_date`, `inventory_movements` by `movement_date`. Each run's new rows land in the partition corresponding to their date, and old partitions are never touched.

This alignment gives you three operational advantages:

- #strong[Backfill] is a partition replace: re-extract a date range, load into the corresponding partitions using 0401, done. The rest of the table is untouched.
- #strong[Retention] is a partition drop: `ALTER TABLE events DROP PARTITION '2024-01-01'` removes a day of history without scanning or rewriting anything.
- #strong[Cost control] in columnar engines: queries that filter on `event_date` scan only the relevant partitions. Without partition pruning, a query over yesterday's events scans the entire table.

#ecl-warning("Late-arriving events land in past partitions")[An event with `event_date = 2026-03-10` arriving on `2026-03-14` lands in the March 10 partition. If that partition was already "closed" by a retention policy or a downstream process that assumed it was complete, the late arrival is either lost or creates an inconsistency. See 0309 for overlap sizing that absorbs this.]

// ---

== By Corridor
<by-corridor-1>
#ecl-info("Transactional to columnar")[Pure APPEND is the cheapest load operation available -- no partition rewriting, no key matching. BigQuery charges for bytes written on INSERT, not bytes scanned. Snowflake appends to micro-partitions without compaction cost at load time. Dedup view adds read cost only when queried, not on every load.]

#ecl-warning("Transactional to transactional")[`INSERT ... ON CONFLICT DO NOTHING` handles duplicates at load time with minimal overhead. The primary key index absorbs the conflict check. For high-volume append tables (`events` with millions of rows per day), ensure the destination is partitioned and that `autovacuum` keeps up with the insert rate.]

// ---

// ---

= Merge / Upsert
<merge-upsert>
#quote(block: true)[
#strong[One-liner:] Match on a key, update if exists, insert if new. The workhorse of incremental loading -- and the most expensive operation in columnar engines.
]

// ---

== The Problem
<the-problem-2>
The extraction side (0302, 0303) produces a batch of changed rows. The destination already has prior versions of some of those rows. The load needs to reconcile: insert the new ones, update the existing ones, and leave everything else untouched.

// ---

== MERGE Across Engines
<merge-across-engines>
The syntax varies, the semantics are the same -- match on a key, decide between INSERT and UPDATE:

```sql
-- destination: columnar (BigQuery / Snowflake)
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

```sql
-- destination: transactional (PostgreSQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON CONFLICT (order_id)
DO UPDATE SET
  status = EXCLUDED.status,
  total = EXCLUDED.total,
  updated_at = EXCLUDED.updated_at;
```

```sql
-- destination: transactional (MySQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON DUPLICATE KEY UPDATE
  status = VALUES(status),
  total = VALUES(total),
  updated_at = VALUES(updated_at);
```

All three produce the same result: rows that existed get overwritten, rows that didn't get inserted.

// ---

== Cost Anatomy
<cost-anatomy>
In transactional engines, MERGE cost scales with the batch size -- the engine looks up each incoming row by primary key (index seek), decides INSERT or UPDATE, and writes the result. A 10K-row batch against a 50M-row table does 10K index lookups and 10K writes. Cheap.

In columnar engines, the cost structure is fundamentally different. BigQuery's MERGE reads the #strong[entire destination table] (or at minimum every partition that the batch touches) to find matches, then rewrites those partitions with the merged result. A 10K-row batch that touches 30 date partitions rewrites all 30 partitions in full -- even if 9,990 of the 10K rows land in a single partition. The read + rewrite cost dominates, and it scales with table size and partition spread, not batch size.

#ecl-warning("BigQuery MERGE partition cost")[Every DML statement in BigQuery rewrites every partition it touches -- not just the affected rows within each partition. If your batch contains rows spread across 30 dates, that's 30 full partition rewrites. Keep load batches aligned to as few partitions as possible. See 0104 for per-engine DML behavior.]

Snowflake rewrites affected micro-partitions, which is more granular than BigQuery's date-partition model but still means a MERGE touching scattered micro-partitions across the table is significantly more expensive than one touching a contiguous range.

// ---

== Key Selection
<key-selection>
The MERGE key determines how the destination identifies "the same row." Two options:

#strong[Natural key] -- a column that uniquely identifies the entity at the source: `order_id`, `invoice_id`, `customer_id`. This is the default and the simplest choice when the source has a single-column primary key. Compound natural keys (`order_id + line_num`) work too but make the ON clause larger.

#strong[Surrogate key] -- a hash or synthetic key generated during extraction (see 0502). Necessary when the source has no stable primary key, when the natural key is compound and unwieldy, or when multiple sources feed the same destination table and keys can collide.

#ecl-danger("Non-unique keys compound duplicates")[If the MERGE key matches more than one row in the destination, the behavior is engine-dependent and always bad. BigQuery raises an error when multiple destination rows match a single source row. PostgreSQL's `ON CONFLICT` requires the conflict target to be a unique index -- non-unique columns can't be used. Snowflake silently updates all matching rows, which means a single source row can overwrite multiple destination rows. Ensure the MERGE key is unique in the destination, or duplicates will compound on every run -- see 0613.]

#ecl-warning("Unenforced PKs cause silent data loss")[If the source has no unique constraint on what you're using as the merge key, two rows can share the same key value. The merge collapses them into one -- the second overwrites the first, and the destination ends up with fewer rows than the source. This is data loss, not duplication, and it's invisible: the pipeline reports success, row counts look close enough, and the missing rows only surface when someone reconciles at the record level. Verify uniqueness on the actual data before committing to a merge key (0105). If the source genuinely has duplicate PKs, you need a synthetic key (0502).]

// ---

== Full Row Replace vs.~Partial Update
<full-row-replace-vs.-partial-update>
The ECL philosophy is to clone the source exactly -- `DO UPDATE SET (all columns)` is the simplest approach and matches that goal. Every MERGE overwrites the entire row with the source's current state, which means the destination always reflects the source regardless of which columns changed.

```sql
-- destination: transactional (PostgreSQL)
INSERT INTO orders (order_id, status, total, created_at, updated_at)
SELECT order_id, status, total, created_at, updated_at
FROM _stg_orders
ON CONFLICT (order_id)
DO UPDATE SET
  status = EXCLUDED.status,
  total = EXCLUDED.total,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at;
```

Partial updates -- `DO UPDATE SET status = EXCLUDED.status` while leaving other columns untouched -- earn their complexity only when partial column loading (0209) forces them. If you're extracting all columns, update all columns. Deciding which columns "matter" is a business decision that breaks the conforming boundary (0102).

// ---

== Delete-Insert as a MERGE Alternative
<delete-insert-as-a-merge-alternative>
An alternative to a true MERGE is delete-insert: delete all destination rows that match the incoming batch's keys, then insert the full batch. The result is identical (destination ends up with the source's current state for every key in the batch), but the execution plan avoids the columnar MERGE cost on engines where DELETE + INSERT is cheaper than a single MERGE statement.

```sql
-- destination: columnar
-- Delete-insert pattern
DELETE FROM orders
WHERE order_id IN (SELECT order_id FROM _stg_orders);

INSERT INTO orders
SELECT * FROM _stg_orders;
```

On BigQuery, this still rewrites the affected partitions twice (once for DELETE, once for INSERT), so the cost advantage over MERGE depends on how many partitions are touched and whether the batch is pre-deduplicated. On Snowflake, the two operations inside a transaction can be cheaper than a MERGE because Snowflake's MERGE has additional overhead for the MATCHED/NOT MATCHED evaluation.

// ---

== MERGE and Schema Evolution
<merge-and-schema-evolution>
A new column appears in the source. What happens to the MERGE?

#strong[Column-explicit MERGE] (listing columns in the INSERT and UPDATE clauses) silently ignores the new column -- the MERGE succeeds, but the new column's data is dropped on every load. The destination never gets it, and nothing alerts you to the gap.

#strong[`SELECT *` extraction + dynamic MERGE] (building the MERGE statement from the staging table's schema at runtime) fails with a column mismatch if the staging table has a column that doesn't exist in the destination. The error is loud, which is better than silent data loss, but it breaks the pipeline.

Neither outcome is good. Schema evolution needs handling #strong[before] the MERGE executes:

+ #strong[Detect] -- compare the staging table's schema against the destination's schema before running the MERGE. New columns, dropped columns, and type changes are all detectable at this point.
+ #strong[Decide] -- a schema policy determines the response. Two modes are compatible with ECL:

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto,auto,auto,),
    table.header([Entity], [`evolve`], [`freeze`],),
    table.hline(),
    [New table], [Create it], [Raise error],
    [New column], [Add it via `ALTER TABLE`], [Raise error],
    [Type change], [Widen if compatible], [Raise error],
  )]
  , kind: table
  )

Some loaders offer `discard_row` and `discard_value` modes that drop data silently when the schema doesn't match. These are transformation decisions -- deciding what data to keep based on schema fit -- and they break the conforming boundary (0102). If the source sent it, the destination should have it. Either accept the change or reject the load; don't silently drop data.

3. #strong[Apply] -- if the policy is `evolve`, add the column to the destination (`ALTER TABLE ADD COLUMN`) before the MERGE runs. If it's `freeze`, the pipeline stops and alerts.

The recommended production default is `evolve` for new columns and `freeze` for type changes -- new nullable columns appearing in the destination are harmless (downstream queries that don't reference them are unaffected), while type changes that silently widen a column can break downstream logic. See 0609 for formalizing schema policies into enforceable contracts, and 0104 for how each engine handles `ALTER TABLE ADD COLUMN`.

#ecl-warning("Column-explicit MERGE silently freezes schema")[If your MERGE statement lists columns explicitly and you don't have a detection step before it, the destination schema is frozen at whatever columns existed when the MERGE was written. New source columns are silently dropped on every load, type changes are never propagated, and the destination drifts further from the source with every schema change. Either build the MERGE dynamically from the staging schema, or add a schema comparison step that catches drift before the MERGE executes.]

// ---

== Staging Deduplication
<staging-deduplication>
The extraction batch can contain duplicates: the overlap buffer from 0302, the dual cursor overlap from 0310, or simply a source that returns the same row twice within the extraction window.

If the staging table contains two rows with the same MERGE key, the behavior is engine-dependent:

- #strong[BigQuery] raises a runtime error: "UPDATE/MERGE must match at most one source row for each target row"
- #strong[Snowflake] processes both rows non-deterministically -- one wins, but which one is undefined
- #strong[PostgreSQL] `ON CONFLICT` processes rows in order, so the last one wins -- but "in order" depends on the staging query's sort

Deduplicate the staging table before the MERGE to avoid all three problems:

```sql
-- destination: columnar
-- Keep the latest version of each key in staging
CREATE OR REPLACE TABLE _stg_orders_deduped AS
SELECT *
FROM _stg_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;
```

Some loaders deduplicate staging automatically when a primary key is defined on the resource. If yours doesn't, or if you're building the pipeline yourself, add this step explicitly.

// ---

== By Corridor
<by-corridor-2>
#ecl-info("Transactional to columnar")[MERGE is the most expensive DML operation in columnar engines. The cost scales with the number of partitions touched, not the batch size. Minimize partition spread in each batch, consider delete-insert as an alternative, and evaluate whether 0404 (append + dedup view) is cheaper for tables with low mutation rates relative to their size.]

#ecl-warning("Transactional to transactional")[`INSERT ... ON CONFLICT` is cheap -- each row is an index lookup + point write. Cost scales linearly with batch size. The primary key index handles conflict detection efficiently. For large batches (100K+ rows), load into a staging table first and run the `INSERT ... ON CONFLICT ... SELECT FROM staging` as a single statement rather than row-by-row inserts.]

// ---

// ---

= Append and Materialize
<append-and-materialize>
#quote(block: true)[
#strong[One-liner:] Append every extraction as new rows. Deduplicate to current state with a view. Run as often as you want -- the load cost is near zero.
]

// ---

== The Problem
<the-problem-3>
MERGE cost in columnar engines scales per run: every execution reads the destination, matches keys, and rewrites the affected partitions. If a single MERGE costs $X$, running it 24 times per day costs $24 times X$ -- and the cost scales with table size and partition spread -- never batch size (see 0403). This creates a ceiling on extraction frequency: you can only afford to run as often as the MERGE budget allows.

That ceiling directly limits purity. The less often you extract, the longer the destination drifts from the source between runs. Missed corrections, late-arriving data, and accumulated cursor gaps all widen with the interval. Running more often closes the gap -- but MERGE makes running more often expensive.

This pattern removes the per-run cost ceiling by replacing MERGE with a pure append. The load cost drops to near zero regardless of frequency, and the deduplication cost is paid separately -- once, on a schedule you control, decoupled from the extraction cadence.

// ---

== The Pattern
<the-pattern-1>
Every extraction run appends its results to a log table with a metadata column (`_extracted_at` or `_batch_id`) that identifies when the row was loaded:

```sql
-- destination: columnar
INSERT INTO orders_log
SELECT *, CURRENT_TIMESTAMP AS _extracted_at
FROM _stg_orders;
```

A view named `orders` -- the same name consumers would use with any other load strategy -- deduplicates to the latest version:

```sql
-- destination: columnar
CREATE OR REPLACE VIEW orders AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

Consumers query `orders` and see the current state. The view abstracts the log entirely.

// TODO: Convert mermaid diagram to Typst or embed as SVG

// ---

== Why This Maximizes Purity
<why-this-maximizes-purity>
The 0108 tradeoff frames purity and freshness as opposing forces -- full replace maximizes purity but caps freshness, incremental maximizes freshness but carries purity debt. Append-and-materialize shifts the balance toward both:

#strong[Higher frequency = less drift.] With near-zero load cost, nothing stops you from extracting every 15 minutes instead of every hour. The shorter the interval between extractions, the smaller the window where the destination can diverge from the source -- missed corrections, late-arriving rows, and cursor gaps have less time to accumulate before the next extraction picks them up.

#strong[The log is a temporary buffer.] The append log holds recent extractions until the next materialization compacts it -- a few days or weeks of overlap, scoped to the compaction cycle. Keeping the log short is what makes the pattern affordable: storage stays bounded, and the dedup scan stays fast.

#strong[The dedup view absorbs duplicates by design.] Regardless of how many redundant copies sit in the log from overlapping windows, pipeline retries, or stateless extractions, `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1` always returns the latest version. Duplicates cost storage until the next compaction, but they never corrupt the current state.

// ---

== The Duplicate Reality
<the-duplicate-reality>
With a cursor-based extraction (0302), most of the batch is genuinely new or changed rows, and duplicates come from the overlap buffer -- a small fraction of each run.

With a stateless window (0303), the situation inverts. A 7-day window re-extracts 7 days of data on every run, so if the pipeline runs daily, \~6/7 of each batch is rows the destination already has from previous runs -- deliberate duplicates built into the extraction window. The append log grows proportionally to window size × run frequency.

#ecl-warning("Size retention to the extraction window")[A daily run with a 7-day window appends 7× the window volume to the log per week; after a month without compaction, the log holds \~30× the window volume. If the window is a small slice of the table (say, 7 days of changes on a 5-year table), the log overhead is modest. If the window is large relative to the table -- say, all open invoices at 40% of the table -- the log grows fast. The dedup view handles all of it correctly (the latest `_extracted_at` always wins), but the `ROW_NUMBER()` scan gets heavier with every run. The compaction cycle (covered below) keeps both storage and read cost under control.]

// ---

== The Cost Shift
<the-cost-shift>
The cost of reconciling source and destination shifts from load time to read time and storage:

#figure(
  align(center)[#table(
    columns: (16.9%, 38.03%, 45.07%),
    align: (auto,auto,auto,),
    table.header([], [MERGE (0403)], [Append and Materialize],),
    table.hline(),
    [#strong[Load cost];], [Scales with table size and partition spread, per run], [Near zero -- pure INSERT, per run],
    [#strong[Query overhead];], [None -- destination is already reconciled at load time], [Dedup scan on every query against the view],
    [#strong[Materialization cost];], [N/A], [Full dedup scan, but on your schedule],
    [#strong[Storage];], [1× source volume], [\~1× source volume after compaction + window size × runs until compaction],
  )]
  , kind: table
  )

The shift is favorable when extraction frequency matters more than read frequency. If you load 24 times per day but consumers query the current state 4 times per day, paying for 4 dedup scans is cheaper than paying for 24 MERGEs. It's unfavorable when many consumers query `orders` constantly -- the dedup scan runs on every query, and the cost could exceed what the MERGE would have been.

It's usually the case that you want data freshness more frequently than consumption, since most business customers want "New data" whenever they ask for it, but aren't constantly consuming it. More "on demand" than "live".

Compaction (below) is the lever that controls the read-side cost: compact the log regularly and the view's dedup scan stays fast, regardless of extraction frequency.

// ---

== Compaction
<compaction>
The dedup view runs `ROW_NUMBER()` against the full log on every query. Without compaction, the log grows with every run -- a daily pipeline with a 7-day stateless window adds 7× the window volume per week, and the view's scan grows proportionally. Compaction collapses the log to one row per key, run as a periodic scheduled job:

```sql
-- destination: columnar
-- Compact to latest-only: one row per key, all extraction history gone
CREATE OR REPLACE TABLE orders_log AS
SELECT *
FROM orders_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1;
```

// TODO: Convert mermaid diagram to Typst or embed as SVG

Compaction replaces the log with the deduplicated result -- every key retains its latest version, all duplicate extractions and historical versions are gone. Storage reclaims completely and the view's `ROW_NUMBER()` scan drops to near-trivial size. Compaction frequency determines how large the log gets between runs and how heavy the dedup scan is at peak, not how stale the view is -- the view always reflects the latest version of every row in the log.

The tradeoff is that version history disappears after each compaction. If consumers need point-in-time reconstruction from the log, compaction must run less frequently than their lookback window -- or not at all. See 0706 for strategies that preserve history.

#ecl-tip("Partition the log by business date")[After a compact-to-latest, the log holds one row per key -- partition it by a business date (`order_date`) so the view's scan benefits from partition pruning on the dimension consumers actually filter on. Before compaction, partitioning by `_extracted_at` is tempting but doesn't help the dedup view.]

// ---

== Historicizing Non-Historical Data
<historicizing-non-historical-data>
A less common use case, but valuable when it comes up. Most mutable tables in a transactional source overwrite in place without keeping version history -- the previous state is gone the moment the row is updated. If someone later asks "what was the product price on March 5?" and you loaded with full replace or MERGE, there's nothing to reconstruct from.

With append-and-materialize, the log already contains the answer. Each extraction captures the state of changed rows at that moment, and historical queries are a `WHERE _extracted_at <= target_date` filter over the log. The version history is a side effect of the load strategy, not an additional mechanism.

This works without changing anything about the load -- the mechanism is the same append + dedup view. The only change is the compaction policy: instead of collapsing to latest-only, you either skip compaction entirely (expensive on storage) or use tiered retention to keep recent history at full granularity and compress older history.

=== Tiered Retention
<tiered-retention>
Keeping every extraction indefinitely is a storage problem. Tiered retention sits in between full compaction and no compaction: daily granularity for the recent window, monthly snapshots for older data.

I had a client requesting daily `inventory` snapshots for stock-level analysis across warehouses. After three months the log was large and growing linearly. They realized they only needed daily granularity for the last 60-90 days -- further back, a single snapshot per month was enough for seasonal trends and year-over-year comparisons.

```sql
-- destination: columnar
-- engine: bigquery
-- Monthly job: daily for last 60 days, compress older to monthly
CREATE OR REPLACE TABLE inventory_log AS

-- Recent: keep every daily extraction
SELECT * FROM inventory_log
WHERE _extracted_at >= DATE_SUB(CURRENT_DATE, INTERVAL 60 DAY)

UNION ALL

-- Older: last extraction per PK per month
(SELECT *
FROM inventory_log
WHERE _extracted_at < DATE_SUB(CURRENT_DATE, INTERVAL 60 DAY)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sku_id, warehouse_id, DATE_TRUNC(_extracted_at, MONTH)
  ORDER BY _extracted_at DESC
) = 1);
```

The result is two tiers in a single table: daily extractions for the recent window (operational analysis), monthly for older data (trend analysis). The dedup view works identically -- `MAX(_extracted_at)` per PK still returns the latest -- and downstream queries that filter by date range naturally hit the appropriate granularity.

#ecl-warning("Match compression boundary to actual needs")[What's the shortest period where daily granularity changes a decision? If nobody looks at daily stock levels older than 30 days, compress at 30. If finance needs daily for quarter-close reconciliation, compress at 90. Ask the consumer before picking the number -- they usually need less daily granularity than they think.]

See 0706 for the full treatment of point-in-time reconstruction from append logs, event tables, and SCD2.

// ---

== By Corridor
<by-corridor-3>
#ecl-info("Transactional to columnar")[This is the primary corridor for this pattern -- columnar engines are where MERGE is expensive and append is cheap. Partition `orders_log` by `_extracted_at` (date) for retention management and cluster by `order_id` for dedup performance. BigQuery storage at \~\$0.02/GB/month means the log overhead is affordable when the window is a small fraction of the table, but monitor growth -- a large window on a large table accumulates fast.]

#ecl-warning("Transactional to transactional")[Less common here because `INSERT ... ON CONFLICT` is already cheap on transactional engines -- the MERGE cost ceiling that motivates this pattern doesn't exist. Use append-and-materialize on PostgreSQL when the auditing use case justifies the overhead.]

// ---

// ---

= Hybrid Append-Merge
<hybrid-append-merge>
#quote(block: true)[
#strong[One-liner:] Extract once, load to two engines: append-only log in columnar, current-state table in transactional.
]

// ---

== The Problem
<the-problem-4>
The previous load strategies each optimize for one consumer type. 0404 gives you cheap appends and a full extraction log, but every read pays a `ROW_NUMBER()` dedup scan -- fine for analytical queries that run a few times a day, painful for an API that hits the table hundreds of times per minute. 0403 gives you a clean current-state table with zero read overhead, but MERGE in columnar engines is expensive per run, which caps your extraction frequency.

If you have consumers on both sides -- analysts who want history and operational systems that need low-latency point queries on current state -- neither pattern alone covers both without a painful tradeoff on the other side.

// ---

== The Pattern
<the-pattern-2>
Extract once. Load the same batch to two destinations in different engines, each playing to its strength:

+ #strong[Columnar] (e.g.~BigQuery): append-only log table. Pure INSERT, near-zero load cost. History lives here -- analysts query it, and the dedup view from 0404 gives them current state when they need it. High-volume, low-frequency consumption

+ #strong[Transactional] (e.g.~PostgreSQL): current-state table via `INSERT ... ON CONFLICT UPDATE`. Cheap upsert, instant point queries. Operational consumers -- APIs, application backends, services that validate state before acting (e.g.~stock check before order confirmation) -- read from here without touching the log. Best for high-frequency, low-volume consumption

// TODO: Convert mermaid diagram to Typst or embed as SVG

The log gives you replay and history; the current table gives you low-latency reads without dedup overhead. Neither destination is redundant because each serves a consumer type the other engine handles poorly.

// ---

== Why It Only Makes Sense with Two Destinations
<why-it-only-makes-sense-with-two-destinations>
On a single columnar engine, adding a current-state table means running a MERGE alongside the append -- you're paying the exact cost of 0403 plus the append, which is strictly worse than choosing one or the other. On a single transactional engine, the append log doesn't give you anything that `INSERT ... ON CONFLICT` doesn't already handle cheaply, since transactional engines do upserts and point queries well on the same table.

The pattern earns its complexity only when each destination plays to a different engine's strength. If you don't have two engines in your architecture, use 0404 for columnar or 0403 for transactional and stop there.

// ---

== The Complexity Ceiling
<the-complexity-ceiling>
This is the most operationally complex load strategy in this book, and for most pipelines it's the upper bound of what's reasonable. You're maintaining two destinations per table, two sets of failure modes, two retention policies, two schema-evolution policies, and the orchestrator needs to treat the pair as a unit. Every table you add to this pattern doubles the surface area you monitor.

#ecl-warning("Earn this complexity per table")[Don't apply it as a default. Most tables don't have both analytical and operational consumers. Run 0404 or 0403 as the default and promote individual tables to 0405 only when a real consumer can't be served by the simpler strategy. If you find yourself putting more than a handful of tables through this pattern, reconsider whether the operational consumers truly need a separate engine or whether a compacted 0404 with a materialization schedule is good enough.]

// ---

== Orchestration
<orchestration>
The two writes must be treated as a single pipeline unit. If the append to columnar succeeds but the upsert to transactional fails, the log has a batch that the current-state table doesn't reflect -- consumers see different versions of the truth depending on which engine they query.

#strong[Idempotency on both sides.] The append side is naturally idempotent if combined with the dedup view from 0404 -- duplicate rows in the log don't corrupt the current state. The upsert side is idempotent by design (`INSERT ... ON CONFLICT UPDATE` with the same data produces the same result). A retry of the full pipeline unit is safe as long as both writes use the same batch.

#strong[Failure handling.] If either write fails, the orchestrator should retry the full unit -- not just the failed half. Retrying only the failed side risks the two destinations drifting apart on `_extracted_at` or `_batch_id` if the batch is regenerated between retries.

// ---

== When to Use This
<when-to-use-this>
- You already have both a columnar and a transactional engine in your architecture
- Operational consumers need low-latency point queries on current state (APIs, app backends, validation services) that a dedup view can't serve fast enough
- Analytical consumers need history or replay from the append log
- Without both consumer types, this pattern is overhead: use 0404 for columnar-only, 0403 for transactional-only

// ---

== By Corridor
<by-corridor-4>
This pattern is inherently cross-corridor: columnar for the log side, transactional for the current-state side. It doesn't apply within a single corridor -- that's exactly why the simpler patterns exist.

// ---

// ---

= Reliable Loads
<reliable-loads>
#quote(block: true)[
#strong[One-liner:] Checkpointing, partial failure recovery, idempotent loads. How to make the load step survive failures without losing or duplicating data.
]

// ---

== The Problem
<the-problem-5>
A pipeline can die after extraction but before load, mid-load with half the batch written, or after load but before the cursor advances. Each failure point leaves different residue -- a dangling staging table, a partially written partition, a cursor pointing to data the destination never received. The extraction strategy determines what you pulled; this pattern determines whether the destination survives it.

Full replace (0401) sidesteps most of this: every run overwrites everything, so there's no residue from a prior failure to clean up. The load is idempotent by construction. The patterns below matter when you're running incremental loads -- 0403, 0404, or 0405 -- where the destination accumulates state across runs and a bad load can corrupt that state permanently.

// ---

== Idempotency at the Load Step
<idempotency-at-the-load-step>
A load is idempotent if running it twice with the same batch leaves the destination unchanged. This is the single most important property for reliability -- retries are always safe, and the orchestrator doesn't need to know whether the previous attempt succeeded, partially succeeded, or crashed mid-flight.

#strong[MERGE/upsert (0403)] is naturally idempotent: `INSERT ... ON CONFLICT UPDATE` with the same data produces the same result regardless of how many times it runs. The key match absorbs duplicates, and the update overwrites with identical values.

#strong[Append (0404)] is idempotent at the view level but not at the table level. A retry appends the same rows again, doubling them in the log -- but the `ROW_NUMBER()` dedup view still returns the correct current state because it picks the latest `_extracted_at` per key. Storage cost goes up, correctness doesn't break. Compaction cleans up the duplicates later.

#strong[Full replace (0401)] is idempotent by definition: the destination is rebuilt from scratch on every run, so no prior state can interfere.

#ecl-warning("Test idempotency by running twice")[The simplest validation: run a load, record the destination state, run the exact same load again, compare. If anything changed, the load isn't idempotent and you need to understand why before going to production.]

// ---

== Statelessness
<statelessness>
A pipeline that can run on a fresh machine with no local state is valuable -- especially when the orchestrator dies at 2am and you're debugging from a laptop. No local files, no SQLite checkpoint databases, no environment variables from a wrapper script. Just clone, set credentials, run.

Two things break statelessness:

#strong[Local cursor files.] If the high-water mark lives in a local file or an in-memory store, a new machine doesn't know where the last successful run left off. Store the cursor in the destination itself (query `MAX(_extracted_at)` from the target table) or in an external state store that survives machine replacement -- see 0302 for the tradeoffs.

#strong[Local staging artifacts.] Some pipelines extract to local disk (Parquet files, CSV dumps) before loading. If the machine dies between extraction and load, the artifacts are gone and the cursor may have already advanced past the data they contained. Either re-extract on retry (stateless window via 0303 handles this naturally) or stage to durable storage (S3, GCS) before advancing any cursor.

#ecl-warning("\"Works on my machine\" is not stateless")[If the pipeline depends on a prior run having populated a temp directory, a local SQLite checkpoint database, or an environment variable set by a wrapper script, it will fail on a fresh machine. The test is simple: clone the repo, set credentials, run. If it doesn't work, it's not stateless.]

// ---

== Checkpoint Placement
<checkpoint-placement>
The checkpoint is when you declare success -- advance the cursor, mark a partition materialized. Where you place it determines what breaks when something fails:

#strong[Before load (gap risk).] The cursor advances, then the load starts. If the load fails, the cursor points past data that was never loaded. The next run starts from the new cursor position and skips the failed batch entirely -- unless the extraction uses a lookback window or overlap buffer (see 0303) that covers the gap. Even with lookback, this placement relies on the safety net catching every failure, which is the wrong default.

#strong[After load, before confirmation (reprocessing risk).] The load completes, but the cursor update fails (network error, orchestrator crash). The next run re-extracts and re-loads the same batch. With an idempotent load strategy (MERGE or append + dedup view), this is harmless -- the data lands twice but the destination state is correct. With a non-idempotent load (raw INSERT without dedup), you get duplicates.

#strong[After confirmed load (correct).] The cursor advances only after the destination confirms the load succeeded -- a successful MERGE, a confirmed partition swap, a row count check on the target. This is the safe default: failures before confirmation mean the next run reprocesses the same batch, which is safe if the load is idempotent.

// TODO: Convert mermaid diagram to Typst or embed as SVG

The gap between "load completes" and "cursor advances" is the vulnerability window. Keep it as small as possible -- ideally a single transaction that writes the data and updates the cursor atomically. When that's not possible (columnar engines don't support cross-table transactions), make the load idempotent so the reprocessing path is always safe.

// ---

== Partial Load Recovery
<partial-load-recovery>
Not every failure is total. A batch of 10 partitions where 8 succeed and 2 fail leaves the destination in a mixed state: some data is current, some is stale or missing.

#strong[MERGE/upsert recovers naturally.] Re-running the full batch re-applies all 10 partitions; the 8 that already succeeded are overwritten with identical data (idempotent), and the 2 that failed are applied for the first time. No special handling needed, no data loss.

#strong[Append without compaction needs care.] Re-running the full batch appends all 10 partitions again, including the 8 that already landed. The dedup view handles it correctly (latest `_extracted_at` wins), but the log now has duplicate copies of the successful partitions. Not a correctness issue, but it inflates storage and slows the dedup scan until the next compaction.

#strong[Full replace is immune.] The entire destination is rebuilt, so partial state from a prior failure is overwritten completely.

#ecl-warning("Retry the full batch")[Unless the source can't handle it, retrying only the 2 failed partitions introduces complexity: the orchestrator needs to track per-partition success/failure, and the retry batch has a different shape than the original. Reserve per-partition retry for sources that can't afford a second full extraction -- an overloaded transactional database, a rate-limited API, or a query that takes hours to run. If the source can give you the full batch again without pain, re-extract and re-load everything; with an idempotent load strategy, the cost of re-applying the successful partitions is just wasted compute, not a correctness risk.]

// ---

== Orchestrator Integration
<orchestrator-integration>
Orchestrator retries and backfills interact with cursor state in ways that will surprise you.

#strong[Automatic retries.] Most orchestrators can retry a failed run automatically. If the load is idempotent, automatic retries are safe and you should enable them. If the load is not idempotent (raw INSERT), automatic retries create duplicates -- disable them or fix the load strategy first.

#strong[Backfills.] A backfill replays a date range or partition range, typically to repair corrupted data or to onboard a new table. The backfill should not advance the production cursor -- it's filling in historical data, not moving the pipeline forward. Partition-based orchestrators handle this naturally (each partition has its own materialization status). Cursor-based pipelines need a separate code path that loads the backfill range without touching the high-water mark.

#strong[Concurrent runs.] If the orchestrator allows overlapping runs (a retry starts before the previous attempt finishes), the two runs can race on cursor advancement or produce interleaved writes. Either enforce mutual exclusion (one run at a time per table) or ensure the load is idempotent and the cursor advancement is atomic.

// ---

== Health Monitoring
<health-monitoring>
A pipeline that fails silently is worse than one that fails loudly. Your orchestrator can tell you when a run fails -- but if the orchestrator itself dies, or a run hangs forever, or a run "succeeds" with 0 rows, nobody gets paged. You find out Monday morning when someone asks why the dashboard is stale.

#strong[Monitor from outside the pipeline.] The destination should be observable independently of the orchestrator. A scheduled query that checks `MAX(_extracted_at)` against the current time and alerts when it exceeds a threshold works regardless of whether the orchestrator is alive. If the orchestrator dies at 2am and the pipeline doesn't run, the freshness check fires at 8am and somebody knows.

#strong[Distinguish "0 rows extracted" from "extraction failed."] A successful run that returns 0 rows is normal for some tables (no changes since last run, empty table) and a red flag for others (a table that always has activity). 0610 covers this in detail -- gate the load on extraction status so a silent failure doesn't advance the cursor past a real gap.

#strong[Push, then alert on absence.] After each successful load, push a heartbeat (a row in a monitoring table, a metric to your observability stack, a timestamp in a health-check endpoint). Alert when the heartbeat stops arriving. This catches every failure mode: orchestrator crash, hung run, infrastructure outage, credential expiration -- anything that prevents the pipeline from completing.

// ---

== By Corridor
<by-corridor-5>
#ecl-info("Transactional to columnar")[The cursor-and-confirmation gap is wider here because columnar engines don't support cross-table transactions -- you can't atomically write data and advance a cursor in a single commit. Rely on idempotent loads (MERGE or append + dedup view) so the reprocessing path after a gap is always safe. External freshness monitoring is especially important because columnar loads are often async (BigQuery load jobs, Snowflake COPY INTO) and can fail silently.]

#ecl-info("Transactional to transactional")[Transactional engines allow atomic cursor advancement: write the data and update the cursor in the same transaction, making the confirmation gap effectively zero. This is the simplest path to reliable loads -- if you can use it, do. Partial load recovery is also simpler because you can wrap the entire batch in a single transaction and roll back on failure.]

// ---

// ---
