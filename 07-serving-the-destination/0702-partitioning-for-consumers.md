---
title: "Partitioning, Clustering, and Pruning"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Partitioning, Clustering, and Pruning

> **One-liner:** Partition by business date, cluster by consumer filters, enforce partition filters. The physical layout decisions that protect every downstream query.

## The Problem

A table without a partition scheme in a columnar engine forces a full scan on every query. An analyst filtering `orders` by last week's dates scans the entire table -- five years of history, every row, every column they selected -- and the bill reflects it. Partitioning by date means that same query reads only the seven partitions that contain last week's data, and the engine skips everything else. Clustering goes one level deeper: within those seven partitions, it organizes data so a filter on `customer_id` reads fewer blocks instead of scanning every row in the partition.

Both decisions are made at load time, and both affect every downstream query for the lifetime of the table. The ECL engineer picks the partition key and the cluster keys -- two of the few load-time choices that directly shape what consumers pay.

## Choosing the Partition Key

Partition by the column consumers filter on most. For transactional data, that's almost always an immutable business date: `order_date`, `event_date`, `invoice_date`. These dates describe when the business event happened -- not when the row was last modified or when the pipeline extracted it -- and they never change. An order placed on March 5 always has `order_date = 2026-03-05` regardless of how many times its status, amount, or shipping address gets updated. That stability is what makes it safe as a partition key: the row stays in the same partition across every load.

Never partition by `updated_at` or `_extracted_at`. These are mutable -- `updated_at` changes on every source modification, `_extracted_at` changes on every extraction. A row updated today lands in a different partition than its previous version, which forces the MERGE to touch both the old and new partition. In BigQuery, every partition touched in a DML statement is a full partition rewrite ([[04-load-strategies/0403-merge-upsert|0403]]), so a batch of 10,000 rows scattered across 200 dates rewrites 200 partitions. If the load strategy doesn't clean up the old version in the previous partition, you also end up with cross-partition duplicates ([[06-operating-the-pipeline/0613-duplicate-detection|0613]]).

## Partition Granularity

**Daily** is the default, and it works for most tables. `events`, `order_lines`, `invoices` -- daily partitions give tight pruning for date-range queries and align naturally with the schedule (one partition per day's extraction). Tables rarely have enough history to hit partition limits at daily granularity -- 30 years of daily partitions is ~11,000, which exceeds BigQuery's 10,000-partition cap, but most tables don't span 30 years.

**Monthly** is the fallback when daily creates too many partitions or when the table's query patterns are month-oriented. If consumers always aggregate by month and never filter by individual days, monthly partitions match their access pattern and reduce partition management overhead. It's also the right choice when daily granularity hits engine limits -- a table with 30+ years of history at daily granularity exceeds BigQuery's cap, and switching to monthly brings it well under.

**Yearly** is rare -- only for archival tables with low query frequency where even monthly is more granularity than anyone uses.

The practical approach: start with daily. If you hit the partition limit or discover the table has decades of history, switch to monthly. The rebuild is a one-off `CREATE TABLE ... AS SELECT` with the new partition clause.

## Clustering

Partitioning controls which date slices a query reads. Clustering controls how data is physically organized within those slices. A query that filters `orders` by `customer_id` within a single day's partition still reads every row in that partition without clustering -- with it, the engine skips blocks that don't contain the target customer.

Choose cluster keys based on how consumers filter: `customer_id`, `product_id`, `status` -- the columns that appear in WHERE clauses and JOIN conditions. Column order matters on BigQuery: the first column clusters most effectively, so put the highest-cardinality filter first. Don't cluster by columns nobody filters on -- it costs storage reorganization for no query benefit, and don't cluster by pipeline metadata like `_extracted_at`.

Clustering interacts with load strategy. Append-only loads naturally cluster by ingestion time -- good for time-series queries, bad for entity lookups. Full replace rebuilds clustering from scratch on every load. MERGE can fragment clustering over time as updates scatter across micro-partitions -- BigQuery auto-reclusters in the background, Snowflake reclustering costs warehouse credits.

## `require_partition_filter`

BigQuery's `require_partition_filter = true` rejects any query that doesn't include the partition column in the `WHERE` clause. It's the single most effective cost-protection mechanism for large tables -- an analyst who forgets to filter by date gets an error instead of a bill for scanning 3TB.

The tradeoff is friction. Consumers who are used to `SELECT * FROM orders LIMIT 100` for a quick look now get an error and have to add a date filter. BI tools that generate queries without partition awareness fail until someone configures the date filter in the tool's connection settings. For tables where consumers query frequently and know the schema, the protection is worth the friction. For tables where non-technical consumers explore ad hoc and the hand-holding cost is high, consider whether the enforcement helps more than it annoys -- and whether a pre-built view ([[07-serving-the-destination/0703-pre-built-views|0703]]) with a built-in default date range is a better answer than forcing the filter on the raw table.

No other columnar engine has an equivalent enforcement mechanism. Snowflake, ClickHouse, and Redshift rely on documentation, query review, and cost monitoring ([[06-operating-the-pipeline/0603-cost-monitoring|0603]]) to catch unfiltered scans after they happen.

## Per Engine

**BigQuery.** `PARTITION BY` on date, timestamp, datetime, or integer range. `CLUSTER BY col1, col2` up to 4 columns -- auto-reclusters in the background at no explicit cost. `require_partition_filter = true` for enforcement. Hard limit of 10,000 partitions per table and 4,000 partitions per DML job. Every DML statement rewrites every partition it touches in full.

**Snowflake.** No explicit partition key -- Snowflake manages micro-partitions automatically based on ingestion order. `CLUSTER BY (col1, col2)` influences how micro-partitions are organized; pruning happens automatically when queries filter on clustered columns. Reclustering costs warehouse credits. No partition filter enforcement.

**ClickHouse.** `PARTITION BY` expression in the MergeTree definition, fixed at table creation. `ORDER BY` in the MergeTree definition is the cluster key -- the most important physical layout decision in ClickHouse, also fixed at creation. Partition pruning and block skipping are automatic on filtered queries.

**Redshift.** No native partitioning in the columnar sense. Sort keys determine scan efficiency for range queries (a sort key on `order_date` lets Redshift skip blocks outside the filtered range). Dist keys control how data is distributed across nodes for JOIN performance. Both are fixed at creation -- changing requires a full table rebuild.

## Partition Alignment with Load Strategy

The partition scheme and the load strategy interact directly -- a mismatch between them turns a cheap operation into an expensive one.

**Full replace** via partition swap ([[02-full-replace-patterns/0202-partition-swap|0202]]) is partition-native: you replace entire partitions atomically, and the partition key determines which slices get swapped. BigQuery partition copies are near-free metadata operations; Snowflake and Redshift use DELETE + INSERT within a transaction scoped to the partition range.

**Incremental MERGE** cost scales with the number of partitions the batch touches ([[04-load-strategies/0403-merge-upsert|0403]]). A batch aligned to a single day's partition rewrites one partition. A batch scattered across 30 dates rewrites 30. Keep load batches as aligned to partition boundaries as the data allows.

**Append-and-materialize** ([[04-load-strategies/0404-append-and-materialize|0404]]) introduces a split: partition the log table by `_extracted_at` for cheap retention drops (each day's extraction is its own partition, dropping old extractions is a metadata operation). The dedup view sits on top and can't be partitioned itself -- but if consumers filter by a business date in their query, the engine still prunes the underlying log's partitions. If read cost becomes a problem, materialize the dedup result into a separate table partitioned by business date ([[07-serving-the-destination/0703-pre-built-views|0703]]).

## Retrofitting a Partition Scheme

If you didn't partition a table at creation and it's grown large enough to matter, the fix is a table rebuild: `CREATE TABLE` with the partition clause from a `SELECT` on the original, then rename.

```sql
-- destination: bigquery
CREATE TABLE orders_partitioned
PARTITION BY order_date AS
SELECT * FROM orders;
```

Follow up by renaming the original and swapping the new table in. A script that renames the original tables, rebuilds them with partitions, and drops the originals can run across dozens of tables in a single overnight window -- it's a one-off that doesn't need to be elegant, just correct. Verify row counts match before dropping anything.

The rebuild is a full table rewrite, so it costs bytes scanned on the read and bytes written on the write. For a 10GB table that's a few dollars; for a 10TB table it's a conversation worth having before running. Partitioning at creation costs nothing and avoids the rebuild entirely -- but if you're inheriting a destination that was built without partitions, the retrofit is straightforward and the improvement in query cost pays for itself within days.

## Anti-Patterns

> [!danger] Don't partition by `updated_at`
> A row that gets updated lands in a different partition than its previous version. The MERGE touches both partitions -- the old one to find the existing row and the new one to write the updated version. In BigQuery, both partitions are fully rewritten. The cost scales with how scattered the updates are across dates, not with how many rows changed.

> [!danger] Don't cluster by `_extracted_at`
> Pipeline metadata isn't a consumer filter. Cluster by business columns that appear in downstream WHERE clauses.

## Related Patterns

- [[01-foundations-and-archetypes/0104-columnar-destinations|0104-columnar-destinations]] -- per-engine storage mechanics
- [[02-full-replace-patterns/0202-partition-swap|0202-partition-swap]] -- partition-aligned load operations
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- MERGE cost scales with partitions touched
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- materialized views as an alternative when partition pruning isn't enough
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- partition misalignment shows up as cost spikes
- [[07-serving-the-destination/0705-cost-optimization-by-engine|0705-cost-optimization-by-engine]] -- partitioning and clustering as two levers among several
