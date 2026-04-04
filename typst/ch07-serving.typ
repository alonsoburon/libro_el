#import "theme.typ": gruvbox, ecl-tip, ecl-warning, ecl-danger, ecl-info
= Don't Pre-Aggregate
<dont-pre-aggregate>
#quote(block: true)[
#strong[One-liner:] Land the movements, build the photo downstream. Resist the pressure to transform at extraction.
]

== The Problem
The first request from every non-technical consumer sounds the same: "how much did we sell?" They want a total. They want it per month, per product, per warehouse. The temptation is to build that aggregation into the extraction -- `SELECT product_id, SUM(quantity) FROM order_lines GROUP BY product_id` -- and hand them exactly what they asked for. Clean, simple, one table with the numbers they need.

Then they ask "which orders drove the spike in product X?" And the detail isn't in your warehouse, because you extracted the SUM and threw away the rows. You can't drill down from a total to its components. You can't recompute the aggregation with a different grouping. You can't debug a number that looks wrong because the individual records that produced it were never loaded. Every client who starts with "just give me the totals" eventually asks for the detail -- and if you aggregated at extraction, the only way to answer is to rebuild the pipeline.

This pattern is about protecting the destination from that moment. Land the detail. Build the totals downstream, in views that consumers can query, that you can change when the business logic changes, and that don't require a pipeline rebuild when someone asks a question you didn't anticipate.

== Where the Boundary Is
<where-the-boundary-is>
The line between conforming and transformation runs through aggregation. Landing `inventory_movements` as-is is conforming -- the source has those rows and you're cloning them. Building `inventory_current` by summing movements is transformation -- you're computing a derived state that encodes business logic: which movement types to include, how to handle negative quantities, whether to count pending transfers.

The same applies to derived columns. `revenue = quantity * unit_price` looks harmless in the extraction query, but it's a business calculation. The moment discounts apply, taxes enter the formula, or currency conversion becomes relevant, that column is wrong in every historical row and the only fix is a full backfill. Land `quantity` and `unit_price` as separate columns and let downstream compute whatever formula the business currently uses.

The distinction matters because aggregation and derivation encode decisions that belong to the people who understand the business context -- and those decisions change. A grouping that makes sense today ("revenue by product category") stops making sense when the category taxonomy changes. A formula that's correct today is wrong next quarter when the pricing model shifts. If the pipeline made those decisions at extraction, every change requires a pipeline change. If a downstream view made them, the view changes and the pipeline keeps running untouched.

See 0102 for the full framework.

== The Exception: `metrics_daily`
<the-exception-metrics_daily>
Some source tables are already pre-aggregated. `metrics_daily` in the domain model is computed by the source system -- the aggregation decision was made upstream, not by your pipeline. Landing a pre-aggregated table as-is is conforming because you're cloning what the source has, aggregation included. The rule isn't "never land aggregates" -- it's "don't aggregate in the pipeline."

== Movements vs.~Photos
<movements-vs.-photos>
Two kinds of data, two different representations of the same reality:

#strong[Movements] are append-only event records: `inventory_movements` (stock received, sold, adjusted), `order_lines` (items ordered), `events` (clickstream, transactions). Each row is something that happened. The history is in the rows themselves.

#strong[Photos] are point-in-time snapshots: `inventory` (current stock levels), `metrics_daily` (today's aggregated numbers). Each row is the state of something right now. The history is gone the moment the next snapshot overwrites it.

Land both when both exist at the source. The `inventory` table and the `inventory_movements` table are different data -- the photo and the movements don't always agree (bulk imports that update `inventory` without logging a movement, the soft rule from 0002), and it's your job to make that discrepancy visible to consumers rather than hiding it by building one from the other.

Downstream can reconstruct photos from movements if they want to (see 0706) -- stock as of any date is a `SUM(quantity) WHERE created_at <= target_date`. The inverse isn't possible: you can't recover individual movements from a snapshot total. Detail produces aggregates; aggregates don't produce detail.

== The Conversation
<the-conversation>
The request always starts simple. "How much did we sell last month?" You land `order_lines`, build a view that sums `quantity * unit_price` grouped by month, and the consumer is happy. Then:

"Can I see that by product?" -- change the GROUP BY in the view. No pipeline change.

"Can I see which orders had returns?" -- filter on `quantity < 0` in the detail. Only possible because the detail is there.

"Can I see the monthly trend for this specific SKU?" -- filter the view by SKU. Still no pipeline change.

"Wait, these numbers don't match the ERP's report" -- compare your `order_lines` detail against the source, row by row. Only possible because you have the rows, not just the total.

Every one of these follow-up questions is answerable because the detail was landed. None of them would be answerable if the pipeline had extracted `SUM(quantity * unit_price) GROUP BY month`. The consumer who said "I just need the monthly total" needed the detail all along -- they just didn't know it yet.

== What Consumers Actually Need
<what-consumers-actually-need>
A pre-built view (0703) that aggregates the raw data for their specific use case. The view is downstream, documented, and changeable without touching the pipeline. Different consumers can have different aggregations over the same raw data: the sales team sees revenue by product, the finance team sees revenue by cost center, the warehouse team sees units shipped by location -- all from the same `order_lines` table, each through their own view.

When the business logic changes -- a new product category, a different grouping, a revised pricing formula -- the view changes. The pipeline doesn't.

== Anti-Patterns
#ecl-warning("Don't extract SUMs instead of rows")[`SELECT product_id, SUM(quantity) FROM order_lines GROUP BY product_id` as your extraction query means the per-line detail never reaches the destination. The total looks correct until someone needs to drill down, and then there's nothing to drill into.]

#ecl-danger("Don't compute derived columns at extraction")[`revenue = quantity \* unit_price` in the extraction query is a business calculation baked into the pipeline. When the formula changes -- and it will -- every historical row is wrong and the only fix is a full backfill of the entire table. Land the raw columns, compute downstream.]

== Related Patterns
- @what-is-conforming -- the conforming boundary that this pattern defends
- @pre-built-views -- the right place for consumer-facing aggregations
- @point-in-time-from-events -- reconstructing state from movements downstream
- @append-and-materialize -- the load pattern that preserves every version for downstream use

// ---

= Partitioning, Clustering, and Pruning
<partitioning-clustering-and-pruning>
#quote(block: true)[
#strong[One-liner:] Partition by business date, cluster by consumer filters, enforce partition filters. The physical layout decisions that protect every downstream query.
]

== The Problem
<the-problem-1>
A table without a partition scheme in a columnar engine forces a full scan on every query. An analyst filtering `orders` by last week's dates scans the entire table -- five years of history, every row, every column they selected -- and the bill reflects it. Partitioning by date means that same query reads only the seven partitions that contain last week's data, and the engine skips everything else. Clustering goes one level deeper: within those seven partitions, it organizes data so a filter on `customer_id` reads fewer blocks instead of scanning every row in the partition.

Both decisions are made at load time, and both affect every downstream query for the lifetime of the table. The ECL engineer picks the partition key and the cluster keys -- two of the few load-time choices that directly shape what consumers pay.

== Choosing the Partition Key
<choosing-the-partition-key>
Partition by the column consumers filter on most. For transactional data, that's almost always an immutable business date: `order_date`, `event_date`, `invoice_date`. These dates describe when the business event happened -- not when the row was last modified or when the pipeline extracted it -- and they never change. An order placed on March 5 always has `order_date = 2026-03-05` regardless of how many times its status, amount, or shipping address gets updated. That stability is what makes it safe as a partition key: the row stays in the same partition across every load.

Never partition by `updated_at` or `_extracted_at`. These are mutable -- `updated_at` changes on every source modification, `_extracted_at` changes on every extraction. A row updated today lands in a different partition than its previous version, which forces the MERGE to touch both the old and new partition. In BigQuery, every partition touched in a DML statement is a full partition rewrite (0403), so a batch of 10,000 rows scattered across 200 dates rewrites 200 partitions. If the load strategy doesn't clean up the old version in the previous partition, you also end up with cross-partition duplicates (0613).

== Partition Granularity
<partition-granularity>
#strong[Daily] is the default, and it works for most tables. `events`, `order_lines`, `invoices` -- daily partitions give tight pruning for date-range queries and align naturally with the schedule (one partition per day's extraction). Tables rarely have enough history to hit partition limits at daily granularity -- 30 years of daily partitions is \~11,000, which exceeds BigQuery's 10,000-partition cap, but most tables don't span 30 years.

#strong[Monthly] is the fallback when daily creates too many partitions or when the table's query patterns are month-oriented. If consumers always aggregate by month and never filter by individual days, monthly partitions match their access pattern and reduce partition management overhead. It's also the right choice when daily granularity hits engine limits -- a table with 30+ years of history at daily granularity exceeds BigQuery's cap, and switching to monthly brings it well under.

#strong[Yearly] is rare -- only for archival tables with low query frequency where even monthly is more granularity than anyone uses.

The practical approach: start with daily. If you hit the partition limit or discover the table has decades of history, switch to monthly. The rebuild is a one-off `CREATE TABLE ... AS SELECT` with the new partition clause.

== Clustering
<clustering>
Partitioning controls which date slices a query reads. Clustering controls how data is physically organized within those slices. A query that filters `orders` by `customer_id` within a single day's partition still reads every row in that partition without clustering -- with it, the engine skips blocks that don't contain the target customer.

Choose cluster keys based on how consumers filter: `customer_id`, `product_id`, `status` -- the columns that appear in WHERE clauses and JOIN conditions. Column order matters on BigQuery: the first column clusters most effectively, so put the highest-cardinality filter first. Don't cluster by columns nobody filters on -- it costs storage reorganization for no query benefit, and don't cluster by pipeline metadata like `_extracted_at`.

Clustering interacts with load strategy. Append-only loads naturally cluster by ingestion time -- good for time-series queries, bad for entity lookups. Full replace rebuilds clustering from scratch on every load. MERGE can fragment clustering over time as updates scatter across micro-partitions -- BigQuery auto-reclusters in the background, Snowflake reclustering costs warehouse credits.

== `require_partition_filter`
<require_partition_filter>
BigQuery's `require_partition_filter = true` rejects any query that doesn't include the partition column in the `WHERE` clause. It's the single most effective cost-protection mechanism for large tables -- an analyst who forgets to filter by date gets an error instead of a bill for scanning 3TB.

The tradeoff is friction. Consumers who are used to `SELECT * FROM orders LIMIT 100` for a quick look now get an error and have to add a date filter. BI tools that generate queries without partition awareness fail until someone configures the date filter in the tool's connection settings. For tables where consumers query frequently and know the schema, the protection is worth the friction. For tables where non-technical consumers explore ad hoc and the hand-holding cost is high, consider whether the enforcement helps more than it annoys -- and whether a pre-built view (0703) with a built-in default date range is a better answer than forcing the filter on the raw table.

No other columnar engine has an equivalent enforcement mechanism. Snowflake, ClickHouse, and Redshift rely on documentation, query review, and cost monitoring (0603) to catch unfiltered scans after they happen.

== Per Engine
<per-engine>
#strong[BigQuery.] `PARTITION BY` on date, timestamp, datetime, or integer range. `CLUSTER BY col1, col2` up to 4 columns -- auto-reclusters in the background at no explicit cost. `require_partition_filter = true` for enforcement. Hard limit of 10,000 partitions per table and 4,000 partitions per DML job. Every DML statement rewrites every partition it touches in full.

#strong[Snowflake.] No explicit partition key -- Snowflake manages micro-partitions automatically based on ingestion order. `CLUSTER BY (col1, col2)` influences how micro-partitions are organized; pruning happens automatically when queries filter on clustered columns. Reclustering costs warehouse credits. No partition filter enforcement.

#strong[ClickHouse.] `PARTITION BY` expression in the MergeTree definition, fixed at table creation. `ORDER BY` in the MergeTree definition is the cluster key -- the most important physical layout decision in ClickHouse, also fixed at creation. Partition pruning and block skipping are automatic on filtered queries.

#strong[Redshift.] No native partitioning in the columnar sense. Sort keys determine scan efficiency for range queries (a sort key on `order_date` lets Redshift skip blocks outside the filtered range). Dist keys control how data is distributed across nodes for JOIN performance. Both are fixed at creation -- changing requires a full table rebuild.

== Partition Alignment with Load Strategy
<partition-alignment-with-load-strategy>
The partition scheme and the load strategy interact directly -- a mismatch between them turns a cheap operation into an expensive one.

#strong[Full replace] via partition swap (0202) is partition-native: you replace entire partitions atomically, and the partition key determines which slices get swapped. BigQuery partition copies are near-free metadata operations; Snowflake and Redshift use DELETE + INSERT within a transaction scoped to the partition range.

#strong[Incremental MERGE] cost scales with the number of partitions the batch touches (0403). A batch aligned to a single day's partition rewrites one partition. A batch scattered across 30 dates rewrites 30. Keep load batches as aligned to partition boundaries as the data allows.

#strong[Append-and-materialize] (0404) introduces a split: partition the log table by `_extracted_at` for cheap retention drops (each day's extraction is its own partition, dropping old extractions is a metadata operation). The dedup view sits on top and can't be partitioned itself -- but if consumers filter by a business date in their query, the engine still prunes the underlying log's partitions. If read cost becomes a problem, materialize the dedup result into a separate table partitioned by business date (0703).

== Retrofitting a Partition Scheme
<retrofitting-a-partition-scheme>
If you didn't partition a table at creation and it's grown large enough to matter, the fix is a table rebuild: `CREATE TABLE` with the partition clause from a `SELECT` on the original, then rename.

```sql
-- destination: bigquery
CREATE TABLE orders_partitioned
PARTITION BY order_date AS
SELECT * FROM orders;
```

Follow up by renaming the original and swapping the new table in. A script that renames the original tables, rebuilds them with partitions, and drops the originals can run across dozens of tables in a single overnight window -- it's a one-off that doesn't need to be elegant, just correct. Verify row counts match before dropping anything.

The rebuild is a full table rewrite, so it costs bytes scanned on the read and bytes written on the write. For a 10GB table that's a few dollars; for a 10TB table it's a conversation worth having before running. Partitioning at creation costs nothing and avoids the rebuild entirely -- but if you're inheriting a destination that was built without partitions, the retrofit is straightforward and the improvement in query cost pays for itself within days.

== Anti-Patterns
<anti-patterns-1>
#ecl-warning("Don't partition by updated_at")[A row that gets updated lands in a different partition than its previous version. The MERGE touches both partitions -- the old one to find the existing row and the new one to write the updated version. In BigQuery, both partitions are fully rewritten. The cost scales with how scattered the updates are across dates, not with how many rows changed.]

#ecl-danger("Don't cluster by _extracted_at")[Pipeline metadata isn't a consumer filter. Cluster by business columns that appear in downstream WHERE clauses.]

== Related Patterns
<related-patterns-1>
- @columnar-destinations -- per-engine storage mechanics
- @partition-swap -- partition-aligned load operations
- @merge-upsert -- MERGE cost scales with partitions touched
- @pre-built-views -- materialized views as an alternative when partition pruning isn't enough
- @cost-monitoring -- partition misalignment shows up as cost spikes
- @cost-optimization-by-engine -- partitioning and clustering as two levers among several

// ---

= Pre-Built Views
<pre-built-views>
#quote(block: true)[
#strong[One-liner:] Materialized views, scheduled queries, and pre-cooked tables -- the serving layer you build on top of landed data to protect consumers from themselves.
]

== The Problem
<the-problem-2>
The pipeline did its job: the data landed correctly, partitioned, with metadata columns and clean types. The destination is a faithful clone of the source. Now an analyst opens their query editor, writes `SELECT * FROM orders_log`, and gets back 90 million rows -- every version of every order from the append log, duplicates and all. They aggregate on it, get numbers that are 3x what the source shows, and file a bug against your pipeline. The data is correct; the query is wrong.

This is the gap the serving layer fills. The pipeline lands raw data. The serving layer builds clean, queryable surfaces on top of it -- dedup views that expose current state from append logs, flattening views that extract fields from JSON columns, materialized tables that pre-compute expensive aggregations so consumers don't pay for them on every query. None of this is in the pipeline. It's what you build after the data lands, as a service to the people who consume it.

The goal is to put a guardrail between the consumer and the raw data. Not because the raw data is wrong -- it's exactly what the source has -- but because raw data in a columnar engine is expensive to misuse, and most consumers don't know how their queries translate into bytes scanned or warehouse time. A well-built view costs you minutes to create and saves consumers thousands of dollars in accidental full scans over the life of the table.

In practice, the serving layer is smaller than you'd expect. A typical client with 70 base tables needs around 15 views for their entire reporting surface, and many of those are variations on the same core query with different filters or groupings -- maybe 5 distinct view designs that cover the full reporting need. When the client runs multiple companies on the same ERP schema (separate databases, identical structure), the base tables multiply but the views don't -- each view UNIONs the same table across databases with a `_database` column to distinguish the source. The effort is low; the impact on consumer experience and cost control is disproportionately high.

== The Hierarchy
<the-hierarchy>
Four tools, from lightest to heaviest. Start at the top and move down only when the lighter option doesn't serve the consumer well enough.

#strong[SQL views.] A saved query, computed fresh on every read. The dedup view from 0404 is the most common example: a `ROW_NUMBER()` over the append log that exposes only the latest version of each row. Column-filtering views that hide internal metadata (`_extracted_at`, `_batch_id`) are another. Free to create, not free to consume -- every query against the view scans the underlying table. A well-written view can reduce cost by baking in partition filters and column selection that consumers would otherwise forget, but it doesn't pre-compute anything.

#strong[Materialized views.] Pre-computed and stored. The engine refreshes them on a schedule or on data change, and routes queries to the materialized result instead of recomputing from the base table. The query cost drops to scanning the materialized result (generally smaller than the base table), at the expense of storage and refresh overhead. This is where the cost savings happen -- the consumer queries the pre-built result, not the raw data.

Materialized views work best when the view has a single base table or a fact table with a few dimension lookups -- one source of truth driving the refresh. When the view joins multiple independently-refreshed fact tables, the "update on data change" trigger gets messy: every participating table's load triggers a refresh, and if five tables contribute to one view, you're refreshing it five times per pipeline run with partially-stale data each time. For views like these, scheduled query tables are the cleaner option.

#strong[Scheduled query tables.] A query that runs on a schedule and writes its results to a destination table. The simplest form of materialization -- no special engine feature needed, works on every engine. Your orchestrator or a cron job runs the query after all the participating tables have landed, and consumers query the output table directly. Less elegant than a native materialized view, but more portable, easier to debug, and the right choice when the view joins many tables that refresh at different times -- one scheduled run after all sources have landed produces a consistent result without multiple partial refreshes.

#strong[Consumer-specific tables.] A table shaped for a specific dashboard, report, or API. Pre-joined, pre-filtered, pre-aggregated -- exactly the columns and rows the consumer needs, nothing else. The most expensive to maintain (a pipeline change or a business logic change can invalidate it) and the most efficient to query (consumers scan only what they need with zero overhead). Reserve these for high-frequency, high-cost query patterns where even a materialized view isn't cheap enough.

== When to Materialize
<when-to-materialize>
The dedup view from 0404 is a SQL view by default, and during development that's fine -- you're the only one querying it. Once analysts start using it daily, the cost shifts: 50 queries per day against a view that scans 90 days of append log means 50 full scans of that log per day. At that point the materialization cost (one refresh per load) is a fraction of the repeated read cost, and the switch is justified.

A view over 90 days of append log is an even clearer case -- every query scans 90x the base table volume to find the latest version per key. Materialization is almost always worth it here, even at low query frequency.

The rule: don't materialize speculatively. Wait until the query cost shows up in 0603, then materialize the views that actually get hit. A materialized view for a table queried once a week is wasted storage and refresh compute -- and at 15 views across 70 base tables, most of the serving layer stays as simple SQL views that never need materialization.

== Flattening Views for JSON
<flattening-views-for-json>
JSON and nested data land as-is (0507) -- the pipeline doesn't parse or restructure them. Consumers who need tabular access get a flattening view. The syntax depends on how the data landed:

#strong[JSON string columns] (landed as `STRING` or `JSON` type):

```sql
-- destination: bigquery
CREATE VIEW orders_flat AS
SELECT
    order_id,
    customer_id,
    status,
    JSON_EXTRACT_SCALAR(details, '$.shipping.method') AS shipping_method,
    JSON_EXTRACT_SCALAR(details, '$.shipping.address.city') AS shipping_city,
    order_date
FROM orders;
```

#strong[Repeated records / STRUCTs] (BigQuery's native nested types, common when loading from Avro or when the loader normalizes JSON into typed structs):

```sql
-- destination: bigquery
CREATE VIEW order_items_flat AS
SELECT
    o.order_id,
    o.customer_id,
    item.sku,
    item.qty,
    item.price
FROM orders o, UNNEST(o.items) AS item;
```

`UNNEST` explodes the array into rows -- one row per item per order. This is the BigQuery-native way to flatten repeated fields; `JSON_EXTRACT_SCALAR` only works on string-typed JSON, not on STRUCTs or repeated records.

Different consumer groups can have different flattening views over the same nested data -- the sales team sees shipping and pricing fields, logistics sees warehouse and carrier fields -- each shaped for their use case without duplicating the underlying data.

When the nested schema mutates -- a new field appears, a field is renamed -- the view definition changes. The pipeline doesn't. This is the same principle as 0701: the pipeline lands what the source has, the serving layer adapts it for consumption.

== Per Engine
<per-engine-1>
#strong[BigQuery.] SQL views are free to create but every query scans the underlying table and bills for bytes read. Materialized views auto-refresh and BigQuery routes queries to the MV when it can -- this is where the cost savings happen, because the query scans the pre-computed result instead of the full base table. Scheduled queries write to destination tables on a cron and are the workhorse for consumer-facing aggregation tables that join multiple sources.

#strong[Snowflake.] SQL views are free to create, same caveat -- every query costs warehouse time against the underlying table. Materialized views refresh automatically on data change, costing warehouse credits for each refresh. Snowflake's `SECURE VIEW` hides the view definition from consumers, useful when the view encodes business logic you don't want exposed.

#strong[PostgreSQL.] `CREATE MATERIALIZED VIEW` with `REFRESH MATERIALIZED VIEW CONCURRENTLY` for zero-downtime refreshes. No auto-refresh -- schedule via cron, orchestrator, or a post-load hook. Standard SQL views are free and fast for simple cases.

#strong[ClickHouse.] Materialized views trigger on INSERT and pre-compute aggregations at write time -- a fundamentally different model from the others. The compute happens at ingestion, not at refresh time, so the materialized result is always current with zero read-time overhead. Powerful for dashboards that need real-time aggregations, but the logic is baked into the write path, making it harder to change than a post-hoc refresh.

== Anti-Patterns
<anti-patterns-2>
#ecl-warning("Don't build consumer-specific ECL tables")[A table shaped for one dashboard is transformation. The pipeline lands data generically; the serving layer shapes it for consumption. If the dashboard needs a different shape, change the view, not the pipeline.]

#ecl-danger("Don't materialize before you measure")[A materialized view for every table \"just in case\" is wasted storage and refresh compute. Materialize the views that actually get queried, based on observed cost from 0603.]

== Related Patterns
<related-patterns-2>
- @append-and-materialize -- the dedup view that most commonly needs materialization
- @append-and-materialize -- append logs that benefit most from materialized current-state views
- @nested-data-and-json -- raw JSON landing that needs flattening views
- @dont-pre-aggregate -- the boundary between serving and conforming
- @cost-monitoring -- the signal that tells you when to materialize

// ---

= Query Patterns for Analysts
<query-patterns-for-analysts>
#quote(block: true)[
#strong[One-liner:] Cheat sheet: how to query append-only tables, how to get latest state, how to not blow up costs.
]

== Who This Is For
<who-this-is-for>
This is the reference you hand analysts when they get access to the destination. They didn't design the schema, they don't know what append-and-materialize means, and they will `SELECT *` on a 3TB table if nobody tells them not to. The patterns below are the minimum they need to query ECL-landed data correctly and cheaply.

One thing to internalize before querying: the destination is not a moment-to-moment replica of the source. Data has to be extracted, conformed, and loaded before it appears -- that takes time, and the freshness depends on the table's schedule (0604). If you need transactional-level freshness for point lookups ("is this order shipped right now?"), query the source directly. Columnar destinations are for analysis, not real-time lookups.

== Current State from Append-Only Tables
<current-state-from-append-only-tables>
Some tables in the destination are append logs -- every extraction appends new rows without overwriting old ones (0404). The log contains every version of every row your pipeline has ever seen: order 123 with `status = pending`, then order 123 with `status = shipped`, then order 123 with `status = delivered`. All three rows are in the log. The current state is the latest one.

If the table has a dedup view, query the view. The view handles the deduplication logic and returns one row per entity -- the latest version. The view is named after the business object (`orders`), and the log is suffixed (`orders_log` or `orders_raw`). If you're not sure which is which, the one with fewer rows is the view.

```sql
-- destination: columnar
-- Use the view -- always one row per order
SELECT * FROM orders WHERE order_id = 123;
```

If no dedup view exists, dedup manually:

```sql
-- destination: columnar
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) AS rn
    FROM orders_log
) WHERE rn = 1 AND order_id = 123;
```

The `ROW_NUMBER()` picks the row with the most recent `_extracted_at` for each `order_id`. Everything else is a prior version that's been superseded.

#ecl-warning("Don't aggregate on the log table")[`SELECT SUM(total) FROM orders_log` sums every version of every order -- if an order was extracted 5 times, its total appears 5 times in the sum. Use the dedup view, or wrap the aggregation around a deduped subquery.]

== `_extracted_at` vs `updated_at`
<extracted_at-vs-updated_at>
Two timestamp columns, two different clocks:

#strong[`_extracted_at`] is when the pipeline pulled the row. It's set by the pipeline, not the source, and it's always accurate -- it reflects when this version of the row arrived in the destination.

#strong[`updated_at`] is when the source last modified the row. It's maintained by the source application -- triggers, ORMs, manual updates -- and its reliability varies by table (0301).

Which one to filter on depends on the question:

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Question], [Filter on],),
    table.hline(),
    ["What changed at the source this week?"], [`updated_at`],
    ["What arrived in our warehouse today?"], [`_extracted_at`],
    ["Show me the freshest version of each row"], [`ORDER BY _extracted_at DESC` (the dedup view does this)],
  )]
  , kind: table
  )

A row with `updated_at = 2026-03-01` and `_extracted_at = 2026-03-15` was modified at the source two weeks before it was extracted -- maybe the pipeline runs weekly, maybe the row fell outside the extraction window until a periodic full replace picked it up. Both timestamps are correct; they answer different questions.

== Partition Filters
<partition-filters>
Most tables in the destination are partitioned by a business date -- `order_date`, `event_date`, `invoice_date`. This partition key controls how much data the engine reads: a query with `WHERE order_date >= '2026-01-01'` reads only partitions from January onward, while a query without a date filter reads the entire table.

On BigQuery with `require_partition_filter = true`, the engine rejects queries that don't include the partition column in the WHERE clause -- you'll get an error instead of a surprise bill. On other engines, the filter isn't enforced but the cost difference is the same.

```sql
-- destination: bigquery
-- This works and scans only 2026 data
SELECT order_id, status, total
FROM orders
WHERE order_date >= '2026-01-01';

-- This is rejected (require_partition_filter = true)
-- or scans the entire table (no enforcement)
SELECT order_id, status, total
FROM orders;
```

The partition filter is a cost filter. `WHERE order_date >= '2026-01-01'` doesn't just narrow your business results -- it tells the engine to skip every partition before January 2026. Always include it.

== Querying JSON Columns
<querying-json-columns>
Some source tables have JSON or nested data columns that land as-is (0507). If a flattening view exists (0703), use it -- the view extracts the fields you need into regular columns. If not, use the engine's JSON path syntax:

```sql
-- destination: bigquery (JSON string column)
SELECT
    order_id,
    JSON_EXTRACT_SCALAR(details, '$.shipping.method') AS shipping_method
FROM orders
WHERE order_date = '2026-03-15';

-- destination: bigquery (repeated records / STRUCTs)
SELECT
    o.order_id,
    item.sku,
    item.qty
FROM orders o, UNNEST(o.items) AS item
WHERE o.order_date = '2026-03-15';

-- destination: snowflake
SELECT
    order_id,
    details:shipping:method::STRING AS shipping_method
FROM orders
WHERE order_date = '2026-03-15';
```

`JSON_EXTRACT_SCALAR` works on string-typed JSON. `UNNEST` works on BigQuery's native repeated records and STRUCTs. Snowflake uses `:` path notation on `VARIANT` columns.

== JOINs on ECL Tables
<joins-on-ecl-tables>
Header-detail JOINs (`orders` JOIN `order_lines`) work the same as on the source -- the foreign keys are the same, the column names are the same, the relationship is the same.

The one difference is freshness. `orders` and `order_lines` may have been extracted minutes apart within the same pipeline run. A very recent order might exist in `orders` but not yet have its lines in `order_lines`, or vice versa. For any analysis that doesn't require real-time accuracy -- which is virtually all analysis on a columnar destination -- this gap is invisible.

```sql
-- destination: columnar
SELECT
    o.order_id,
    o.status,
    ol.product_id,
    ol.quantity,
    ol.unit_price
FROM orders o
JOIN order_lines ol ON o.order_id = ol.order_id
WHERE o.order_date >= '2026-03-01';
```

== Cost Traps
<cost-traps>
#strong[`SELECT *` scans every column.] Columnar engines store each column separately and only read the columns you name. `SELECT order_id, status` reads two columns. `SELECT *` reads all of them -- including that 2MB JSON blob you didn't need.

#strong[`COUNT(*)` is free (or nearly free).] BigQuery resolves it from metadata at zero bytes scanned. Snowflake resolves it from micro-partition headers. Use it freely for row counts.

#strong[`LIMIT` does NOT reduce cost on BigQuery.] `SELECT * FROM events LIMIT 10` still scans the full table; the LIMIT only caps the result set, not the bytes read. Filter with WHERE first, then LIMIT.

#strong[Preview modes scan less.] BigQuery's query preview and Snowflake's `SAMPLE` function read a subset of the table for exploration. Use these for "what does the data look like?" instead of `SELECT * LIMIT 100`.

== Anti-Patterns
<anti-patterns-3>
#ecl-danger("Don't assume LIMIT reduces cost")[In BigQuery, `SELECT \* FROM events LIMIT 10` scans the full table. Filter by partition first, select only the columns you need, then LIMIT.]

#ecl-warning("Don't expect real-time destination data")[The destination reflects the source as of the last successful extraction, not as of right now. Check `_extracted_at` or the health table (0602) to know how fresh the data is. If you need live data, query the source.]

== Related Patterns
<related-patterns-3>
- @append-and-materialize -- the dedup view analysts should query
- @metadata-column-injection -- understanding `_extracted_at` and `_batch_id`
- @partitioning-clustering-and-pruning -- why partition filters matter for cost
- @pre-built-views -- views built to save consumers from the raw data
- @sla-management -- freshness expectations and what "up to date" means

// ---

= Cost Optimization by Engine
<cost-optimization-by-engine>
#quote(block: true)[
#strong[One-liner:] Engine-specific strategies for keeping query costs under control -- because BigQuery bills differently from Snowflake, and the optimizations don't transfer.
]

== The Problem
<the-problem-3>
Cost optimization is engine-specific. What saves money on BigQuery (reducing bytes scanned) is irrelevant on Snowflake (which bills by warehouse time). Generic advice like "use partitions" applies everywhere, but the specifics -- what to partition on, how clustering interacts with the billing model, which operations are free and which are traps -- differ enough across engines that generic advice doesn't help with the decisions that actually move the bill.

The ECL engineer's load-time decisions have permanent cost consequences on every consumer query. A partition key chosen at table creation, a clustering configuration, a table format -- these compound across every query for the lifetime of the table. This chapter is the engine-specific reference for making those decisions correctly, and for knowing which levers to pull when the cost monitoring from 0603 surfaces a table that's too expensive.

I once wasted \$500 in a single night because of unlimited retries on a badly merged table. The retries ran all night, rescanning the table roughly 30 times a minute. By next morning the bill was already in, and the lesson was clear: set per-day cost limits on the project, and understand what each query costs before you let it retry indefinitely.

== BigQuery (Bytes Scanned)
<bigquery-bytes-scanned>
BigQuery on-demand billing charges per byte scanned: \$6.25/TB. Every query pays for the bytes it reads from the columns it touches, regardless of how many rows the result returns. The optimization target is reducing bytes scanned per query.

#ecl-info("BigQuery documentation")[#link("https://cloud.google.com/bigquery/pricing")[Pricing] -- #link("https://cloud.google.com/bigquery/docs/partitioned-tables")[Partitioned tables] -- #link("https://cloud.google.com/bigquery/docs/clustered-tables")[Clustered tables] -- #link("https://cloud.google.com/bigquery/docs/materialized-views-intro")[Materialized views] -- #link("https://cloud.google.com/bigquery/docs/reservations-intro")[Reservations (slots)]]

#strong[Partitioning + `require_partition_filter`.] Mandatory cost control for any table over a few GB. A query that filters on the partition column reads only the partitions that match; everything else is skipped at zero cost. `require_partition_filter = true` rejects queries that forget the filter, turning a potential \$50 full scan into an error message. See 0702 for partition key selection.

#strong[Clustering.] Reduces bytes scanned within partitions. Up to 4 columns, ordered by filtering priority -- the first column clusters most effectively. A query filtering on a clustered column reads fewer storage blocks because the engine skips blocks whose min/max range doesn't include the target value. BigQuery auto-reclusters in the background at no explicit cost.

#strong[Column selection.] `SELECT col1, col2` scans only those two columns. `SELECT *` scans every column in the table, including that 2MB JSON blob nobody needed. Columnar storage physically separates columns -- the engine literally reads fewer bytes when you name fewer columns.

#strong[`COUNT(*)`.] Free -- resolved from table metadata at 0 bytes scanned. Use it for row counts without cost anxiety.

#strong[`LIMIT`.] Does NOT reduce bytes scanned. `SELECT * FROM events LIMIT 10` still scans the full table; the LIMIT only caps the result set.

#strong[Materialized views.] BigQuery auto-refreshes materialized views and can route queries to the MV when the optimizer determines the MV can answer the query. The consumer queries the base table, but the engine reads the smaller MV instead. Effective for repetitive aggregation patterns -- dashboards that always GROUP BY the same dimensions.

#strong[Slots vs on-demand.] Flat-rate pricing (reservations/slots) makes bytes-scanned irrelevant -- you pay for compute capacity, not data read. The optimization target shifts from "scan fewer bytes" to "avoid slot contention." Most of the advice above still helps because it reduces execution time, which frees slots for other queries.

#strong[Per-day cost limits.] BigQuery supports custom cost controls at the project and user level -- maximum bytes billed per query and per day. Set these before your first production run, not after the first surprise bill. A runaway retry loop is bounded by the daily limit instead of running until someone notices.

== Snowflake (Warehouse Time)
<snowflake-warehouse-time>
Snowflake bills per second of warehouse compute. No per-byte charge -- a query that scans 1TB and one that scans 1GB cost the same if they run for the same duration on the same warehouse size. The optimization target is reducing query runtime and minimizing idle warehouse time.

#ecl-info("Snowflake documentation")[#link("https://docs.snowflake.com/en/user-guide/warehouses-overview")[Warehouses overview] -- #link("https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions")[Micro-partitions and clustering] -- #link("https://docs.snowflake.com/en/user-guide/querying-persisted-results")[Persisted query results (caching)]]

#strong[Warehouse sizing.] Right-size the warehouse for the workload. A larger warehouse (XL) finishes queries faster but costs more per second; a smaller warehouse (XS) costs less but takes longer. For batch loads, an XS or S warehouse running for 10 minutes is usually cheaper than an XL running for 2 minutes -- the XL's per-second rate is higher and the minimum billing increment (60 seconds) means short bursts on a large warehouse are disproportionately expensive.

#strong[Auto-suspend and auto-resume.] Set auto-suspend aggressively -- 60 seconds is reasonable for most workloads. An idle warehouse that stays running burns credits for nothing. Auto-resume starts the warehouse on the next query, so the only cost of aggressive suspend is a brief cold-start delay.

#strong[Clustering.] Snowflake manages micro-partitions automatically, but declaring cluster keys helps when the natural ingestion order doesn't match how consumers filter. Reclustering costs warehouse credits, so don't cluster tables where the natural order already matches the query pattern.

#strong[Result caching.] Identical queries within 24 hours return cached results at zero compute cost. Significant for dashboards that refresh periodically with the same query -- the first execution pays, subsequent ones are free until the underlying data changes or 24 hours pass.

#strong[Query queuing.] Too many concurrent queries on a small warehouse queue instead of fail. Queued queries wait for a slot, which is fine for batch loads but terrible for interactive dashboards. Monitor queue times and scale the warehouse if interactive queries are consistently queuing.

== ClickHouse (Self-Hosted Compute)
<clickhouse-self-hosted-compute>
ClickHouse is self-hosted (or managed via ClickHouse Cloud) -- cost is infrastructure (CPU, memory, disk), not per-query metering. The optimization target is making queries fast enough that the infrastructure you're already paying for can handle the workload.

#ecl-info("ClickHouse documentation")[#link("https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree")[MergeTree engine] -- #link("https://clickhouse.com/docs/en/sql-reference/statements/alter/projection")[Projections]]

#strong[MergeTree `ORDER BY`.] The primary cost lever. The `ORDER BY` clause in the MergeTree definition determines how data is physically sorted on disk. Queries that filter on the `ORDER BY` prefix skip entire granules (\~8,192 rows) that don't match -- the ClickHouse equivalent of partition pruning. Choose the `ORDER BY` to match the most common consumer query pattern. Fixed at creation -- can't be changed without recreating the table.

#strong[Compression.] ClickHouse compresses aggressively by default, and column types affect compression ratio. `LowCardinality(String)` for columns with a small number of distinct values (status fields, country codes, category names) replaces each value with a dictionary-encoded integer, reducing both storage and scan time. Apply it at table creation for columns with fewer than \~10,000 distinct values.

#strong[Materialized views.] ClickHouse materialized views trigger on INSERT and pre-compute aggregations at write time -- a fundamentally different model from the others. The materialized result is always current with zero read-time overhead, which makes them ideal for dashboards that need real-time aggregations. The tradeoff is that the aggregation logic runs on every insert, adding load-time overhead.

#strong[Projections.] Alternative physical orderings of the same data. If your table is `ORDER BY (event_date, event_type)` but some queries filter only on `user_id`, a projection ordered by `user_id` lets those queries skip granules efficiently. Multiple projections optimize multiple query patterns simultaneously, at the cost of additional storage and insert overhead.

== Redshift (Cluster Compute)
<redshift-cluster-compute>
Redshift bills per node per hour (provisioned) or per RPU-second (Serverless). Provisioned clusters pay for the hardware regardless of utilization; Serverless pays per compute consumed. The optimization target depends on the model: provisioned optimizes for query efficiency (get more done on the same nodes), Serverless optimizes for query cost (reduce compute time per query).

#ecl-info("Redshift documentation")[#link("https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html")[Sort keys] -- #link("https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html")[Distribution styles] -- #link("https://docs.aws.amazon.com/redshift/latest/dg/t_Reclaiming_storage_space202.html")[VACUUM] -- #link("https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html")[Redshift Spectrum]]

#strong[Sort keys.] The equivalent of clustering -- they determine physical sort order on disk and enable block skipping on filtered queries. Compound sort keys work for range queries on a prefix of columns. Interleaved sort keys work for multi-column filters where queries might filter on any combination, at the cost of slower VACUUM. Fixed at creation -- changing requires a full table rebuild.

#strong[Dist keys.] Control how data is distributed across nodes. When two tables are distributed on the same key (e.g., both `orders` and `order_lines` on `order_id`), JOINs between them don't need to redistribute data across the network -- co-located rows are already on the same node.

#strong[VACUUM and ANALYZE.] After heavy DELETE or UPDATE operations, Redshift doesn't fully reclaim space automatically. Dead rows from deleted records inflate scans and waste I/O until VACUUM runs. ANALYZE updates the query planner's statistics. If your pipeline does heavy deletes (hard delete detection, merge patterns), schedule VACUUM as part of the post-load step.

#strong[Spectrum.] Query data in S3 directly without loading it into the cluster. Useful for cold data that's too large or too infrequent to justify cluster storage. Spectrum bills per byte scanned (like BigQuery), so the optimization advice for S3-resident data is the same: partition, use Parquet, select only the columns you need.

== Cross-Engine Principles
<cross-engine-principles>
#strong[Partition by business date, cluster/sort by consumer filter columns.] Universal. The partition key controls which slices the engine reads; the cluster/sort key controls how efficiently it reads within those slices.

#strong[Select only the columns you need.] Matters most on BigQuery (bytes scanned = money), still reduces I/O and speeds up queries on every engine.

#strong[Monitor before optimizing.] Cost attribution from 0603 tells you which tables and queries to focus on. Optimizing a table that costs \$0.02/month is wasted effort.

#strong[Set cost guardrails early.] BigQuery's per-day cost limits, Snowflake's resource monitors, Redshift's query monitoring rules -- every engine has a mechanism to cap runaway costs. Configure them before production, not after.

== Anti-Patterns
<anti-patterns-4>
#ecl-warning("Don't apply BigQuery optimizations to Snowflake")[Reducing bytes scanned doesn't affect Snowflake's bill -- it's warehouse time that matters. Conversely, warehouse sizing is irrelevant on BigQuery's serverless model. Know which billing model you're optimizing for.]

#ecl-danger("Don't optimize tables that aren't expensive")[A 10k-row lookup table costs fractions of a cent per query regardless of partitioning or clustering. Optimize what shows up in the top-10 cost report from 0603.]

#ecl-warning("Don't let unlimited retries run unbound")[A retry loop on BigQuery rescans the table on every attempt. 30 retries per minute on a 100GB table is 4.3TB scanned per hour -- \$27/hour, \$216 overnight. Set retry limits and per-day cost caps before the first production run.]

== Related Patterns
<related-patterns-4>
- @columnar-destinations -- per-engine storage and DML mechanics
- @cost-monitoring -- measure before optimizing
- @partitioning-clustering-and-pruning -- partition and cluster key selection
- @pre-built-views -- materialized views as a cost reduction tool

// ---

= Point-in-Time from Events
<point-in-time-from-events>
#quote(block: true)[
#strong[One-liner:] Reconstruct past state from event tables, not snapshots. Events are cheaper to store and replay than periodic copies of the full state.
]

== The Problem
<the-problem-4>
A consumer asks "what was the inventory level on March 5?" or "what was the order status at 2pm last Tuesday?" If you only have the current state -- the latest version of each row from a full replace or a dedup view -- the answer is gone, overwritten by subsequent updates. The destination reflects right now, not any point in the past.

Two mechanisms preserve history. An append-and-materialize log (0404) accumulates extracted versions over time -- each extraction appends rows tagged with `_extracted_at`, and prior versions survive alongside current ones until compaction. Event tables take a different approach -- `inventory_movements`, append-only `events` -- each row is something that happened, and the full history is in the log itself. Any point-in-time state is computable by replaying events up to the target date, without storing a single snapshot.

In practice, the most common use case is inventory auditing. A client wants to reconcile their physical stock count against what the system said the stock was on the count date. That's a point-in-time reconstruction: sum all movements up to the count date, compare against the physical count, and the difference tells you whether the system or the warehouse is wrong.

== Movements to Photos
<movements-to-photos>
`inventory_movements` records every stock change: +50 received, -3 sold, -1 adjustment, -10 transferred out. Current stock is the running sum of all movements per SKU per warehouse. Stock as of any date is the same sum, filtered to movements before that date.

```sql
-- destination: columnar
-- Current inventory, reconstructed from movements
SELECT
    sku_id,
    warehouse_id,
    SUM(quantity) AS on_hand
FROM inventory_movements
GROUP BY sku_id, warehouse_id;
```

```sql
-- destination: columnar
-- Inventory as of March 5, reconstructed from movements
SELECT
    sku_id,
    warehouse_id,
    SUM(quantity) AS on_hand
FROM inventory_movements
WHERE created_at <= '2026-03-05'
GROUP BY sku_id, warehouse_id;
```

The two queries are identical except for the WHERE clause. Any point-in-time snapshot is computable from the event log by moving the date boundary -- this is why 0701 insists on landing the movements: the detail produces any aggregate, but the aggregate can't reproduce the detail.

For high-frequency point-in-time queries -- a dashboard showing stock levels at close-of-business for each day of the month -- replaying the full movement history on every query gets expensive fast. A materialized table built from movements avoids the rescan: a scheduled query (0703) runs after each extraction, replays movements up to each date, and writes the result.

The trap is materializing the full grid. 200 warehouses, 100,000 SKUs, 365 days -- that's 7.3 billion rows for a single year, most of them zeros because a given SKU doesn't move every day in every warehouse. Materialize sparse: only SKU/warehouse/date combinations where a movement actually occurred. A consumer who needs "stock on March 5 for SKU X in warehouse Y" gets the answer from the most recent materialized row on or before that date, not from a row for every day.

Even sparse, the table grows with activity volume over time. Tiered granularity keeps it manageable: daily materialization for the current month, monthly rollups for anything older. You can also scope by warehouse type -- sales warehouses that move thousands of SKUs daily need daily granularity, while a low-activity storage warehouse that sees a handful of movements per week is fine at monthly resolution. The goal is to pre-compute the queries consumers actually run, not every possible combination of dimensions and dates.

== Status History from Append Logs
<status-history-from-append-logs>
Not every table has a natural event log. `orders` doesn't have a changelog -- it's a mutable table that gets updated in place. But if `orders` is loaded via append-and-materialize (0404), the log table has every extracted version of each order: order 123 with `status = pending` from Monday's extraction, order 123 with `status = shipped` from Wednesday's. The extraction log becomes an implicit event trail, with one "event" per extraction run.

Status at a point in time is the latest extracted version before the target timestamp:

```sql
-- destination: columnar
-- Order status as of March 5 at 2pm
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _extracted_at DESC
        ) AS rn
    FROM orders_log
    WHERE _extracted_at <= '2026-03-05 14:00:00'
)
WHERE rn = 1;
```

The granularity is limited to extraction frequency. If you extract daily, you can reconstruct state at daily resolution -- you know what the order looked like at each extraction, not at every moment between them. For most analytical use cases this is sufficient; for audit-level granularity, the source system's own changelog is the authoritative record.

#ecl-warning("Compaction destroys version history")[0404 recommends compacting the log -- trimming old extractions or collapsing to latest-only -- to keep the dedup view fast and storage bounded. That compaction deletes the version history this section depends on. If consumers need point-in-time reconstruction from the append log, the compaction retention window must be longer than their lookback requirement. A 90-day lookback needs at least 90 days of log retention, which means 90 days of extraction overlap sitting in storage. That's a real cost on a large table -- decide upfront whether the log is a temporary buffer or a historical record, because it can't cheaply be both.]

== When Events Aren't Enough
<when-events-arent-enough>
Not all state changes produce events. A `customers` table updated in-place with no changelog has no event trail -- the previous state is gone the moment the row is overwritten. `products` has the same problem: a price change replaces the old price, and unless someone stored the before-and-after, the history is lost.

For tables where point-in-time matters but no event log exists:

#strong[Append-and-materialize with history retention (0404).] Skip compaction (or compact less frequently) and the append log becomes an explicit version history. Each extraction appends the current state of changed rows, and prior versions accumulate. Storage grows with extraction frequency, but the history is queryable -- point-in-time state is the latest extracted version before the target date.

#strong[Append-and-materialize log (0404).] The extraction log provides event-like history as a side effect of the load strategy -- cheaper than full snapshots because each extraction appends only the changed rows, not the entire table. The tradeoff: the history exists only at extraction granularity, and compacting the log destroys it. Once you compact to latest-only, the prior versions are gone. If consumers depend on point-in-time queries against the log, the compaction retention window must be longer than their lookback requirement -- and they need to know that compaction is happening so they don't build a process that assumes the history is permanent.

#strong[SCD Type 2 (Slowly Changing Dimension).] When point-in-time queries are a first-class requirement -- not an occasional audit but something dashboards and reports depend on daily -- an SCD2 structure makes the history explicit in the schema itself. Each row gets `valid_from` and `valid_to` columns, and a query for "what did this customer look like on March 5?" becomes a range filter instead of a window function over an extraction log:

```sql
-- destination: columnar
-- Customer record as of March 5
SELECT *
FROM customers_scd2
WHERE customer_id = 42
  AND valid_from <= '2026-03-05'
  AND (valid_to > '2026-03-05' OR valid_to IS NULL);
```

Building the SCD2 table is a downstream transformation, not a conforming operation -- the pipeline lands the current state or the append log, and a scheduled job compares consecutive extractions to detect changes and maintain the `valid_from`/`valid_to` bookkeeping. The mechanics are well-documented elsewhere; what matters for this pattern is that SCD2 gives you point-in-time queries that are cheap to run (a range filter that benefits from partitioning and clustering -- 0702), explicit in their semantics (no ambiguity about what `_extracted_at` means versus when the change actually happened), and immune to compaction -- the history is the table, not a side effect of a retention window.

The cost is maintaining the SCD2 pipeline itself. Every extraction needs to be diffed against the previous state to detect what changed, close out old rows, and open new ones. For a `customers` table with 100K rows that changes slowly (hence the name), this is trivial. For an `orders` table with millions of rows and high mutation rates, the daily diff becomes expensive. SCD2 earns its place on tables where the change rate is low relative to the table size and the point-in-time queries are frequent -- dimension tables like `customers`, `products`, `warehouses`. For high-mutation fact tables, the append log or snapshot approaches are usually cheaper to maintain.

== Storage: Events vs Snapshots
<storage-events-vs-snapshots>
#figure(
  align(center)[#table(
    columns: (25%, 25%, 25%, 25%),
    align: (auto,auto,auto,auto,),
    table.header([Approach], [Storage grows with], [Point-in-time granularity], [Completeness],),
    table.hline(),
    [Event log (`inventory_movements`)], [Activity volume], [Per-event (every change)], [Only as complete as the event source],
    [Snapshot append (`_snapshot_at`)], [Snapshot frequency x table size], [Per-snapshot (daily, hourly)], [Always complete -- it's a full copy],
    [Append-and-materialize log], [Extraction frequency x change volume], [Per-extraction], [Only changes captured by the extraction],
    [SCD Type 2], [Change volume (one row per change per key)], [Per-extraction (when the diff detected it)], [Only changes captured between consecutive extractions],
  )]
  , kind: table
  )

Low-mutation tables store far less with events than with snapshots -- 10 changes per day adds 10 rows, while a daily snapshot adds the entire table. High-mutation tables may store more with events. The break-even depends on the mutation rate relative to the table size.

Tiered retention applies to all approaches: keep daily granularity for the recent window, compress older data to monthly, drop anything beyond the retention requirement.

== Completeness
<completeness>
Replay is only as accurate as the event log, and event logs have gaps. The domain model's `inventory_movements` table has a soft rule: "every stock change creates a movement." But bulk import scripts that update `inventory` directly without logging a movement violate this silently (0002). The reconstructed snapshot from movements will differ from the actual `inventory` table, and the difference is the sum of all unlogged changes.

We had a client whose `inventory` table and the reconstructed-from-movements inventory diverged by hundreds of units on certain SKUs. The client refused to believe our data was correct -- their expectation was that movements and inventory should always match. We had to pull both from the source, show the same discrepancy in the source system itself, and demonstrate that the gap came from bulk operations that bypassed the movement log. The pipeline was cloning faithfully; the source was inconsistent.

The periodic full replace of the `inventory` table catches the drift -- it reflects the source's current state, including unlogged changes. The event-based reconstruction doesn't. When both exist in the destination, consumers should understand which one to trust: the `inventory` table for current state (it's what the source says right now), the movement log for historical reconstruction (it's what the source recorded happening). When they disagree, the source has unlogged changes -- that's a source data quality problem, not a pipeline problem.

== Anti-Patterns
<anti-patterns-5>
#ecl-danger("Don't assume every table has events")[`customers`, `products`, dimension tables are overwritten in place with no changelog. Point-in-time reconstruction requires either snapshots or an append-and-materialize log. Choose the load strategy before the consumer asks for the history -- retrofitting history onto a table that was loaded with full replace from day one means there's nothing to reconstruct.]

#ecl-warning("Don't replay without knowing completeness")[If the event log has gaps (bulk operations that bypass it, the soft rule from the domain model), the reconstructed state is wrong. Document which event sources are incomplete and surface the discrepancy rather than hiding it.]

#ecl-danger("Don't compact without considering consumers")[Compacting the append log to latest-only destroys version history. If consumers depend on point-in-time queries against the log, the compaction retention window must be longer than their lookback requirement.]

== Related Patterns
<related-patterns-5>
- @append-and-materialize -- append log as version history when compaction is deferred
- @append-and-materialize -- extraction log as an implicit event trail
- @dont-pre-aggregate -- land movements, build photos downstream
- @pre-built-views -- materialized tables for pre-computed daily running totals
- @activity-driven-extraction -- `inventory_movements` as the activity signal

// ---

= Schema Naming Conventions
<schema-naming-conventions>
#quote(block: true)[
#strong[One-liner:] Table and column naming at the destination: as-is from source, snake\_case, normalized? Pick a convention and apply it consistently -- changing it later is a full migration.
]

== The Problem
<the-problem-5>
Source systems name things however they want: `OrderID`, `@ORDER_VIEW`, `invoice_line`, `OACT`, `Column Name With Spaces`. The destination needs identifiers that are consistent and queryable without quoting gymnastics. A column called `order` clashes with a reserved word on every engine, `@Status` collides with SQL Server's variable syntax, and `Column Name With Spaces` demands quotes everywhere it appears.

This is a one-time decision with permanent consequences -- changing a naming convention on a running pipeline means rebuilding every table and rewriting every downstream query, view, and dashboard that touches those names. Identifier normalization is a conforming operation: it happens at load time, not downstream. Get it right and consistently applied, and downstream teams will (mostly) follow your lead. Get it wrong, and you'll find `Vw_Sales-backup_FINAL-JSmith_2026-05` in your catalog within the year.

== Naming Schemes
<naming-schemes>
Three schemes see real use, and each behaves differently depending on the destination engine.

=== Preserve source names
<preserve-source-names>
Land `OrderID` as `OrderID`, `OACT` as `OACT`, `invoice_line` as `invoice_line`. Anyone looking at the destination can trace a column straight back to the source without a mapping table.

The real argument for this approach comes from upstream teams. They send you queries written against their system and expect them to run against yours -- it's one of the most common requests you'll get. When the destination preserves `OrderId`, adapting a source query is mechanical: quote the identifiers, adjust the `FROM` clause, done. When the destination has normalized to `order_id`, every column in a 30-column query needs translating back, and someone will get one wrong. If your consumers regularly cross-reference with the source system, preserving names saves everyone time.

The cost is that five sources produce five different conventions in the same destination. `OrderID` sits next to `order_id` sits next to `ORDER_STATUS`, and every consumer has to know which source uses which style.

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Destination], [What happens],),
    table.hline(),
    [BigQuery], [Case-sensitive -- names land exactly as provided, preserve works cleanly],
    [Snowflake], [Folds to uppercase by default. `OrderID` quietly becomes `ORDERID` unless you double-quote at create time #emph[and] in every query],
    [PostgreSQL], [Folds to lowercase by default. `OrderID` becomes `orderid` unless quoted],
    [ClickHouse], [Case-sensitive -- names preserved exactly],
    [SQL Server], [Case-insensitive (collation-dependent). `OrderID` and `orderid` resolve to the same column; the original casing is stored but not enforced],
  )]
  , kind: table
  )

#ecl-warning("Snowflake and PostgreSQL destroy mixed case")[Committing to preserve-source-names on either engine means double-quoting every identifier in every DDL and every query. Most teams that start here end up quoting nothing and losing the casing by default -- arriving at lowercase-only by accident rather than by choice.]

=== Normalize to snake\_case
<normalize-to-snake_case>
`OrderID` becomes `order_id`, `Column Name With Spaces` becomes `column_name_with_spaces`, and `invoice_line` stays put. This is the standard analytics warehouse convention -- consistent, quoting-free, and what analysts expect when they write SQL by hand.

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Destination], [What happens],),
    table.hline(),
    [BigQuery], [The ecosystem convention -- BigQuery's own `INFORMATION_SCHEMA` uses snake\_case. Store original source names in column descriptions for traceability],
    [Snowflake], [Lands as `ORDER_ID` due to the uppercase fold, but `order_id` and `ORDER_ID` resolve identically so it reads fine],
    [PostgreSQL], [The native convention. System catalogs use it, `psql` tab-completion expects it],
    [ClickHouse], [Works, though ClickHouse's own system tables mix camelCase (`query_id` alongside `formatDateTime`). No strong ecosystem standard],
    [SQL Server], [Technically fine, but the SQL Server world expects PascalCase (`OrderId`, `CustomerName`). Landing snake\_case puts your ECL tables at odds with every system table and most existing schemas. Right call if consumers write ad-hoc SQL; friction if they're .NET applications expecting `dbo.Orders.OrderId`],
  )]
  , kind: table
  )

The cost is irreversibility. Once `OrderID` becomes `order_id`, the original casing is gone -- and if two source columns normalize to the same string (`OrderID` and `Order_ID` both become `order_id`), you have a collision to detect and resolve at load time.

For most pipelines, snake\_case is still the better default -- it reads clean, requires no quoting on case-insensitive engines, and it's what analysts expect to find. We use it across the board and it's never been the wrong call. But we've also worked with clients whose upstream teams live in the source system and send us queries daily, and for those cases preserve-source-names would have saved us hours of translation work every week.

=== Lowercase only
<lowercase-only>
`OrderID` becomes `orderid`, `Column Name With Spaces` becomes `columnamewithspaces`. Fold to lowercase, strip illegal characters, done.

Many teams are already doing this without realizing it -- PostgreSQL and Snowflake both fold unquoted identifiers automatically. Single-word names survive fine (`orderid` is readable enough), but multi-word names lose all structure. Try parsing `inventorymovementlogs` or `abortedsessioncount` at a glance.

Lowercase-only is viable when the source uses short, single-word identifiers (common in legacy ERPs: `OACT`, `BUKRS`, `WAERS`). For anything with compound names, snake\_case is worth the extra transformation.

== Handling Special Characters and Reserved Words
<handling-special-characters-and-reserved-words>
No naming scheme saves you from these -- they need explicit handling regardless of which convention you pick.

#strong[Reserved words] like `order`, `select`, `from`, `table`, and `group` break unquoted queries on every engine, and snake\_case doesn't help because `order` stays `order`. Prefix with the source context (`source_order`), suffix with an underscore (`order_`), or accept that the column will always need quoting.

#strong[Syntactic characters] -- `@Status`, `#Temp`, `$Amount` -- carry engine-specific meaning. SQL Server interprets `@` as a variable prefix and `#` as a temp table marker, so a column named `@Status` requires quoting there even though PostgreSQL handles it fine. Strip or replace any character that has syntactic meaning on your destination.

#strong[Spaces] require quoting on every engine without exception. Replace with underscores -- the one normalization everyone agrees on.

#strong[Accented characters] like `línea_factura` or `straße` are valid UTF-8 and every modern engine supports them, but BI tools and older ODBC connectors can choke. Replace accents at load time (`línea` → `linea`, `straße` → `strasse`) -- the readability cost is negligible, and you avoid discovering the incompatibility at the worst possible moment.

#strong[Collisions after normalization] happen when a case-sensitive source has columns like `OrderID` and `orderid` that collapse to the same string after any normalization. Detect these at load time and fail loudly -- a silent overwrite is worse than a broken load. Resolve by suffixing (`orderid`, `orderid_1`) and document the original-to-normalized mapping in column descriptions or a schema contract (0609). Ugly, but it preserves every source column.

== Schema Naming
<schema-naming>
Column naming decides what identifiers look like. Schema naming decides where tables live -- which schema (PostgreSQL, Snowflake, SQL Server) or dataset (BigQuery) holds each source's data, and how consumers know where to look.

=== Connection as schema
<connection-as-schema>
One destination schema per source connection, named to encode both the server and the specific database. `production` as a schema name is useless when you pull from three production databases on two servers. `erp_prod_finance` tells you the server and the database in one glance.

When a single connection exposes multiple schemas -- SQL Server defaults to `dbo`, PostgreSQL to `public`, MySQL has no schema layer at all -- flatten them into the destination with a double underscore separator:

```
connection__schema.table
```

Single underscores already appear inside names (`order_lines`), so `__` is an unambiguous boundary:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Source], [Source table], [Destination table],),
    table.hline(),
    [PostgreSQL `erp_prod`, schema `public`], [`public.orders`], [`erp_prod__public.orders`],
    [PostgreSQL `erp_prod`, schema `accounting`], [`accounting.invoices`], [`erp_prod__accounting.invoices`],
    [SQL Server `crm_main`, schema `dbo`], [`dbo.customers`], [`crm_main__dbo.customers`],
    [SQL Server `crm_main`, schema `sales`], [`sales.leads`], [`crm_main__sales.leads`],
    [MySQL `shopify_prod` (database = schema)], [`orders`], [`shopify_prod.orders`],
    [SAP B1 `sap_prod`, schema `dbo`], [`dbo.OACT`], [`sap_prod__dbo.OACT`],
  )]
  , kind: table
  )

For MySQL and other engines where database and schema are the same thing, `connection__schema` collapses naturally -- `shopify_prod.orders` instead of `shopify_prod__shopify_prod.orders`. But when a connection has a single schema that isn't the only one it #emph[could] have (SQL Server with just `dbo`, PostgreSQL with just `public`), keep the full `connection__schema` form anyway. `erp_prod.orders` reads cleaner than `erp_prod__public.orders` today, but the moment that server gets a second schema you're facing a rename across every table and every downstream reference. Use `connection__schema` from day one and the second schema slots in without touching anything that already exists.

You can extend the prefix with a business domain (`finance__erp_prod__accounting.invoices`) to group related schemas alphabetically, but this extra nesting is rarely worth it until your schema list outgrows a single screen.

=== Layer prefixes
<layer-prefixes>
Prefixing schemas with `raw_`, `bronze_`, or `landing_` marks the data layer: `raw__erp_prod.orders` versus `curated__erp_prod.orders`. The benefit is alphabetic grouping in catalog UIs and `INFORMATION_SCHEMA` queries -- all raw schemas cluster together, all curated schemas cluster together. Apply layer prefixes consistently across every schema or not at all; a mix of prefixed and bare names is worse than no prefixes.

=== Opaque sources and layered schemas
<opaque-sources-and-layered-schemas>
Systems like SAP name every table with codes that mean nothing outside the source -- `OACT`, `OINV`, `INV1`. The temptation to rename `OACT` to `chart_of_accounts` at load time is strong, especially when your analysts keep asking "what's OACT?", but that rename is a semantic transformation that crosses the conforming boundary. Land the source name, use table metadata (column descriptions, table comments) to explain what it means, and let consumers discover the mapping without a separate lookup table.

We run a SAP B1 deployment where we landed everything raw at first -- one schema, hundreds of opaque tables. It worked until it didn't scale. The approach that survived:

#figure(
  align(center)[#table(
    columns: (18.31%, 38.03%, 43.66%),
    align: (auto,auto,auto,),
    table.header([Schema], [What lives here], [How tables are named],),
    table.hline(),
    [`bronze__sap_b1__schema_1`], [Raw landing from SAP schema 1], [Source codes: `OACT`, `OINV`, `INV1`, `ORDR`, `RDR1`],
    [`bronze__sap_b1__schema_2`], [Raw landing from SAP schema 2], [Source codes],
    [`bronze__sap_b1__schema_3`], [Raw landing from SAP schema 3], [Source codes],
    [`silver__sap_b1`], [All bronze layers consolidated, enriched with metadata], [Still source codes: `OACT`, `OINV` -- same table, more columns],
    [`gold__sap_b1`], [Business-facing models], [Human names: `chart_of_accounts`, `ar_documents`, `balance`],
  )]
  , kind: table
  )

Bronze lands what SAP calls it, separated by source schema. Silver consolidates across schemas and enriches with metadata, joins, and deduplication, but the tables keep their opaque names -- `OACT` is still `OACT`, just with more columns. Gold is where `OACT` finally becomes `chart_of_accounts`, because at that layer the consumers are analysts who have never logged into SAP. The semantic rename belongs here, not in the ECL layer.

=== Staging conventions
<staging-conventions>
Staging tables need their own namespace to avoid colliding with production. Table prefix (`stg_orders` in the same schema) or parallel schema (`orders_staging.orders`) -- the tradeoffs are covered in 0203.

== Per-Table Overrides
<per-table-overrides>
The convention should be configurable at two levels: a destination-wide default that covers the common case, and per-table overrides for the exceptions. Collisions from character stripping are the most common reason you'll need them.

We learned this the hard way with a client who had `ProductStock` and `ProductStock$` in the same source -- identical structure, one holding unit quantities and the other monetary values. Our stripping rule removed the `$`, both tables landed as `product_stock`, and whichever loaded second silently overwrote the first. We didn't catch it until the numbers stopped making sense downstream. The fix was a per-table override renaming one to `product_stock_value` -- a borderline transformation, but better than losing data. The general rule works until it doesn't, and when it doesn't, the alternative to a per-table escape hatch is rewriting the entire convention.

0609 treats the naming convention as a schema contract -- any change to it, including per-table overrides, is a breaking change that should go through the contract process.

== Migrating a Convention
<migrating-a-convention>
We've done this once. It was a week of hell -- rebuilding tables, rewriting queries, repointing every report and dashboard that referenced the old names. We thought we were done by Friday. We weren't. For three months afterward, people came back from vacation to broken dashboards, scheduled exports failed silently because nobody had updated the column references, and ad-hoc queries saved in personal notebooks kept surfacing the old names. Every time we thought we'd caught the last one, someone opened a ticket.

Don't do it if you can avoid it. If you can't, treat it as a formal breaking change: announce a cutover window, run a deprecation period where both conventions coexist (old names as views over new tables), and set a hard deadline for tearing down the aliases. And budget three months of intermittent cleanup after the deadline, because you will need them.

== Anti-Patterns
<anti-patterns-6>
#ecl-danger("Don't change convention on a running pipeline")[Changing from camelCase to snake\_case across 200 tables means rebuilding every table and updating every downstream query, view, and dashboard. The only thing better than perfect is #strong[standardized];.]

#ecl-warning("Don't rename source tables for readability")[`OACT` → `chart_of_accounts` is a semantic rename that crosses the conforming boundary. The pipeline lands what the source calls it. If consumers need readable names, build an alias layer downstream.]

#ecl-danger("Don't mix conventions in one destination")[Tables from source A in snake\_case and tables from source B in camelCase within the same dataset confuses every consumer. Avoid when possible.]

== Related Patterns
<related-patterns-6>
- @charset-and-encoding -- encoding of identifier names, not just data values
- @data-contracts -- naming convention as a schema contract
- @staging-swap -- staging table naming
- @columnar-destinations -- per-engine identifier behavior

// ---
