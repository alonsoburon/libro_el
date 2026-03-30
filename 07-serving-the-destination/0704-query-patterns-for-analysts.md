---
title: "Query Patterns for Analysts"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Query Patterns for Analysts

> **One-liner:** Cheat sheet: how to query append-only tables, how to get latest state, how to not blow up costs.

## Who This Is For

This is the reference you hand analysts when they get access to the destination. They didn't design the schema, they don't know what append-and-materialize means, and they will `SELECT *` on a 3TB table if nobody tells them not to. The patterns below are the minimum they need to query ECL-landed data correctly and cheaply.

One thing to internalize before querying: the destination is not a moment-to-moment replica of the source. Data has to be extracted, conformed, and loaded before it appears -- that takes time, and the freshness depends on the table's schedule ([[06-operating-the-pipeline/0604-sla-management|0604]]). If you need transactional-level freshness for point lookups ("is this order shipped right now?"), query the source directly. Columnar destinations are for analysis, not real-time lookups.

## Current State from Append-Only Tables

Some tables in the destination are append logs -- every extraction appends new rows without overwriting old ones ([[04-load-strategies/0404-append-and-materialize|0404]]). The log contains every version of every row your pipeline has ever seen: order 123 with `status = pending`, then order 123 with `status = shipped`, then order 123 with `status = delivered`. All three rows are in the log. The current state is the latest one.

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

> [!danger] Don't aggregate on the log table
> `SELECT SUM(total) FROM orders_log` sums every version of every order -- if an order was extracted 5 times, its total appears 5 times in the sum. Use the dedup view, or wrap the aggregation around a deduped subquery.

## `_extracted_at` vs `updated_at`

Two timestamp columns, two different clocks:

**`_extracted_at`** is when the pipeline pulled the row. It's set by the pipeline, not the source, and it's always accurate -- it reflects when this version of the row arrived in the destination.

**`updated_at`** is when the source last modified the row. It's maintained by the source application -- triggers, ORMs, manual updates -- and its reliability varies by table ([[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]]).

Which one to filter on depends on the question:

| Question | Filter on |
|---|---|
| "What changed at the source this week?" | `updated_at` |
| "What arrived in our warehouse today?" | `_extracted_at` |
| "Show me the freshest version of each row" | `ORDER BY _extracted_at DESC` (the dedup view does this) |

A row with `updated_at = 2026-03-01` and `_extracted_at = 2026-03-15` was modified at the source two weeks before it was extracted -- maybe the pipeline runs weekly, maybe the row fell outside the extraction window until a periodic full replace picked it up. Both timestamps are correct; they answer different questions.

## Partition Filters

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

## Querying JSON Columns

Some source tables have JSON or nested data columns that land as-is ([[05-conforming-playbook/0507-nested-data-and-json|0507]]). If a flattening view exists ([[07-serving-the-destination/0703-pre-built-views|0703]]), use it -- the view extracts the fields you need into regular columns. If not, use the engine's JSON path syntax:

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

## JOINs on ECL Tables

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

## Cost Traps

**`SELECT *` scans every column.** Columnar engines store each column separately and only read the columns you name. `SELECT order_id, status` reads two columns. `SELECT *` reads all of them -- including that 2MB JSON blob you didn't need.

**`COUNT(*)` is free (or nearly free).** BigQuery resolves it from metadata at zero bytes scanned. Snowflake resolves it from micro-partition headers. Use it freely for row counts.

**`LIMIT` does NOT reduce cost on BigQuery.** `SELECT * FROM events LIMIT 10` still scans the full table; the LIMIT only caps the result set, not the bytes read. Filter with WHERE first, then LIMIT.

**Preview modes scan less.** BigQuery's query preview and Snowflake's `SAMPLE` function read a subset of the table for exploration. Use these for "what does the data look like?" instead of `SELECT * LIMIT 100`.

## Anti-Patterns

> [!danger] Don't assume LIMIT reduces cost
> In BigQuery, `SELECT * FROM events LIMIT 10` scans the full table. Filter by partition first, select only the columns you need, then LIMIT.

> [!danger] Don't expect real-time data from a columnar destination
> The destination reflects the source as of the last successful extraction, not as of right now. Check `_extracted_at` or the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) to know how fresh the data is. If you need live data, query the source.

## Related Patterns

- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- the dedup view analysts should query
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- understanding `_extracted_at` and `_batch_id`
- [[07-serving-the-destination/0702-partitioning-for-consumers|0702-partitioning-for-consumers]] -- why partition filters matter for cost
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- views built to save consumers from the raw data
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- freshness expectations and what "up to date" means
