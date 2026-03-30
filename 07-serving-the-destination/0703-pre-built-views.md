---
title: "Pre-Built Views"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Pre-Built Views

> **One-liner:** Materialized views, scheduled queries, and pre-cooked tables -- the serving layer you build on top of landed data to protect consumers from themselves.

## The Problem

The pipeline did its job: the data landed correctly, partitioned, with metadata columns and clean types. The destination is a faithful clone of the source. Now an analyst opens their query editor, writes `SELECT * FROM orders_log`, and gets back 90 million rows -- every version of every order from the append log, duplicates and all. They aggregate on it, get numbers that are 3x what the source shows, and file a bug against your pipeline. The data is correct; the query is wrong.

This is the gap the serving layer fills. The pipeline lands raw data. The serving layer builds clean, queryable surfaces on top of it -- dedup views that expose current state from append logs, flattening views that extract fields from JSON columns, materialized tables that pre-compute expensive aggregations so consumers don't pay for them on every query. None of this is in the pipeline. It's what you build after the data lands, as a service to the people who consume it.

The goal is to put a guardrail between the consumer and the raw data. Not because the raw data is wrong -- it's exactly what the source has -- but because raw data in a columnar engine is expensive to misuse, and most consumers don't know how their queries translate into bytes scanned or warehouse time. A well-built view costs you minutes to create and saves consumers thousands of dollars in accidental full scans over the life of the table.

In practice, the serving layer is smaller than you'd expect. A typical client with 70 base tables needs around 15 views for their entire reporting surface, and many of those are variations on the same core query with different filters or groupings -- maybe 5 distinct view designs that cover the full reporting need. When the client runs multiple companies on the same ERP schema (separate databases, identical structure), the base tables multiply but the views don't -- each view UNIONs the same table across databases with a `_database` column to distinguish the source. The effort is low; the impact on consumer experience and cost control is disproportionately high.

## The Hierarchy

Four tools, from lightest to heaviest. Start at the top and move down only when the lighter option doesn't serve the consumer well enough.

**SQL views.** A saved query, computed fresh on every read. The dedup view from [[04-load-strategies/0404-append-and-materialize|0404]] is the most common example: a `ROW_NUMBER()` over the append log that exposes only the latest version of each row. Column-filtering views that hide internal metadata (`_extracted_at`, `_batch_id`) are another. Free to create, not free to consume -- every query against the view scans the underlying table. A well-written view can reduce cost by baking in partition filters and column selection that consumers would otherwise forget, but it doesn't pre-compute anything.

**Materialized views.** Pre-computed and stored. The engine refreshes them on a schedule or on data change, and routes queries to the materialized result instead of recomputing from the base table. The query cost drops to scanning the materialized result (generally smaller than the base table), at the expense of storage and refresh overhead. This is where the cost savings happen -- the consumer queries the pre-built result, not the raw data.

Materialized views work best when the view has a single base table or a fact table with a few dimension lookups -- one source of truth driving the refresh. When the view joins multiple independently-refreshed fact tables, the "update on data change" trigger gets messy: every participating table's load triggers a refresh, and if five tables contribute to one view, you're refreshing it five times per pipeline run with partially-stale data each time. For views like these, scheduled query tables are the cleaner option.

**Scheduled query tables.** A query that runs on a schedule and writes its results to a destination table. The simplest form of materialization -- no special engine feature needed, works on every engine. Your orchestrator or a cron job runs the query after all the participating tables have landed, and consumers query the output table directly. Less elegant than a native materialized view, but more portable, easier to debug, and the right choice when the view joins many tables that refresh at different times -- one scheduled run after all sources have landed produces a consistent result without multiple partial refreshes.

**Consumer-specific tables.** A table shaped for a specific dashboard, report, or API. Pre-joined, pre-filtered, pre-aggregated -- exactly the columns and rows the consumer needs, nothing else. The most expensive to maintain (a pipeline change or a business logic change can invalidate it) and the most efficient to query (consumers scan only what they need with zero overhead). Reserve these for high-frequency, high-cost query patterns where even a materialized view isn't cheap enough.

## When to Materialize

The dedup view from [[04-load-strategies/0404-append-and-materialize|0404]] is a SQL view by default, and during development that's fine -- you're the only one querying it. Once analysts start using it daily, the cost shifts: 50 queries per day against a view that scans 90 days of append log means 50 full scans of that log per day. At that point the materialization cost (one refresh per load) is a fraction of the repeated read cost, and the switch is justified.

A view over 90 days of daily snapshots ([[02-full-replace-patterns/0202-snapshot-append|0202]]) is an even clearer case -- every query scans 90x the base table to find the latest snapshot per key. Materialization is almost always worth it here, even at low query frequency.

The rule: don't materialize speculatively. Wait until the query cost shows up in [[06-operating-the-pipeline/0603-cost-monitoring|0603]], then materialize the views that actually get hit. A materialized view for a table queried once a week is wasted storage and refresh compute -- and at 15 views across 70 base tables, most of the serving layer stays as simple SQL views that never need materialization.

## Flattening Views for JSON

JSON and nested data land as-is ([[05-conforming-playbook/0507-nested-data-and-json|0507]]) -- the pipeline doesn't parse or restructure them. Consumers who need tabular access get a flattening view. The syntax depends on how the data landed:

**JSON string columns** (landed as `STRING` or `JSON` type):

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

**Repeated records / STRUCTs** (BigQuery's native nested types, common when loading from Avro or when the loader normalizes JSON into typed structs):

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

When the nested schema mutates -- a new field appears, a field is renamed -- the view definition changes. The pipeline doesn't. This is the same principle as [[07-serving-the-destination/0701-dont-pre-aggregate|0701]]: the pipeline lands what the source has, the serving layer adapts it for consumption.

## Per Engine

**BigQuery.** SQL views are free to create but every query scans the underlying table and bills for bytes read. Materialized views auto-refresh and BigQuery routes queries to the MV when it can -- this is where the cost savings happen, because the query scans the pre-computed result instead of the full base table. Scheduled queries write to destination tables on a cron and are the workhorse for consumer-facing aggregation tables that join multiple sources.

**Snowflake.** SQL views are free to create, same caveat -- every query costs warehouse time against the underlying table. Materialized views refresh automatically on data change, costing warehouse credits for each refresh. Snowflake's `SECURE VIEW` hides the view definition from consumers, useful when the view encodes business logic you don't want exposed.

**PostgreSQL.** `CREATE MATERIALIZED VIEW` with `REFRESH MATERIALIZED VIEW CONCURRENTLY` for zero-downtime refreshes. No auto-refresh -- schedule via cron, orchestrator, or a post-load hook. Standard SQL views are free and fast for simple cases.

**ClickHouse.** Materialized views trigger on INSERT and pre-compute aggregations at write time -- a fundamentally different model from the others. The compute happens at ingestion, not at refresh time, so the materialized result is always current with zero read-time overhead. Powerful for dashboards that need real-time aggregations, but the logic is baked into the write path, making it harder to change than a post-hoc refresh.

## Anti-Patterns

> [!danger] Don't build consumer-specific tables in the ECL layer
> A table shaped for one dashboard is transformation. The pipeline lands data generically; the serving layer shapes it for consumption. If the dashboard needs a different shape, change the view, not the pipeline.

> [!danger] Don't materialize before you measure
> A materialized view for every table "just in case" is wasted storage and refresh compute. Materialize the views that actually get queried, based on observed cost from [[06-operating-the-pipeline/0603-cost-monitoring|0603]].

## Related Patterns

- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- the dedup view that most commonly needs materialization
- [[02-full-replace-patterns/0202-snapshot-append|0202-snapshot-append]] -- snapshot tables that benefit most from materialized current-state views
- [[05-conforming-playbook/0507-nested-data-and-json|0507-nested-data-and-json]] -- raw JSON landing that needs flattening views
- [[07-serving-the-destination/0701-dont-pre-aggregate|0701-dont-pre-aggregate]] -- the boundary between serving and conforming
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- the signal that tells you when to materialize
