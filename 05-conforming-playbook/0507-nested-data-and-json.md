---
title: "Nested Data and JSON"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Nested Data and JSON

> **One-liner:** JSON column in the source? Land it as-is. Normalizing nested data into relational tables is transformation, not conforming.

---

## The Playbook

Prefer landing JSON columns as they are -- `STRING` or the destination's native JSON type (BigQuery `JSON`, Snowflake `VARIANT`, PostgreSQL `JSONB`). The source has a JSON column, the destination gets a JSON column. That's conforming.

Flattening JSON into normalized tables (`order`, `order__details`, `order__details__items`) is closer to ETL than ECL. The C in ECL makes data survive the crossing -- it doesn't restructure it. If you have a strong reason to flatten (a consumer that absolutely cannot work with JSON and there's no downstream layer to do it), document the decision and know that you're stepping outside the conforming boundary. Most of the time, you don't need to.

Avoid the hybrid approach (land raw JSON + flatten to normalized tables) at the ECL layer. Two representations means double the storage, double the schema maintenance, and a synchronization problem when one updates and the other doesn't. One representation is enough -- pick the simpler one and let downstream build the other if they need it.

---

## Land As-Is

The default. The destination gets what the source has, with no interpretation and no decisions about which fields matter.

```sql
-- source: transactional (postgresql)
SELECT
    order_id,
    customer_id,
    details,  -- JSONB column, landed as-is
    CURRENT_TIMESTAMP AS _extracted_at
FROM orders
WHERE updated_at >= :last_run;
```

The `details` column might contain:

```json
{
  "shipping": {
    "method": "express",
    "address": {"city": "Santiago", "zip": "7500000"}
  },
  "items": [
    {"sku": "A100", "qty": 2, "price": 15.50},
    {"sku": "B200", "qty": 1, "price": 42.00}
  ],
  "notes": "Gift wrap requested"
}
```

Land it as-is. The structure, the nesting, the array of items -- all of it arrives at the destination exactly as the source stores it. No field selection, no type inference on nested values, no decision about whether `shipping.address` should be its own table.

This works well when consumers are data-conscious and comfortable with JSON query syntax. BigQuery's `JSON_EXTRACT_SCALAR(details, '$.shipping.method')`, Snowflake's `details:shipping:method`, PostgreSQL's `details->>'shipping'->'method'` -- all of these give consumers access to every field without the ECL layer making structural decisions on their behalf.

---

## Know Your Consumer

Not all BI tools handle nested data the same way, and that's worth understanding even though it doesn't change the ECL approach.

**Tools that handle JSON well.** Looker, BigQuery BI Engine, Metabase (with JSON path support), any tool where the query author writes SQL. These consumers can reach into the JSON with path expressions and extract what they need.

**Tools that struggle with JSON.** Power BI's handling of nested fields is limited -- it can expand JSON into columns, but the experience is clunky and the performance degrades with deeply nested structures. Some reporting tools expect flat tabular data and have no JSON path support at all.

But here's the thing: the same consumers who can't handle JSON often can't handle joins either. Normalizing the JSON into 5 relational tables and expecting a business analyst to JOIN `order` → `order__details` → `order__details__items` correctly is optimistic. You've traded one problem (they can't query JSON) for another (they can't join tables), and the second problem is arguably worse because wrong joins produce silently incorrect results while failing to query JSON produces an error.

If the consumer truly can't work with JSON, the answer is a downstream transformation -- a view or materialized table that flattens the JSON into the shape the consumer needs. That's a serving concern ([[07-serving-the-destination/0703-pre-built-views|0703]]), not an ECL concern. The ECL layer lands the data; the serving layer shapes it for consumption.

> [!tip] A flattening view is cheap and reversible
> A `CREATE VIEW orders_flat AS SELECT order_id, JSON_EXTRACT_SCALAR(details, '$.shipping.method') AS shipping_method, ...` gives the consumer a flat table without modifying the landed data. If the JSON structure changes, you update the view. If a new consumer needs a different shape, you create another view. The raw JSON in the landed table is always the source of truth.

---

## Schema Mutation in JSON

JSON columns mutate without warning. A new field appears because the application team shipped a feature. A field disappears because someone removed it from the API response. A field that was always a string is now sometimes a number because a third-party integration changed its output format. None of this is visible in the source schema -- the column type is still `JSONB`, the DDL hasn't changed, and your extraction query returns the same column.

This is downstream's problem, not the ECL layer's. Land the JSON as-is and let the consumer or the transformation layer handle schema evolution within the blob. The ECL layer doesn't parse the JSON, so it doesn't break when the JSON changes -- which is exactly the property you want.

The one exception: when schema mutation causes the *load itself* to fail. BigQuery `STRUCT` is schema-on-write -- every row must match the declared field names and types. If the JSON gains a new field that the `STRUCT` definition doesn't include, the load rejects the row. Two options:

**Land as `STRING` instead of `STRUCT`.** The destination stores the raw JSON text with no schema enforcement. Any valid JSON string loads successfully regardless of what fields it contains. Consumers parse the JSON at query time. This is the safest choice for mutating JSON because the schema is the consumer's problem, not the load's problem.

**Use a schema-on-read type.** Snowflake `VARIANT` accepts arbitrary JSON without a predefined schema. PostgreSQL `JSONB` does the same. These types give you native JSON query syntax without the rigidity of `STRUCT`. If your destination supports schema-on-read, prefer it over `STRING` for the better query ergonomics.

If you must use a typed `STRUCT` (because the destination requires it or because query performance on `STRING` is unacceptable), a full replace ([[04-load-strategies/0401-full-replace|0401]]) with an updated `STRUCT` definition handles the schema change cleanly -- drop and rebuild the table with the new field included.

---

## By Corridor

> [!example]- Transactional → Columnar
> Native JSON support varies significantly:
> - **BigQuery**: `JSON` type (schema-on-read, recommended) or `STRUCT`/`REPEATED` (typed, schema-on-write). Use `JSON` for mutating data, `STRUCT` only when the schema is genuinely stable and you need the query performance. Landing as `STRING` is always safe.
> - **Snowflake**: `VARIANT` is schema-on-read and handles arbitrary JSON natively. The natural choice -- it's flexible, queryable, and doesn't break on schema changes.
> - **ClickHouse**: `JSON` type (experimental in recent versions) or `String`. ClickHouse's JSON support is less mature -- `String` with `JSONExtract*` functions is the safe choice.
> - **Redshift**: `SUPER` type accepts semi-structured data. Queryable with `PartiQL` syntax.

> [!example]- Transactional → Transactional
> Usually straightforward:
> - **PostgreSQL → PostgreSQL**: `JSONB` → `JSONB`. Native, queryable, indexed with GIN indexes. Zero conversion needed.
> - **MySQL → MySQL / PostgreSQL**: MySQL `JSON` → PostgreSQL `JSONB`. Both accept arbitrary JSON. The query syntax differs (`->` vs `->>` semantics) but the data transfers as-is.
> - Both engines accept arbitrary JSON without schema definition, so schema mutation within the JSON is never a load problem.

---

## Related Patterns

- [[01-foundations-and-archetypes/0102-what-is-conforming|0102]] -- the line between conforming and transforming applies directly here
- [[04-load-strategies/0403-merge-upsert|0403]] -- schema evolution and how JSON columns interact with MERGE
- [[04-load-strategies/0401-full-replace|0401]] -- full replace as a clean way to handle STRUCT schema changes
- [[07-serving-the-destination/0703-pre-built-views|0703]] -- flattening views for consumers who can't query JSON
