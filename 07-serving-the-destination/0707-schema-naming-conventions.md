---
title: "Schema Naming Conventions"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-30
updated: 2026-03-31
---

# Schema Naming Conventions

> **One-liner:** Table and column naming at the destination: as-is from source, snake_case, normalized? Pick a convention and apply it consistently -- changing it later is a full migration.

## The Problem

Source systems name things however they want: `OrderID`, `@ORDER_VIEW`, `invoice_line`, `OACT`, `Column Name With Spaces`. The destination needs identifiers that are consistent and queryable without quoting gymnastics. A column called `order` clashes with a reserved word on every engine, `@Status` collides with SQL Server's variable syntax, and `Column Name With Spaces` demands quotes everywhere it appears.

This is a one-time decision with permanent consequences -- changing a naming convention on a running pipeline means rebuilding every table and rewriting every downstream query, view, and dashboard that touches those names. Identifier normalization is a conforming operation: it happens at load time, not downstream. Get it right and consistently applied, and downstream teams will (mostly) follow your lead. Get it wrong, and you'll find `Vw_Sales-backup_FINAL-JSmith_2026-05` in your catalog within the year.

## Naming Schemes

Three schemes see real use, and each behaves differently depending on the destination engine.

### Preserve source names

Land `OrderID` as `OrderID`, `OACT` as `OACT`, `invoice_line` as `invoice_line`. Anyone looking at the destination can trace a column straight back to the source without a mapping table.

The real argument for this approach comes from upstream teams. They send you queries written against their system and expect them to run against yours -- it's one of the most common requests you'll get. When the destination preserves `OrderId`, adapting a source query is mechanical: quote the identifiers, adjust the `FROM` clause, done. When the destination has normalized to `order_id`, every column in a 30-column query needs translating back, and someone will get one wrong. If your consumers regularly cross-reference with the source system, preserving names saves everyone time.

The cost is that five sources produce five different conventions in the same destination. `OrderID` sits next to `order_id` sits next to `ORDER_STATUS`, and every consumer has to know which source uses which style.

| Destination | What happens |
|---|---|
| BigQuery | Case-sensitive -- names land exactly as provided, preserve works cleanly |
| Snowflake | Folds to uppercase by default. `OrderID` quietly becomes `ORDERID` unless you double-quote at create time *and* in every query |
| PostgreSQL | Folds to lowercase by default. `OrderID` becomes `orderid` unless quoted |
| ClickHouse | Case-sensitive -- names preserved exactly |
| SQL Server | Case-insensitive (collation-dependent). `OrderID` and `orderid` resolve to the same column; the original casing is stored but not enforced |

> [!warning] Snowflake and PostgreSQL silently destroy mixed-case names
> Committing to preserve-source-names on either engine means double-quoting every identifier in every DDL and every query. Most teams that start here end up quoting nothing and losing the casing by default -- arriving at lowercase-only by accident rather than by choice.

### Normalize to snake_case

`OrderID` becomes `order_id`, `Column Name With Spaces` becomes `column_name_with_spaces`, and `invoice_line` stays put. This is the standard analytics warehouse convention -- consistent, quoting-free, and what analysts expect when they write SQL by hand.

| Destination | What happens |
|---|---|
| BigQuery | The ecosystem convention -- BigQuery's own `INFORMATION_SCHEMA` uses snake_case. Store original source names in column descriptions for traceability |
| Snowflake | Lands as `ORDER_ID` due to the uppercase fold, but `order_id` and `ORDER_ID` resolve identically so it reads fine |
| PostgreSQL | The native convention. System catalogs use it, `psql` tab-completion expects it |
| ClickHouse | Works, though ClickHouse's own system tables mix camelCase (`query_id` alongside `formatDateTime`). No strong ecosystem standard |
| SQL Server | Technically fine, but the SQL Server world expects PascalCase (`OrderId`, `CustomerName`). Landing snake_case puts your ECL tables at odds with every system table and most existing schemas. Right call if consumers write ad-hoc SQL; friction if they're .NET applications expecting `dbo.Orders.OrderId` |

The cost is irreversibility. Once `OrderID` becomes `order_id`, the original casing is gone -- and if two source columns normalize to the same string (`OrderID` and `Order_ID` both become `order_id`), you have a collision to detect and resolve at load time.

For most pipelines, snake_case is still the better default -- it reads clean, requires no quoting on case-insensitive engines, and it's what analysts expect to find. We use it across the board and it's never been the wrong call. But we've also worked with clients whose upstream teams live in the source system and send us queries daily, and for those cases preserve-source-names would have saved us hours of translation work every week.

### Lowercase only

`OrderID` becomes `orderid`, `Column Name With Spaces` becomes `columnamewithspaces`. Fold to lowercase, strip illegal characters, done.

Many teams are already doing this without realizing it -- PostgreSQL and Snowflake both fold unquoted identifiers automatically. Single-word names survive fine (`orderid` is readable enough), but multi-word names lose all structure. Try parsing `inventorymovementlogs` or `abortedsessioncount` at a glance.

Lowercase-only is viable when the source uses short, single-word identifiers (common in legacy ERPs: `OACT`, `BUKRS`, `WAERS`). For anything with compound names, snake_case is worth the extra transformation.

## Handling Special Characters and Reserved Words

No naming scheme saves you from these -- they need explicit handling regardless of which convention you pick.

**Reserved words** like `order`, `select`, `from`, `table`, and `group` break unquoted queries on every engine, and snake_case doesn't help because `order` stays `order`. Prefix with the source context (`source_order`), suffix with an underscore (`order_`), or accept that the column will always need quoting.

**Syntactic characters** -- `@Status`, `#Temp`, `$Amount` -- carry engine-specific meaning. SQL Server interprets `@` as a variable prefix and `#` as a temp table marker, so a column named `@Status` requires quoting there even though PostgreSQL handles it fine. Strip or replace any character that has syntactic meaning on your destination.

**Spaces** require quoting on every engine without exception. Replace with underscores -- the one normalization everyone agrees on.

**Accented characters** like `línea_factura` or `straße` are valid UTF-8 and every modern engine supports them, but BI tools and older ODBC connectors can choke. Replace accents at load time (`línea` → `linea`, `straße` → `strasse`) -- the readability cost is negligible, and you avoid discovering the incompatibility at the worst possible moment.

**Collisions after normalization** happen when a case-sensitive source has columns like `OrderID` and `orderid` that collapse to the same string after any normalization. Detect these at load time and fail loudly -- a silent overwrite is worse than a broken load. Resolve by suffixing (`orderid`, `orderid_1`) and document the original-to-normalized mapping in column descriptions or a schema contract ([[06-operating-the-pipeline/0609-data-contracts|0609]]). Ugly, but it preserves every source column.

## Schema Naming

Column naming decides what identifiers look like. Schema naming decides where tables live -- which schema (PostgreSQL, Snowflake, SQL Server) or dataset (BigQuery) holds each source's data, and how consumers know where to look.

### Connection as schema

One destination schema per source connection, named to encode both the server and the specific database. `production` as a schema name is useless when you pull from three production databases on two servers. `erp_prod_finance` tells you the server and the database in one glance.

When a single connection exposes multiple schemas -- SQL Server defaults to `dbo`, PostgreSQL to `public`, MySQL has no schema layer at all -- flatten them into the destination with a double underscore separator:

```
connection__schema.table
```

Single underscores already appear inside names (`order_lines`), so `__` is an unambiguous boundary:

| Source | Source table | Destination table |
|---|---|---|
| PostgreSQL `erp_prod`, schema `public` | `public.orders` | `erp_prod__public.orders` |
| PostgreSQL `erp_prod`, schema `accounting` | `accounting.invoices` | `erp_prod__accounting.invoices` |
| SQL Server `crm_main`, schema `dbo` | `dbo.customers` | `crm_main__dbo.customers` |
| SQL Server `crm_main`, schema `sales` | `sales.leads` | `crm_main__sales.leads` |
| MySQL `shopify_prod` (database = schema) | `orders` | `shopify_prod.orders` |
| SAP B1 `sap_prod`, schema `dbo` | `dbo.OACT` | `sap_prod__dbo.OACT` |

For MySQL and other engines where database and schema are the same thing, `connection__schema` collapses naturally -- `shopify_prod.orders` instead of `shopify_prod__shopify_prod.orders`. But when a connection has a single schema that isn't the only one it *could* have (SQL Server with just `dbo`, PostgreSQL with just `public`), keep the full `connection__schema` form anyway. `erp_prod.orders` reads cleaner than `erp_prod__public.orders` today, but the moment that server gets a second schema you're facing a rename across every table and every downstream reference. Use `connection__schema` from day one and the second schema slots in without touching anything that already exists.

You can extend the prefix with a business domain (`finance__erp_prod__accounting.invoices`) to group related schemas alphabetically, but this extra nesting is rarely worth it until your schema list outgrows a single screen.

### Layer prefixes

Prefixing schemas with `raw_`, `bronze_`, or `landing_` marks the data layer: `raw__erp_prod.orders` versus `curated__erp_prod.orders`. The benefit is alphabetic grouping in catalog UIs and `INFORMATION_SCHEMA` queries -- all raw schemas cluster together, all curated schemas cluster together. Apply layer prefixes consistently across every schema or not at all; a mix of prefixed and bare names is worse than no prefixes.

### Opaque sources and layered schemas

Systems like SAP name every table with codes that mean nothing outside the source -- `OACT`, `OINV`, `INV1`. The temptation to rename `OACT` to `chart_of_accounts` at load time is strong, especially when your analysts keep asking "what's OACT?", but that rename is a semantic transformation that crosses the conforming boundary. Land the source name, use table metadata (column descriptions, table comments) to explain what it means, and let consumers discover the mapping without a separate lookup table.

We run a SAP B1 deployment where we landed everything raw at first -- one schema, hundreds of opaque tables. It worked until it didn't scale. The approach that survived:

| Schema                     | What lives here                                        | How tables are named                                           |
| -------------------------- | ------------------------------------------------------ | -------------------------------------------------------------- |
| `bronze__sap_b1__schema_1` | Raw landing from SAP schema 1                          | Source codes: `OACT`, `OINV`, `INV1`, `ORDR`, `RDR1`           |
| `bronze__sap_b1__schema_2` | Raw landing from SAP schema 2                          | Source codes                                                   |
| `bronze__sap_b1__schema_3` | Raw landing from SAP schema 3                          | Source codes                                                   |
| `silver__sap_b1`           | All bronze layers consolidated, enriched with metadata | Still source codes: `OACT`, `OINV` -- same table, more columns |
| `gold__sap_b1`             | Business-facing models                                 | Human names: `chart_of_accounts`, `ar_documents`, `balance`    |

Bronze lands what SAP calls it, separated by source schema. Silver consolidates across schemas and enriches with metadata, joins, and deduplication, but the tables keep their opaque names -- `OACT` is still `OACT`, just with more columns. Gold is where `OACT` finally becomes `chart_of_accounts`, because at that layer the consumers are analysts who have never logged into SAP. The semantic rename belongs here, not in the ECL layer.

### Staging conventions

Staging tables need their own namespace to avoid colliding with production. Table prefix (`stg_orders` in the same schema) or parallel schema (`orders_staging.orders`) -- the tradeoffs are covered in [[02-full-replace-patterns/0203-staging-swap|0203]].

## Per-Table Overrides

The convention should be configurable at two levels: a destination-wide default that covers the common case, and per-table overrides for the exceptions. Collisions from character stripping are the most common reason you'll need them.

We learned this the hard way with a client who had `ProductStock` and `ProductStock$` in the same source -- identical structure, one holding unit quantities and the other monetary values. Our stripping rule removed the `$`, both tables landed as `product_stock`, and whichever loaded second silently overwrote the first. We didn't catch it until the numbers stopped making sense downstream. The fix was a per-table override renaming one to `product_stock_value` -- a borderline transformation, but better than losing data. The general rule works until it doesn't, and when it doesn't, the alternative to a per-table escape hatch is rewriting the entire convention.

[[06-operating-the-pipeline/0609-data-contracts|0609]] treats the naming convention as a schema contract -- any change to it, including per-table overrides, is a breaking change that should go through the contract process.

## Migrating a Convention

We've done this once. It was a week of hell -- rebuilding tables, rewriting queries, repointing every report and dashboard that referenced the old names. We thought we were done by Friday. We weren't. For three months afterward, people came back from vacation to broken dashboards, scheduled exports failed silently because nobody had updated the column references, and ad-hoc queries saved in personal notebooks kept surfacing the old names. Every time we thought we'd caught the last one, someone opened a ticket.

Don't do it if you can avoid it. If you can't, treat it as a formal breaking change: announce a cutover window, run a deprecation period where both conventions coexist (old names as views over new tables), and set a hard deadline for tearing down the aliases. And budget three months of intermittent cleanup after the deadline, because you will need them.

## Anti-Patterns

> [!danger] Don't change the convention on a running pipeline
> Changing from camelCase to snake_case across 200 tables means rebuilding every table and updating every downstream query, view, and dashboard. The only thing better than perfect is **standardized**.

> [!danger] Don't rename source tables for readability in the ECL layer
> `OACT` → `chart_of_accounts` is a semantic rename that crosses the conforming boundary. The pipeline lands what the source calls it. If consumers need readable names, build an alias layer downstream.

> [!danger] Don't mix conventions across the same destination
> Tables from source A in snake_case and tables from source B in camelCase within the same dataset confuses every consumer. Avoid when possible.

## Related Patterns

- [[05-conforming-playbook/0506-charset-encoding|0506-charset-encoding]] -- encoding of identifier names, not just data values
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- naming convention as a schema contract
- [[02-full-replace-patterns/0203-staging-swap|0203-staging-swap]] -- staging table naming
- [[01-foundations-and-archetypes/0104-columnar-destinations|0104-columnar-destinations]] -- per-engine identifier behavior
