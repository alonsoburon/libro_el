---
title: "Schema Naming Conventions"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: outline
created: 2026-03-30
updated: 2026-03-30
---

# Schema Naming Conventions

> **One-liner:** Table and column naming at the destination: as-is from source, snake_case, normalized? Pick a convention and apply it consistently -- changing it later is a full migration.

## The Problem
- Source systems name things however they want: `OrderID`, `@ORDER_VIEW`, `invoice_line`, `OACT`, `Column Name With Spaces`
- The destination needs consistent, queryable identifiers -- a column named `order` (reserved word) breaks every unquoted query
- The naming convention is a one-time decision with permanent consequences: changing it later requires rebuilding every table and updating every downstream query
- This is a conforming operation -- identifier normalization happens at load time, not downstream

## The Decision
- **Preserve source names**: land `OrderID` as `OrderID`. Zero ambiguity about the mapping. Mixed conventions across sources make the destination inconsistent and you may face characters you can't replicate.
- **Normalize to snake_case**: `OrderID` → `order_id`, `Column Name With Spaces` → `column_name_with_spaces`. Consistent, queryable without quoting, the convention analysts expect. Irreversible -- once normalized, the original casing is gone
- **Lowercase only**: `OrderID` → `orderid`. Simpler than snake_case but produces unreadable identifiers for multi-word names

## Edge Cases
- **Reserved words**: `order`, `select`, `from`, `table`. Must be quoted or renamed. snake_case doesn't help -- `order` stays `order`
- **Special characters**: `@Status`, `#Temp`. Strip characters that require quoting on the destination
- **Accented characters**: `línea_factura`. Valid UTF-8 but some BI tools choke. Whether to strip accents is a serving decision
- **Collisions after normalization**: `OrderID` and `orderid` could coexist in the source. After normalization, they collide. Detect at load time
- **SAP table names**: `OACT`, `OINV`, `INV1`. Source identifiers, not human-readable. Whether to rename (`OACT` → `chart_of_accounts`) is a serving decision that crosses the conforming boundary -- the pipeline should land the source name, renaming for readability belongs in a view or alias layer

## Table Naming
- Source schema as destination dataset/schema prefix: `sap.OACT`, `erp_prod.orders`
- Separating sources by schema prevents name collisions across sources with identically named tables
- Staging conventions: `stg_` prefix or parallel schema ([[02-full-replace-patterns/0204-staging-swap|0204]])

## Per Engine
- **BigQuery**: case-sensitive identifiers. snake_case is the natural convention. Column descriptions in schema metadata can document original source names
- **Snowflake**: case-insensitive by default (folds to uppercase). `order_id` becomes `ORDER_ID` unless double-quoted. Most teams accept the uppercase fold
- **PostgreSQL**: case-insensitive by default (folds to lowercase). `OrderID` becomes `orderid` unless quoted. snake_case is natural
- **ClickHouse**: case-sensitive. Names are preserved exactly as created

## Configuration
- The convention should be configurable at two levels: per destination (consumers expect consistency within a dataset) and per table (migrating a source sometimes means fixing individual tables)
- [[06-operating-the-pipeline/0609-data-contracts|0609]] treats the naming convention as a schema contract -- changing it is a breaking change

## Anti-Patterns

> [!danger] Don't change the convention on a running pipeline
> Changing from camelCase to snake_case on 200 tables means rebuilding every table and updating every downstream query, view, and dashboard. Choose once.

> [!danger] Don't rename source tables for readability in the ECL layer
> `OACT` → `chart_of_accounts` is a semantic rename that crosses the conforming boundary. The pipeline lands what the source calls it. If consumers need readable names, build an alias layer downstream.

> [!danger] Don't mix conventions across the same destination
> Tables from source A in snake_case and tables from source B in camelCase within the same dataset confuses every consumer.

## Related Patterns

- [[05-conforming-playbook/0506-charset-encoding|0506-charset-encoding]] -- encoding of identifier names, not just data values
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- naming convention as a schema contract
- [[02-full-replace-patterns/0204-staging-swap|0204-staging-swap]] -- staging table naming
- [[01-foundations-and-archetypes/0104-columnar-destinations|0104-columnar-destinations]] -- per-engine identifier behavior

## Notes
- **Author prompt**: What convention do you use? snake_case across the board, or preserve source names?
- **Author prompt**: Have you had to migrate a naming convention? How many tables, how long, what broke?
- **Author prompt**: How do you handle SAP table names? Land as `OACT` and let downstream rename, or rename at load time?
