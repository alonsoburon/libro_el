---
title: "Sparse Table Extraction"
aliases: []
tags:
  - pattern/full-replace
  - chapter/part-2
status: first_iteration
created: 2026-03-06
updated: 2026-03-09
---

# Sparse Table Extraction

> **One-liner:** Cross-product tables where 90%+ of rows are zeros -- filter at extraction to pull only meaningful combinations, but know that "empty" is a business definition, not a data one.

## The Problem

Some tables are the cartesian product of two dimensions. Every SKU against every Warehouse. Every Employee against every Benefit. Every Product against every Location. The source system pre-computes all combinations and fills in zeros where nothing is happening.

The result is a table that's technically large but informationally sparse. A retailer with 50,000 SKUs and 200 warehouses has a 10-million-row inventory table -- and in most businesses, the vast majority of those rows have `OnHand = 0` and `OnOrder = 0`. Extracting all of them is expensive, slow, and loads mostly noise into the destination.

The obvious fix is to filter: `WHERE OnHand <> 0 OR OnOrder <> 0`. Pull only the combinations with actual activity. The destination shrinks dramatically, queries are faster, and the pipeline runs in a fraction of the time.

The risk is that filtering zeros is not neutral. A zero row and a missing row look identical in the destination but mean different things in the source.

## The Filter

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

## Zero vs. Missing

This is the decision that matters. In the destination, a missing row and a filtered-out zero row look the same. Consumers have no way to distinguish them unless you tell them.

| In the source | In the destination (after filter) | What a consumer sees |
|---|---|---|
| `on_hand = 5` | Row present | Active combination |
| `on_hand = 0` | Row absent | ??? |
| No row | Row absent | ??? |

The third column is the problem. If a consumer does `COALESCE(on_hand, 0)` on a JOIN, they get zero for both cases -- which may be exactly right. But if they're counting rows, or checking for row existence, or relying on the destination having the full cartesian product, the filtered data produces wrong results.

If the source table actually contains the full cartesian product of both dimensions, you can reconstruct existence data in the destination by cross-joining the two dimension tables (`skus` and `warehouses`). The sparse table becomes an enrichment on top of a complete baseline, not the source of truth for which combinations exist.

> [!danger] Don't filter silently
> A destination that has filtered rows looks exactly like a destination with missing data. Every consumer who queries it will eventually hit this. Document the filter explicitly -- in the table description, in a metadata table, in a comment on the asset. "This table excludes rows where on_hand = 0 AND on_order = 0" should be impossible to miss.

## When It's Safe

- Consumers only care about active combinations -- reporting on what's in stock, not on what's never been stocked
- The filter matches a real business concept ("active inventory") that consumers already think in terms of
- The dimension tables exist separately and can be used to reconstruct the full combination space if needed

## When It's Not Safe

- Consumers need to distinguish zero stock from never-tracked -- "we have none" vs. "we don't carry this here"
- Downstream aggregations count rows where a zero is a valid data point
- The source uses explicit zeros as a business signal: a zero `on_hand` with a `replenishment_blocked` flag means something different from a row that simply doesn't exist. Filtering removes that signal entirely.

## The Filter Is a Business Decision

`WHERE on_hand <> 0 OR on_order <> 0` sounds technical but it encodes a business definition of "active." Who decided that `on_order = 1` with `on_hand = 0` is worth tracking, but `on_hand = 0` and `on_order = 0` is not? Someone did. Find out if that definition matches what consumers expect.

The filter is a contract. If the business changes the definition -- "now we also want rows where `min_stock > 0`" -- the destination needs a full reload, not an incremental correction. Any row that was filtered out and then became relevant won't be caught by the next run unless you reload.

## Relation to Activity-Driven Extraction

[[02-full-replace-patterns/0207-activity-driven-extraction|0207]] solves a related problem differently. Sparse table extraction still scans the full source table -- it just drops most rows before loading. Activity-driven extraction avoids scanning the sparse table at all: it uses recent transaction history to determine which dimension combinations are worth pulling, then queries only those.

0207 is simpler and works for any sparse table. 0208 is more surgical -- it trades source query complexity for a much smaller extraction scope. If your sparse table is large enough that even the filtered extraction is slow, [[02-full-replace-patterns/0207-activity-driven-extraction|0207]] is the next step.

## Related Patterns

- [[02-full-replace-patterns/0207-activity-driven-extraction|0207-activity-driven-extraction]] -- avoid scanning the sparse table entirely
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- if the table is small enough, the filter isn't worth the complexity
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- formalize the filter as a documented contract
