---
title: "Don't Pre-Aggregate"
aliases: []
tags:
  - pattern/serving
  - chapter/part-7
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Don't Pre-Aggregate

> **One-liner:** Land the movements, build the photo downstream. Resist the pressure to transform at extraction.

## The Problem

The first request from every non-technical consumer sounds the same: "how much did we sell?" They want a total. They want it per month, per product, per warehouse. The temptation is to build that aggregation into the extraction -- `SELECT product_id, SUM(quantity) FROM order_lines GROUP BY product_id` -- and hand them exactly what they asked for. Clean, simple, one table with the numbers they need.

Then they ask "which orders drove the spike in product X?" And the detail isn't in your warehouse, because you extracted the SUM and threw away the rows. You can't drill down from a total to its components. You can't recompute the aggregation with a different grouping. You can't debug a number that looks wrong because the individual records that produced it were never loaded. Every client who starts with "just give me the totals" eventually asks for the detail -- and if you aggregated at extraction, the only way to answer is to rebuild the pipeline.

This pattern is about protecting the destination from that moment. Land the detail. Build the totals downstream, in views that consumers can query, that you can change when the business logic changes, and that don't require a pipeline rebuild when someone asks a question you didn't anticipate.

## Where the Boundary Is

The line between conforming and transformation runs through aggregation. Landing `inventory_movements` as-is is conforming -- the source has those rows and you're cloning them. Building `inventory_current` by summing movements is transformation -- you're computing a derived state that encodes business logic: which movement types to include, how to handle negative quantities, whether to count pending transfers.

The same applies to derived columns. `revenue = quantity * unit_price` looks harmless in the extraction query, but it's a business calculation. The moment discounts apply, taxes enter the formula, or currency conversion becomes relevant, that column is wrong in every historical row and the only fix is a full backfill. Land `quantity` and `unit_price` as separate columns and let downstream compute whatever formula the business currently uses.

The distinction matters because aggregation and derivation encode decisions that belong to the people who understand the business context -- and those decisions change. A grouping that makes sense today ("revenue by product category") stops making sense when the category taxonomy changes. A formula that's correct today is wrong next quarter when the pricing model shifts. If the pipeline made those decisions at extraction, every change requires a pipeline change. If a downstream view made them, the view changes and the pipeline keeps running untouched.

See [[01-foundations-and-archetypes/0102-what-is-conforming|0102]] for the full framework.

## The Exception: `metrics_daily`

Some source tables are already pre-aggregated. `metrics_daily` in the domain model is computed by the source system -- the aggregation decision was made upstream, not by your pipeline. Landing a pre-aggregated table as-is is conforming because you're cloning what the source has, aggregation included. The rule isn't "never land aggregates" -- it's "don't aggregate in the pipeline."

## Movements vs. Photos

Two kinds of data, two different representations of the same reality:

**Movements** are append-only event records: `inventory_movements` (stock received, sold, adjusted), `order_lines` (items ordered), `events` (clickstream, transactions). Each row is something that happened. The history is in the rows themselves.

**Photos** are point-in-time snapshots: `inventory` (current stock levels), `metrics_daily` (today's aggregated numbers). Each row is the state of something right now. The history is gone the moment the next snapshot overwrites it.

Land both when both exist at the source. The `inventory` table and the `inventory_movements` table are different data -- the photo and the movements don't always agree (bulk imports that update `inventory` without logging a movement, the soft rule from [[00-front-matter/0002-domain-model|0002]]), and it's your job to make that discrepancy visible to consumers rather than hiding it by building one from the other.

Downstream can reconstruct photos from movements if they want to (see [[07-serving-the-destination/0706-point-in-time-from-events|0706]]) -- stock as of any date is a `SUM(quantity) WHERE created_at <= target_date`. The inverse isn't possible: you can't recover individual movements from a snapshot total. Detail produces aggregates; aggregates don't produce detail.

## The Conversation

The request always starts simple. "How much did we sell last month?" You land `order_lines`, build a view that sums `quantity * unit_price` grouped by month, and the consumer is happy. Then:

"Can I see that by product?" -- change the GROUP BY in the view. No pipeline change.

"Can I see which orders had returns?" -- filter on `quantity < 0` in the detail. Only possible because the detail is there.

"Can I see the monthly trend for this specific SKU?" -- filter the view by SKU. Still no pipeline change.

"Wait, these numbers don't match the ERP's report" -- compare your `order_lines` detail against the source, row by row. Only possible because you have the rows, not just the total.

Every one of these follow-up questions is answerable because the detail was landed. None of them would be answerable if the pipeline had extracted `SUM(quantity * unit_price) GROUP BY month`. The consumer who said "I just need the monthly total" needed the detail all along -- they just didn't know it yet.

## What Consumers Actually Need

A pre-built view ([[07-serving-the-destination/0703-pre-built-views|0703]]) that aggregates the raw data for their specific use case. The view is downstream, documented, and changeable without touching the pipeline. Different consumers can have different aggregations over the same raw data: the sales team sees revenue by product, the finance team sees revenue by cost center, the warehouse team sees units shipped by location -- all from the same `order_lines` table, each through their own view.

When the business logic changes -- a new product category, a different grouping, a revised pricing formula -- the view changes. The pipeline doesn't.

## Anti-Patterns

> [!danger] Don't extract SUMs instead of rows
> `SELECT product_id, SUM(quantity) FROM order_lines GROUP BY product_id` as your extraction query means the per-line detail never reaches the destination. The total looks correct until someone needs to drill down, and then there's nothing to drill into.

> [!danger] Don't compute derived columns at extraction
> `revenue = quantity * unit_price` in the extraction query is a business calculation baked into the pipeline. When the formula changes -- and it will -- every historical row is wrong and the only fix is a full backfill of the entire table. Land the raw columns, compute downstream.

## Related Patterns

- [[01-foundations-and-archetypes/0102-what-is-conforming|0102-what-is-conforming]] -- the conforming boundary that this pattern defends
- [[07-serving-the-destination/0703-pre-built-views|0703-pre-built-views]] -- the right place for consumer-facing aggregations
- [[07-serving-the-destination/0706-point-in-time-from-events|0706-point-in-time-from-events]] -- reconstructing state from movements downstream
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- the load pattern that preserves every version for downstream use
