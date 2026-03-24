---
title: "Scheduling and Dependencies"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Scheduling and Dependencies

> **One-liner:** `order_lines` can't land before `orders` -- orchestration patterns for pipelines with real dependencies.

## The Problem
- Tables have relationships in the source, and those relationships create ordering constraints at load time
- A detail table that lands before its header breaks FK constraints in transactional destinations and produces orphan rows in columnar ones
- Cron schedules don't express dependencies -- "run at 6am" and "run at 6:05am" is a race condition, not a dependency

## Dependency Types

### Hard Dependencies
- Header-detail: `orders` before `order_lines`, `invoices` before `invoice_lines`
- Lookup tables: `customers` and `products` before `orders` (if the destination enforces FKs)
- Must be expressed in the orchestrator's DAG, not approximated with schedule offsets

### Soft Dependencies
- Tables that benefit from running together but don't break if one is late
- `metrics_daily` after `events` -- stale metrics are wrong but not structurally broken
- Express as preferred ordering, not hard blockers

### No Dependency
- Most tables are independent -- `customers` and `events` share no extraction dependency
- Independent tables should run in parallel; serializing them wastes time and delays SLAs (see [[06-operating-the-pipeline/0604-sla-management|0604]])

## Scheduling Strategies

### DAG-Based (Orchestrator-Native)
- Declare dependencies in the orchestrator; let it handle ordering, parallelism, and retry
- The dependency graph is the source of truth for "what runs when"
- Some orchestrators resolve this naturally: declare that `order_lines` depends on `orders`, and execution order follows

### Cron with Offsets (Fragile)
- `orders` at 6:00, `order_lines` at 6:15 -- works until `orders` takes 20 minutes
- No retry coordination: if `orders` fails, `order_lines` runs anyway against stale data
- Acceptable only when the orchestrator has no dependency primitives

### Event-Driven
- `order_lines` triggers when `orders` completes successfully -- sensor or callback
- More reactive than cron, but adds complexity; use when freshness demands it

## Safe Hours and Source Etiquette

- Large extractions during business hours can overload the source (see [[06-operating-the-pipeline/0607-source-system-etiquette|0607]])
- Schedule massive tables (above a configurable row threshold) for off-peak windows only
- Safe hours are per-source, not per-pipeline -- a source shared by three pipelines needs coordinated scheduling

## Parallelism Within a Schedule

- Independent tables within the same schedule should run concurrently
- Limit concurrency per source system to avoid overloading it -- 10 parallel extractions against the same database is a DoS
- Your orchestrator's concurrency controls (run queues, tag-based limits) handle this

## Anti-Pattern

> [!danger] Don't serialize everything "just to be safe"
> - Running 200 tables sequentially because "it's simpler" turns a 30-minute pipeline into a 6-hour pipeline. Only serialize what actually depends on each other.

> [!danger] Don't use sleep/offset as a dependency mechanism
> - "Wait 10 minutes for orders to finish" is not a dependency. Use the orchestrator's native dependency graph.

## Related Patterns
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- dependency chains directly affect SLA achievability
- [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]] -- concurrency limits and safe hours protect the source
- [[06-operating-the-pipeline/0608-tiered-freshness|0608-tiered-freshness]] -- different tiers may have different schedules and dependency chains
- [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] -- header-detail extraction ordering

## Notes
- **Author prompt -- safe hours**: You implemented safe hours (19:00-06:00) with a massive table threshold. What row count threshold did you land on? Did you arrive at it through a blowup, or did you set it proactively?
- **Author prompt -- schedule architecture**: Your schedules are cron-based with selection types (table, dataset, connection, job). Did you ever have a schedule structure that didn't work and had to redesign? What broke?
- **Author prompt -- header-detail ordering**: With SAP tables like OINV/INV1 (invoices/invoice_lines), do you enforce extraction ordering? Or does it not matter because you're loading to BigQuery with no FK enforcement?
- **Author prompt -- concurrency**: How many parallel extractions do you run per client source? Have you ever brought a client's SAP database to its knees with too many concurrent queries?
