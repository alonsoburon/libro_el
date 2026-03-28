---
title: "Scheduling and Dependencies"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-24
---

# Scheduling and Dependencies

> **One-liner:** Most tables are independent. For the ones that aren't, group them so they update together -- but don't enforce strict ordering unless you have a real reason.

## The Problem

With a handful of tables, scheduling is simple: run everything on a cron, wait for it to finish, done. At hundreds or thousands of tables, three questions dominate your scheduling decisions -- how often each table should update, how many extractions your source and infrastructure can handle at once, and which tables need to land in the same window. Get any of these wrong and the consequences are immediate: SLA breaches because heavy tables crowd out critical ones, angry DBAs because you're hammering their production system during business hours, or a pipeline that takes six hours because someone chained 200 independent tables into a single sequence years ago and nobody questioned it.

## The Pattern

### How Often: Schedule Frequency

Every table needs a schedule, and the schedule should reflect how the table is consumed, not how often the source changes. A `customers` table that changes ten times a day but feeds a weekly report doesn't need hourly extraction -- once a day is fine. An `orders` table that feeds a real-time dashboard needs to update as frequently as your source and infrastructure can sustain. Watch for schedule pile-ups as tables grow: an extraction that used to finish in 10 minutes may creep to 40 and start overlapping with the next scheduled run, silently turning two clean windows into one messy one.

[[06-operating-the-pipeline/0608-tiered-freshness|0608]] covers the framework for assigning freshness tiers. The scheduling implication is straightforward: group tables by the freshness their consumers need, not by their source system or their size.

Most teams evolve through a predictable sequence:

1. **Single cron** -- everything runs together. Simple but slow, works when you have few tables and falls apart when it takes longer than ~4 hrs or you need to update within the day.
2. **Weight-based groups** -- tables split by size or duration, distributed across time slots. Better throughput, but the groupings don't map to anything the business cares about.
3. **Consumer-driven groups** -- tables grouped by the downstream report or dashboard that consumes them, scheduled to meet that consumer's freshness target. If the sales report goes live at 8 AM, its tables update at 6:30. If the warehouse team doesn't check inventory until noon, those tables can run later and spread the source load across a wider window.

The third stage is where you want to end up, but each stage is the right answer at a certain scale -- don't over-engineer a consumer-driven architecture when you have three dashboards.

> [!tip] Group by downstream consumer, not by source
> Early designs tend to group tables by source connection -- "all SAP tables run at midnight." That works until the finance team needs invoice data at 7 AM while the warehouse team doesn't check inventory until noon. Grouping by consumer lets you schedule tighter windows for the tables that matter most and spread the rest across off-peak hours.

### How Many: Concurrency and Source Load

Every concurrent extraction consumes RAM and CPU on your pipeline infrastructure *and* an open connection plus query load on the source. Getting the concurrency level wrong hurts in both directions: too few concurrent extractions and your pipeline takes hours longer than it should, too many and you overload the source system or exhaust your own memory.

Start conservative -- 3 to 5 concurrent extractions per source for a typical transactional database. The beefiest production setups might run up to 8 tables concurrently against a strong source, but mostly during off-peak hours when the source has headroom. Monitor source response times and pipeline memory, and increase the limit only when you have evidence that both sides can handle it.

The mechanism is your orchestrator's concurrency controls -- run queues, tag-based limits, or pool-based workers. The limit itself comes from knowing your environment: what the source can tolerate and what your infrastructure can sustain.

> [!tip] Per-source, not per-pipeline
> Concurrency limits should be set per source system, not per schedule. If three schedules each run 5 extractions against the same database, that's 15 concurrent queries -- the source doesn't care that they came from different schedules.
> However, always keep in mind your orchestrator's limit as a general maximum of available operations.

> [!warning] Lock contention on SQL Server
> Some databases handle concurrent reads worse than others. SQL Server in particular can lock tables during long reads, blocking the source application's writes. The usual workaround is `WITH (NOLOCK)`, which avoids locks but introduces dirty reads -- rows mid-transaction, partially updated, or about to be rolled back. I've seen dirty reads lead to erroneous business decisions when an in-flight transaction appeared as committed data in the destination. Schedule heavy SQL Server extractions for off-peak hours rather than reaching for `NOLOCK`, and if you must use it, document the risk so downstream consumers know what they're looking at.
> Please de-duplicate on source, since batched loads with `NOLOCK` can repeat records even when having enforced primary keys.

### When: Safe Hours

Large extractions during business hours can slow down or even lock the source system (see [[06-operating-the-pipeline/0607-source-system-etiquette|0607]]). Gate heavy extractions behind a safe-hours window -- typically off-peak, like 19:00 to 06:00 -- with a row-count or size threshold that determines which tables qualify as "heavy." Tables below the threshold run during business hours on their normal schedule; tables above it get deferred to the safe window automatically.

A threshold around 100,000 rows is a reasonable starting point, set proactively before an incident forces the decision. The exact number depends on the source -- a well-provisioned cloud database tolerates larger reads during business hours than an on-prem ERP running on aging hardware.

> [!warning] Safe hours are per-source, not per-pipeline
> If three pipelines each respect their own safe-hours window against the same source, they might all stack into the same off-peak slot. Coordinate safe hours at the source level: one window, one concurrency limit, shared across every pipeline that touches that source.

### Which Together: Grouping Related Tables

Most tables in a pipeline are independent -- `customers` and `events` share no relationship that affects extraction, and there's no reason they need to land at the same time. The few that *are* related -- header-detail pairs like `orders`/`order_lines` or `invoices`/`invoice_lines` -- should land in the same schedule window so the destination doesn't show today's headers with yesterday's lines.

Within that window, arrival order shouldn't matter. Make sure no table depends on the other's data being present at load time so that joins work regardless of which side finished first. What matters is that both sides reflect roughly the same point in time, which co-scheduling achieves naturally without any dependency graph.

Lookup tables like `customers` and `products` ideally land before `orders` so a consumer querying right after the load sees consistent references, but if `products` is 30 minutes stale while `orders` is fresh, the join still works -- the data is slightly behind, not broken. Express this as a preferred ordering in your orchestrator if it supports it, but don't block `orders` on `products` completing unless you want slower loads.

The only time you need strict ordering is when one extraction's *input* depends on another extraction's *output* -- which is uncommon in ECL because each table is extracted independently from the source. If you do have this case, express it as a real dependency in the orchestrator's DAG, but confirm you actually need it before building the graph.

### DAG vs. Schedule Groups

For the vast majority of table relationships, co-scheduling is enough: put related tables on the same cron, let them run concurrently within the window, done. No dependency graph, no ordering logic.

Reserve DAG-based dependencies for actual extraction-feeds-extraction cases or for coordinating with downstream transformations that must wait for a group of tables to complete. Building a 200-node extraction DAG when 190 of those nodes are independent is complexity that buys nothing -- and a fragile DAG where one table's failure cascades into blocking dozens of unrelated tables is worse than no DAG at all.

If your orchestrator can't group tables into a single schedule that runs them concurrently, that's a serious limitation -- grouping related tables for parallel extraction within a window is a basic scheduling requirement, and working around it with cron offsets (`orders` at 6:00, `order_lines` at 6:15) is fragile enough that it should push you toward a better orchestrator rather than deeper into workarounds.

## Tradeoffs

| Pro                                                                  | Con                                                                                           |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Schedule groups keep related tables coherent without strict ordering | Consumers may briefly see one side of a relationship fresher than the other within the window |
| Consumer-driven grouping aligns freshness with business needs        | Tables needed by multiple consumers may run on multiple schedules, increasing source load     |
| Conservative concurrency limits protect the source                   | Lower concurrency means longer total pipeline duration                                        |
| Safe-hours gating prevents source impact during business hours       | Heavy tables only update during the off-peak window, which may not meet freshness SLAs        |

## Anti-Patterns

> [!danger] Don't serialize everything "just to be safe"
> Running 200 tables sequentially because "it's simpler" turns a 30-minute pipeline into a 6-hour one. Most tables are independent and can run concurrently within your concurrency limits -- group the few that are related, and only add explicit dependencies when one extraction actually needs another's output.

> [!danger] Don't model FK relationships as extraction dependencies
> Source tables have foreign keys; that doesn't mean your extraction needs to respect their ordering. The destination's landing layer doesn't enforce FKs, and joins work regardless of which side arrived first. Treating every FK as a hard dependency turns simple co-scheduling into a fragile DAG that blocks unrelated tables on each other.

> [!danger] Don't use sleep/offset as a dependency mechanism
> "Wait 10 minutes for orders to finish" is a guess that breaks the first time extraction duration changes. Use schedule groups or the orchestrator's native dependency graph.

> [!danger] Don't assume your concurrency limit is the source's concurrency limit
> Your orchestrator might allow 20 parallel tasks, but the on-prem database you're extracting from might buckle under 8. The constraint is always the weakest link -- your infrastructure *or* the source, whichever gives first. Test against the actual source before increasing limits.

## Related Patterns

- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- schedule design directly affects whether SLAs are achievable
- [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]] -- concurrency limits and safe hours protect the source
- [[06-operating-the-pipeline/0608-tiered-freshness|0608-tiered-freshness]] -- different freshness tiers drive different schedule groups
- [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] -- header-detail extraction strategy when the detail table has no cursor

## What Comes Next

[[06-operating-the-pipeline/0607-source-system-etiquette|0607]] covers the source side of the equation in depth -- what your pipeline does to the database it reads from, and how to keep your access when extracting thousands of tables on a schedule.
