---
title: "Tiered Freshness"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-10
updated: 2026-03-15
---

# Tiered Freshness

> **One-liner:** Cold/warm/hot zones -- weekly full for history, daily for current, intraday incremental for freshness.

## The Problem
- Refreshing every table at the same cadence wastes compute on data that doesn't change and starves tables that need to be fresh
- A `events` table with today's data needs a 15-minute refresh; the same table's 2024 partitions haven't changed in months and don't need refreshing at all
- A single schedule for everything either over-refreshes the cold data (expensive) or under-refreshes the hot data (stale)

## The Tiers

### Hot (Intraday)
- Tables or partitions with actively changing data -- today's `orders`, open `invoices`, recent `events`
- Refresh every 15-60 minutes via incremental extraction (see [[03-incremental-patterns/0302-cursor-based-extraction|0302]])
- Smallest extraction window, lowest volume per run, highest frequency

### Warm (Daily)
- Current month or current quarter -- data that still receives occasional updates but not at high frequency
- Refresh once or twice daily, often overnight
- Full replace of the warm window (see [[02-full-replace-patterns/0206-rolling-window-replace|0206]]) or incremental with a wider lag

### Cold (Weekly / On-Demand)
- Historical data -- prior years, closed fiscal periods, archived partitions
- Refresh weekly as a checkpoint, or only on demand (backfill, correction)
- Full replace is fine here -- the volume is bounded and the frequency is low
- This is where [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]] plays out: cold data trades freshness for purity

## Assigning Tables to Tiers

| Signal | Tier |
|---|---|
| Has active writes in the last hour | Hot |
| Has writes in the last 7 days but not the last hour | Warm |
| No writes in > 7 days | Cold |
| Append-only, partitioned by date | Hot for today's partition, cold for everything else |
| Open documents (`invoices` with status = draft) | Hot regardless of write frequency |

- Tier assignment can be static (configured per table) or dynamic (based on recent activity signal from [[02-full-replace-patterns/0208-activity-driven-extraction|0208]])

## Schedule Configuration

- Each tier maps to a separate schedule or schedule group in your orchestrator
- Hot: cron every 15-60 minutes
- Warm: cron 1-2x daily
- Cold: cron weekly, or triggered manually for backfills
- A table can move between tiers -- month-end closes, fiscal year rolls, seasonal patterns

## Interaction with Other Patterns

- Tiered freshness is the scheduling implementation of the SLA tiers defined in [[06-operating-the-pipeline/0604-sla-management|0604]]
- Hot-tier extractions must respect source system concurrency limits (see [[06-operating-the-pipeline/0607-source-system-etiquette|0607]])
- Cold-tier full replaces are natural backfill opportunities (see [[06-operating-the-pipeline/0611-backfill-strategies|0611]])

## Anti-Pattern

> [!danger] Don't refresh everything on the same cron
> - One schedule for 500 tables means the hot tables are waiting in line behind cold tables that didn't need refreshing. Separate the schedules; let hot tables run independently and frequently.

## Related Patterns
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- SLA tiers define freshness requirements; tiered freshness implements them
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606-scheduling-and-dependencies]] -- schedule structure and safe hours
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- tiered freshness is partly a cost optimization strategy
- [[02-full-replace-patterns/0206-rolling-window-replace|0206-rolling-window-replace]] -- the warm tier often uses rolling window
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the hot tier uses cursor-based incremental
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]] -- the fundamental tradeoff that tiered freshness navigates

## Notes
- **Author prompt -- incremental_lag_days**: The default is 7 days. How did you arrive at 7? Have any clients needed more or less? What drives the decision?
- **Author prompt -- full vs incremental split**: Across your ~6500 tables, roughly what percentage are full replace vs incremental? Has that ratio changed over time as you've learned what works?
- **Author prompt -- month-end patterns**: ERP systems get heavy at month-end / period close. Do you adjust extraction cadence around these events, or is it the same schedule year-round?
- **Author prompt -- one schedule for all**: Before you had tiered schedules (if you did), were all tables on the same cron? What was the bottleneck that forced the split?
