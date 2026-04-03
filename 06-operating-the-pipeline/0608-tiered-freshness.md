---
title: "Tiered Freshness"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-10
updated: 2026-03-28
---

# Tiered Freshness

> **One-liner:** Not every row needs the same refresh cadence -- partition your pipeline into hot, warm, and cold tiers so the tables that matter most get attention first.

## The Problem

The naive approach is one schedule for everything: all tables, same cadence, same extraction method. It works when you have a dozen tables and a daily overnight window. It stops working when some of those tables need to be fresh within the hour while others haven't changed in months -- because now you're either over-refreshing cold data (wasting compute, money and source load) or under-refreshing hot data (delivering stale results to the consumers).

The subtler version of this problem is not refreshing everything at the same *frequency* but with the same *method*. We had an `orders` table that ran a full replace of the entire year's data many times a day. The frequency was right -- the table needed intraday updates -- but full-replacing twelve months of data every run was not. The DBA noticed before we did. The fix wasn't changing the schedule; it was splitting the table's extraction into tiers: recent data incrementally and often, historical data fully but rarely.

## The Tiers

The model is three zones, each with its own cadence and extraction method. The boundaries between them depend on the table, the source system, and the consumer's SLA -- the names are universal, the numbers are not.

### Hot (Intraday)

Tables or partitions with actively changing data: today's `orders`, open `invoices`, recent `events`. Refreshed multiple times per day via incremental extraction when neccesary ([[03-incremental-patterns/0302-cursor-based-extraction|0302]]). The actual interval depends on the table's volume, source capacity, and consumer SLA -- a 500-row lookup table can refresh every few minutes while a 50M-row fact table might only sustain hourly.

The hot tier tolerates impurity. Slight gaps from late-arriving data or cursor lag aren't catastrophic here because the warm tier catches them on the next pass. This is where you accept a tradeoff: the data is fresh but might not be perfectly pure, and that's fine because purity comes later.

### Warm (Daily)

Current month or current quarter -- data that still receives occasional updates but not at high frequency. Refreshed daily, often overnight when the source is under less load. The extraction method is either a full replace of the warm window ([[02-full-replace-patterns/0205-rolling-window-replace|0205]]) or incremental with a wider lag.

This tier takes advantage of harder business boundaries. A closed month in an ERP is unlikely to change (though "unlikely" is not "impossible" -- see the soft rules in [[0106-hard-rules-soft-rules|0106]]). The warm tier's job is to re-read recent history with enough depth to catch what the hot tier missed: late cursor updates, backdated transactions, documents that changed without updating their `updated_at`. Here purity is a lot more important, and you should expect your destination to be exactly equal to source 99% of the time after loading.

### Cold (Weekly / On-Demand)

Historical data: prior years, closed fiscal periods, archived partitions. Refreshed on a slow cadence -- weekly, monthly, or only on demand for backfills and corrections. Full replace is the right method here because the volume is bounded and the frequency is low enough that the cost is negligible.

The cold tier is where [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]] plays out most directly: cold data trades freshness for purity. A weekly full replace of last year's data resets accumulated drift from the hot and warm tiers -- any row that was missed by a cursor, any late update that arrived outside the warm window, gets picked up here. The cold tier is your cleanup pass.

### The Lag Window

The warm tier's extraction window needs to overlap with the hot tier's territory -- otherwise changes that happen between the last hot run and the warm run's cutoff fall through the gap. This overlap is the lag window: how far back the warm tier reads beyond its own boundary.

The right lag depends on how reliably the source system updates its cursors. For well-organized systems where every modification touches `updated_at`, 7 days of lag is enough -- especially when the cold tier runs weekly and catches anything the warm tier missed. For messier systems where documents get modified without updating any cursor (common in ERPs where back-office edits bypass the application layer), 30 days is safer. The decision is empirical: start at 7, watch for rows that appear in the cold tier's full replace but were never picked up by warm, and widen the window if it happens regularly.

The same logic applies between cold and warm. The cold tier's full replace naturally covers everything, so it doesn't need a lag window -- it reads the entire historical range. That's what makes it the safety net.

## Assigning Tables to Tiers

| Signal | Tier |
|---|---|
| Has active writes in the last hour | Hot |
| Has writes in the last 7 days but not the last hour | Warm |
| No writes in > 7 days | Cold |
| Append-only, partitioned by date | Hot for today's partition, cold for everything else |
| Open documents (`invoices` with status = draft) | Hot regardless of write frequency |

Tier assignment can be static (configured per table in your orchestrator) or dynamic (based on recent activity signal from [[02-full-replace-patterns/0207-activity-driven-extraction|0207]]). Static is simpler and covers most cases -- you know which tables are transactional and which are archival. Dynamic earns its complexity when you have hundreds of tables and can't manually classify each one, or when the same table's activity profile shifts seasonally.

Most pipelines don't need all three tiers from day one. About two-thirds of tables in a typical pipeline are lookups and dimensions that full-replace daily and never need anything faster. Incrementalizing everything you can is tempting but generates more errors than it saves time -- or money. The simpler approach is to maximize full replace and reserve incremental for the cases that actually demand it. The tier system matters most for the remaining third.

Being in the hot tier doesn't automatically mean incremental. A `products` table with 10k rows that needs intraday freshness can full-replace every run without anyone noticing -- the volume is trivial, the extraction takes seconds, and you avoid maintaining cursor state entirely. The same applies to tables on a low-enough frequency: if you're only refreshing twice a day, a full replace of even a moderately large table might be cheaper than the complexity of tracking what changed. Incremental earns its place when the table is too large to full-replace at the cadence you need -- `events` growing by millions of rows per day, `orders` with years of history. For everything else, full replace at whatever frequency the consumer requires is simpler, purer, and usually fast enough.

## Month-End and Seasonal Shifts

ERP systems behave differently at month-end and period close. Whether that affects your tiered schedule depends on who consumes the data and why.

If the extracted data drives quick decision-making -- collections teams chasing receivables before month-end, sales managers tracking targets -- consumers will ask for *more* frequency. Promoting tables to the hot tier during the last week of the month gives them fresher data when the stakes are highest.

If the extracted data feeds a historical analysis engine -- a data warehouse that produces reports after the period closes -- consumers will often ask for the *opposite*: reduce extraction frequency during month-end to avoid competing with the ERP's own close process for database resources. The source system is already under pressure from period-end batch jobs, and your pipeline hammering it with intraday reads doesn't help anyone.

For pipelines that run overnight only, month-end rarely changes the schedule. The overnight window already avoids the daytime contention, and the warm tier's daily refresh picks up whatever happened during the close.

## Schedule Configuration

Each tier maps to a separate schedule or schedule group in your orchestrator:

- **Hot**: frequent cron, interval driven by table volume and source tolerance
- **Warm**: daily cron, typically overnight
- **Cold**: weekly or monthly cron, or triggered manually for backfills

A table can move between tiers as business cycles shift. Month-end promotes some tables to hot; fiscal year rollover pushes last year's data from warm to cold; seasonal patterns (Black Friday, harvest season, enrollment periods) can temporarily increase the hot tier's population. If your orchestrator supports dynamic schedule assignment, encode these transitions as rules rather than manual changes.

> [!danger] Don't mix tiers on the same cron
> This anti-pattern applies when you have tables at different cadences. If some tables need intraday freshness but share a cron with everything else, the hot tables wait in line behind cold tables that didn't need refreshing. Separate the schedules when you have tables that genuinely need different cadences.

> [!danger] Same frequency, wrong method
> Refreshing a table many times a day is fine. Full-replacing a year's worth of data many times a day is not. If a table needs intraday freshness, the hot tier should extract only the recent window incrementally -- not reload the entire history on every run. The frequency is a schedule concern; the method is a pattern concern. Getting one right and the other wrong is how you end up on the phone with the DBA.

## Tradeoffs

| Pro | Con |
|---|---|
| Hot data gets to consumers faster without over-refreshing cold data | Three schedules to configure and monitor instead of one |
| Cold-tier full replace acts as a purity checkpoint, resetting drift | Lag window tuning is empirical -- too short misses rows, too long wastes reads |
| Tables can shift tiers as business needs change | Dynamic tier assignment adds orchestrator complexity |
| Cost scales with actual freshness needs, not with table count | Month-end and seasonal shifts require manual or rule-based tier promotions |

## Related Patterns

- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- SLA tiers define freshness requirements; tiered freshness implements them
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606-scheduling-and-dependencies]] -- schedule structure and safe hours
- [[06-operating-the-pipeline/0603-cost-monitoring|0603-cost-monitoring]] -- tiered freshness is partly a cost optimization strategy
- [[02-full-replace-patterns/0205-rolling-window-replace|0205-rolling-window-replace]] -- the warm tier often uses rolling window
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the hot tier uses cursor-based incremental
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]] -- the fundamental tradeoff that tiered freshness navigates
