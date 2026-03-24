---
title: "SLA Management"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-24
---

# SLA Management

> **One-liner:** "The data must be fresh by 8am" -- how to define, measure, and enforce freshness commitments.

## The Problem

Stakeholders care about one thing: is the data fresh when they need it? Without an explicit SLA, freshness expectations are implicit -- discovered only when violated, usually via an angry email or an angry call from your boss. A pipeline that finishes at 8:15 AM is fine until someone builds a report that refreshes at 8:00 AM, and now you have an SLA you didn't know about.

We had a client who we *told* -- but didn't write down -- that data updated once daily. They built automated collection emails that fired before midday, but most of their customers had already paid by then. The emails were going out with stale receivables data, and the client blamed the pipeline for the embarrassment. The fix wasn't technical -- it was documenting the SLA in the contract so both sides agreed on what "once daily" actually meant: data reflects the previous night's extraction, available by 9 AM, not refreshed throughout the day. **Everything that isn't written down can be reinterpreted against you.** Document the SLA.

## When You'll See This

| Signal                                                  | Example                                                                             |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Stakeholder discovers their own SLA the hard way        | A report scheduled at 8 AM assumes fresh data; nobody told you about the deadline   |
| "The data is wrong" when the data is just stale         | Client sees yesterday's receivables and thinks the pipeline broke                   |
| On-demand requests creep into production expectations   | You gave them an extra midday refresh as a favor; now they depend on it             |
| Source-side delays cascade into your SLA                | The ERP's nightly batch job ran late, your extraction started late, 9 AM data stale |

## Defining an SLA

### What an SLA Contains

An SLA for a data pipeline is four numbers and a signature:

| Component              | Example                                                            |
| ---------------------- | ------------------------------------------------------------------ |
| **Table or group**     | `orders`, `order_lines`, `invoices` -- the receivables group       |
| **Freshness target**   | Data reflects source state as of no more than 24 hours ago         |
| **Deadline**           | Available in the destination by 09:00 UTC-3                        |
| **Measurement point**  | Last successful load timestamp in the health table, not run start  |

The measurement point matters. A run that starts at 7 AM but fails and retries until 8:45 AM doesn't meet a 9 AM SLA -- it barely makes it, and the next slow day it won't. Measure from `MAX(extracted_at) WHERE status = 'SUCCESS'` in the health table ([[06-operating-the-pipeline/0602-health-table|0602]]), not from when the orchestrator kicked off the job.

### SLA Tiers

Not every table deserves the same freshness. `metrics_daily` refreshed once a day has a different SLA than `orders` refreshed every 15 minutes or a balance sheet refreshed monthly. Group tables by consumer urgency, not by source system -- the tables that most often need more than daily are sales data (especially during Black Friday or seasonal peaks), receivables (for end-of-month collection runs), and inventory stock levels (for in-store availability decisions). Everything else is usually fine at daily.

Daily is the best default. It handles the vast majority of use cases, and the contract should say so explicitly: no more than one scheduled update per day, data reflects the previous night's extraction. When you increase frequency for specific tables -- an extra midday refresh for receivables, intraday incremental for sales -- make it clear in writing that the increased cadence is outside the base SLA and can be adjusted at any time. Give consumers `_extracted_at` in their reports ([[05-conforming-playbook/0501-metadata-column-injection|0501]]) so they always know how fresh the data actually is, rather than assuming.

> [!tip] On-demand refreshes can replace high-frequency schedules
> If a consumer needs fresh data once or twice a day at unpredictable times, an on-demand refresh button that triggers the pipeline is often better than scheduling loads every 30 minutes "just in case." One triggered run costs far less than 48 idle runs per day, and the consumer gets exactly-when-needed freshness instead of at-most-30-minutes-stale. On-demand *can* be part of the SLA ("consumer may trigger up to N refreshes per day"), but keep it bounded -- without a cap, a trigger-happy user can spam refreshes and compete with scheduled runs for source connections and orchestrator slots. Document the limit, enforce it with a cooldown or queue, and monitor trigger frequency in the health table.

## Measuring Freshness

Staleness is the gap between now and the last successful load. The health table gives you this with a single query:

```sql
-- destination: bigquery
-- Freshness report: staleness per table against declared SLA thresholds.
WITH last_success AS (
  SELECT
    table_id,
    MAX(extracted_at) AS last_load,
    TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(), MAX(extracted_at), HOUR
    ) AS staleness_hours
  FROM health.runs
  WHERE status = 'SUCCESS'
  GROUP BY table_id
)
SELECT
  ls.table_id,
  ls.last_load,
  ls.staleness_hours,
  sla.freshness_hours,
  CASE
    WHEN ls.staleness_hours > sla.freshness_hours THEN 'BREACH'
    WHEN ls.staleness_hours > sla.freshness_hours * 0.8 THEN 'WARNING'
    ELSE 'OK'
  END AS sla_status
FROM last_success ls
JOIN health.sla_config sla USING (table_id)
ORDER BY sla_status DESC, staleness_hours DESC;
```

The `sla_config` table is a simple lookup: one row per table or table group, with the `freshness_hours` threshold from the SLA. Hard-code it, load it from a config API, or manage it in a spreadsheet -- the mechanism doesn't matter as long as the thresholds are explicit and queryable rather than living in someone's head.

This query is the second item in the morning routine from [[06-operating-the-pipeline/0601-monitoring-observability|0601]]: after checking failures, check which tables are stale beyond their SLA.

## What Erodes SLAs

**Upstream delays** are the most common cause and the hardest to control. ERP systems run their own batch jobs -- posting runs, period closes, nightly aggregation -- and those jobs determine when your source data is ready to extract. The ERP itself is rarely the problem; it's the people operating it. When a client has a technical team that runs ad-hoc processes or overloads the database during the window they designated to you, you're the one who gets blamed for stale data. **Build buffer into the SLA** for exactly this -- if the source is ready by 7 AM on a good day, don't promise 7:30 AM.

**Extraction duration creep** turns a comfortable SLA into a tight one over months. The health table's `extraction_seconds` column ([[06-operating-the-pipeline/0602-health-table|0602]]) catches this trend before it becomes a breach -- a 3-minute extraction that silently creeps to 25 minutes eats into your buffer without anyone noticing until the SLA breaks.
```
Example line graph, X axis is time (last 30 days), Y axis is max staleness (measured as distance from last successful timestamp to SLA)

Have a static line on Y axis representing max tolerated staleness (24 hours) and a line that grows past it.

Something LIKE that, think about a table that updates once daily starting at 8 to end at 9, with SLA at 930. and it exceeds it, maybe Y axis should be different.
```

**Stale joins at consumption** are the subtler freshness problem. `orders` and `order_lines` can extract and load independently -- there's no dependency between them at load time. But if only one of the two refreshes on a given run, consumers joining them will see orphan records: order lines pointing at a non-existent order header, or a refreshed header missing today's new lines. The SLA for header-detail pairs should cover both tables on the same schedule, not because the pipeline requires it, but because the consumer's query does ([[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]]).

**Backfills that steal capacity** from scheduled runs are a less obvious risk. A 6-month backfill running alongside production extractions competes for source connections, orchestrator slots, and destination DML quota ([[06-operating-the-pipeline/0611-backfill-strategies|0611]]).

## SLA Breach Response

| Severity              | Trigger                            | Action                                                                                                                     |
| --------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Warning**           | Staleness > 80% of SLA window     | Increase priority of next scheduled run; investigate if it's a trend                                                       |
| **Breach**            | Staleness > SLA window             | Alert via [[06-operating-the-pipeline/0605-alerting-and-notifications\|0605]], investigate root cause, notify consumers     |
| **Sustained breach**  | Multiple consecutive violations    | Escalate -- the schedule, the pattern, or the SLA itself needs to change                                                   |

A single breach is an incident. Sustained breaches mean the SLA is wrong -- either the pipeline can't deliver what was promised, or the consumer's actual needs have shifted. Renegotiate the SLA rather than patching around it with increasingly fragile workarounds.

## Tradeoffs

| Pro                                                         | Con                                                              |
| ----------------------------------------------------------- | ---------------------------------------------------------------- |
| Explicit SLAs set expectations before they're violated      | Requires upfront agreement with stakeholders                     |
| Staleness query catches breaches before consumers notice    | Only measures load completion, not data correctness              |
| Tiered SLAs avoid over-engineering low-priority tables      | More tiers means more schedules and more monitoring surface      |

## Anti-Patterns

> [!danger] Don't promise SLAs you can't control
> If your pipeline depends on a source system batch job that finishes "sometime between 5 AM and 7 AM," your SLA cannot be 7:30 AM. Build buffer or set the SLA at 9 AM and be honest about it. A missed SLA erodes trust in the pipeline and in you -- a conservative SLA that's always met builds more credibility than an aggressive one that breaks monthly.

> [!danger] Don't confuse desire for freshness with willingness to pay for it
> We had a client who wanted 15-minute maximum delay on their invoicing data. They weren't willing to pay the increased BigQuery bill, and their source had terrible metadata, hard deletes, and no reliable cursor -- making high-frequency extraction expensive to build and expensive to run. After scoping the effort and cost, they realized all they actually needed was one extra on-demand refresh per day. The Head of Sales wanted fresh numbers on his dashboard mid-morning, and a refresh button that triggered the pipeline solved the problem at a fraction of the cost and complexity. Ask what decision the freshness enables before engineering the SLA around it.

## What Comes Next

[[06-operating-the-pipeline/0605-alerting-and-notifications|0605]] covers the mechanics of turning SLA breaches into alerts -- the thresholds defined here are the input, and 0605 decides who gets paged, how, and at what severity.
