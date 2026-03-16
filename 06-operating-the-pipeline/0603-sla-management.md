---
title: "SLA Management"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# SLA Management

> **One-liner:** "The data must be fresh by 8am" -- how to define, measure, and enforce freshness commitments.

## The Problem
- Stakeholders care about one thing: is the data current when they need it?
- Without an explicit SLA, freshness expectations are implicit and discovered only when violated -- usually via an angry email or worse -- an angry call from your boss.
- A pipeline that finishes at 8:15am is fine until someone builds a report that refreshes at 8:00am; now you have an SLA you didn't know about
	- Cover your ass! Everything that's not written down, can be tergiversated.

## Defining an SLA

### What an SLA Contains
- **Table or group of tables** -- which tables are covered
- **Freshness target** -- "data must reflect source state as of no more than N hours ago"
- **Deadline** -- "must be available by HH:MM in timezone X"
- **Measurement point** -- last successful materialization timestamp, not last run start

### SLA Tiers
- Not every table deserves the same freshness; `metrics_daily` refreshed once a day has a different SLA than `orders` refreshed every 15 minutes or a balance sheet every month.
- Group tables and updates by consumer urgency, not by source system (see [[06-operating-the-pipeline/0607-tiered-freshness|0607]])

## Measuring Freshness

- **Staleness** = `NOW() - last_successful_materialization` -- the clock starts when the load completes, not when extraction starts
- Use `_extracted_at` from [[05-conforming-playbook/0501-metadata-column-injection|0501]] as the ground truth for when data was captured
- If your orchestrator tracks materialization timestamps natively -- use that, don't rebuild it

### SQL Example (sketch)
- Query `_extracted_at` across tables to build a freshness report
- Flag anything where staleness exceeds (or is close to exceeding) the declared SLA threshold

## SLA Breach Response

- **Approaching breach** (staleness > 80% of SLA window) -- warning, increase priority of next scheduled run
- **Breach** (staleness > SLA window) -- alert via [[06-operating-the-pipeline/0604-alerting-and-notifications|0604]], investigate root cause
- **Sustained breach** (multiple consecutive violations) -- escalate; the schedule or the pattern may need to change

## What Erodes SLAs

- Upstream delays: the source system's batch job that populates the table ran late
- Extraction duration creep: the table grew and the query takes longer (see [[06-operating-the-pipeline/0601-monitoring-observability|0601]], duration trend)
- Dependency chains: `order_lines` can't load until `orders` finishes; a slow parent delays every child (see [[06-operating-the-pipeline/0605-scheduling-and-dependencies|0605]])
- Backfills that steal capacity from scheduled runs (see [[06-operating-the-pipeline/0610-backfill-strategies|0610]])

## Anti-Pattern

> [!danger] Don't promise SLAs you can't control
> - If your pipeline depends on a source system batch job that finishes "sometime between 5am and 7am," your SLA cannot be 7am. Build buffer or set the SLA at 9am and be honest about it.

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- freshness is a data health metric tracked in the observability layer
- [[06-operating-the-pipeline/0604-alerting-and-notifications|0604-alerting-and-notifications]] -- SLA breaches trigger alerts
- [[06-operating-the-pipeline/0605-scheduling-and-dependencies|0605-scheduling-and-dependencies]] -- dependency chains affect SLA achievability
- [[06-operating-the-pipeline/0607-tiered-freshness|0607-tiered-freshness]] -- different SLA tiers drive different refresh cadences
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_extracted_at` is the freshness measurement column

## Notes
- **Author prompt -- implicit SLAs**: Have you ever had a client discover their own SLA the hard way -- e.g. a report scheduled at 8am that assumed data was fresh, but nobody told you about the 8am deadline?
	- Yes, we had a client who we **told** (but didn't write) that our data updated once daily, they started making automated "correos de cobranza" to their clients and most had already paid by mid day, before he sent the emails.
- **Author prompt -- ERP batch delays**: SAP and Softland ERPs run their own batch jobs (posting, period close). Have these upstream jobs ever delayed your pipeline enough to breach an SLA? How variable is the timing?
	- Generally speaking the ERP is not the guilty one, its the people operating it. When your client has a "technical team", you can be a bit more scared since whenever they fuck up or overload the database during periods they themselves designated to you, the one blamed will be you.
- **Author prompt -- freshness policies**: You have Dagster freshness policies available. Are you using them in production? If so, have they surfaced violations that would have gone unnoticed? If not, why?
	- Actually not, we have a contract with each client in which we explicitly say NO MORE than one update a day. Whenever we increase this for "on demand" we tell them this is not actually part of SLA and it can cut off at any moment. We give them last extracted_at for their reports so they are conscious of this.
- **Author prompt -- multi-client SLAs**: With 35+ clients, do SLAs vary per client, or is there a default? Has a client ever demanded freshness that your architecture couldn't deliver?
	- Absolutely. daily is the best default and it handles most stuff wonderfully. Most times what your clients will need more often than daily is sales (especially in the busiest times of year, Black friday sales by SKU, stuff like that), receivables (to send "correos de cobranza" during the day, especially during end of month), and maybe warehouse stock reports (to know how much to sell within a store).
	- A client asked for a 15 minute maximum delay for their invoicing. However they weren't willing to pay the increased bill from Bigquery and processing, they also had horrendous metadata columns, hard deletes, the whole thing. We had to let them know if they wanted a system like that they'd better implement it within their production environment and later realized all they needed was On-demand updates once more per day. The **Head of Sales** wanted fresh data on their dashboard once a day, and he got it by setting up a refresh button on his dashboard that triggered our pipelines.
