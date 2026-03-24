---
title: "Monitoring and Observability"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-23
---

# Monitoring and Observability

> **One-liner:** Row counts tell you the pipeline ran. They don't tell you it ran *well*, or that the data it produced is worth trusting.

## The Problem

Most pipelines start with a single check: did it succeed? That binary signal covers maybe 40% of what can go wrong. A pipeline can succeed while producing garbage -- a query timed out and returned partial results, a full replace that used to take 3 minutes now takes 45 because the table grew 10x, or the source schema changed and the loader silently dropped columns. Every one of these scenarios reports SUCCESS. Every one of them delivers broken data to consumers.

Without structured **observability**, you discover these problems when a stakeholder asks why the dashboard is wrong -- often days after the data actually broke. By that point the blast radius is wide: downstream models have consumed the bad data, reports have been sent, and the person asking is already frustrated. The monitoring pattern in this chapter is about catching those failures before anyone else does, ideally within minutes of the pipeline run that caused them.

The key insight is that you need to track more than pass/fail, but you also need to resist the urge to track everything. Every metric you record has a storage cost and a cognitive cost -- someone has to look at it, and if the dashboard has 40 numbers, nobody looks at any of them carefully. The goal is a small set of raw measurements that cover the important failure modes, from which you can derive everything else.

## When You'll See This

| Signal                                             | Example                                                                                                       |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Stakeholder reports stale data before you notice   | "The dashboard hasn't updated since yesterday" and your pipeline shows SUCCESS                                |
| Pipeline succeeds but row counts are wrong         | `orders` usually extracts 450k rows; today it extracted 12k and nobody flagged it                             |
| Gradual performance degradation goes unnoticed     | A 4-minute extraction creeps to 25 minutes over 3 months, then starts overlapping with the next scheduled run |
| Schema changes at source break downstream silently | Source adds a column, loader drops it, downstream query references it, consumer sees NULLs                    |
| Partial failures produce incomplete data           | Half the batch loaded, the other half timed out, destination has rows from two different points in time       |

## Four Layers of Pipeline Observability

Observability breaks into four layers, each covering a different failure mode. You don't need all of them on day one -- Run Health and Data Health cover the critical cases, and the other two earn their place as your pipeline count grows.

### 1. Run Health

The basics: did the pipeline run, did it succeed, and how long did it take? Every orchestrator tracks this natively -- run status, duration, dependency graphs -- so there's rarely anything to build here. What the orchestrator gives you for free is already enough.

The one thing worth adding is trend tracking on duration. A 3-minute job that creeps to 30 minutes is a signal even when it still succeeds, because it tells you the table is growing or the source is degrading before either becomes an emergency. We had a table silently grow enough that its extraction started overlapping with the next scheduled run, causing 3 PM crashes for weeks before we charted duration and saw it had been climbing steadily for months -- the fix was moving heavy tables to a less frequent schedule ([[06-operating-the-pipeline/0608-tiered-freshness|0608]]), but the signal was in the health table long before the failure. Without duration trends, you discover these problems when jobs start timing out, which is too late to fix gracefully.

Retry counts are worth recording if your pipeline retries on transient failures. A job that succeeds on the third retry every day is not healthy -- it's masking an unstable connection or a source system under load.

### 2. Data Health

This is where monitoring earns its keep. Run Health tells you the pipeline executed; Data Health tells you what the pipeline produced.

**Row counts** are the single most useful metric. Track three numbers: `source_rows` (counted at the source before extraction), `rows_extracted` (returned by the extraction query), and `destination_rows` (counted at the destination after load). Each pair tells you something different. On a full replace, `rows_extracted` should equal `destination_rows` -- you pulled N rows and loaded them, so the destination should have N. If it doesn't, something was lost or duplicated during the load. `source_rows` vs `destination_rows` over time is a drift indicator for incremental tables -- if the totals diverge across runs, you're accumulating missed rows or orphaned deletes. A 50% drop in any of the three is a signal worth investigating, but row counts have a blind spot: they measure volume, not composition. We had a client whose `invoices` table hard-deleted draft invoices regularly while new ones replaced them at roughly the same rate -- the count stayed stable, but the destination accumulated stale drafts the source had already removed. Only a daily PK comparison ([[06-operating-the-pipeline/0614-reconciliation-patterns|0614]]) caught the problem, because row counts told us the right *number* of rows existed without revealing they were the wrong rows.

For incremental tables specifically, `rows_extracted` over time is revealing. It shows big moments of change -- month-end closes, batch corrections, seasonal spikes -- where you may want to widen your extraction window or shift the schedule to avoid overlapping with the source system's heaviest period.

> [!tip] Alert on big spikes in `rows_extracted`
> If an incremental that usually returns 2k rows suddenly returns 50k, the source had a large batch operation -- month-end close, bulk import, data migration. That spike means there may be more rows changed than your window caught. Consider triggering a full replace that night to reset state and catch anything the incremental missed.

**Freshness** is the other critical data health metric: when was this table last successfully loaded? The health table records `extracted_at` on every run (complementing the per-row `_extracted_at` from [[05-conforming-playbook/0501-metadata-column-injection|0501]], which tags individual records rather than pipeline runs), so staleness is a simple aggregation -- [[06-operating-the-pipeline/0604-sla-management|0604]] covers the query and the SLA thresholds that give the number meaning.

**Schema fingerprints and null rates** are worth tracking here as changes between runs, but enforcement -- what to do when they change -- belongs in [[06-operating-the-pipeline/0609-data-contracts|0609]].

### 3. Source Health

Source health metrics are less about your pipeline and more about the system you're extracting from. Query duration at the source, isolated from load performance, tells you whether the source database is degrading or whether your extraction query needs tuning. Timeout frequency -- queries that hit the threshold even when they eventually return on retry -- reveals instability before it becomes a failure.

Source system load impact is worth tracking for a less obvious reason: it's a sales tool. If you can demonstrate that your extraction uses less than 1% of the source database's capacity, you can sell the pipeline as a lightweight, non-invasive solution to more technical stakeholders who are nervous about letting you query their production system. See [[06-operating-the-pipeline/0607-source-system-etiquette|0607]] for the full treatment.

### 4. Load Health

Load **cost** generally matters more than load duration. Duration tends to be stable for a given table size and load strategy -- it's predictable and boring. Cost is the variable that shifts under your feet: a MERGE on BigQuery at 100k rows costs differently than at 10M, DML pricing changes without warning, and switching from full replace to incremental changes the operation type entirely. Tracking `load_seconds` is still useful for spotting bottlenecks, but if you had to pick one dimension to watch on the load side, it's cost -- and [[06-operating-the-pipeline/0603-cost-monitoring|0603]] covers how to capture and attribute it.

The destination row count after load closes the loop on reconciliation. On a full replace, `destination_rows` should match `rows_extracted` -- if it doesn't, rows were lost or duplicated during the load. On an incremental, tracking `source_rows` vs `destination_rows` over time reveals whether the totals are drifting apart across runs, which is the signal that your incremental is accumulating missed rows or undetected deletes. See [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]] for the full treatment.

## The Morning Routine

Before diving into implementation, it's worth naming what you're actually looking at when you open the dashboard. The sequence matters -- it's a triage, not a survey.

> [!tip] The four numbers you check first
> (1) How many tables failed overnight.
> (2) Which tables are stale beyond their SLA.
> (3) Any row count anomalies -- spikes, drops, or reconciliation deltas above threshold.
> (4) Cost per day. Everything else is drill-down from one of these four.

In a single-orchestrator setup, the orchestrator's native UI covers items 1 and 2 well enough. Items 3 and 4 come from the health table and the cost monitoring layer from [[06-operating-the-pipeline/0603-cost-monitoring|0603]]. In a multi-orchestrator setup, the health table is the only place where all four numbers converge -- which is why it exists.

## The Pattern

```mermaid
flowchart TD
    run["Run Health<br/>success/fail, duration,<br/>retries, error class"] --> dash["Observability<br/>Dashboard"]
    data["Data Health<br/>row counts, freshness,<br/>schema changes (0609)"] --> dash
    src["Source Health<br/>connection status,<br/>query duration, timeouts"] --> dash
    load["Load Health<br/>rows loaded, load duration,<br/>reconciliation delta"] --> dash
    dash --> alert["Alerting Layer<br/>(0605)"]
    dash --> sla["SLA Tracking<br/>(0604)"]
```

The pattern is straightforward: after every pipeline run, append a row to a health table. One row per table per run, with the raw measurements needed to answer the four morning questions. Everything else -- dashboards, alerts, SLA reports -- is a query on top of this table. [[06-operating-the-pipeline/0602-health-table|0602]] covers the schema, the column-by-column rationale, and how to populate it.

## Anti-Patterns

> [!danger] Don't confuse monitoring with alerting
> Monitoring is the dashboard you look at; alerting is the pager that wakes you up. They share data, but the threshold for "worth recording" is much lower than "worth paging someone." Record everything in the health table. Alert on a carefully tuned subset. See [[06-operating-the-pipeline/0605-alerting-and-notifications|0605]] for how to calibrate the boundary.

> [!danger] Don't track everything at the same granularity
> Per-row metrics on a 100M-row table are storage, not observability. The health table is one row per table per run -- aggregate metrics only. If you need row-level diagnostics, run them ad hoc against the source or destination, not as part of every pipeline run.

> [!danger] Don't build a custom monitoring stack when you don't need one
> If you're running a single orchestrator with 50 tables, the orchestrator's native run history, duration tracking, and status page are probably enough. The health table pattern earns its complexity at scale -- hundreds of tables, multiple pipelines, or a multi-orchestrator cluster where no single UI gives you the full picture. Build monitoring infrastructure in proportion to the monitoring problem you actually have.

## What Comes Next

[[06-operating-the-pipeline/0602-health-table|0602]] covers the health table implementation -- the schema, column rationale, derived metrics, and how to populate it reliably. From there, [[06-operating-the-pipeline/0603-cost-monitoring|0603]] extends it with cost attribution, [[06-operating-the-pipeline/0604-sla-management|0604]] builds freshness SLAs on the staleness data, and [[06-operating-the-pipeline/0605-alerting-and-notifications|0605]] draws the line between what's worth recording and what's worth paging someone about.
