---
title: "Alerting and Notifications"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-14
updated: 2026-03-24
---

# Alerting and Notifications

> **One-liner:** Schema drift, row count drops, partial failures -- calibrate severity so not everything is an incident.

## The Problem

Pipelines fail silently. Zero rows extracted successfully, schema changed upstream, row counts drifting apart between source and destination -- all of these can happen while the orchestrator reports SUCCESS. The monitoring layer from [[06-operating-the-pipeline/0601-monitoring-observability|0601]] and the health table from [[06-operating-the-pipeline/0602-health-table|0602]] capture these signals; this pattern is about deciding which of them deserve to wake someone up.

The calibration problem has two failure modes.
1. Too many alerts -- every run sends a notification, every minor discrepancy triggers a warning -- produces alert fatigue, and alert fatigue produces ignored alerts, and ignored alerts produce missed failures.
2. Too few alerts -- only page on total outages -- means silent data loss accumulates for days before anyone notices.  
The goal is a narrow band between the two: alert on conditions that require human attention, monitor everything else on the dashboard.

## When You'll See This

| Signal                                  | Example                                                                                             |
| --------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Alert fatigue leads to ignored failures | 200 Slack notifications per day; a real load failure sat unread for 3 days                          |
| No alert for a silent failure mode      | Schema changed at source, loader dropped a column, downstream queries returned NULLs for a week     |
| Same severity for unequal failures      | Schema drift on a lookup table paged the on-call at 3 AM alongside a total load failure on `orders` |
| Alerts fire but nobody knows what to do | "Row count anomaly on `events`" -- is that bad? Who investigates? What's the threshold?             |

## Severity Calibration

Not every failure is equally urgent, and not every table is equally important. A load failure on `orders` during month-end close is a different severity than a stale `item_groups` lookup table on a Saturday. Calibrate on two axes: what broke and how much the table matters.

| Severity     | Condition                                                   | Example                                                                                                              |
| ------------ | ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Critical** | Destination data lost or significantly diverged from source | Table empty after load, row count dropped 80%, source/destination totals diverged beyond recovery                    |
| **Error**    | Load failed, destination stale, SLA breach                  | Permission denied, query timeout, staleness exceeds SLA from [[06-operating-the-pipeline/0604-sla-management\|0604]] |
| **Warning**  | Anomaly detected but data is present and current            | Row count drop > threshold, schema drift (new columns), extraction duration 3x historical average                    |
| **Info**     | Nothing wrong                                               | Successful run, counts in range, no drift. Log it, dashboard it, never notify                                        |

Table importance is the second axis. Sales and receivables tables failing during end-of-month is critical; a dimension lookup table being 2 hours stale is a warning at most. Classify tables into importance tiers and let the combination of condition severity and table importance determine the alert routing -- a WARNING on a critical table might route the same as an ERROR on a low-priority one.

## What to Alert On

The rule: alert on things your pipeline can't handle automatically. If the pipeline already has a pattern that resolves the condition -- retry logic, automatic schema evolution, reconciliation with auto-recovery -- the alert is redundant. Monitor it, log it, but don't page on it.

### Always Alert

These conditions require human attention because no automatic pattern resolves them:

**Load failure** -- the destination didn't accept the data (quota exceeded, permission revoked, timeout). Nothing auto-recovers this; someone needs to look. The health table's `status = 'FAILED'` with `error_message` gives you the triage starting point without leaving the dashboard.

**SLA breach or approaching breach** -- staleness exceeds the threshold defined in [[06-operating-the-pipeline/0604-sla-management|0604]]. The pipeline may be running fine but too slowly -- check whether it's duration creep, an upstream delay, or a schedule that needs adjustment.

**Partial failure** -- some tables loaded, others didn't. The successful ones are fine; the failed ones need attention. This is particularly dangerous because the overall run may report partial success and fly under the radar ([[06-operating-the-pipeline/0612-partial-failure-recovery|0612]]).

**Extraction error on a previously healthy table** -- the transition from "had rows" to "connection error" or "query failed" is the signal. A table that returned data yesterday and errors today has a real problem: a permission change, a dropped table at source, a network issue. Don't confuse this with "returned 0 rows," which can be normal for quiet incrementals ([[06-operating-the-pipeline/0610-extraction-status-gates|0610]]).

**Duration anomaly** -- extraction or load time exceeds 2-3x the historical average. The run may still succeed, but it's heading toward an SLA breach or source overload. The health table's `extraction_seconds` trend is the early warning.

**Cost spike** -- daily compute cost exceeds threshold ([[06-operating-the-pipeline/0603-cost-monitoring|0603]]). A runaway MERGE or an unpartitioned scan can burn budget before anyone notices.

### Alert Only When Unhandled

These conditions may or may not need attention, depending on whether the pipeline has automatic recovery:

**Row count deviation** -- if the table uses hard-delete detection ([[03-incremental-patterns/0306-hard-delete-detection|0306]]) or reconciliation with auto-recovery, the pipeline handles it. Alert only when the discrepancy exceeds the threshold *and* no automatic pattern resolves it ([[06-operating-the-pipeline/0614-reconciliation-patterns|0614]]).

**Schema drift** is nuanced. New columns with an `evolve` policy are accepted automatically -- log them, don't alert. Dropped columns deserve an alert even with `evolve`, because a missing column can break downstream queries silently and an `evolve` policy should reject column removal anyway. Type changes depend on direction: widening (INT → BIGINT) is usually safe; narrowing or type-class changes (INT → VARCHAR) are probably a problem. See [[06-operating-the-pipeline/0609-data-contracts|0609]] for the policy framework.

### Never Alert

**Successful runs.** Log them, put them on the dashboard, never send a notification. If you get a "success" message for every table on every run, you'll have 200 Slack messages per day and you'll stop reading any of them.

**Zero rows on an incremental** -- quiet periods are normal. The cursor is caught up or the source had no changes. This is a data health metric in the health table, not an alert condition.

**Minor reconciliation discrepancies** within the configured threshold -- a 0.05% drift on a busy table is likely noise, not a signal.

## Alert Channels

Route by severity, not by table. Critical alerts go to the pager or a DM -- something that demands immediate attention. Warnings go to a Slack channel where they're visible but not intrusive. Info stays on the dashboard where it's available on demand but never pushes a notification.

Your orchestrator's alerting layer handles the routing -- configure severity-based rules, not per-table rules. If you find yourself managing per-table routing for more than a handful of exceptions, the severity classification isn't doing its job.

> [!tip] Pre-filter before you fix
> When multiple tables fail overnight, resist the urge to investigate all of them at once. Filter to critical failures first, fix those, then work down to warnings. A critical failure on `orders` that blocks month-end reporting matters more than a warning on `products` with a new column. If you try to process every alert in arrival order, the important ones get buried and you burn your morning on problems that could have waited.

## Tradeoffs

| Pro                                           | Con                                                                        |
| --------------------------------------------- | -------------------------------------------------------------------------- |
| Severity tiers prevent alert fatigue          | Requires upfront classification of tables and conditions                   |
| "Alert only when unhandled" reduces noise     | Under-alerting is a real risk if the automatic recovery pattern has a bug  |
| Channel routing keeps critical alerts visible | Warning thresholds need periodic tuning as tables grow and patterns change |

## Anti-Patterns

> [!danger] Don't use the same severity for all failures
> Schema drift on a lookup table and a total load failure on `orders` are not the same event. If everything is "Error," nothing is -- the on-call engineer can't prioritize and will eventually stop responding to any of them.

> [!danger] Don't alert without an escalation path
> A warning that persists for 3 consecutive days is no longer a warning -- it's either a real problem being ignored or a miscalibrated threshold. Build automatic severity promotion: warning → error after N consecutive violations. If a threshold triggers daily and nobody investigates, the threshold is wrong, not the data.

## What Comes Next

[[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]] covers the scheduling layer that determines when pipelines run and in what order -- the timing decisions that directly affect whether SLAs from [[06-operating-the-pipeline/0604-sla-management|0604]] are achievable and which alert conditions fire.
