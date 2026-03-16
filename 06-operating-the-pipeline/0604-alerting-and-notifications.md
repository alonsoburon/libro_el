---
title: "Alerting and Notifications"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-14
updated: 2026-03-14
---

# Alerting and Notifications

> **One-liner:** Schema drift, row count drops, partial failures. Calibrate severity so not everything is an incident.

## The Problem
- Pipelines fail silently: 0 rows extracted successfully, schema changed upstream, row counts drift
- Too many alerts = alert fatigue = ignored alerts = missed failures
- Too few alerts = silent data loss

## Severity Calibration
- Critical: load failed, destination has stale data, SLA breach imminent
- Warning: row count drop > threshold, schema drift detected, extraction slower than usual
- Info: successful run, row counts within range, no drift

- Also categorize tables on importance. Item Groups being late is probably less critical of a failure than sales being late.

## What to Alert On

The rule: alert on things your pipeline can't handle automatically. If the pipeline already has a pattern for it, the alert is redundant -- monitor it, don't page on it.

### Always Alert (pipeline can't self-heal)
- **Load failure** -- the destination didn't accept the data (quota, permission, timeout). Nothing auto-recovers this; someone needs to look
- **SLA breach or approaching breach** -- staleness exceeds the threshold defined in [[06-operating-the-pipeline/0603-sla-management|0603]]. The pipeline may be running fine but too slowly. Check your CRONs
- **Partial failure** -- some tables loaded, others didn't. The successful ones are fine; the failed ones need attention (see [[06-operating-the-pipeline/0611-partial-failure-recovery|0611]])
- **Extraction error on a table that previously had data** -- the transition from "had rows" to "connection error" or "query failed" is the signal, not "returned 0 rows" which can be normal for quiet incrementals or empty tables (see [[06-operating-the-pipeline/0609-extraction-status-gates|0609]])
- **Duration anomaly** -- extraction or load time exceeds 2-3x the historical average. The run may still succeed, but it's heading toward an SLA breach or a source overload (see [[06-operating-the-pipeline/0601-monitoring-observability|0601]])
- **Cost spike** -- daily cost exceeds threshold (see [[06-operating-the-pipeline/0602-cost-monitoring|0602]]). A runaway MERGE or an unpartitioned scan can burn budget before anyone notices

### Alert Only When Unhandled
- **Row count deviation** -- if the table uses [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] or reconciliation with auto-recovery, the pipeline handles it. Alert when the discrepancy exceeds the threshold *and* no automatic pattern resolves it (see [[06-operating-the-pipeline/0613-reconciliation-patterns|0613]])
- **Schema drift: new columns** -- if your schema evolution policy is `evolve`, new columns are accepted automatically. Log it, don't alert. If the policy is `freeze`, this is a load failure and already covered above
- **Schema drift: dropped columns** -- this one deserves an alert even with `evolve` policy; a dropped column may break downstream queries silently (see [[06-operating-the-pipeline/0608-data-contracts|0608]])
- **Schema drift: type changes** -- depends on the change. Widening (INT → BIGINT) is usually safe; narrowing or type-class change (INT → VARCHAR) is an alert

### Never Alert (monitor only)
- Successful runs -- log them, put them on the dashboard, don't send a notification or you'll go crazy
- 0 rows extracted on an incremental run -- quiet periods are normal; this is a data health metric in [[06-operating-the-pipeline/0601-monitoring-observability|0601]], not an alert condition
- Minor discrepancies within the configured reconciliation threshold

## Alert Channels
- Route by severity: critical → pager/Slack DM, warning → Slack channel, info → dashboard only
- Your orchestrator's alerting layer handles routing -- configure severity-based routing, not per-table rules
- DON'T FATIGUE YOURSELF!
	- It's better to pre-filter only the critical failures and fix them, then go for the rest, than get overloaded with failures and only hit the top 10.

## Anti-Patterns
- Alerting on every run (info-level noise drowns real problems)
- Same severity for all failures (schema drift ≠ total load failure)
- No escalation path (warning that persists for 3 days should promote to critical)

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- what to track (alerting is what to act on)
- [[06-operating-the-pipeline/0603-sla-management|0603-sla-management]] -- SLA breach triggers alerts
- [[06-operating-the-pipeline/0608-data-contracts|0608-data-contracts]] -- schema drift detection feeds alerts
- [[06-operating-the-pipeline/0609-extraction-status-gates|0609-extraction-status-gates]] -- 0-row gate triggers alerts
