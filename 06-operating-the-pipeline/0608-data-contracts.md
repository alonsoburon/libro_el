---
title: "Data Contracts"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Data Contracts

> **One-liner:** Schema drift, row counts, null rates, freshness -- what to enforce at the boundary between source and destination.

## The Problem
- Source schemas change without notice: a column is renamed, a type changes from INT to VARCHAR, a new column appears, an old one is dropped
- Without a contract, drift propagates silently into the destination and breaks downstream queries, dashboards, and models
- The source team doesn't know your pipeline exists; they won't tell you before they deploy a schema migration

## What a Data Contract Covers

### Schema Contract
- Expected column names and types -- the fingerprint from [[06-operating-the-pipeline/0601-monitoring-observability|0601]]
- Policy on new columns: accept (evolve), reject (freeze), or warn (log and continue)
- Policy on dropped columns: fail the load, load without them, or backfill with NULLs
- Policy on type changes: fail, cast, or warn

### Volume Contract
- Expected row count range per extraction (min/max from recent history)
- A 90% drop in row count is likely a problem, not a quiet day
- Feeds into [[06-operating-the-pipeline/0609-extraction-status-gates|0609]] for inline enforcement

### Null Contract
- Expected null rates on key columns -- a cursor column should never be NULL; a description column being NULL is normal
- Threshold-based: fail if null rate on `updated_at` exceeds 0%, warn if null rate on `email` exceeds 50%

### Freshness Contract
- Maximum acceptable staleness per table -- the SLA from [[06-operating-the-pipeline/0603-sla-management|0603]] expressed as a contract

## Enforcement Points

### Pre-Load (Gate)
- Check schema, row count, and null rates after extraction, before loading
- If the contract is violated, block the load and alert (see [[06-operating-the-pipeline/0604-alerting-and-notifications|0604]])
- This is the extraction status gate from [[06-operating-the-pipeline/0609-extraction-status-gates|0609]] extended with richer checks

### Post-Load (Validation)
- Run checks after the load completes: destination row count vs source, schema matches expected, null rates within bounds
- Your orchestrator's post-load check primitives are built for this -- run them as part of the load, not as a separate job

### Continuous (Monitoring)
- Schema fingerprint comparison on every run
- Volume trend tracking over time
- Feeds the observability layer from [[06-operating-the-pipeline/0601-monitoring-observability|0601]]

## Schema Evolution Policies

| Policy | Behavior | When to use |
|---|---|---|
| **Evolve** | Accept new columns, add them to destination | Default for most tables -- source schemas grow |
| **Freeze** | Reject any schema change, fail the load | Critical tables where downstream depends on exact schema |
| **Discard** | Ignore new columns, load only known ones | Tables with noisy schemas (wide ERP tables with hundreds of columns) |

## Anti-Pattern

> [!danger] Don't enforce contracts you haven't baselined
> - A contract based on assumptions ("this column should never be NULL") will fire false positives on the first run. Baseline the actual data first: run a profiling pass, measure real null rates and row counts, then set thresholds from observed behavior.

> [!danger] Don't freeze schemas on tables that legitimately evolve
> - `products` gains a new attribute column every quarter. Freezing its schema means a load failure every quarter and a manual intervention to update the contract. Use evolve for tables with expected growth; freeze only for tables with stable, critical schemas.

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- schema fingerprinting and null rate tracking feed contracts
- [[06-operating-the-pipeline/0604-alerting-and-notifications|0604-alerting-and-notifications]] -- contract violations trigger alerts
- [[06-operating-the-pipeline/0609-extraction-status-gates|0609-extraction-status-gates]] -- pre-load gate is the inline enforcement mechanism
- [[06-operating-the-pipeline/0603-sla-management|0603-sla-management]] -- freshness contract is the SLA expressed as a checkable rule
- [[06-operating-the-pipeline/0613-reconciliation-patterns|0613-reconciliation-patterns]] -- volume contract enforcement post-load

## Notes
- **Author prompt -- naming convention as contract**: The direct vs snake_case naming convention is essentially a schema contract -- changing it requires full reload of all tables + downstream query updates. Has a client ever requested a convention change mid-production? How painful was it?
- **Author prompt -- schema evolution in ERPs**: SAP and Softland add columns when modules are activated or updated. Have you had a source table gain columns that broke your pipeline? How does dlt's schema evolution (evolve/freeze/discard_row) work in practice for you?
- **Author prompt -- type surprises**: The SafeNumericTypeAdapter in warp handles type casting. What kind of type mismatches have you encountered between source and BigQuery? Any that were especially surprising?
- **Author prompt -- _dlt_version**: DLT tracks schema versions internally. Have you ever used that to diagnose when a schema changed? Or is it mostly invisible infrastructure?
