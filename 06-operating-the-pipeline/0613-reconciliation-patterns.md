---
title: "Reconciliation Patterns"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Reconciliation Patterns

> **One-liner:** Source count vs destination count -- row-level, hash-level, and aggregate reconciliation.

## The Problem
- A load can succeed (no errors) while producing incorrect results: missing rows, extra rows, stale data, wrong values
- Without reconciliation, you trust the pipeline blindly -- and discover problems only when a consumer reports them
- Reconciliation is the post-load verification that the destination actually reflects the source

## Reconciliation Levels

### Row Count Reconciliation (Cheapest)
- `COUNT(*)` at source vs `COUNT(*)` at destination
- Catches: missing rows, duplicates, dropped partitions
- Misses: rows with wrong values, rows that were updated but not reloaded
- Per-table, run after every load or on a scheduled cadence

### Aggregate Reconciliation (Medium)
- Compare key aggregates: `SUM(amount)`, `MAX(updated_at)`, `COUNT(DISTINCT customer_id)`
- Catches: value drift that row count alone misses -- a row that changed from $100 to $0 has the same count but different sum
- Choose aggregates that are meaningful for the table: totals for financial tables, max timestamps for activity tables

### Hash Reconciliation (Expensive)
- Hash every row (or a sample) at source and destination, compare
- Catches: any difference at any granularity -- the nuclear option
- Expensive at scale; reserve for critical tables or periodic audits, not every run

## Configuring Thresholds

- An exact match (`source_count == destination_count`) is ideal but not always achievable -- timing windows, in-flight transactions, and concurrent writes create natural small discrepancies
- Configure a tolerance threshold per table or per client (e.g., accept discrepancies up to 10,000 rows)
- Discrepancy within threshold → log as info
- Discrepancy above threshold → alert as critical (see [[06-operating-the-pipeline/0604-alerting-and-notifications|0604]])

## Timing Matters

- The source count and destination count must be measured as close together as possible
- If you count the source at 6:00am and the destination at 6:10am, and the source received 500 new rows in between, you'll report a false discrepancy
- Best practice: count source during extraction (before new data arrives), count destination after load completes

## Reconciliation Jobs

- A dedicated reconciliation job that runs after the main pipeline -- iterates all tables, compares counts, produces a summary report
- Report format: table name, source count, destination count, delta, status (OK / warning / critical)
- Store results historically to detect drift trends over time

## By Corridor

> [!example]- Transactional → Columnar
> - Source count: `SELECT COUNT(*) FROM schema.table` at source
> - Destination count: query destination's metadata or `SELECT COUNT(*)` (expensive in columnar; use table metadata when available)
> - Columnar engines often expose row counts in `INFORMATION_SCHEMA` or table metadata without scanning

> [!example]- Transactional → Transactional
> - Both sides support `COUNT(*)` efficiently
> - Can also compare `MAX(pk)` or `MAX(updated_at)` as a quick proxy before running a full count

## Anti-Pattern

> [!danger] Don't reconcile only on count
> - A table with 1M rows at source and 1M rows at destination can still be wrong: 1,000 rows missing, 1,000 duplicates. Count matches, data doesn't. Use aggregate reconciliation for critical tables.

> [!danger] Don't skip reconciliation for full replaces
> - "It's a full replace, so it's always correct." Until the extraction query has a WHERE clause you forgot about, or the source connection timed out and returned partial results. Reconcile everything.

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- reconciliation delta is a data health metric
- [[06-operating-the-pipeline/0604-alerting-and-notifications|0604-alerting-and-notifications]] -- threshold breaches trigger alerts
- [[06-operating-the-pipeline/0608-data-contracts|0608-data-contracts]] -- volume contracts are reconciliation expressed as a pre-load check
- [[06-operating-the-pipeline/0609-extraction-status-gates|0609-extraction-status-gates]] -- pre-load gating is reconciliation's inline cousin
- [[06-operating-the-pipeline/0612-duplicate-detection|0612-duplicate-detection]] -- count mismatch is often the first signal of duplication

## Notes
- **Author prompt -- reconciliation_threshold**: You have a configurable threshold per client (default 10K rows). How did you arrive at 10K? Have clients needed different thresholds? What drives the variation?
- **Author prompt -- integrity_summary_job results**: When the integrity job runs across all clients, what's the typical result? Mostly clean, or are there always a few tables with discrepancies? What are the usual causes?
- **Author prompt -- timing window**: Source count at extraction time vs destination count after load -- have you had false positives from transactions that arrived between the two counts?
- **Author prompt -- beyond row count**: Do you do any aggregate reconciliation (SUM of amounts, MAX of dates) in production, or is it purely row count? Has count-only ever missed something that an aggregate check would have caught?
