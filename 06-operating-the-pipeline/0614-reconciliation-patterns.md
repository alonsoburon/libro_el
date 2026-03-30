---
title: "Reconciliation Patterns"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-29
---

# Reconciliation Patterns

> **One-liner:** Source count vs destination count -- row-level, hash-level, and aggregate reconciliation.

## The Problem

A load that completes without errors can still produce wrong results: missing rows, duplicate rows, stale data, wrong values. The pipeline reports success; the destination is quietly wrong. Without a verification step, discrepancies surface only when a consumer notices something off -- a report that doesn't tie out, a dashboard metric that jumped overnight, an analyst who ran the numbers twice and got different answers.

Reconciliation is the scheduled check that the destination actually reflects the source. It runs after the load, compares what arrived against what should have arrived, and alerts when the numbers don't add up. It's not a replacement for staging validation ([[02-full-replace-patterns/0201-full-scan-strategies|0201]]) or extraction gates ([[06-operating-the-pipeline/0610-extraction-status-gates|0610]]) -- those catch failures before the load commits. Reconciliation catches what those gates didn't see: rows that fell within tolerance, drift that accumulated over multiple runs, or discrepancies that only surface after downstream queries start running.

## Reconciliation Levels

### Row Count Reconciliation (Cheapest)

Compare `COUNT(*)` at the source against `COUNT(*)` at the destination. A count mismatch is the first and cheapest signal that something went wrong -- missing rows from a failed extraction, duplicates from a retry that double-wrote, a dropped partition that nobody noticed.

**Catches:** missing rows, duplicates, dropped partitions, undetected hard deletes (destination surplus).
**Misses:** rows with wrong values, rows updated at the source but not reloaded.

### Aggregate Reconciliation (Medium)

Compare key aggregates -- `SUM(amount)`, `MAX(updated_at)`, `COUNT(DISTINCT customer_id)` -- between source and destination. A row that changed from $100 to $0 has the same count but a different sum; row count alone misses it. Choose aggregates that are meaningful for the table: financial totals for transaction tables, max timestamps for activity tables.

The cost caveat: running `SUM(amount)` against a transactional source during business hours is expensive. Columnar destinations handle aggregation cheaply; transactional sources don't. If you're running aggregate reconciliation, run it against a read replica or during off-peak hours -- or accept that it runs less frequently than row count checks. At scale, with tables spanning dozens of clients and schemas, the variety makes it impractical to standardize meaningful aggregates across the board. Row count reconciliation runs on everything; aggregate reconciliation is reserved for critical tables where value integrity matters.

### Hash Reconciliation (Expensive)

Hash every row at source and destination and compare the hashes. Any difference at any column surfaces -- this is the nuclear option. At scale it's too expensive to run on every load; reserve it for critical tables or periodic audits, not routine runs.

## Configuring Thresholds

An exact match between source and destination count is rarely achievable in practice. In-flight transactions committed after the extraction window but before the destination count is taken create natural discrepancy on live transactional sources. A tolerance threshold absorbs this noise without masking real failures.

Two asymmetric rules drive threshold configuration:

**Destination has fewer rows than source** -- acceptable within a threshold, but the right threshold depends on when you extract. For off-peak extractions -- overnight, early morning -- in-flight transactions are rare and the tolerance should be tight; a deficit of more than a handful of rows is a real signal. For extractions running during business hours, the threshold needs to cover transactions committed after the extraction window closes; calibrate it from your actual discrepancy history, not a guess. 100 rows is a starting point for a busy source during business hours, but the right number varies by table and schedule.

**Destination has more rows than source** -- interpretation depends on whether hard delete detection is running. If it is, a surplus means duplicates and warrants an immediate alert. If it isn't, the surplus is expected: rows deleted from the source still exist in the destination. Know which case you're in before alerting.

When a deficit above threshold surfaces, the resolution depends on the gap size. A small gap is a candidate for pk-to-pk detection ([[03-incremental-patterns/0306-hard-delete-detection|0306]]) to identify exactly which rows are missing without reloading the whole table. A large gap points to a structural failure -- a missed extraction window, a dropped partition, a load strategy mismatch -- and the right fix is a full reload or partition swap ([[02-full-replace-patterns/0201-full-scan-strategies|0201]], [[02-full-replace-patterns/0203-partition-swap|0203]]).

## Timing Matters

Source count and destination count must be taken as close together as possible. A gap between counting the source and counting the destination lets new rows arrive at the source in between, generating a false discrepancy that looks like a pipeline failure but is just timing.

The right approach: record the source count at extraction time, before new data arrives; record the destination count after the load completes; compare in your orchestrator. The comparison cannot happen inside either database -- source and destination are different engines with no shared query context.

<!-- [ADD GRAPH HERE OF TIMING] -->

## Reconciliation Jobs

A per-run inline reconciliation check is ideal but expensive at scale. An alternative is a dedicated reconciliation job on a schedule -- typically once daily -- that iterates all tables, compares counts against source, and produces a summary report. Run it before business hours: discrepancies surface to operators before downstream consumers act on bad data. 07:00 is a reasonable default if your pipeline runs overnight.

The tradeoff: inline checks catch discrepancies before downstream queries run on bad data. A scheduled reconciliation job may surface an issue hours after downstream consumers have already seen it. For critical tables, inline is worth the overhead. For the long tail of lower-priority tables, a daily reconciliation job is the right balance.

Store the results in the health table ([[06-operating-the-pipeline/0602-health-table|0602]]): table name, source count, destination count, delta, status (OK / warning / critical). Storing results historically lets you detect drift trends -- a table that's consistently 50 rows short is a different problem from one that's suddenly 50,000 rows short. If you store per-run source and destination counts as part of normal pipeline operation, the dedicated reconciliation job becomes a comparison of already-collected numbers rather than a fresh round of queries against both systems -- which makes it cheap enough to run across everything, every morning.

## By Corridor

> [!example]- Transactional → Columnar
> - Source count: `SELECT COUNT(*) FROM schema.table` at source
> - Destination count: `SELECT COUNT(*)` -- BigQuery bills 0 bytes for it (resolved from table metadata internally); Snowflake resolves it from micro-partition headers without scanning data. Both engines also expose row counts in `INFORMATION_SCHEMA`, but those update asynchronously and can lag after a recent load, making them unreliable for post-load verification.

> [!example]- Transactional → Transactional
> - Both sides support `COUNT(*)` efficiently -- no reason not to use it.

## Anti-Pattern

> [!danger] Don't reconcile only on count
> A table with 1M rows at source and 1M rows at destination can still be wrong: 1,000 rows missing, 1,000 duplicates. Count matches, data doesn't. Use aggregate reconciliation for critical tables.

## Related Patterns

- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- reconciliation delta is a data health metric
- [[06-operating-the-pipeline/0602-health-table|0602-health-table]] -- stores per-table counts for historical comparison
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- threshold breaches trigger alerts
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- volume contracts are reconciliation expressed as a pre-load check
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- pre-load gating is reconciliation's inline cousin
- [[06-operating-the-pipeline/0613-duplicate-detection|0613-duplicate-detection]] -- count mismatch is often the first signal of duplication
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- pk-to-pk comparison for resolving small discrepancies
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- full reload for resolving large discrepancies
- [[02-full-replace-patterns/0203-partition-swap|0203-partition-swap]] -- partition-scoped reload for resolving discrepancies in a date range
