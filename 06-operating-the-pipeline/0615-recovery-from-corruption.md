---
title: "Recovery from Corruption"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-30
---

# Recovery from Corruption

> **One-liner:** A bad deploy corrupted 3 months of data -- identifying the blast radius and rebuilding.

## The Problem

Something broke and bad data has been landing for a while. A schema migration that silently changed types, a cursor that skipped a range, a load strategy that dropped a column, a conforming bug that mangled values. The pipeline reported success on every run because the failure was in the data, not in the execution -- no errors, no alerts, no signal that anything was wrong until someone downstream noticed the numbers didn't add up.

The gap between when corruption starts and when it's detected is the blast radius. A bug introduced three months ago that nobody caught until today means three months of data in the destination is suspect, every downstream model that consumed it is suspect, and every report built on those models has been wrong for three months. The recovery isn't just reloading the data -- it's scoping the damage, fixing the root cause, rebuilding what's affected, and communicating what happened so consumers can reassess decisions they made on bad data.

The worst corruptions are the ones that look plausible. A date format that flipped from D-M-Y to M-D-Y after a source ERP version upgrade produces dates that parse successfully -- January through December, day 1 through 12, nothing fails, nothing alerts. Every date-based partition, every month-end report, every time-series chart is silently wrong. You discover it when someone notices March 5th orders showing up in May, and by then the entire destination is corrupted across every table that has a date column.

## Triage: Assess the Blast Radius

### When Did It Start?

`_extracted_at` from [[05-conforming-playbook/0501-metadata-column-injection|0501]] narrows the window. Filter destination rows by `_extracted_at` ranges and compare against the source to find where the data starts diverging -- the first batch where values don't match is the start of the corruption window. Cross-reference that timestamp with your deploy history and git log: a commit that shipped on the same day as the first bad batch is the likely root cause.

If `_batch_id` is populated, the scoping is even tighter -- "all rows from batch 47 onward are corrupted" is a precise statement that drives the recovery scope. Without metadata columns, you're left correlating deploy dates with destination anomalies by hand, which is slower and less certain.

### What Tables Are Affected?

The blast radius depends on where the root cause lives. A pipeline code change that affects the conforming layer corrupts every table processed by that code path -- potentially hundreds. A source schema change affects only tables from that source. A destination-side issue (quota exhaustion, permission change) affects only tables on that destination.

Start with the narrowest plausible scope and widen if evidence points further. Checking a handful of tables from each source against their current source state is faster than assuming everything is wrong and rebuilding the world.

### What's Downstream?

Every downstream model, materialized view, dashboard, and report that reads from the corrupted tables is also affected. Map the lineage from corrupted tables to downstream consumers -- if the destination feeds a transformation layer that builds aggregates, those aggregates are wrong too, and they need rebuilding after the source tables are clean.

## Recovery Strategies

Three strategies, from broadest to most surgical. The right choice depends on how much data is affected and how precisely you can scope it.

### Full Replace (Simplest)

Reload the entire table from source using `full_refresh: true`. Resets the destination to the current source state -- every row, every column, clean baseline. Downstream models rebuild from the clean data. This is the right default when the table is small enough to reload within the schedule window, when the corruption is widespread, or when you can't precisely scope the damage.

Full replace always works for current state. The source has the correct data right now, so reloading it produces a correct destination. The caveat is historical state: if the source is transactional and rows were modified or deleted since the corruption started, the full replace reflects the source's current state, not the state at any point during the corruption window. For most tables this is exactly what you want -- the destination should match the source as of now, not as of three months ago.

### Date-Range Rebuild

Reload only the corruption window via backfill ([[06-operating-the-pipeline/0611-backfill-strategies|0611]]). Less disruptive than full replace because data outside the window stays untouched, but it requires knowing the exact corruption range. Scope the range slightly wider than the first bad batch -- corruption boundaries are rarely as precise as a single timestamp suggests, and a few extra days of reload is cheap insurance against missing rows at the edges.

Use partition swap ([[02-full-replace-patterns/0203-partition-swap|0203]]) for the destination-side replacement so the rebuild is atomic per partition and the rest of the table stays live throughout. For tables too large to full-replace but where the corruption window is bounded, this is the sweet spot.

### PK-to-PK Repair

Compare primary keys between source and destination to identify exactly which rows are wrong -- missing, surplus, or mismatched values. Fix only the discrepancies: insert missing rows, delete surplus rows, update changed values. This is the same mechanism as hard delete detection ([[03-incremental-patterns/0306-hard-delete-detection|0306]]) and the small-gap resolution described in [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]].

Use this when the corruption is narrow -- a handful of rows in a large table, a specific set of PKs identified during triage -- and reloading an entire table or date range would be disproportionate. The tradeoff is that you need to know exactly which rows are affected, which requires either a full PK comparison against the source or a reconciliation pass that identified the discrepancies.

All three strategies may require a state reset if the table uses cursor-based extraction. A full replace or date-range rebuild that reloads the data but leaves the old cursor in place means the next incremental run skips everything between the stale high-water mark and now -- the same problem [[06-operating-the-pipeline/0611-backfill-strategies|0611]] warns about. Stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) sidesteps this entirely -- the next run re-reads its normal trailing window regardless of what the rebuild did, and there's no cursor to forget about. This is one of the operational arguments for defaulting to stateless: recovery is simpler because there's less state to manage.

## Recovery Checklist

Regardless of which strategy you choose, the sequence is the same: confirm the fix, verify the source, rebuild, verify the result, notify.

- [ ] If consumers have already acted on corrupted data (reports sent, decisions made), notify them now -- they need to know before you start, not after
- [ ] Confirm the root cause is fixed and deployed
- [ ] Test the fix on a small range before committing to the full rebuild
- [ ] Verify source connectivity and schema haven't changed since the corruption started
- [ ] If the table uses cursor-based extraction: reset incremental state (cursor position, schema versions) so the rebuild sets a clean baseline -- not needed for stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]])
- [ ] Run the rebuild (full replace, date-range backfill, or PK-to-PK repair depending on scope)
- [ ] Reconcile post-rebuild: source count vs destination count ([[06-operating-the-pipeline/0614-reconciliation-patterns|0614]])
- [ ] Notify downstream consumers that data is clean and they can rebuild dependent models

## Prevention

None of these prevent corruption from happening -- source schemas change, bugs ship, scripts run without warning. What they do is make corruption detectable early and recoverable fast, which limits the blast radius.

**Metadata columns** (`_extracted_at`, `_batch_id`) make triage possible. Without them you can't scope the corruption to specific batches -- you're left guessing which runs introduced the bad data based on deploy dates and git blame. See [[05-conforming-playbook/0501-metadata-column-injection|0501]].

**Schema contracts** catch drift before it corrupts data. A new column appearing is harmless; a column disappearing or a type changing is a signal that something upstream changed without coordination. Contracts surface these changes before the load commits, not after consumers have already consumed the result. See [[06-operating-the-pipeline/0609-data-contracts|0609]].

**Reconciliation** catches silent count and value drift between source and destination. A table that's consistently 50 rows short is a different problem from one that's suddenly 50,000 rows short, and both are problems that row-level pipeline success doesn't reveal. See [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]].

**Stateless, idempotent pipelines** reduce the recovery surface. Pipeline state -- cursors, schema version tracking, checkpoint files -- is itself a corruption vector. When the state is wrong, the pipeline produces wrong output from correct source data, and the failure mode is invisible because no query failed and no error fired. The less state your pipeline carries between runs, the fewer ways it can silently break. Full replace and stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) both minimize carried state; cursor-based extraction with external state stores maximizes it.

## Anti-Patterns

> [!danger] Don't fix forward without fixing backward
> Fixing the pipeline so future runs are correct doesn't fix the corrupted historical data already in the destination. You need both: fix the code AND rebuild the affected range. A pipeline that's producing correct data going forward while three months of bad data sits in the destination is a pipeline that's still wrong -- it's just wrong in a way that's harder to notice.

> [!danger] Don't rebuild without confirming the root cause is fixed
> Reloading 3 months of data only to have the same bug corrupt it again is wasted work and a wasted weekend. Confirm the fix is deployed, test it on a small range, then run the full rebuild. The checklist above puts "test on a small range" before the rebuild for exactly this reason.

## Related Patterns

- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_batch_id` and `_extracted_at` scope the corruption to specific loads
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- PK-to-PK comparison for surgical repair
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- contracts catch the drift before it becomes corruption
- [[06-operating-the-pipeline/0611-backfill-strategies|0611-backfill-strategies]] -- the mechanism for rebuilding a date range
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- post-rebuild verification and small-gap resolution
- [[06-operating-the-pipeline/0612-partial-failure-recovery|0612-partial-failure-recovery]] -- when corruption is caused by a partial failure
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- no state to reset after recovery
