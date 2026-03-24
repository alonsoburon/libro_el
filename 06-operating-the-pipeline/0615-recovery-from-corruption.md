---
title: "Recovery from Corruption"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Recovery from Corruption

> **One-liner:** A bad deploy corrupted 3 months of data -- identifying the blast radius and rebuilding.

## The Problem
- Something broke and bad data has been landing for a while -- a schema migration that silently changed types, a cursor that skipped a range, a load strategy that dropped a column, a conforming bug that mangled values
- The corruption was silent: the pipeline reported success on every run
- You need to figure out what's affected, how far back the damage goes, and how to fix it without making things worse

## Triage: Assess the Blast Radius

### When Did It Start?
- Use `_extracted_at` and `_batch_id` from [[05-conforming-playbook/0501-metadata-column-injection|0501]] to identify the first corrupted batch
- Cross-reference with deploy history, config changes, schema changes
- The first bad batch defines the start of the corruption window

### What Tables Are Affected?
- If the root cause is a pipeline code change, every table processed by that code path is suspect
- If the root cause is a source schema change, only tables from that source are affected
- If the root cause is a destination-side issue (quota, permission change), only tables on that destination

### What's Downstream?
- Every materialized view, dbt model, dashboard, and report that reads from the corrupted tables is also affected
- Map the lineage from corrupted tables to downstream consumers
- Notify downstream consumers before you start the fix -- they need to know their data is suspect

## Recovery Strategies

### Full Replace (Simplest)
- Reload the entire table from source: `full_refresh: true`
- Resets everything to the current source state
- Downstream models rebuild from the clean base
- Works when the source still has the correct data (it always does for the current state; historical state may be lost)

### Date-Range Rebuild
- Reload only the corruption window via backfill (see [[06-operating-the-pipeline/0611-backfill-strategies|0611]])
- Less disruptive than full replace; preserves data outside the window
- Requires knowing the exact corruption range

### State Reset
- Clear incremental state (cursor position, pipeline state, schema versions) and let the pipeline rebuild from scratch
- Equivalent to "first run" -- next execution will be a full load regardless of configuration
- Use when the corruption is in the pipeline's internal state, not in the source data

## State Cleanup Checklist

- [ ] Clear incremental cursor / high-water mark
- [ ] Drop or truncate destination tables in the corruption range
- [ ] Delete internal loader state (pipeline state tables, schema version tables)
- [ ] Delete orphaned staging datasets
- [ ] Verify source connectivity and schema before re-extracting
- [ ] Run the rebuild
- [ ] Reconcile post-rebuild (see [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]])
- [ ] Notify downstream consumers that data is clean

## Prevention

- Metadata columns (`_extracted_at`, `_batch_id`) make triage possible -- without them, you can't scope the corruption (see [[05-conforming-playbook/0501-metadata-column-injection|0501]])
- Schema contracts catch drift before it corrupts data (see [[06-operating-the-pipeline/0609-data-contracts|0609]])
- Reconciliation catches silent count/value drift (see [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]])
- Post-load validation catches type mismatches and null spikes before downstream consumes the data

## Anti-Pattern

> [!danger] Don't fix forward without fixing backward
> - Fixing the pipeline so future runs are correct doesn't fix the corrupted historical data already in the destination. You need both: fix the code AND rebuild the affected range.

> [!danger] Don't rebuild without confirming the root cause is fixed
> - Reloading 3 months of data only to have the same bug corrupt it again is wasted work. Confirm the fix is deployed, test it on a small range, then run the full rebuild.

## Related Patterns
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_batch_id` scopes the corruption to specific loads
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- contracts catch the drift before it becomes corruption
- [[06-operating-the-pipeline/0611-backfill-strategies|0611-backfill-strategies]] -- the mechanism for rebuilding a date range
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- post-rebuild verification
- [[06-operating-the-pipeline/0612-partial-failure-recovery|0612-partial-failure-recovery]] -- when corruption is caused by a partial failure

## Notes
- **Author prompt -- the _dlt_cleanup nuclear option**: Cleanup drops all DLT state and forces full refresh on everything. Have you ever had to run this as an emergency recovery? What was the trigger -- corrupted state, or something else?
- **Author prompt -- blast radius**: When something has been silently corrupting data, how do you figure out when it started? Do you use `_extracted_at` / `_batch_id` to scope it, or is it more of a "check git blame and correlate with deploy dates" process?
- **Author prompt -- downstream notification**: With clients consuming BigQuery data in Looker, dbt, etc., how do you notify them that data was corrupted and is being rebuilt? Is there a formal process, or is it ad-hoc?
- **Author prompt -- worst recovery**: What's the longest / most painful data recovery you've had to do? How many tables, how far back, and how long did the rebuild take?
