---
title: "Partial Failure Recovery"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-29
---

# Partial Failure Recovery

> **One-liner:** Half the batch loaded, the other half didn't -- now what?

## The Problem

A pipeline run that processes multiple tables can fail partway through: 40 tables succeed, 10 fail. Rerunning the entire job wastes time reprocessing the 40 tables that already landed correctly. Not rerunning leaves 10 tables stale, and the staleness compounds with every subsequent run that doesn't fix them. The real problem is knowing which tables failed, at which step, and whether to retry now or wait for the next scheduled run.

At scale, partial failures are daily. With hundreds of tables extracting from multiple sources, something fails every run -- a connection timeout on one source, a DML quota hit on the destination, a schema change on a table nobody warned you about. The pipeline that handles partial failures well isn't the one where nothing ever fails; it's the one where failures are visible, scoped, and retriable without disrupting the tables that succeeded.

> [!tip] Cursor safety and partial failures
> If your cursors advance only after a confirmed successful load ([[03-incremental-patterns/0302-cursor-based-extraction|0302]]), partial failures don't create data gaps -- the failed tables simply get re-extracted on the next run. Stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) avoids the question entirely.

## Failure Modes

### Extraction Failed for Some Tables

Some tables extracted successfully, others hit a timeout, a connection error, or a source that was temporarily unavailable. The successful tables can proceed to load; the failed ones need re-extraction. Each table should have automatic retry on extraction errors -- a connection timeout on the first attempt often succeeds on the second, and waiting for the next scheduled run to discover that wastes an entire cycle. Two or three retries with a short backoff is enough; if the source is genuinely down, retrying indefinitely just adds load to a system that's already struggling ([[06-operating-the-pipeline/0607-source-system-etiquette|0607]]). Your orchestrator should track per-table status, not just per-run status -- the successful tables should proceed to load even though the run as a whole is failed.

The common causes are connection timeouts (especially on slow on-prem sources), connection pool exhaustion when too many tables extract from the same source simultaneously, and source maintenance windows that nobody told you about. A table that fails for the same reason every Monday morning is a scheduling problem, not a retry problem -- move it to a different window or investigate the source's maintenance calendar ([[06-operating-the-pipeline/0607-source-system-etiquette|0607]]).

### Extraction Succeeded, Load Failed

The data was extracted correctly but the destination rejected it -- DML quota exceeded, permission error, schema mismatch, disk full. The extraction is valid and may still be sitting in staging; if it is, you can retry the load without re-extracting. If staging is ephemeral (cleaned up per run), the extraction has to run again.

Destination quotas are the most common cause at scale. Columnar engines like BigQuery impose daily DML limits, and a pipeline that runs hundreds of merges can exhaust the quota partway through -- the first 150 tables land fine, the remaining 50 get rejected. The fix isn't more quota (though that helps); it's knowing which tables didn't land and retrying them in the next window when the quota resets. This is also where full replace earns its keep: a `DELETE + INSERT` or partition swap avoids the DML-heavy merge path entirely, and quota limits on batch loads are generally higher than on row-level DML.

### Load Partially Applied

The load started but didn't finish -- rows were written but the job died mid-stream. What happens next depends on the load strategy: full replace and partition swaps are idempotent and can be safely rerun since the incomplete load gets overwritten. Append may have produced duplicates that need deduplication ([[06-operating-the-pipeline/0613-duplicate-detection|0613]]). A merge may be partially applied -- some rows updated, others not -- leaving the table in an inconsistent state where the same extraction's data is half-landed. See [[04-load-strategies/0406-reliable-loads|0406]] for making the load step itself resilient to interruption.

## Recovery Strategy

### Per-Table Retry

The first principle: retry only what failed, not the entire job. If 97/100 tables succeeded, rerunning all 100 wastes compute, risks introducing new failures on previously successful tables, and delays recovery. Your orchestrator should support re-running individual tables from a failed run -- if it doesn't, this is worth building, because the alternative is choosing between "rerun everything" and "wait for the next schedule." Some orchestrators support this natively -- Dagster lets you retry individual failed assets from a run's status page without touching the ones that succeeded (see [[08-appendix/0805-orchestrators|0805]]).

The retry should also target the right step. A table that failed at extraction needs re-extraction; a table that extracted successfully but failed at load only needs the load retried -- preferably from the data already in staging, not from a fresh extraction that hits the source again for no reason.

### Staging as a Safety Net

If staging tables persist after extraction, a load failure can be retried from staging without hitting the source again. This is the faster recovery path and the one that's gentler on the source system -- the data is already extracted, you just need to land it. The tradeoff is storage cost: persistent staging means keeping a copy of every extracted table until the load confirms success (see [[06-operating-the-pipeline/0603-cost-monitoring|0603]]). For most tables the cost is trivial; for a few massive ones it may matter.

If staging is ephemeral, a failed load requires full re-extraction. Whether that's acceptable depends on how expensive the extraction is and how soon the data needs to land. For small tables on a healthy source, re-extraction is fast and harmless. For a 50M-row table on a slow on-prem database during business hours, you may have to wait until the next safe window ([[06-operating-the-pipeline/0607-source-system-etiquette|0607]]).

### Per-Table Status Tracking

Track each table's lifecycle explicitly: `extracting` -> `extracted` -> `loading` -> `loaded` / `failed`. On restart, tables stuck in `loading` are failed tables, not running ones -- treat them accordingly. A table that's been in `loading` for longer than its expected load duration either crashed or is hanging, and leaving it in limbo means nobody investigates.

The health table ([[06-operating-the-pipeline/0602-health-table|0602]]) should record the outcome per table per run -- not just `success` / `failure` but which step failed and why. This is what makes per-table retry possible: without a record of where each table stopped, every retry is a guess.

## Alerting on Partial Failures

Any failure, no matter how small, should mark the pipeline run as failed. A run where 197 tables succeeded and 3 failed is a failed run -- not a successful run with caveats. If your orchestrator reports it as success, the 3 broken tables disappear into the noise and nobody investigates until a consumer complains. The run status should be unambiguous: if anything didn't land, the run failed.

The tension is failure fatigue. If the pipeline fails every single run because one flaky table times out on Mondays, the team learns to ignore the failure status -- and the one time 50 tables fail for a real reason, nobody notices because the alert looks the same as every other Monday. Your alerting ([[06-operating-the-pipeline/0605-alerting-and-notifications|0605]]) needs to distinguish between the two: include the count of failed tables, which ones, which step failed, and whether the failure is retryable. "Run failed: 3 tables (invoices, order_lines, products) -- extraction timeout, auto-retry exhausted" is actionable. "Run failed" with no context trains people to click dismiss.

## Anti-Patterns

> [!danger] Don't rerun the entire pipeline to fix 3 failed tables
> Retry only what failed. Rerunning everything wastes compute, risks new failures on previously successful tables, and delays recovery.

> [!danger] Don't leave tables stuck in a loading state after a crash
> A table in `loading` after the run process has died is a failed table. If your recovery logic doesn't detect and reset orphaned states, those tables sit in limbo indefinitely -- neither loaded nor marked for retry.

## Related Patterns

- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- cursor advances only after confirmed load; partial failures don't create gaps
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- no cursor state, so partial failure recovery is automatic on next run
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- idempotent loads that survive interruption
- [[06-operating-the-pipeline/0602-health-table|0602-health-table]] -- per-table per-run outcome tracking
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- partial failures must alert, not just log
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- gates prevent load on suspect extraction results
- [[06-operating-the-pipeline/0613-duplicate-detection|0613-duplicate-detection]] -- deduplication after a partially applied append
- [[06-operating-the-pipeline/0615-recovery-from-corruption|0615-recovery-from-corruption]] -- when partial failure leads to corrupted data
