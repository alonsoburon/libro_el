---
title: "Partial Failure Recovery"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Partial Failure Recovery

> **One-liner:** Half the batch loaded, the other half didn't -- now what?

## The Problem
- A pipeline run that processes multiple tables can fail partway through: 40 tables succeeded, 10 failed
- The cursor advanced for the successful tables but not for the failed ones -- or worse, the cursor advanced for all of them because the extraction succeeded but the load didn't
- Rerunning the entire job wastes time reprocessing the 40 tables that already landed correctly
- Not rerunning leaves 10 tables stale, and the next scheduled run may not pick up the gap if the cursor already moved

## Failure Modes

### Extraction Succeeded, Load Failed
- The data was extracted correctly but the destination rejected it (quota, permission, schema mismatch)
- The extraction is valid; retry only the load step
- If the cursor already advanced past this data, you need the extracted data in staging or you'll lose it

### Extraction Failed for Some Tables
- Some tables extracted successfully, others hit a timeout or connection error
- Successful tables can proceed to load; failed tables need re-extraction
- Your orchestrator should track per-table status, not just per-run status

### Load Partially Applied
- The load started but didn't complete -- rows were written but the job died mid-stream
- Depending on the load strategy: full replace is idempotent (rerun safely), append may have duplicates, merge may be partially applied
- See [[04-load-strategies/0406-reliable-loads|0406]] for making loads idempotent

## Recovery Strategies

### Per-Table Retry
- Retry only the failed tables, not the entire job
- Your orchestrator should support re-running individual tables from a failed run
- The successful tables don't need reprocessing

### Staging as a Safety Net
- If staging persists after extraction, a failed load can be retried from staging without re-extracting
- If staging is ephemeral (cleaned up per run), a failed load means re-extraction is required
- The tradeoff: persistent staging costs storage but enables faster recovery

### State Machine Discipline
- Track each table's lifecycle explicitly: `extracting` → `extracted` → `loading` → `loaded` / `failed`
- On restart, detect tables stuck in `loading` and either retry or mark them as failed
- Don't leave tables in limbo -- a table stuck in `loading` after a crash is a failed table, not a running one

## Cursor Safety

- The cursor must not advance until the load is confirmed successful (see [[04-load-strategies/0406-reliable-loads|0406]])
- If extraction succeeds but the load fails, the cursor should stay where it was so the next run re-extracts the same window
- If the cursor already advanced (because extraction and cursor advancement aren't atomic), you have a gap -- the data exists only in staging (if it's still there) or must be re-extracted with explicit date overrides

## Anti-Pattern

> [!danger] Don't rerun the entire pipeline to fix 3 failed tables
> - If 97 tables succeeded, rerunning all 100 wastes compute, risks introducing new failures on previously successful tables, and delays recovery. Retry only what failed.

> [!danger] Don't ignore partial failures
> - "Most of the tables loaded" is not success. 10 stale tables is 10 stale tables, and the staleness compounds with every run that doesn't fix them. Track and retry.

## Related Patterns
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- idempotent loads and cursor safety
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- partial failures should alert, not just log
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- gates prevent cursor advancement on failed extraction
- [[06-operating-the-pipeline/0615-recovery-from-corruption|0615-recovery-from-corruption]] -- when partial failure leads to corrupted data

## Notes
- **Author prompt -- orphaned loading state**: The warp integration docs describe detecting jobs stuck in "loading" after a restart. Has this actually happened in production? How did you detect and recover?
- **Author prompt -- partial failure frequency**: With 35+ clients and ~6500 tables, how often do partial failures happen? Daily? Weekly? Is it mostly connection timeouts, or are there other common causes?
- **Author prompt -- retry granularity**: Can you retry a single failed asset from a Dagster run, or do you have to rerun the whole job? Has the retry granularity ever been a bottleneck?
- **Author prompt -- cursor gap**: Have you ever had a situation where the cursor advanced past data that didn't load -- creating a permanent gap? How did you discover it and what was the fix?
