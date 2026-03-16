---
title: "Cost Monitoring"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Cost Monitoring

> **One-liner:** Per-table, per-query, per-consumer -- know where the money goes before the invoice arrives.

## The Problem
- Cloud data warehouses bill by bytes scanned, slots consumed, or storage volume, and the bill arrives after the damage is done
- A single bad query pattern -- an unpartitioned scan, a MERGE on a table that should be a full replace, a staging dataset nobody cleaned up -- can dominate the monthly bill
- Without per-table cost attribution, "costs went up 40%" is a mystery; with it, you can point at the table and the pattern that caused it

## What to Track

### Compute Costs
- Bytes scanned per load operation (the MERGE, the DELETE+INSERT, the partition copy)
- Slot-seconds or query-seconds per table -- columnar engines charge for compute differently, but every one exposes it
- Cost per run vs cost per table vs cost per schedule -- same data, three useful aggregations

### Storage Costs
- Table size growth over time -- append-only tables grow forever; is that growth expected or a sign of missing cleanup?
- Staging dataset lifecycle -- staging tables that outlive their load job are dead weight (see [[06-operating-the-pipeline/0610-backfill-strategies|0610]] for when staging intentionally persists)
- Snapshot tables accumulate fast; [[02-full-replace-patterns/0202-snapshot-append|0202]] needs a retention policy or storage grows linearly with schedule frequency

### Extraction Costs
- Source-side query cost -- some sources meter reads (API rate limits, licensed query slots)
- Overlapping extraction windows in incremental-with-lag reload the same rows repeatedly; the overlap is intentional, but its cost should be visible

## Cost Attribution

- Tag every load with the table name, schedule name, and run ID so destination-side cost logs can be joined back to the pipeline
- Your orchestrator's run metadata is the join key -- if the destination's audit log doesn't capture it, inject it as a job label or query comment

## The Expensive Patterns

| Pattern | Why it's expensive | Mitigation |
|---|---|---|
| MERGE on large tables | Full scan of both sides | Partition-scoped merge, or switch to [[04-load-strategies/0401-full-replace\|0401]] |
| Unpartitioned full scan | Every query reads the entire table | Partition by date, enforce `require_partition_filter` |
| Staging cleanup missed | Orphaned staging datasets accumulate storage | Scheduled cleanup job (weekly or after each run) |
| Snapshot append without TTL | Storage grows linearly with schedule frequency | Retention policy: drop snapshots older than N days |

## Anti-Pattern

> [!danger] Don't let cost optimization drive pattern selection
> - Switching from full replace to incremental to "save money" introduces complexity that costs engineering time and creates failure modes (see [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108]]). The cheaper pipeline is the one that breaks less, not the one with the lowest bytes-scanned number.
> - Pick the pattern that's correct, then optimize its cost.

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- cost is one dimension of the broader observability surface
- [[06-operating-the-pipeline/0607-tiered-freshness|0607-tiered-freshness]] -- tiered schedules are partly a cost strategy: don't refresh history at the same cadence as current data
- [[02-full-replace-patterns/0202-snapshot-append|0202-snapshot-append]] -- snapshot storage grows without a retention policy
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- MERGE is the most expensive load operation in columnar engines

## Notes
- **Author prompt -- staging_staging**: The `staging_staging` dataset bug in warp -- orphaned staging datasets accumulating in BigQuery. Did you ever get a cost surprise from staging that wasn't cleaned up? How big did it get before you noticed?
	- Only slight cost from storing staging with all appended data loaded and not cleaning it up. It wasn't a big deal.
- **Author prompt -- MERGE vs alternatives**: You switched from `staging-optimized` to `truncate-and-insert` for replace_strategy. Was that partly a cost decision, or purely correctness? Have you compared the BigQuery bill between MERGE-heavy and partition-copy-heavy load patterns?
	- Too DLT centric question. but Merge heavy is definetly a lot more expensive. Watch out for that.
- **Author prompt -- the cleanup job**: The `_dlt_cleanup` job runs Saturday 10pm. Before that existed, what was the cost of accumulated DLT state tables and staging datasets across 35+ clients?
	- almost nothing. The cleanup job is specifically to avoid schema bugs since dlt is so stateful. dont include here and bad question.
- **Author prompt -- cost attribution**: Do you currently track cost per client / per table in BigQuery, or is it still aggregated? Has a single client or table ever dominated the bill unexpectedly?
	- I do track to the maximum level, using drilldowns in powerBI. I don't actually check this too often though, if aggregated doesn't trigger my curiosity, its probably tankable.
- IF you're vibe coding or unsure about results, try the pattern with not a MASSIVE table first, then with a massive one, then with all of them. Measure twice, cut once.
