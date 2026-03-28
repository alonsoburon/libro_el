---
title: "Source System Etiquette"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-26
---

# Source System Etiquette

> **One-liner:** Your pipeline is a guest on someone else's production database. Act like it.

## The Problem

READ-ONLY access doesn't mean zero impact. A full table scan on a 50-million-row table locks pages, consumes I/O, and competes with the application for CPU and memory -- and the DBA watching the monitoring dashboard doesn't care that your query is a harmless SELECT. Their job is to keep the application fast for the users who generate revenue; your extraction is a background process that, from their perspective, exists only to slow things down. If you're careless about when and how you extract, you'll lose access -- and if you're unlucky, you'll bring the database down on your way out.

I had a client whose IT team didn't mention they ran full database backups between 5 and 6 AM. A load failed overnight, and the automatic retry kicked in at 5:30 AM -- right on top of the backup window. The database went down. It was back up within the hour, but the conversation about revoking our access lasted a week. The mistake wasn't the retry logic; it was not knowing the source's maintenance windows.

## Know Your Source

The sensitivity of a source system determines how carefully you need to tread. Before writing the first extraction query, understand what you're connecting to:

**Production OLTP** -- a live transactional database serving the application's users. Every query competes with their transactions. Full scans lock pages, long reads block writes on some engines (see the SQL Server warning in [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]]), and a bad retry at the wrong time can cascade into an outage. Treat these with maximum care: off-peak scheduling, conservative concurrency, explicit timeouts.

**Read replica** -- lower sensitivity, but not zero. Replicas share storage I/O with the primary or lag behind it on the same hardware. A full scan on a replica can saturate disk throughput, increase replication lag, and degrade the primary indirectly. Treat replicas with the same patterns, just wider tolerances -- more concurrent queries, wider safe-hours windows.

**Vendor-controlled ERP** -- systems like SAP where the vendor owns the schema and you have no leverage to change it. You can't add indexes, you can't create views, and the timestamp columns your incremental queries need were designed for the application's audit trail, not for your `WHERE` clause. Tread carefully and accept that some extractions will be slower than you'd like.

## The Pattern

### Check Your Cursor Columns

`updated_at`, `UpdateDate`, `CreateDate` -- the columns your incremental queries filter on exist for the application, not for your extraction. Check whether they're indexed before assuming your `WHERE updated_at > :cursor` will be fast. If they're not indexed, you're forcing a full table scan every run, and the DBA will notice before you do.

Ask the DBA to add an index. This is more achievable than it sounds -- adding an index on a timestamp column is a low-risk change that benefits anyone querying by date, and technical stakeholders on the source side often stand to gain from it too. We've had clients proactively add indexes after noticing our scans were slow, before we even asked. It's a soft rule -- officially read-only, but the performance improvement is large enough that most DBAs will cooperate.

If they can't add an index -- vendor-controlled schemas sometimes make this difficult or unsupported -- schedule those extractions for off-peak hours and accept that the scan will be heavier than ideal (see [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]], safe hours).

### Respect Business Hours

Whether extraction load during business hours is a problem depends entirely on the database and the client. Some clients proactively ask for intraday updates and are willing to absorb the source load. Others will escalate immediately if they see any query from your pipeline during working hours. This is a conversation for the SLA stage (see [[06-operating-the-pipeline/0604-sla-management|0604]]) -- agree on what hours are acceptable before the pipeline goes live, not after the first complaint.

As a baseline: small incremental pulls during business hours are usually fine on a healthy source, because the query filters on a recent cursor and touches a small number of rows. Full table scans and backfills are a different story -- they read the entire table and should be gated behind a safe-hours window. Enforce this automatically for very weak or very massive databases by deferring tables above a row-count threshold to the off-peak window (see [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]]). Sources in the middle ground -- decent hardware, moderate table sizes -- generally don't need the gate.

> [!warning] Know the source's maintenance windows
> Backup jobs, index rebuilds, integrity checks -- these run during off-peak hours too, which means your "safe" extraction window may overlap with the source's heaviest internal workload. Ask the DBA for their maintenance schedule and avoid stacking your largest extractions on top of their backup window.

### Limit Concurrency

Multiple parallel extractions against the same source multiply the load. Cap concurrent connections per source system -- not per pipeline or per schedule, because the source doesn't care which schedule spawned the query. 3 to 5 concurrent extractions is a reasonable starting point for a typical transactional database; tune based on the DBA's feedback and the source's monitoring (see [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]] for the full treatment of parallelism tradeoffs).

### Set Timeouts

Set query timeouts explicitly. A query that runs for hours without a timeout is holding a connection, consuming source resources, and probably blocking something. When a query times out, fail the table explicitly (see [[06-operating-the-pipeline/0610-extraction-status-gates|0610]]) -- don't retry immediately, because the condition that caused the timeout is likely still present.

Timeout thresholds depend on whether you're reading in batches. For unbatched reads, keep timeouts tight: a few minutes for regular tables, longer for known large ones. For batched reads (see below), individual batch timeouts can be shorter since each batch is small, while the overall extraction can run for hours.

### Batched Reads for Massive Tables

For tables too large to extract in a single query within a reasonable time, SQLAlchemy's `yield_per()` with `stream_results=True` lets you read in batches using a server-side cursor. Each batch is small and fast -- 100,000 rows is a solid default -- even if the full read takes hours. This keeps your pipeline's memory flat (you're never holding millions of rows at once, just the current batch) and reduces the per-query impact on the source.

The tradeoff: you hold an open connection and server-side cursor for the entire duration, so the source is occupied for longer even though the per-second load is lighter. Schedule batched reads for off-peak hours, and make sure the source's connection pool can accommodate a long-lived session alongside normal application traffic.

```sql
-- Batched read: 100k rows at a time, server-side cursor
-- source: transactional
-- engine: sqlalchemy (pseudocode)

with engine.connect() as conn:
    result = conn.execution_options(
        stream_results=True
    ).execute(
        text("SELECT * FROM orders")
    )
    for batch in result.yield_per(100_000):
        load_batch(batch)
```

## What You Can and Can't Do

**Never**: triggers, stored procedures, temp tables, or writes of any kind on someone else's production database. You are a reader.

**Schema modifications** -- officially off-limits without DBA approval, but adding an index is worth asking for. It's a low-risk change with high payoff, and framing it as a performance improvement that benefits the application (not just your pipeline) makes the conversation easier.

**Views** -- useful when downstream needs a subset of data that would be expensive to reconstruct from base tables. The recommended approach: build the query in your destination first, validate it works, then send the "translated" query to the DBA and ask them to create the view on the source. This keeps the DBA in control of their schema while giving you a stable, optimized read target.

## Building Trust with the DBA

The relationship with the source team determines how much access you keep and how much flexibility you get. A DBA who trusts your pipeline will add indexes, extend your safe hours, and warn you before maintenance windows. A DBA who doesn't trust you will restrict your hours, throttle your connections, and eventually revoke access.

**Share your schedule** -- what you extract, when, how often, how much data. No surprises.

**Report your own impact** -- query duration, rows scanned, connection time. If you can show the source team that your extraction uses a small fraction of their database's capacity, you've answered the question before they ask it. The source health metrics from [[06-operating-the-pipeline/0601-monitoring-observability|0601]] give you the numbers.

**Own your incidents** -- when your query causes a slowdown, acknowledge it and fix the schedule before they have to ask. Nothing destroys trust faster than a DBA discovering your pipeline caused an issue and you didn't notice or didn't say anything.

## Anti-Patterns

> [!danger] "It's a read replica, so it doesn't matter"
> Read replicas share storage or lag behind the primary on the same hardware. A full scan on a replica can saturate disk I/O, increase replication lag, and affect the primary indirectly. Treat replicas with the same patterns, just wider tolerances.

> [!danger] Don't retry failed extractions blindly
> A retry that hits the source during a backup window, a peak traffic period, or while the condition that caused the failure is still present makes things worse, not better. Retry logic should respect safe hours and back off on repeated failures rather than hammering the source immediately.

> [!danger] Don't assume the DBA knows you exist
> If nobody on the source team knows your pipeline connects to their database, the first time they find out will be during an incident -- which is the worst possible time to introduce yourself. Establish the relationship before you go live.

## Related Patterns

- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- source health metrics measure your impact
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606-scheduling-and-dependencies]] -- safe hours, concurrency limits, and parallelism tradeoffs
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- timeout handling and explicit failure
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- full scans are the highest-impact extraction pattern
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- business hours and freshness expectations are agreed during SLA stage

## What Comes Next

[[06-operating-the-pipeline/0608-tiered-freshness|0608]] covers how to assign different update frequencies to different tables based on consumer needs -- the framework that determines which tables run during business hours, which wait for the off-peak window, and which only need a daily refresh.
