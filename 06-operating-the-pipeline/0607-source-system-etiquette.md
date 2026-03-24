---
title: "Source System Etiquette"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Source System Etiquette

> **One-liner:** Don't make the DBA want to kill you.

## The Problem
- Your pipeline is a guest on someone else's database -- a production system serving real users and real transactions
- READ-ONLY access doesn't mean zero impact: a full table scan on a 50M row table locks pages, consumes I/O, and competes with the application for CPU and memory
- The DBA's job is to keep the application fast; your extraction is a threat to that, and they will revoke your access if you abuse it

## Rules of Engagement

### Know Your Source
- Is it a transactional OLTP database serving a live application? (high sensitivity)
- Is it a reporting replica or read replica? (lower sensitivity, but still shared)
- Is it an ERP where the vendor controls the schema and you have no leverage? (tread carefully)

### Cursor Columns Are Probably Not Indexed
- `updated_at`, `UpdateDate`, `CreateDate` -- the columns your incremental queries filter on are almost never indexed at the source
- Your `WHERE updated_at > :cursor` forces a full table scan every run on an unindexed column
- Ask the DBA to add an index; if they can't (vendor-controlled schema), schedule extractions for off-peak hours

### Respect Business Hours
- Configure safe hours per source system -- a window (typically 19:00-06:00) for large extractions
- Small incremental pulls during business hours are usually fine; full table scans and backfills are not
- Tables above a configurable row count threshold should be automatically deferred to the safe window (see [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]])

### Limit Concurrency
- N parallel extractions against the same source multiply the load; cap concurrent connections per source
- 3-5 concurrent queries is a reasonable starting point for most transactional sources; tune based on the DBA's feedback
- Your orchestrator's tag-based concurrency limits or run queues enforce this

### Set Timeouts
- Set query timeouts explicitly -- 5 minutes for regular extractions, 15-20 minutes for known large tables
- A query that runs for 45 minutes without a timeout is holding a connection, consuming source resources, and probably blocking something
- When a query times out, fail the table explicitly (see [[06-operating-the-pipeline/0610-extraction-status-gates|0610]]); don't retry immediately

## What You Can Never Do
- No triggers, stored procedures, views, or temp tables on someone else's production database
- No schema modifications -- even adding an index requires explicit DBA approval
- No writes of any kind -- you are a reader, period

## Building Trust with the DBA
- Share your schedule: what you extract, when, how often, how much data
- Report your own impact: query duration, rows scanned, connection time (see [[06-operating-the-pipeline/0601-monitoring-observability|0601]], source health)
- When your query causes a slowdown, own it and fix the schedule before they have to ask

## Anti-Pattern

> [!danger] "It's a read replica, so it doesn't matter"
> - Read replicas share storage or lag behind primary on the same hardware. A full scan on a replica can saturate disk I/O, increase replication lag, and affect the primary indirectly. Treat replicas with the same care, just a wider tolerance.

## Related Patterns
- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- source health metrics measure your impact
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606-scheduling-and-dependencies]] -- safe hours and massive table thresholds
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- timeout handling and explicit failure
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- full scans are the highest-impact extraction pattern

## Notes
- **Author prompt -- unindexed cursors**: You mention in warp's CLAUDE.md that cursor columns (UpdateDate, CreateDate) are "often NOT indexed." Has a client's DBA ever complained about your queries doing full scans on these columns? What happened?
- **Author prompt -- timeout thresholds**: You set 20-minute timeouts for active records queries. How did you arrive at that number? Was there an incident where a query ran longer and caused problems?
- **Author prompt -- access revoked**: Have you ever actually had access revoked or threatened by a client's IT team because your pipeline was too aggressive?
- **Author prompt -- read-only constraint**: The warp architecture is explicitly READ-ONLY on client databases. Was there ever a moment where you wished you could add an index or a view on the source? What was the workaround?
- **Author prompt -- business hours**: Do clients actually notice extraction load during business hours, or is the safe hours config mostly precautionary? Any concrete incidents?
