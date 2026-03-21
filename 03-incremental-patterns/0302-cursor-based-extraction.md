---
title: "Cursor-Based Timestamp Extraction"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-06
updated: 2026-03-13
---

# Cursor-Based Timestamp Extraction

> **One-liner:** Track a cursor -- the high-water mark of the last successful run. Each run extracts only rows updated after that point.

See [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] for when `updated_at` lies, how to validate it, and when to run a periodic full replace.

---

## How It Works

```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= :last_run;
```

After a confirmed successful load, advance the cursor to the current timestamp. On the next run, use the new value.

### Where to Store the Cursor

**Option 1: `MAX(updated_at)` from the destination.**

```sql
-- source: columnar
SELECT MAX(updated_at) AS last_run
FROM orders;
```

Simple, zero extra infrastructure, self-contained. The cursor lives in the data.

The risk: it's tied to what's actually in the destination. If the destination is rebuilt, truncated, or has rows with stale timestamps from a bad load, the max is wrong. A cursor that's too low causes re-extraction (harmless, upsert handles it). A cursor that's too high skips rows permanently.

**Option 2: External state store.** Orchestrator metadata, a dedicated state table, a key-value store. Survives destination rebuilds and is decoupled from data quality.

The risk: more moving parts. If a load partially succeeds and the cursor advances anyway, you have a permanent gap.

Both are valid. `MAX` from destination is the simpler default. External state earns its overhead when destination rebuilds are a real operational scenario.

> [!warning] Advance the cursor only after a confirmed successful load
> A partial load followed by a cursor advance is a permanent gap. The rows in the failed batch will never be re-extracted. Treat cursor advancement as the final step of the pipeline, gated on load confirmation -- not something that happens at the start of the next run.

### Boundary Handling

Always use `>=` not `>`. A missed row has no recovery path; a duplicate row is handled by the destination's upsert.

Add a small buffer on the lower bound (5--30 seconds) to absorb clock skew between source and extractor. The overlap mechanism is the same as [[03-incremental-patterns/0309-late-arriving-data|0309-late-arriving-data]] -- just measured in seconds instead of hours.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> PostgreSQL `TIMESTAMPTZ` maps cleanly to BigQuery `TIMESTAMP`. MySQL `DATETIME` has no timezone and second-level precision -- the buffer compensates. A cursor limits the extracted row count but doesn't eliminate the destination load cost -- see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] for the MERGE cost anatomy.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> `MAX(updated_at)` from the destination is cheap -- a simple indexed column scan. The buffer overlap produces duplicates; the destination upsert handles them (see [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]]).

---

## Related Patterns

- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301-timestamp-extraction-foundations]] -- when `updated_at` lies, validation checklist, periodic full replace
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- cursor-free alternative; always re-extracts a fixed trailing window
- [[03-incremental-patterns/0305-sequential-id-cursor|0305-sequential-id-cursor]] -- when `updated_at` doesn't exist but the PK is monotonic
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- checkpointing, atomicity, and what "confirmed successful load" actually means
- [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]] -- when the trigger fires on UPDATE only and INSERT rows are invisible
