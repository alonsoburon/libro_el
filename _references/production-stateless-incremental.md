---
title: "Stateless Incremental Loading -- Why and How"
type: reference
tags:
  - reference/production
  - reference/incremental
relevant_chapters:
  - 0303-stateless-window-extraction
  - 0109-idempotency
  - 0406-reliable-loads
  - 0612-partial-failure-recovery
---

# Stateless Incremental Loading in Production

Source: Warp production system. Every incremental run is stateless -- no cursor state persisted between runs.

## The Core Idea

Instead of tracking "last extracted timestamp" in a state store, extract a fixed trailing window every run:

```sql
WHERE UpdateDate >= DATEADD(day, -7, GETDATE())
```

The window (7 days by default) is wide enough to catch:
- Late-arriving updates (row modified 3 days ago, `UpdateDate` only set today)
- Missed rows from failed runs
- Source timezone drift

Combined with a merge that deduplicates on PK, re-extracting the same row multiple times is harmless.

## Why Not Stateful Cursors?

Stateful cursors (`WHERE UpdateDate > :last_high_water_mark`) fail when:

1. **Run fails mid-extraction:** The cursor advances but not all rows were loaded. Gap appears.
2. **Source resets UpdateDate:** ERP migration, manual correction, bulk update. Cursor is ahead of the data.
3. **Multiple pipeline instances:** Two runs overlap, both advance the cursor. One's data clobbers the other's.
4. **Infrastructure rebuild:** New server, new container, cursor state is gone.

Each failure mode requires its own recovery mechanism. Stateless extraction eliminates all of them: every run extracts the same window, every run produces the same result.

## The Cost of Stateless

You re-extract ~7 days of data on every run. For most tables, this is a few thousand rows -- negligible. For large tables (millions of rows per day), adjust the window:

| Table size | Window | Rationale |
|-----------|--------|-----------|
| < 100K rows/day | 7 days | Safe default, catches everything |
| 100K-1M rows/day | 3 days | Reduce extraction volume |
| > 1M rows/day | 1 day | Minimize source load, accept tighter recovery margin |

## The Merge Makes It Safe

Stateless extraction produces duplicates (same row extracted in consecutive runs). The merge handles this:

```sql
-- Staging has duplicates? QUALIFY deduplicates
INSERT INTO destination
SELECT *
FROM staging
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _dlt_id) = 1;
```

The DELETE+INSERT merge deletes the old version first, then inserts the new version. If the "new" version is identical to the old, the net effect is zero change.

## Pipeline Directory: Nuke It

Loader tools (dlt, etc.) store state in a local "pipeline directory" -- schema caches, load history, cursor checkpoints. For stateless operation, delete this directory before every run:

```python
if os.path.exists(pipeline_dir):
    shutil.rmtree(pipeline_dir)
```

This prevents:
- **Stale schema caches** from causing 404s on deleted staging tables
- **Pending packages** from failed runs being retried against nonexistent tables
- **Accumulated state** from interfering with fresh extractions

If `rmtree` fails (locked files), it means another process is using the same pipeline directory -- a duplicate execution that should fail loudly, not silently continue.

## Loader Schema Cache: The Hidden State

Even with a nuked pipeline directory, loaders may cache schema metadata in the DESTINATION (not locally). dlt stores schema hashes in `_dlt_version` table. If the staging table is deleted but the schema cache survives, the next load skips table creation and fails.

**Detection:**
```sql
SELECT * FROM `project.dataset_staging._dlt_version`
ORDER BY inserted_at DESC
```

**Fix options (in order of preference):**
1. Loader's built-in refresh mechanism (`refresh="drop_resources"` in dlt)
2. Delete cache entries before each load (targeted, per-pipeline)
3. Delete the entire cache table (dangerous for concurrent loads)

## Backfill with start_date

For historical backfills, override the window with an explicit `start_date`:

```
start_date=2024-01-01
```

This extracts from January 1st instead of the trailing 7-day window. Combined with the merge, it's safe to run alongside regular incremental runs.

## Recovery Is Automatic

The biggest advantage of stateless: recovery is a no-op. If a run fails:
1. Staging table may or may not exist (doesn't matter)
2. No cursor was advanced (there is no cursor)
3. Next run extracts the same window + any new data
4. Merge applies cleanly

No manual intervention, no cursor reset, no gap analysis.
