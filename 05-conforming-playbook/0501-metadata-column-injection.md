---
title: "Metadata Column Injection"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Metadata Column Injection

> **One-liner:** `_extracted_at`, `_batch_id`, `_source_hash` -- columns the source doesn't have that your pipeline needs for debugging, dedup, and reconciliation.

---

## The Playbook

Metadata columns are just new columns added to the extraction query. Every destination supports them -- columnar or transactional, doesn't matter. You're not changing what the data means; you're tagging each row with information about how and when it arrived.

Three metadata columns, each with a different purpose and a different cost/benefit ratio. Not every table needs all three.

```sql
-- source: transactional
SELECT
    order_id,
    customer_id,
    status,
    total,
    updated_at,
    -- metadata columns
    CURRENT_TIMESTAMP                          AS _extracted_at,
    :batch_id                                  AS _batch_id,
    MD5(CONCAT(order_id, '|', status, '|', total)) AS _source_hash
FROM orders
WHERE updated_at >= :last_run;
```

---

## `_extracted_at`

The pipeline's timestamp: when your extraction ran, not when the source row was last modified. A row updated 3 days ago and extracted today has `_extracted_at = today`. This distinction matters because `updated_at` is the source's clock -- maintained by the application layer, subject to all the reliability problems covered in [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] -- while `_extracted_at` is your clock, set by your pipeline, and always correct.

Always add this. The cost is trivial (`CURRENT_TIMESTAMP` in the SELECT) and the debugging value is enormous. When something goes wrong -- and it will -- `_extracted_at` is how you answer "when did this bad data arrive?" and "which extraction run brought it?"

`_extracted_at` is also the foundation for dedup ordering in [[04-load-strategies/0404-append-and-materialize|0404]]. The `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1` view depends entirely on this column to determine which version of a row is the latest. Without it, the dedup view has no ordering key and the pattern doesn't work.

> [!tip] Use the same `_extracted_at` for all tables in a run
> If you extract `orders` and `order_lines` in the same pipeline run, they should share the same `_extracted_at` value. This makes it easy to identify which rows were extracted together and simplifies cross-table debugging. Set the timestamp once at the start of the run and pass it to every extraction query.

---

## `_batch_id`

Correlates all rows from the same extraction run. Where `_extracted_at` tells you *when*, `_batch_id` tells you *which run* -- and that distinction matters when you have multiple runs with the same timestamp (retries, overlapping schedules) or when you need to operate on an entire batch at once.

Three use cases earn the column:

**Rollback.** "Batch 47 loaded bad data. Delete everything from batch 47." With `_batch_id`, that's a single `DELETE WHERE _batch_id = 47`. Without it, you're reverse-engineering which rows came from that run using timestamp ranges and hoping you don't catch rows from adjacent runs.

**Debugging.** "The destination has 11,998 rows but the source had 12,000. Which batch lost them?" With `_batch_id`, you can trace each row to the run that loaded it and compare batch-level counts against source-side logs.

**Reconciliation.** A `_batches` table that tracks batch-level metadata -- source row count, extraction start/end time, status -- gives you an audit trail for every extraction. When [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]] compares source and destination counts, `_batch_id` is the join key.

UUID or sequential integer -- consistency matters more than format. If your orchestrator already generates run IDs, reuse those.

### The `_batches` Table

A lightweight metadata table on the destination that tracks each extraction run:

```sql
-- destination: any
CREATE TABLE _batches (
    batch_id        TEXT PRIMARY KEY,
    table_name      TEXT NOT NULL,
    extracted_at    TIMESTAMP NOT NULL,
    source_row_count INTEGER,
    dest_row_count  INTEGER,
    status          TEXT DEFAULT 'running',  -- running, completed, failed
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP
);
```

Your pipeline writes a row to `_batches` at the start of each run (`status = 'running'`), updates it after load completes (`status = 'completed'`, `dest_row_count` filled in), and marks it failed on error. This gives you a single place to answer "when did each table last load successfully?" and "which tables are currently loading?" -- questions that become surprisingly hard to answer without it.

---

## `_source_hash`

A hash of the source row at extraction time. Enables [[02-full-replace-patterns/0208-hash-based-change-detection|0208]] (compare hashes between runs to detect changes without relying on `updated_at`) and post-load reconciliation (compare source-side hash vs destination-side hash to verify the row arrived intact).

```sql
-- source: transactional
-- Hash all business columns, excluding _extracted_at and _batch_id
SELECT
    *,
    MD5(CONCAT(
        COALESCE(order_id::TEXT, '__NULL__'), '|',
        COALESCE(status, '__NULL__'), '|',
        COALESCE(total::TEXT, '__NULL__')
    )) AS _source_hash
FROM orders;
```

**Expensive at scale.** Hashing every row adds compute on the source or in the pipeline. At ~800 tables, this can add 20 minutes to an already tight extraction window. The cost is proportional to row count × column count -- wide tables with millions of rows are where it hurts.

**Tiered approach.** Not every table earns the overhead. High-value mutable tables (`invoices`, `orders`) where change detection matters and `updated_at` is unreliable -- those earn `_source_hash`. Stable config tables that change once a quarter, append-only tables like `events` where you never need to detect mutations -- skip it.

**NULL handling.** COALESCE every column to a sentinel before hashing. Most hash functions return NULL if any input is NULL, which means a row with a single NULL column produces a NULL hash -- indistinguishable from every other row with a NULL in the same position. `COALESCE(col, '__NULL__')` before concatenation prevents this.

> [!warning] Hash the business columns, not the metadata
> Exclude `_extracted_at` and `_batch_id` from the hash input. These change every run by design -- including them means the hash changes every run too, defeating the purpose of change detection.

---

## Where to Inject

Every metadata column runs *somewhere* -- in the source query, in Python between extraction and load, or in a staging transform on the destination. The choice depends on what the source can handle and how much compute you're willing to add to the extraction.

**Source query.** Cheapest if the source can handle it. `CURRENT_TIMESTAMP` is free on every engine. `MD5()` is available on PostgreSQL, MySQL, SQL Server, and SAP HANA with slightly different syntax. The conforming happens in the same query that extracts the data -- no extra hop, no extra infrastructure.

**Orchestrator / middleware.** Python adds the columns after extraction, before load. More control (you can use a consistent hashing library across all sources regardless of engine), but you're adding an extra data hop and holding the full batch in memory or on disk while you process it.

**Staging.** Land the raw data without metadata, then add the columns in a staging transform on the destination. Works well when you want to keep the extraction query minimal and offload all conforming to the destination's compute. Common in BigQuery workflows where staging + transform is the standard pattern.

For `_extracted_at` and `_batch_id`, the source query is almost always the right place -- the cost is negligible. For `_source_hash`, the source query or Python are both reasonable depending on whether your source engine has a convenient hash function and whether the compute cost on the source is acceptable.

---

## By Corridor

> [!example]- Transactional → Columnar
> No special considerations. Columnar destinations accept new columns without issue. If you're using [[04-load-strategies/0404-append-and-materialize|0404]], `_extracted_at` is the dedup ordering key -- make sure it's populated on every row.

> [!example]- Transactional → Transactional
> Same approach. One advantage: if you need to add metadata columns to an existing destination table retroactively, `ALTER TABLE ADD COLUMN` is cheap and instant on most transactional engines. On columnar engines it's also cheap, but backfilling the column for historical rows is more expensive.

---

## Related Patterns

- [[04-load-strategies/0404-append-and-materialize|0404]] -- `_extracted_at` as the dedup ordering key
- [[02-full-replace-patterns/0208-hash-based-change-detection|0208]] -- `_source_hash` enables hash-based change detection
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614]] -- `_batch_id` for source-destination row count reconciliation
- [[03-incremental-patterns/0301-timestamp-extraction-foundations|0301]] -- why `updated_at` is unreliable and `_extracted_at` is your safety net
