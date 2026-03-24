---
title: "Duplicate Detection"
aliases: []
tags:
  - pattern/recovery
  - chapter/part-6
status: outline
created: 2026-03-06
updated: 2026-03-15
---

# Duplicate Detection

> **One-liner:** Duplicates already landed -- how to find them, quantify the damage, and deduplicate without losing data.

## The Problem
- Duplicates in the destination are a symptom, not a root cause -- they indicate a load strategy mismatch, a failed retry that double-wrote, or an append that should have been a merge
- Consumers don't notice duplicates until aggregations are wrong: revenue doubled, counts inflated, joins producing unexpected fan-out
- Finding duplicates after the fact requires knowing the primary key, and the primary key in the destination may not match the source (naming conventions, synthetic keys)

## How Duplicates Arrive

| Cause | Mechanism |
|---|---|
| Append without dedup | Load strategy is append-only but the source sent the same rows twice (retry, overlapping windows) |
| Failed merge retry | A merge partially applied, then the retry inserted instead of updating |
| Overlapping incremental windows | Incremental-with-lag intentionally re-extracts recent rows, but the load didn't deduplicate them |
| Cursor gap recovery | A manual re-extraction covered a range that was already loaded |
| PK mismatch | The merge key in the destination doesn't match the source PK due to naming conventions or synthetic key issues |

## Detection

### By Primary Key
- Group by PK columns, count > 1 = duplicate

```sql
-- destination: columnar
SELECT pk_col, COUNT(*) AS dupes
FROM orders
GROUP BY pk_col
HAVING COUNT(*) > 1;
```

### By Content Hash
- When there's no reliable PK, hash all columns and group by hash
- Useful for tables loaded via [[05-conforming-playbook/0502-synthetic-keys|0502]]

### By Metadata Columns
- `_extracted_at` or `_batch_id` from [[05-conforming-playbook/0501-metadata-column-injection|0501]] can identify which load introduced the duplicates
- "All duplicates share `_batch_id = 47`" points to a specific run

## Quantification

- How many rows are duplicated?
- What percentage of the table is affected?
- Which downstream tables or reports consumed the duplicated data?
- What date range do the duplicates cover? (scopes the fix)

## Deduplication

### Dedup in Place
- DELETE duplicates, keeping one row per PK (usually the most recent by `_extracted_at`)
- Safe when the destination supports DML; expensive in columnar engines on large tables

### Dedup via Rebuild
- Rebuild the table from staging or re-extract with a full replace
- Cleaner than in-place dedup; resets to a known-good state
- Prefer this when the duplication is widespread

### Dedup View
- Leave the base table as-is, create a view that deduplicates with `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _extracted_at DESC)`
- Fast to deploy, no DML, no data loss risk
- Downstream must query the view, not the base table

## PK Resolution

- Primary key columns may have different names in source vs destination (e.g., `DocEntry` in source, `doc_entry` in destination due to naming conventions)
- Deduplication must use the destination column names, not the source names
- See [[07-serving-the-destination/0708-schema-naming-conventions|0708]] for naming convention implications

## Anti-Pattern

> [!danger] Don't deduplicate without understanding the root cause
> - Deduplication fixes the symptom. If you don't fix the load strategy that produced the duplicates, they'll come back on the next run. Find the cause first, fix the pipeline, then clean up the data.

## Related Patterns
- [[05-conforming-playbook/0501-metadata-column-injection|0501-metadata-column-injection]] -- `_batch_id` identifies which load introduced duplicates
- [[05-conforming-playbook/0502-synthetic-keys|0502-synthetic-keys]] -- synthetic keys and content hashing for dedup when no natural PK exists
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- merge prevents duplicates when the PK is correct
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- row count mismatch is often the first signal of duplication
- [[07-serving-the-destination/0708-schema-naming-conventions|0708-schema-naming-conventions]] -- PK resolution across naming conventions

## Notes
- **Author prompt -- PK resolution fallback**: The `resolve_pk_columns()` function tries exact match → snake_case → lowercase. How often does the fallback actually fire? Is it mostly direct-mode clients or snake_case clients that need it?
- **Author prompt -- bq_schema_job dedup**: The bq_schema_job does deduplication. How often does it actually find duplicates? Is it a "run it and it's always clean" job, or does it regularly find problems?
- **Author prompt -- incremental-with-lag duplicates**: The 7-day lag means you re-extract recent rows every run. How does DLT's merge/upsert handle this -- does it ever produce duplicates when the PK resolution fails?
- **Author prompt -- dedup at scale**: With ~6500 tables, have you had a case where duplicates accumulated silently for weeks before someone noticed? What was the downstream impact?
