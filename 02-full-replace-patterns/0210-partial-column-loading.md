---
title: "Partial Column Loading"
aliases: []
tags:
  - pattern/full-replace
  - chapter/part-2
status: draft
created: 2026-03-10
updated: 2026-03-11
---

# Partial Column Loading

> **One-liner:** When you can't or won't extract all columns, do it explicitly, document what's missing and why, and accept that your destination is no longer a complete clone.

## The Problem

Most pipelines extract all columns. `SELECT *` from source, load to destination -- a complete clone. Partial column loading is a deliberate departure from that: you extract a subset of columns and leave the rest behind.

Three situations justify it:

**PII and restricted data.** GDPR, HIPAA, contractual data processing agreements. Some columns can't land in your analytics destination regardless of what consumers want. `national_id`, `ssn`, `raw_card_number` -- these don't belong in BigQuery, period.

**BLOBs and binary columns.** PDFs, images, audio files, attachments stored in the source database. Extracting them bloats transfer size, explodes storage costs at the destination, and is useless to anyone running SQL. Leave them in the source.

**Columns your destination can't represent.** A PostgreSQL `geometry` type, a SQL Server `hierarchyid`, a custom SAP compound type. Sometimes there's no clean mapping to the destination's type system. Excluding the column is preferable to a failed extraction or a corrupted value landing silently.

What doesn't justify it: filtering for "relevance." A wide table with 200 columns where analytics only uses 40 is not a reason to exclude 160. That's a transformation -- a decision about what matters -- and it belongs downstream, not at the extraction layer. Consumers don't understand the difference between "this column has nulls" and "this column was never loaded."

## The Trap

The danger isn't the exclusion. It's the silence.

A consumer queries `destination.customers` looking for `national_id`. The column doesn't exist. They assume it's null in the source -- or worse, they assume the source doesn't have it. Neither is true. The column exists in the source with valid data; it just wasn't loaded.

This is how a pipeline correctness problem becomes a business trust problem. The consumer makes a decision based on a gap they didn't know existed.

A second trap: schema drift. When a source table adds a new column, `SELECT *` picks it up automatically on the next run. An explicit column list doesn't. The destination falls silently behind the source -- no error, no alert, just a growing gap between what's there and what's available.

## When to Use It

| Reason                      | Example columns                            | Recoverable?                                                                   |
| --------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------ |
| PII / legal restriction     | `ssn`, `national_id`, `raw_email`          | Yes -- with proper access controls at source                                   |
| Binary / attachment columns | `attachment_blob`, `document_pdf`, `photo` | Yes -- if consumers don't need binary data                                     |
| Unextractable type          | `location geometry`, `sap_custom_type`     | Sometimes -- type casting may be an option first, everything *can* be a string |
| "Irrelevant" columns        | Wide table, only 40 of 200 columns used    | No -- this is a transformation, not conforming                                 |

Before excluding a column for type reasons, check [[05-conforming-playbook/0503-type-casting-normalization|0503-type-casting-normalization]]. A type that can't be loaded directly can often be cast to a string or numeric representation. Partial loading is the fallback when casting isn't viable.

## The Pattern

Name every column explicitly. Comment every exclusion inline with the reason.

```sql
-- source: transactional
-- engine: postgresql
SELECT
    id,
    name,
    email,
    is_active,
    created_at,
    updated_at
    -- national_id excluded: GDPR Art. 9 -- special category data, no processing basis
    -- id_photo excluded: BLOB, ~2MB per row, not used by any downstream consumer
FROM customers;
```

The comments serve two purposes: they document intent for the next engineer who touches this query, and they make the exclusion visible in code review.

At the destination, document what's missing at the table level -- not just in the pipeline code. A table description, a metadata entry, a README in the project folder. Wherever consumers go to understand the data, the exclusion needs to be there.

```sql
-- source: columnar
-- engine: bigquery
-- Destination table description (set via DDL or catalog):
-- "Partial load of source customers table. Excluded: national_id (GDPR Art. 9),
--  id_photo (BLOB). See pipeline docs for details."
```

## Schema Drift Risk

Every time the source schema changes, a `SELECT *` pipeline adapts automatically. A named-column pipeline doesn't.

Add a schema diff check to your pipeline: compare the source column list against your extraction column list before each run and alert on new columns. A new column in the source is either something you should be loading (add it) or something you should be explicitly excluding (add it to the exclusion list with a comment). The only unacceptable outcome is not knowing it appeared.

```sql
-- source: transactional
-- engine: postgresql
-- Run before extraction to detect new source columns
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'customers'
  AND column_name NOT IN (
      'id', 'name', 'email', 'is_active', 'created_at', 'updated_at',
      'national_id',  -- excluded: GDPR
      'id_photo'      -- excluded: BLOB
  );
-- Non-empty result = new column appeared. Investigate before proceeding.
```

> [!warning] New columns in `products` are a known risk
> The `products` table in this domain mutates -- new columns appear after deploys. If you're running a partial column extraction on `products`, the schema diff check is not optional. A new `supplier_id` column that appears in the source and gets silently dropped at extraction will be invisible to every downstream consumer.

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> Columnar stores have first-class column-level descriptions in their catalog. Use them. Set the table description and annotate each present column; note which columns are absent and why. Consumers who query the information schema or use a data catalog tool will see it without needing to find the pipeline code.

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Same extraction SQL. At the destination, use `COMMENT ON COLUMN` or `COMMENT ON TABLE` to document the exclusions directly in the schema. It's the closest equivalent to a catalog annotation and it travels with the table.

## Related Patterns

- [[05-conforming-playbook/0503-type-casting-normalization|0503-type-casting-normalization]] -- try casting before excluding; partial loading is the fallback
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- column exclusion applies regardless of how you detect changes
- [[02-full-replace-patterns/0209-hash-based-change-detection|0209-hash-based-change-detection]] -- hash-based detection breaks if the hashed column set doesn't match the extracted column set; align them explicitly
