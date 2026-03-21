---
title: "Open/Closed Documents -- Production Patterns from SAP B1"
type: reference
tags:
  - reference/production
  - reference/erp
relevant_chapters:
  - 0307-open-closed-documents
  - 0306-hard-delete-detection
  - 0308-detail-without-timestamp
---

# Open/Closed Document Patterns in Production

Source: 35+ SAP Business One clients. The `DocStatus` column (`O` = Open, `C` = Closed) is the canonical example, but every ERP has an equivalent.

## The Active Records Problem

Open documents are mutable -- they can be edited, have lines added/removed, or be deleted entirely. Closed documents are immutable. The extraction strategy must handle both:

- **Closed documents:** Extract once via incremental cursor, never re-extract
- **Open documents:** Must be re-extracted on every run (they might have changed)
- **Deleted documents:** Must be detected and removed from the destination

### The Naive Approach (Broken)

```sql
-- Extract changed rows since last run
SELECT * FROM orders WHERE UpdateDate >= :last_extracted
```

This misses:
1. Open orders that were edited but `UpdateDate` didn't change (soft rule violation)
2. Open orders that were deleted (no row to detect the change)
3. Detail lines where the header changed but the line's own timestamp didn't

### The Production Approach

Combine three mechanisms in a single incremental run:

```
1. Incremental extraction    -- WHERE UpdateDate >= :window_start
2. Active records injection  -- OR DocEntry IN (all open DocEntries from BQ)
3. Existence verification    -- Compare open PKs in BQ vs source, delete missing
```

Step 1 catches new and recently modified rows. Step 2 re-extracts all currently-open documents regardless of timestamp. Step 3 catches documents that were deleted from the source.

## Detail Tables (Lines)

Detail tables (`order_lines`, `invoice_lines`) typically have no `UpdateDate` of their own. They inherit changes from their header.

### Cursor from Header

Borrow the header's `UpdateDate` via JOIN:

```sql
SELECT l.*
FROM order_lines l
JOIN orders h ON l.order_id = h.order_id
WHERE h.UpdateDate >= :window_start
```

When an order header is modified, all its lines are re-extracted -- even if the lines themselves didn't change. This is intentional: it catches line additions and deletions.

### The Group Column Pattern

When merging detail lines, DELETE by group column (order_id), not by full PK (order_id + line_num). This ensures that deleted lines are caught:

**Wrong:**
```sql
DELETE FROM dest WHERE (order_id, line_num) IN (SELECT order_id, line_num FROM staging)
```
This only deletes lines that appear in staging. If line 3 was deleted from the source, it's not in staging, so it survives in the destination.

**Correct:**
```sql
DELETE FROM dest WHERE order_id IN (SELECT DISTINCT order_id FROM staging)
```
This deletes ALL lines for any order that appears in staging, then re-inserts the current lines. Deleted lines vanish.

## Active Records: The Query Timeout Problem

Querying "all open documents" on large tables (millions of rows) can take 15-20 minutes, especially on SAP HANA without indexes on `DocStatus`.

### Timeout Configuration by Database

| Database | Timeout Command | Recommendation |
|----------|----------------|----------------|
| SAP HANA | `SET TRANSACTION TIMEOUT 1200000` | 20 min (ms) |
| SQL Server | `SET LOCK_TIMEOUT 1200000` | 20 min (ms) |
| PostgreSQL | `SET statement_timeout = '1200s'` | 20 min |
| MySQL | `SET SESSION max_execution_time = 1200000` | 20 min (ms) |

### Critical Error Handling

If the active records query fails (timeout, connection drop), the pipeline MUST fail -- not return an empty set. Returning empty means "no open documents exist" which triggers deletion of ALL open documents from the destination.

```
Query returns empty set intentionally → 0 open documents → correct
Query fails and returns empty set → all documents look deleted → catastrophic
```

The safest pattern: wrap the query in a try/catch that raises a hard failure on any exception except "table not exist" (which is valid on first run).

## Performance: Request Indexes

The single most effective optimization is asking the client DBA to create an index on the active records column:

```sql
CREATE INDEX IX_DocStatus ON dbo.ORDR (DocStatus);
```

This reduces the "all open documents" query from 15 minutes to seconds. It's a read-only index on the source -- no triggers, no views, no stored procedures. Most clients accept this.

## SAP B1 Specific: LogInstanc

SAP B1 has a `LogInstanc` column that increments on every change to a document. It's monotonically increasing per-document and more reliable than `UpdateDate` for detecting changes. Worth exploring as a cursor column for clients where `UpdateDate` is unreliable.
