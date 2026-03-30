---
title: "Schema Evolution at Load Time -- Production Patterns"
type: reference
tags:
  - reference/production
  - reference/schema
relevant_chapters:
  - 0609-data-contracts
  - 0403-merge-upsert
  - 0503-type-casting-normalization
  - 0707-schema-naming-conventions
---

# Schema Evolution at Load Time

Source: Warp production handling schema changes across 35+ ERP clients.

## The Problem

Source schemas change without warning:
- ERP upgrades add columns (SAP B1 10.0 → 10.1 added 47 columns to ORDR)
- Client customizations add user-defined fields (UDFs)
- Views are modified to include new calculated columns

The destination must handle these gracefully without manual intervention.

## Evolve, Don't Break

The safest schema contract for ECL: `{"tables": "evolve", "columns": "evolve"}`. This means:
- New tables are created automatically
- New columns are added via ALTER TABLE
- Existing columns are never modified or removed
- No data type changes (a STRING column stays STRING even if source changes to INT)

### ALTER TABLE ADD COLUMN

When staging has columns that don't exist in the destination:

```sql
ALTER TABLE `project.dataset.table`
ADD COLUMN `new_column` FLOAT64 OPTIONS(description='')
```

**BigQuery gotcha:** `SchemaField.field_type` returns legacy names that don't work in DDL:

| SchemaField type | ALTER TABLE type |
|-----------------|------------------|
| `FLOAT` | `FLOAT64` |
| `INTEGER` | `INT64` |
| `BOOLEAN` | `BOOL` |

Always map legacy types to standard SQL before executing ALTER TABLE.

### Column Addition Is Nullable

BigQuery doesn't allow adding NOT NULL columns to tables with existing data. All new columns are NULLABLE. This means:
- Rows loaded before the column existed have NULL
- Rows loaded after have the actual value
- Downstream queries must handle NULLs: `COALESCE(new_col, default_value)`

## Naming Conventions

Two approaches for column names at the destination:

| Mode | Behavior | Example |
|------|----------|---------|
| Direct (preserve case) | `DocEntry`, `ACTINDX`, `PostSlsIn` | 34 production clients |
| Snake case (normalize) | `doc_entry`, `actindx`, `post_sls_in` | New clients |

**Trade-off:**
- Direct: 1:1 mapping with source, easy to trace. Case-sensitive queries required.
- Snake case: Standard analytics convention. Loses original casing (irreversible).

Production split: 97% of clients use direct (preserving ERP column names). Snake case is offered for greenfield deployments where the analytics team prefers it.

## Metadata Columns

Every loaded table gets two tracking columns added during extraction (not by the loader's normalizer):

| Column | Type | Purpose |
|--------|------|---------|
| `_dlt_id` | STRING, NULLABLE | UUID per row (dedup tiebreaker) |
| `_dlt_load_id` | STRING, NULLABLE | Unique ID per extraction batch |

These are added as PyArrow columns before yielding to the loader:

```python
arrow_table = arrow_table.append_column("_dlt_id", pa.array(generate_dlt_ids(n)))
arrow_table = arrow_table.append_column("_dlt_load_id", pa.array([load_id] * n))
```

**Why NULLABLE:** Tables created before metadata columns were added have existing rows with NULL values. Adding NOT NULL would fail on BigQuery.

**Why manual (not loader):** Using `row_tuples_to_arrow()` skips the loader's normalizer for speed (20-30x faster). The normalizer is what normally adds these columns, so we add them ourselves.

## Type Casting at Extraction

Some type mismatches must be fixed before the data reaches the loader:

### Float PKs → Int64

Some ERPs (Softland) store integer IDs as FLOAT. BigQuery can't PARTITION BY FLOAT64, and MERGE on floats is unreliable (precision issues).

```python
# Cast float64 PKs to int64 in PyArrow (vectorized, ~1ms per 1M rows)
if col_name in primary_key_set and arrow_table.schema.field(col_idx).type == pa.float64():
    arrow_table = arrow_table.set_column(col_idx, col_name, col.cast(pa.int64()))
```

### Timezone Stripping

Some database drivers (pymysql, hdbcli) return timezone-aware datetimes. PyArrow on Windows can't resolve timezones (no tzdata database). Strip before conversion:

```python
partition = [
    tuple(v.replace(tzinfo=None) if isinstance(v, datetime) and v.tzinfo else v for v in row)
    for row in partition
]
```

## Partitioning Strategy

Tables are partitioned by timestamp column with monthly granularity:

```sql
PARTITION BY DATETIME_TRUNC(`UpdateDate`, MONTH)
```

The partition column is resolved with this priority:
1. Explicit `partition_by` config (if set)
2. `partition_by = []` → explicitly NO partition
3. Fallback to cursor date: `update.date` → `create.date` → `fallback.date`

For detail tables with cursor-from-header (JOIN), the partition column comes from the header table but is added to the detail table's schema.
