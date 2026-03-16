---
title: "dlt -- SQL Database Extraction"
source: https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database
relevant_chapters:
  - 0103-transactional-sources
  - 0107-corridors
  - 0702-source-system-etiquette
---

# dlt SQL Database Extraction

## Reflection Levels

| Level | What it reads | Behavior |
|---|---|---|
| `minimal` | Column names + nullability only | Types inferred from actual data at runtime |
| `full` (default) | Names, nullability, data types | Adds precision/scale for decimals |
| `full_with_precision` | Everything in `full` + precision/scale for text/binary | Integers promoted to bigint |

When reflection causes type-cast failures at the destination (e.g., nanosecond TIME becoming BigQuery bigint), fall back to `minimal`.

## Backend Options

| Backend | Yields | Speed | Gotchas |
|---|---|---|---|
| **SQLAlchemy** (default) | Python dicts | Slowest | Correct types, no extra deps |
| **PyArrow** | Arrow tables | 20-30x with Parquet dest | Preserves decimal precision. Needs `numpy`. |
| **Pandas** | DataFrames | Medium | **Decimal -> double (precision loss). Date/time -> strings. All types nullable.** |
| **ConnectorX** | Arrow tables (via Rust) | ~2x over PyArrow | tz-aware timestamps lose TZ. JSON fields double-wrapped. Ignores chunk_size. |

## Type Adaptation Callbacks

### Table adapter (modify reflected schema)

```python
def table_adapter_callback(table):
    if table.name == 'my_table':
        columns_to_keep = ['id', 'name', 'email']
        for col in list(table._columns):
            if col.name not in columns_to_keep:
                table._columns.remove(col)
    return table
```

### Type adapter (override SQL type detection)

```python
import sqlalchemy as sa
from snowflake.sqlalchemy import TIMESTAMP_NTZ

def type_adapter_callback(sql_type):
    if isinstance(sql_type, TIMESTAMP_NTZ):
        return sa.DateTime(timezone=True)
    return sql_type
```

### Query adapter (filter rows at SQL level)

```python
def query_adapter_callback(query, table):
    if table.name == "family":
        return query.where(table.c.rfam_id.ilike("%bacteria%"))
    return query
```

## Incremental via SQL Source

Default (closed range) generates `WHERE last_modified >= :start_value` with deduplication.
Open range generates `WHERE last_modified > :start_value` without deduplication.

Cursor column names with special characters (e.g., `$`) must be escaped: `"'example_$column'"`.

**Timezone gotcha:** naive Python datetimes use the machine's local timezone, which may differ from the DBMS. Use tz-aware datetimes or `reflection_level="full"`.

## TOML Configuration

```toml
[sources.sql_database]
credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?..."

[sources.sql_database.chat_message]
backend="pandas"
chunk_size=1000

[sources.sql_database.chat_message.incremental]
cursor_path="updated_at"
```

Table and column names in `config.toml` are **case-sensitive** and must match the database exactly.
