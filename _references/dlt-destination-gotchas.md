---
title: "dlt -- Destination-Specific Gotchas"
source: https://dlthub.com/docs/dlt-ecosystem/destinations/
relevant_chapters:
  - 0104-columnar-destinations
  - 0403-type-casting-normalization
  - 0405-timezone-conforming
  - 0408-decimal-precision-loss
---

# dlt Destination Gotchas

## BigQuery

| Feature | Value |
|---|---|
| Preferred loader format | jsonl |
| Preferred staging format | parquet |
| Merge strategies | delete-insert, upsert, scd2 |
| Supports tz-aware datetime | Yes |
| **Supports naive datetime** | **No** |

- **Cannot load JSON columns from Parquet files** -- jobs fail permanently
- Nested dicts from Parquet -> strings. Use `autodetect_schema=True` + PyArrow for proper RECORD types, or switch to JSONL
- INT64 partition: 10,000 partition limit. dlt sets 86,400-second daily boundaries
- Streaming insert: only with `write_disposition="append"`, data locked for editing up to 90 minutes
- `require_partition_filter` -- set it or pay

### BigQuery Configuration

```toml
[destination.bigquery]
location = "US"
http_timeout = 15.0
file_upload_timeout = 1800.0
autodetect_schema = true
```

### Partitioning and Clustering

```python
from dlt.destinations.adapters import bigquery_adapter
bigquery_adapter(resource, partition="event_date")
bigquery_adapter(resource, cluster=["event_date", "user_id"])
```

## Snowflake

| Feature | Value |
|---|---|
| Merge strategies | delete-insert, upsert, scd2 |
| Replace default | `CREATE CLONE` from staging |

- **DECFLOAT only works with jsonl and csv** -- Parquet staging doesn't support it
- DECFLOAT + Arrow fetch: Snowflake connector doesn't recognize DECFLOAT via Arrow. Must increase decimal context precision before fetching
- JSON in Parquet -> `VARIANT` string. Requires `PARSE_JSON`
- `unique` and `primary_key` are **not enforced** -- dlt doesn't instruct Snowflake to `RELY` on them
- Atomic swap: each table retains original permissions. Ensure FUTURE GRANTs

### Snowflake Type Mapping

| Scenario | Snowflake Type |
|---|---|
| `timezone=True` (default) | `TIMESTAMP_TZ` |
| `timezone=False` | `TIMESTAMP_NTZ` |
| Unbound decimals + `use_decfloat=True` | `DECFLOAT` |
| Unbound decimals without decfloat | `NUMBER(38,9)` |
| Bounded decimals | `NUMBER(p,s)` |
| JSON from Parquet | `VARIANT` (string) |

### Minimum Permissions

```sql
CREATE DATABASE dlt_data;
CREATE USER loader WITH PASSWORD='<password>';
CREATE ROLE DLT_LOADER_ROLE;
GRANT ROLE DLT_LOADER_ROLE TO USER loader;
GRANT USAGE ON DATABASE dlt_data TO DLT_LOADER_ROLE;
GRANT CREATE SCHEMA ON DATABASE dlt_data TO ROLE DLT_LOADER_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO DLT_LOADER_ROLE;
```

## PostgreSQL

| Feature | Value |
|---|---|
| Preferred loader format | insert_values |
| Supports tz-aware datetime | Yes |
| Supports naive datetime | Yes |
| Merge strategies | delete-insert, upsert, scd2 |

- **insert_values (default):** Large INSERT VALUES, 20 threads default
- **CSV:** Several times faster than insert_values. COPY command.
- **Parquet (ADBC):** Limitations: no JSONB (uses JSON), no INT8 (128-bit), no TIME(3), decimal issues, no wei
- Creates actual unique indexes for `unique` hint columns (unlike Snowflake)
- PostGIS support via `postgres_adapter(data, geometry="geom")`

### PostgreSQL Timestamp Mapping

| Setting | PostgreSQL Type |
|---|---|
| `timezone=True` (default) | `TIMESTAMP WITH TIME ZONE` |
| `timezone=False` | `TIMESTAMP WITHOUT TIME ZONE` |
| Precision | 0-6 fractional seconds |
