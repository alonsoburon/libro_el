---
title: "dlt -- Type System & Type Mapping"
source: https://dlthub.com/docs/general-usage/schema
relevant_chapters:
  - 0403-type-casting-normalization
  - 0408-decimal-precision-loss
  - 0405-timezone-conforming
---

# dlt Type System

dlt defines an abstract intermediate type system between source and destination. This is a real-world implementation of the conforming layer -- data is extracted with source-native types, conformed to dlt's internal types, then mapped to destination-native types.

## Internal Type Palette

| dlt Type | Python Source | Precision/Scale | Notes |
|---|---|---|---|
| `text` | `str` | VARCHAR(N) | |
| `double` | `float` | -- | |
| `bool` | `bool` | -- | |
| `timestamp` | `datetime.datetime` | 0 (seconds) to 9 (nanoseconds), default 6 (microseconds) | `timezone` flag, default True |
| `date` | `datetime.date` | -- | |
| `time` | `datetime.time` | Same as timestamp | Saved without timezone info |
| `bigint` | `int` | Bit count, default 64 | Maps to TINYINT/INT/BIGINT |
| `binary` | `bytes` | Like text | |
| `json` | `dict`, `list` | -- | Prevents flattening/nested table creation |
| `decimal` | `Decimal` | Precision and scale | |
| `wei` | large int | 256-bit | Postgres/BigQuery only |

## Timestamp Normalization

| Input | `timezone` hint | Result |
|---|---|---|
| naive datetime | None or True | tz-aware UTC |
| naive datetime | False | naive (pass-through) |
| tz-aware datetime | None or True | tz-aware UTC |
| tz-aware datetime | False | naive UTC |

**Naive timestamps are always treated as UTC.** System timezone settings are ignored.

## Destination-Specific Timestamp Handling

- **BigQuery:** Does not support naive timestamps. Interprets as naive UTC. `timestamp -> TIMESTAMP`
- **Snowflake:** `timezone=True -> TIMESTAMP_TZ`, `timezone=False -> TIMESTAMP_NTZ`
- **PostgreSQL:** `timezone=True -> TIMESTAMP WITH TIME ZONE`, `timezone=False -> TIMESTAMP WITHOUT TIME ZONE`. Precision 0-6.
- **DuckDB:** Only microsecond resolution supports tz-aware

## Destination-Specific Decimal Handling

- **Snowflake:** `DECFLOAT` (36 significant digits) with `use_decfloat=True`, else `NUMBER(38,9)`. DECFLOAT only works with jsonl/csv -- not Parquet.
- **PostgreSQL ADBC driver:** Cannot handle large decimals (wei/256-bit). `decimal128(6,2)` decoding problems.
- **BigQuery:** Standard NUMERIC/BIGNUMERIC.

## Precision Support by Destination

Timestamp precision (fractional seconds) supported on: postgres, duckdb, snowflake, synapse, mssql, filesystem (parquet).

## Variant Columns

When type coercion fails, dlt creates `{column}__v_{type}` variant columns. Example: column `id` (bigint) receives `"idx-nr-456"` -> creates `id__v_text`.

Two normalizer options:
- `relational` (default): attempts coercion first
- `relational_no_coercion`: every mismatch creates a variant

## Preferred Types (regex matching on column names)

```yaml
settings:
  preferred_types:
    re:timestamp: timestamp
    inserted_at: timestamp
    created_at: timestamp
    updated_at: timestamp
```

## Type Detection

Default enabled detectors: `timestamp`, `iso_timestamp`, `iso_date`, `large_integer`, `hexbytes_to_text`, `wei_to_double`.

Can be removed/added per source:

```python
source.schema.remove_type_detection("iso_timestamp")
source.schema.add_type_detection("timestamp")
```

## Anti-Pattern: Pandas Backend

dlt's own documentation warns against Pandas for:
- Decimal -> double (precision loss)
- Date/time -> strings
- All types become nullable
