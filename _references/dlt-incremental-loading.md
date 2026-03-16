---
title: "dlt -- Incremental Loading & Cursors"
source: https://dlthub.com/docs/general-usage/incremental-loading
relevant_chapters:
  - 0302-cursor-based-extraction
  - 0308-reliable-incrementals
  - 0312-late-arriving-data
  - 0314-create-vs-update-separation
  - 0503-backfill-strategies
---

# dlt Incremental Loading

## Write Dispositions

| Disposition | Behavior |
|---|---|
| `replace` | Drops and recreates. Full refresh. |
| `append` | Adds new data, doesn't touch existing rows. |
| `merge` | Updates/deletes existing records using keys. |

## Cursor-Based Incremental

### Constructor

```python
dlt.sources.incremental(
    cursor_path,                        # column name used as cursor
    initial_value=None,                 # starting value on first run
    end_value=None,                     # upper bound for backfill (exclusive)
    last_value_func=max,                # max, min, or custom
    row_order=None,                     # "asc", "desc", or None
    range_start="closed",               # "closed" (>=) or "open" (>)
    range_end="open",                   # "closed" or "open"
    primary_key=None,                   # for deduplication only
    on_cursor_value_missing="raise",    # "raise", "include", or "exclude"
    allow_external_schedulers=False
)
```

### Value Parameters

| Parameter | Purpose |
|---|---|
| `initial_value` | Value on first run. Inclusive. |
| `start_value` | Max cursor from previous run (or `initial_value` on first run) |
| `last_value` | Updated in real-time as rows are yielded |
| `end_value` | End of backfill range. Exclusive by default. When set, state is NOT modified -- backfill is stateless. |

### Range Inclusivity

| Setting | Boundary | Dedup | Use case |
|---|---|---|---|
| `range_start="closed"` (default) | `>=` | Enabled | Timestamps that aren't unique |
| `range_start="open"` | `>` | Disabled | Auto-incrementing IDs, unique high-precision timestamps |

Key: `initial_value` is always inclusive, `end_value` is always exclusive. Chaining ranges works without overlaps.

### on_cursor_value_missing

| Value | Behavior |
|---|---|
| `"raise"` (default) | Exception when cursor path is missing or NULL |
| `"include"` | Rows with missing/NULL cursor pass through |
| `"exclude"` | Rows with missing/NULL cursor are filtered out |

Directly relevant to `0314-create-vs-update-separation`: when `updated_at` is NULL on INSERT, you must decide what to do.

```python
# Include rows where updated_at is NULL (new inserts without trigger)
dlt.sources.incremental("updated_at", on_cursor_value_missing="include")
```

### lag Parameter

Adjusts `start_value` backward to re-fetch recent data:
- datetime cursors: lag in seconds
- date cursors: lag in days
- numeric cursors: lag in the cursor's unit

```python
# Re-fetch the last hour of data every run
dlt.sources.incremental("created_at", lag=3600, last_value_func=max)
```

Deduplication is disabled when `lag` is used. Directly relevant to `0312-late-arriving-data` and `0301-timestamp-extraction-foundations`.

### Backfill Without State Mutation

When `end_value` is set, dlt does NOT modify incremental state. This enables parallel historical loads:

```python
# These don't corrupt the ongoing incremental cursor
july = repo_issues(updated_at=dlt.sources.incremental(
    initial_value='2022-07-01T00:00:00Z',
    end_value='2022-08-01T00:00:00Z'
))
august = repo_issues(updated_at=dlt.sources.incremental(
    initial_value='2022-08-01T00:00:00Z',
    end_value='2022-09-01T00:00:00Z'
))
```

### Split Large Loads into Chunks

```python
messages = sql_table(
    table="chat_message",
    incremental=dlt.sources.incremental("created_at", row_order="asc", range_start="open"),
)
while not pipeline.run(messages.add_limit(max_time=60)).is_empty:
    pass
```

### Airflow Integration

With `allow_external_schedulers=True`:
- `data_interval_start` -> `initial_value`
- `data_interval_end` -> `end_value`
- dlt state is NOT used (Airflow manages the windows)

## State Management

Pipeline state is a Python dictionary committed atomically with data. Scoped to the resource.

```python
@dlt.resource()
def tweets():
    last_val = dlt.current.resource_state().setdefault("last_updated", None)
    data = _get_data(start_from=last_val)
    yield data
    dlt.current.resource_state()["last_updated"] = data["last_timestamp"]
```
