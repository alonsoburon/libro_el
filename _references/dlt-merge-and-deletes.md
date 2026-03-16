---
title: "dlt -- Merge Strategies & Hard Delete Handling"
source: https://dlthub.com/docs/general-usage/merge-loading
relevant_chapters:
  - 0307-merge-upsert
  - 0309-hard-delete-detection
  - 0310-open-closed-documents
---

# dlt Merge Strategies

## Three Strategies

### 1. Delete-Insert (Default Merge)

1. Load to staging dataset
2. Deduplicate staging (if `primary_key` exists)
3. Delete matching records from destination (using `merge_key` and/or `primary_key`)
4. Insert new records
5. All in one atomic transaction

```python
@dlt.resource(primary_key="id", write_disposition="merge")
def github_repo_events():
    yield from _get_event_pages()
```

**Fallback:** If you use `merge` but specify no merge or primary keys, it falls back to `append`.

### 2. Upsert

True `MERGE`/`UPDATE` operations. Update if key exists, insert if it doesn't.

```python
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="my_primary_key"
)
def my_upsert_resource():
    ...
```

Supported on: athena, bigquery, databricks, mssql, postgres, snowflake, filesystem (delta/iceberg).

Requires `primary_key` (must be unique). Does NOT support `merge_key`. No deduplication.

### 3. SCD2 (Slowly Changing Dimension Type 2)

Creates validity-windowed records:

| _dlt_valid_from | _dlt_valid_to | customer_key | c1 |
|---|---|---|---|
| 2024-04-09 18:27:53 | 2024-04-09 22:13:07 | 1 | foo |
| 2024-04-09 22:13:07 | NULL | 1 | foo_updated |
| 2024-04-09 18:27:53 | NULL | 2 | bar |

Configuration options:

| Option | Default | Description |
|---|---|---|
| `validity_column_names` | `["_dlt_valid_from", "_dlt_valid_to"]` | Custom column names |
| `active_record_timestamp` | NULL | Literal for active records (e.g., `"9999-12-31"`) |
| `row_version_column_name` | auto hash via `_dlt_id` | Use own row hash column |

Use `merge_key` to prevent retiring absent records in incremental loads:

```python
@dlt.resource(
    merge_key="customer_key",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_customer():
    ...
```

## Primary Key vs Merge Key

| | Primary Key | Merge Key |
|---|---|---|
| Purpose | Identifies unique records | Defines deletion scope |
| Deduplication | Yes (in staging) | No |
| Used in | All strategies | Delete-insert, SCD2 |
| Compound | Yes | Yes |

## dedup_sort Hint

Controls which record survives when duplicates share the same primary key:

```python
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"created_at": {"dedup_sort": "desc"}}  # keep latest
)
```

## Hard Delete Handling

Column hint `hard_delete` removes records from destination.

**Boolean column:** Only `True` triggers delete. `None` and `False` are ignored.

```python
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"deleted_flag": {"hard_delete": True}}
)
def resource():
    yield {"id": 1, "val": "foo", "deleted_flag": False}   # insert
    yield {"id": 1, "val": "bar", "deleted_flag": None}    # update
    yield {"id": 1, "val": "foo", "deleted_flag": True}    # DELETE
```

**Non-boolean column (e.g., timestamp):** Any non-None value triggers delete.

```python
columns={"deleted_at_ts": {"hard_delete": True}}
# {"id": 1, "deleted_at_ts": "2024-02-22T12:34:56Z"}  -> DELETE
# {"id": 1, "deleted_at_ts": None}                     -> keep
```

**Deletes cascade to nested tables** via `_dlt_root_id`. Directly relevant to `invoice_lines` being hard-deleted independently.

## Combined hard_delete + dedup_sort

```python
columns={
    "deleted_flag": {"hard_delete": True},
    "lsn": {"dedup_sort": "desc"}
}
# If the surviving record (highest lsn) has deleted_flag=True -> delete
# If the surviving record has deleted_flag=None/False -> insert/update
```

## Nested Tables and Merge

Merge requires `_dlt_id` of the root table to be propagated to nested tables as `_dlt_root_id`. Automatic for `merge` write disposition.

SCD2 limitation: nested tables don't contain validity columns. Join via `_dlt_root_id`.
