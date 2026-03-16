---
title: "dlt -- Metadata Columns & Tracking Tables"
source: https://dlthub.com/docs/general-usage/schema
relevant_chapters:
  - 0401-metadata-column-injection
  - 0402-synthetic-keys
  - 0506-reconciliation-patterns
---

# dlt Metadata Injection

## Auto-Injected Columns

| Column | Purpose | Injected Into |
|---|---|---|
| `_dlt_id` | Unique row key (hash) | All tables |
| `_dlt_load_id` | References the pipeline execution that loaded the row | Top-level tables |
| `_dlt_parent_id` | References parent row's `_dlt_id` | Nested tables |
| `_dlt_list_idx` | Position in source list | Nested tables from lists |
| `_dlt_root_id` | References root table row (for merge cascading) | Nested tables in merge mode |

## Internal Tracking Tables

| Table | Contents |
|---|---|
| `_dlt_loads` | Every pipeline run: `load_id`, `schema_name`, `status`, `inserted_at`, `schema_version_hash`. Status 0 = completed. |
| `_dlt_pipeline_state` | Serialized pipeline state per run (incremental cursors, checkpoints) |
| `_dlt_version` | Schema evolution history with full schema definitions |

## Key Observations

- `_dlt_id` is a hash-based synthetic key -- directly relevant to `0402-synthetic-keys`
- `_dlt_load_id` is dlt's equivalent of `_batch_id` -- tracks which pipeline run brought each row
- `_dlt_loads` table enables reconciliation: query it to verify load completions
- Schema versioning via `_dlt_version` provides audit trail for schema evolution
- `_dlt_root_id` enables cascade operations on nested tables (hard deletes, SCD2)
