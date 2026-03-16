---
title: "dlt -- Schema Contracts & Evolution"
source: https://dlthub.com/docs/general-usage/schema-contracts
relevant_chapters:
  - 0105-the-lies-sources-tell
  - 0502-data-contracts
---

# dlt Schema Contracts

## Contract Modes

| Mode | Behavior |
|---|---|
| `evolve` | No constraints on schema changes (default) |
| `freeze` | Raises `DataValidationError` -- pipeline stops |
| `discard_row` | Drops the entire row silently |
| `discard_value` | Drops the offending value; loads the row without it |

## Schema Entities

| Entity | When Applied |
|---|---|
| `tables` | New table created |
| `columns` | New column added to existing table |
| `data_type` | Existing column's type changes |

## Configuration Hierarchy

Contracts can be set at three levels, each overriding the previous:

```python
# On resource
@dlt.resource(schema_contract={"tables": "evolve", "columns": "freeze"})

# On source
@dlt.source(schema_contract={"columns": "freeze", "data_type": "freeze"})

# At runtime (overrides everything)
pipeline.run(my_source(), schema_contract="freeze")

# Shorthand applies to all three entities
pipeline.run(my_source, schema_contract="freeze")
# Expands to: {"tables": "freeze", "columns": "freeze", "data_type": "freeze"}
```

## Production Pattern

```python
# Allow new columns, reject new tables and type changes
schema_contract={"tables": "freeze", "columns": "evolve", "data_type": "freeze"}
```

This maps to the book's schema contract rules: always accept column additions, fail on type changes that weren't explicitly handled.

## Pydantic Integration

Default contract with Pydantic models: `{"tables": "evolve", "columns": "discard_value", "data_type": "freeze"}`

## Default Behavior (No Contract)

- New tables: always created
- New columns: always appended
- Type mismatch: goes to a variant column (`column__v_type`)

## Error Handling

```python
try:
    pipeline.run()
except PipelineStepFailed as pip_ex:
    if pip_ex.step == "normalize":
        if isinstance(pip_ex.__context__.__context__, DataValidationError):
            # Schema violation during normalization
            ...
```

`DataValidationError` provides: `schema_name`, `table_name`, `column_name`, `schema_entity`, `contract_mode`, `table_schema`, `data_item`.

## Schema Versioning

dlt tracks schema versions with a `version_hash`. The `_dlt_version` table in the destination stores the full schema history.

## Key Gotcha

Contracts are applied **after** table and column names are normalized (naming convention applied first). A contract on a resource applies to all root tables and nested tables from that resource.
