---
title: "dlt -- Naming Conventions"
source: https://dlthub.com/docs/general-usage/naming-convention
relevant_chapters:
  - 0403-type-casting-normalization
---

# dlt Naming Conventions

Identifier normalization is a conforming operation with permanent consequences. Changing the convention after data exists causes normalization failures.

## Available Conventions

| Convention | Case | Characters | Notes |
|---|---|---|---|
| `snake_case` (default) | Insensitive, lowercased | ASCII alphanumeric + underscore only | `__` reserved as nesting separator |
| `duck_case` | Sensitive | All Unicode including emoji | |
| `direct` | Sensitive | All Unicode | Does not contract underscores |
| `sql_cs_v1` | Sensitive | SQL-safe | |
| `sql_ci_v1` | Insensitive, lowercased | SQL-safe | |

## snake_case Processing Rules

1. Trim surrounding whitespace
2. Retain ASCII alphanumerics and underscores only
3. Replacements: `+`/`*` -> `x`, `-` -> `_`, `@` -> `a`, `|` -> `l`
4. Prepend `_` if starts with a number
5. Collapse consecutive underscores
6. Convert trailing underscores to `x`
7. `__` reserved for nesting separator

## Configuration

```toml
[schema]
naming="sql_ci_v1"

# Per source
[sources.zendesk.schema]
naming="sql_cs_v1"
```

## Critical Gotchas

- **Changing naming convention is destructive.** If destination identifiers would change, the normalize process fails.
- **Dictionary key collisions are silent.** dlt won't detect when two source keys normalize to the same identifier.
- **dlt does not store source identifiers.** Changing convention re-normalizes already normalized identifiers, not the originals.
- **Identifier shortening:** When identifiers exceed destination maximums, dlt generates short deterministic hashes.
