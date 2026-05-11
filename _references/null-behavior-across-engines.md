# NULL Behavior Across SQL Engines

Research notes from fact-checking Chapter 5 (Null Handling, Synthetic Keys). Verified 2026-04-10.

## CONCAT NULL propagation

**Critical divergence across engines:**

| Engine | `CONCAT('ACME', '|', NULL)` | Source |
|---|---|---|
| BigQuery | `NULL` | [boonepeter.github.io](https://boonepeter.github.io/posts/null-in-bigquery/) |
| Snowflake | `NULL` | [Snowflake docs: CONCAT](https://docs.snowflake.com/en/sql-reference/functions/concat) |
| MySQL | `NULL` | MySQL docs |
| PostgreSQL | `'ACME|'` (treats NULL as '') | [neon.com: PostgreSQL CONCAT](https://neon.com/postgresql/postgresql-string-functions/postgresql-concat-function) |
| SQL Server | `'ACME|'` (treats NULL as '') | [Microsoft Learn: CONCAT](https://learn.microsoft.com/en-us/sql/t-sql/functions/concat-transact-sql) |

PostgreSQL's `||` operator DOES propagate NULL. Only the `CONCAT()` function swallows it.
This means `MD5(CONCAT(...))` on PostgreSQL/SQL Server silently produces valid hashes for NULL columns instead of returning NULL -- different rows with different NULL patterns can collide.

## NOT NULL enforcement on columnar engines

**All four enforce NOT NULL when declared:**

| Engine | NOT NULL enforced? | Default | Source |
|---|---|---|---|
| BigQuery | Yes (REQUIRED mode) | NULLABLE | [datawise.dev](https://datawise.dev/the-not-null-constraint-in-bigquery) |
| Snowflake | Yes (the only constraint enforced on standard tables) | NULLABLE | [medium.com/@sanusa100](https://medium.com/@sanusa100/logic-behind-snowflakes-non-enforced-constraints-041c75877136) |
| ClickHouse | Yes, and columns are non-nullable by default | NOT NULL | [ClickHouse docs: Nullable](https://clickhouse.com/docs/sql-reference/data-types/nullable) |
| Redshift | Yes | NULLABLE | [AWS docs: Table Constraints](https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html) |

What they DON'T enforce: UNIQUE, PRIMARY KEY, FOREIGN KEY (informational only, used for query optimization).
Exception: Snowflake Hybrid Tables enforce PK/UNIQUE, but those are transactional tables.

## GROUP BY with NULLs

Universal across all engines: NULLs are "not distinct" per SQL standard, so all NULLs group together. Not engine-specific.
Source: [LearnSQL: NULL Values and GROUP BY](https://learnsql.com/blog/null-values-group-clause/)

## Hash function NULL propagation

MD5() and SHA256() return NULL for NULL input on all engines. Exception: Snowflake's `HASH()` (not MD5) never returns NULL even for NULL inputs.
Source: [Snowflake docs: HASH](https://docs.snowflake.com/en/sql-reference/functions/hash)

## Birthday paradox math

128-bit hash → 50% collision probability at ~2^64 (~18.4 quintillion) values. Correct.
Source: [Wikipedia: Birthday attack](https://en.wikipedia.org/wiki/Birthday_attack)

## Sentinel practice for NULL in keys

Standard practice: COALESCE to a distinctive sentinel before hashing. dbt uses `'_dbt_utils_surrogate_key_null_'`.
Key gotchas: sentinel must not appear in real data, always use separators between fields, maintain consistency across all pipelines that compute the same key.
Source: [dbt docs: Generating Surrogate Keys](https://docs.getdbt.com/blog/sql-surrogate-keys)
