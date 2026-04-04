---
title: "Null Handling"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Null Handling

> **One-liner:** NULL means NULL. Reflect the source as-is -- don't COALESCE, don't mix representations, don't solve downstream's problems in the ECL layer.

---

## The Playbook

ECL is about reflecting the source as faithfully as possible. If the source has NULL, land NULL. If the source has empty string, land empty string. If the source has `'N/A'`, land `'N/A'`. These are three different values with potentially three different meanings, and converting one to the other is a business decision -- it belongs downstream, not in the conforming layer.

The temptation to "clean up" NULLs at extraction is strong, especially when you know downstream consumers will struggle with them. Resist it. A COALESCE in the extraction query looks harmless, but it makes an irreversible choice about what NULL means for every consumer of the table, and that choice may be wrong for some of them. A NULL `email` might mean "not provided" for the marketing team and "not applicable" for the billing team -- collapsing it to `''` destroys the distinction for both.

---

## The Rule: Don't Mix, Don't Match

The worst outcome isn't NULLs in the destination -- it's inconsistent representations of "nothing" across tables, columns, or even rows within the same column.

The source might use any combination of:

| Representation | Where you find it |
|---|---|
| `NULL` | Standard SQL databases, ORMs that use NULL semantics |
| `''` (empty string) | Legacy systems, some ORMs that default to empty string on form fields |
| `'N/A'` / `'NONE'` / `'-'` | Manual data entry, ERP display defaults, CSV exports |
| `0` | Numeric columns where "no value" was entered as zero |

If different source tables use different representations, document the inconsistency but don't normalize them to a single representation at the ECL level. Table A uses NULL for "no email" and table B uses `''` for "no email" -- that's the source's problem, and it's worth documenting, but converting one to match the other is a transformation decision. The downstream team gets to decide how to unify them because they understand the business context better than the pipeline does.

> [!warning] Avoid COALESCE in the conforming layer
> `COALESCE(email, '')` in the extraction query looks like cleanup. What it actually does: permanently destroys the distinction between "this field was never populated" (NULL) and "this field was explicitly set to empty" (empty string). If that distinction matters to even one consumer, you've lost it for all of them. The only justified COALESCE at the ECL level is in synthetic key hashing (see [[05-conforming-playbook/0502-synthetic-keys|0502]]), where NULL would corrupt the hash output -- and that's infrastructure, not business logic.

---

## When NULLs Matter at the ECL Level

NULLs don't need fixing in the ECL layer, but they do need *awareness* -- three places where NULL behavior affects the pipeline itself, not downstream consumption:

**NULL in synthetic key columns.** Most hash functions return NULL if any input is NULL, so a row with a NULL key column produces a NULL `_source_key` and the MERGE can't match it. COALESCE to a sentinel before hashing -- see [[05-conforming-playbook/0502-synthetic-keys|0502]]. This is the one place where COALESCE is justified because it's protecting pipeline mechanics, not making a business decision.

**NULL in cursor columns.** A NULL `updated_at` makes the row invisible to incremental extraction -- `WHERE updated_at >= :last_run` never evaluates to true for NULL values. This is an extraction problem, not a null handling problem, and [[03-incremental-patterns/0310-create-vs-update-separation|0310]] covers the strategies.

**NULL in partition columns.** If you partition `orders` by `order_date` and some rows have `order_date = NULL`, those rows land in a `__NULL__` partition (BigQuery), a default partition (Snowflake), or fail the insert (ClickHouse, depending on config). None of these outcomes are what you want, but the fix belongs in the extraction query (filter or assign a sentinel partition value) -- not in a blanket COALESCE policy.

---

## Downstream Consequences

These are real, and you should document them -- but they're not your problem to fix in the ECL layer.

**`GROUP BY` behavior varies per engine.** BigQuery and PostgreSQL group NULLs together (all NULL values in one group). Some engines don't. An analyst who writes `GROUP BY status` and gets a NULL group isn't looking at a bug -- they're looking at data that has NULLs in the status column, which is what the source has.

**`COUNT(column)` vs `COUNT(*)`.** `COUNT(*)` counts all rows. `COUNT(status)` excludes rows where `status IS NULL`. Analysts who don't know this will report incorrect counts and blame the data. Document the NULL rate per column if it's significant, but don't COALESCE to inflate the count.

**Aggregation with NULLs.** `SUM(amount)` ignores NULLs. `AVG(amount)` ignores NULLs in both numerator and denominator. Both of these are correct SQL behavior, but consumers who expect NULLs to be treated as zeros will get different results than they expect. Again: document, don't fix.

> [!tip] Surface NULL rates in your data quality checks
> A table where `email` is 90% NULL is useful information for consumers. Surface it through [[06-operating-the-pipeline/0609-data-contracts|data contracts]] or quality checks so downstream teams know what they're working with -- but don't change the data to make the numbers look cleaner.

---

## By Corridor

> [!example]- Transactional → Columnar
> Columnar destinations are permissive with NULLs -- BigQuery, Snowflake, ClickHouse, and Redshift all accept NULL in any column regardless of the DDL. There's no NOT NULL enforcement to worry about, so NULLs from the source land without friction. The downstream behavior differences (GROUP BY, COUNT) are the consumer's responsibility.

> [!example]- Transactional → Transactional
> Transactional destinations *can* enforce NOT NULL. If the destination schema has NOT NULL constraints and the source has NULLs, the load fails -- and that's a schema mismatch to resolve by adjusting the destination DDL, not a reason to COALESCE in the extraction. If you genuinely need NOT NULL at the destination (for FK integrity or application requirements), make that a conscious decision and handle the NULLs explicitly in a documented transformation step, not silently in the ECL layer.

---

## Related Patterns

- [[05-conforming-playbook/0502-synthetic-keys|0502]] -- the one case where COALESCE before hashing is justified
- [[03-incremental-patterns/0310-create-vs-update-separation|0310]] -- NULL `updated_at` as an extraction problem
- [[01-foundations-and-archetypes/0102-what-is-conforming|0102]] -- null handling as conforming, not transforming
- [[06-operating-the-pipeline/0609-data-contracts|0609]] -- surfacing NULL rates through quality checks
