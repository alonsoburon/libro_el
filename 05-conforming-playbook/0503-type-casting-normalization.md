---
title: "Type Casting and Normalization"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Type Casting and Normalization

> **One-liner:** Every engine has its own type system and they don't agree on anything. Cast explicitly at extraction or let the loader guess wrong.

---

## The Playbook

Cast explicitly in the extraction query, not at the destination. The source knows its type system better than whatever the loader infers, and implicit casts hide precision loss that you won't notice until someone in accounting finds a discrepancy six months later.

That said, not all precision loss is negative. Very few tables need nanosecond precision -- second or microsecond is fine for most business data. The goal is to be deliberate about what you lose and what you keep, not to preserve every bit of precision across every column. A `DATETIME2(7)` truncated to `TIMESTAMP` (microseconds) is almost certainly fine. A `NUMERIC(18,6)` silently cast to `FLOAT64` is almost certainly not.

---

## The Mapping Table

The central reference for type conforming across engines. Every source-destination pair has a mapping, and the dangerous ones are the implicit casts that look harmless.

| Source type | Source engine | Destination type | Destination engine | Notes |
|---|---|---|---|---|
| `DATETIME2(7)` | SQL Server | `TIMESTAMP` | BigQuery | Nanosecond → microsecond truncation. Rarely matters |
| `DATETIME` | MySQL | `TIMESTAMP` | BigQuery | Second precision. Fine for most business data |
| `NUMERIC(18,6)` | PostgreSQL | `NUMERIC(38,9)` | BigQuery | Safe. Explicit DDL required -- loader defaults to `FLOAT64` |
| `NUMERIC(18,6)` | PostgreSQL | `FLOAT64` | BigQuery | **Dangerous.** Implicit cast loses decimal precision |
| `MONEY` | SQL Server | `NUMERIC(19,4)` | any | `MONEY` is a fixed-point type with 4 decimal places. Cast to NUMERIC explicitly |
| `BIT` | SQL Server | `BOOLEAN` | BigQuery | Works if all values are 0/1. See Boolean section below |
| `TINYINT(1)` | MySQL | `BOOLEAN` | BigQuery | MySQL's pseudo-boolean. May contain values other than 0/1 |
| `NVARCHAR(MAX)` | SQL Server / HANA | `STRING` | BigQuery | No length limit in BigQuery STRING. Safe |
| `TEXT` | PostgreSQL | `STRING` | BigQuery | PostgreSQL `TEXT` is unlimited. BigQuery `STRING` has a 10MB per-value limit. Rarely hit in practice |
| `JSONB` | PostgreSQL | `STRING` or `JSON` | BigQuery / Snowflake | See [[05-conforming-playbook/0507-nested-data-and-json|0507]] |
| `TIMESTAMP WITH TIME ZONE` | PostgreSQL | `TIMESTAMP` | BigQuery | BigQuery `TIMESTAMP` is always UTC. See [[05-conforming-playbook/0505-timezone-conforming|0505]] |

> [!tip] Let the mapping table drive your DDL
> If you define the destination DDL explicitly (rather than letting the loader infer it), the type mapping becomes the source of truth for your schema. New table? Look up each source column in the mapping, generate the DDL. This is mechanical work that belongs in a utility function, not in your head.

---

## Dangerous Implicit Casts

Three categories of implicit cast that loaders get wrong regularly:

### Numeric Precision

The most financially dangerous cast. `NUMERIC(18,6)` in PostgreSQL is exact -- it stores 18 digits with 6 decimal places using fixed-point arithmetic. `FLOAT64` in BigQuery is a 64-bit IEEE 754 floating point that can represent ~15-17 significant digits but rounds everything else.

```sql
-- What you expect:
SELECT CAST(123456789.123456 AS NUMERIC(18,6));
-- 123456789.123456

-- What FLOAT64 gives you:
SELECT CAST(123456789.123456 AS FLOAT64);
-- 123456789.12345600128173828125
```

Multiply that rounding error by a million invoice lines and the aggregate diverges from the source. Business people are surprisingly tolerant of float precision errors as long as it doesn't affect the big numbers -- a `ROUND()` downstream can be risky but worthwhile when you can't avoid `FLOAT64`.

**The fix by engine:**

| Destination | Use instead of FLOAT64 |
|---|---|
| BigQuery | `NUMERIC(38,9)` or `BIGNUMERIC(76,38)` |
| Snowflake | `NUMBER(18,6)` |
| ClickHouse | `Decimal(18,6)` |
| Redshift | `DECIMAL(18,6)` |

Explicit type in the destination DDL -- never let the loader infer the numeric type. Most loaders default to `FLOAT64` because it's the safest generic choice (it accepts everything), and that's exactly the problem.

Replicating with full precision is worth trying but don't get married to it. Some source engines have types that don't map cleanly to any destination type (SQL Server `MONEY` with implicit currency rounding, Oracle `NUMBER` without scale), and chasing exact precision across every column can burn more time than it's worth for columns where nobody cares about the sixth decimal.

### Timestamp Precision

`DATETIME2(7)` in SQL Server stores 100-nanosecond precision. `TIMESTAMP` in BigQuery stores microsecond precision. The cast truncates 1 digit of precision, which sounds alarming until you realize that virtually no business process generates or consumes nanosecond-precision timestamps. If your ERP writes `2026-03-15 14:30:00.1234567` and the destination stores `2026-03-15 14:30:00.123456`, nobody will notice -- and if they do, the conversation is about why the source has nanosecond precision, not about why the destination doesn't.

MySQL's `DATETIME` defaults to second precision. That's coarser, but it's what the source has -- landing it as a higher-precision type at the destination doesn't add information that wasn't there.

### String Length

PostgreSQL `TEXT` is unlimited. BigQuery `STRING` has a 10MB per-value limit. SQL Server `NVARCHAR(MAX)` can hold 2GB. These limits rarely matter for business data, but they matter for columns that store embedded documents, base64-encoded blobs, or serialized objects. If a value exceeds the destination's limit, the load fails silently or truncates -- neither is acceptable. Check the max value length of suspicious columns before committing to a type mapping.

---

## Boolean Representations

The source stores boolean-like values in at least six different ways depending on the engine and the application layer:

| Representation | Source engine / system | Notes |
|---|---|---|
| `BIT` (0/1) | SQL Server | True boolean. Safe to cast |
| `TINYINT(1)` | MySQL | Pseudo-boolean. May contain 2, 3, or -1 |
| `BOOLEAN` | PostgreSQL | True boolean |
| `'Y'` / `'N'` | Various ORMs, SAP B1 (English) | String. Not a boolean at the engine level |
| `'S'` / `'N'` | SAP B1 (Spanish/Portuguese install) | Same table, different literal depending on install language |
| `1` / `0` as `INTEGER` | Legacy systems | Integer semantics, boolean intent |

The ECL layer *can* cast to the destination's native boolean, but only with good justification. "The destination has a `BOOL` type" isn't sufficient reason to reinterpret a `'Y'`/`'N'` string column -- the source stored a string, and reflecting the source faithfully means landing a string. Cast to `BOOLEAN` when the source type is already a boolean (`BIT`, `BOOLEAN`) and the destination has a native equivalent. Leave string representations as strings.

> [!warning] Three-state logic
> Some boolean-looking columns actually carry three states: true, false, and unknown (or not applicable). A `BOOLEAN` can't represent this. If the source column has NULL alongside `'Y'`/`'N'`, and NULL has a distinct business meaning ("not yet evaluated" vs. "evaluated as no"), casting to `BOOLEAN` with COALESCE destroys that distinction. Keep the source type.

---

## Decimal Precision

`NUMERIC(18,6)` in PostgreSQL is exact. `FLOAT64` in BigQuery is not. This section covers the mechanics and the pragmatics.

**Where it hurts:** financial data, unit prices, exchange rates. Multiplied by millions of rows, even tiny rounding errors accumulate into visible discrepancies in aggregate reports. An invoice total that's off by $0.00001 per line becomes $10 off on a million-line summary -- and accounting will find it.

**Where it doesn't hurt:** quantities, counts, percentages, scores. If the column is an integer disguised as a decimal (`quantity = 5.000000`), `FLOAT64` is fine. If the column has meaningful decimal places but nobody aggregates it across millions of rows, `FLOAT64` is probably fine. The damage is proportional to row count × aggregation.

**The pragmatic approach:** explicit `NUMERIC` in the DDL for financial columns, `FLOAT64` for everything else unless proven otherwise. Monitor aggregate differences between source and destination on the critical columns ([[06-operating-the-pipeline/0613-reconciliation-patterns|0613]]) and escalate if the divergence exceeds an acceptable threshold.

---

## Engine-Specific Traps

A few combinations that produce surprising behavior:

**SQL Server `BIT` vs PostgreSQL `BOOLEAN` vs MySQL `TINYINT(1)`.** SQL Server's `BIT` is a true boolean (0 or 1, nothing else). MySQL's `TINYINT(1)` is an integer that the driver *displays* as boolean but happily stores 2, 127, or -1. PostgreSQL's `BOOLEAN` is a true boolean. If you're extracting from MySQL and the column has values outside 0/1, a cast to `BOOLEAN` fails or silently coerces -- check the actual value distribution before casting.

**SAP HANA `NVARCHAR` vs `VARCHAR`.** HANA defaults to `NVARCHAR` (Unicode) for most string columns. When extracting to a destination that distinguishes between Unicode and non-Unicode strings (SQL Server, MySQL), you need to match the encoding or risk truncation on characters outside the ASCII range. When extracting to BigQuery or Snowflake (UTF-8 everywhere), this distinction vanishes.

**Schema evolution interaction.** A new column appears in the source with a type your cast map doesn't cover. If your extraction uses `SELECT *`, the column arrives with whatever type the loader infers -- which might be wrong. If your extraction uses an explicit column list, the column is silently dropped. Both are problems. See [[04-load-strategies/0403-merge-upsert|0403]]'s schema evolution section for the detect → decide → apply workflow.

---

## By Corridor

> [!example]- Transactional → Columnar
> The widest type gap. Type systems are fundamentally different -- transactional engines have dozens of specific types (`MONEY`, `SMALLINT`, `NCHAR(10)`, `DATETIME2(7)`) that columnar engines collapse into a handful (`INT64`, `FLOAT64`, `STRING`, `TIMESTAMP`). Explicit casting is mandatory because the loader's inference maps everything to the broadest compatible type, which is almost always `FLOAT64` for numbers and `STRING` for text. Define your destination DDL explicitly for every table.

> [!example]- Transactional → Transactional
> Narrower gap, but dialect differences still bite. PostgreSQL `BOOLEAN` vs MySQL `TINYINT(1)`, SQL Server `DATETIME2` vs PostgreSQL `TIMESTAMP`, MySQL `UNSIGNED INT` vs PostgreSQL (no unsigned types). If source and destination run the same engine, the type mapping is nearly 1:1 and explicit casting is rarely needed.

---

## Related Patterns

- [[01-foundations-and-archetypes/0104-columnar-destinations|0104]] -- full type mapping tables per engine
- [[04-load-strategies/0403-merge-upsert|0403]] -- schema evolution and how new types interact with MERGE
- [[01-foundations-and-archetypes/0102-what-is-conforming|0102]] -- type casting as a conforming operation, not transformation
- [[05-conforming-playbook/0505-timezone-conforming|0505]] -- timestamp timezone handling, adjacent to timestamp precision
- [[06-operating-the-pipeline/0613-reconciliation-patterns|0613]] -- monitoring precision drift on financial columns
