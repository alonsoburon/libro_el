#import "theme.typ": palette, ecl-tip, ecl-warning, ecl-danger, ecl-info
= Metadata Column Injection
<metadata-column-injection>
#quote(block: true)[
#strong[One-liner:] `_extracted_at`, `_batch_id`, `_source_hash` -- columns the source doesn't have that your pipeline needs for debugging, dedup, and reconciliation.
]

// ---

== The Playbook
<the-playbook>
Metadata columns are just new columns added to the extraction query. Every destination supports them -- columnar or transactional, doesn't matter. You're not changing what the data means; you're tagging each row with information about how and when it arrived.

Three metadata columns, each with a different purpose and a different cost/benefit ratio. Not every table needs all three.

```sql
-- source: transactional
SELECT
    order_id,
    customer_id,
    status,
    total,
    updated_at,
    -- metadata columns
    CURRENT_TIMESTAMP                          AS _extracted_at,
    :batch_id                                  AS _batch_id,
    MD5(CONCAT(order_id, '|', status, '|', total)) AS _source_hash
FROM orders
WHERE updated_at >= :last_run;
```

// ---

== `_extracted_at`
<extracted_at>
The pipeline's timestamp: when your extraction ran, not when the source row was last modified. A row updated 3 days ago and extracted today has `_extracted_at = today`. This distinction matters because `updated_at` is the source's clock -- maintained by the application layer, subject to all the reliability problems covered in 0301 -- while `_extracted_at` is your clock, set by your pipeline, and always correct.

Always add this. The cost is trivial (`CURRENT_TIMESTAMP` in the SELECT) and the debugging value is enormous. When something goes wrong -- and it will -- `_extracted_at` is how you answer "when did this bad data arrive?" and "which extraction run brought it?"

`_extracted_at` is also the foundation for dedup ordering in 0404. The `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _extracted_at DESC) = 1` view depends entirely on this column to determine which version of a row is the latest. Without it, the dedup view has no ordering key and the pattern doesn't work.

#ecl-warning("Share one timestamp per run")[If you extract `orders` and `order_lines` in the same pipeline run, they should share the same `_extracted_at` value. This makes it easy to identify which rows were extracted together and simplifies cross-table debugging. Set the timestamp once at the start of the run and pass it to every extraction query.]

// ---

== `_batch_id`
<batch_id>
Correlates all rows from the same extraction run. Where `_extracted_at` tells you #emph[when];, `_batch_id` tells you #emph[which run] -- and that distinction matters when you have multiple runs with the same timestamp (retries, overlapping schedules) or when you need to operate on an entire batch at once.

Three use cases earn the column:

#strong[Rollback.] "Batch 47 loaded bad data. Delete everything from batch 47." With `_batch_id`, that's a single `DELETE WHERE _batch_id = 47`. Without it, you're reverse-engineering which rows came from that run using timestamp ranges and hoping you don't catch rows from adjacent runs.

#strong[Debugging.] "The destination has 11,998 rows but the source had 12,000. Which batch lost them?" With `_batch_id`, you can trace each row to the run that loaded it and compare batch-level counts against source-side logs.

#strong[Reconciliation.] A `_batches` table that tracks batch-level metadata -- source row count, extraction start/end time, status -- gives you an audit trail for every extraction. When 0614 compares source and destination counts, `_batch_id` is the join key.

UUID or sequential integer -- consistency matters more than format. If your orchestrator already generates run IDs, reuse those.

=== The `_batches` Table
<the-_batches-table>
A lightweight metadata table on the destination that tracks each extraction run:

```sql
-- destination: any
CREATE TABLE _batches (
    batch_id        TEXT PRIMARY KEY,
    table_name      TEXT NOT NULL,
    extracted_at    TIMESTAMP NOT NULL,
    source_row_count INTEGER,
    dest_row_count  INTEGER,
    status          TEXT DEFAULT 'running',  -- running, completed, failed
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP
);
```

Your pipeline writes a row to `_batches` at the start of each run (`status = 'running'`), updates it after load completes (`status = 'completed'`, `dest_row_count` filled in), and marks it failed on error. This gives you a single place to answer "when did each table last load successfully?" and "which tables are currently loading?" -- questions that become surprisingly hard to answer without it.

// ---

== `_source_hash`
<source_hash>
A hash of the source row at extraction time. Enables 0208 (compare hashes between runs to detect changes without relying on `updated_at`) and post-load reconciliation (compare source-side hash vs destination-side hash to verify the row arrived intact).

```sql
-- source: transactional
-- Hash all business columns, excluding _extracted_at and _batch_id
SELECT
    *,*
    MD5(CONCAT(
        COALESCE(order_id::TEXT, '__NULL__'), '|',
        COALESCE(status, '__NULL__'), '|',
        COALESCE(total::TEXT, '__NULL__')
    )) AS _source_hash
FROM orders;
```

#strong[Expensive at scale.] Hashing every row adds compute on the source or in the pipeline. At \~800 tables, this can add 20 minutes to an already tight extraction window. The cost is proportional to row count × column count -- wide tables with millions of rows are where it hurts.

#strong[Tiered approach.] Not every table earns the overhead. High-value mutable tables (`invoices`, `orders`) where change detection matters and `updated_at` is unreliable -- those earn `_source_hash`. Stable config tables that change once a quarter, append-only tables like `events` where you never need to detect mutations -- skip it.

#strong[NULL handling.] COALESCE every column to a sentinel before hashing. Most hash functions return NULL if any input is NULL, which means a row with a single NULL column produces a NULL hash -- indistinguishable from every other row with a NULL in the same position. `COALESCE(col, '__NULL__')` before concatenation prevents this.

#ecl-warning("Hash business columns, not metadata")[Exclude `_extracted_at` and `_batch_id` from the hash input. These change every run by design -- including them means the hash changes every run too, defeating the purpose of change detection.]

// ---

== Where to Inject
<where-to-inject>
Every metadata column runs #emph[somewhere] -- in the source query, in Python between extraction and load, or in a staging transform on the destination. The choice depends on what the source can handle and how much compute you're willing to add to the extraction.

#strong[Source query.] Cheapest if the source can handle it. `CURRENT_TIMESTAMP` is free on every engine. `MD5()` is available on PostgreSQL, MySQL, SQL Server, and SAP HANA with slightly different syntax. The conforming happens in the same query that extracts the data -- no extra hop, no extra infrastructure.

#strong[Orchestrator / middleware.] Python adds the columns after extraction, before load. More control (you can use a consistent hashing library across all sources regardless of engine), but you're adding an extra data hop and holding the full batch in memory or on disk while you process it.

#strong[Staging.] Land the raw data without metadata, then add the columns in a staging transform on the destination. Works well when you want to keep the extraction query minimal and offload all conforming to the destination's compute. Common in BigQuery workflows where staging + transform is the standard pattern.

For `_extracted_at` and `_batch_id`, the source query is almost always the right place -- the cost is negligible. For `_source_hash`, the source query or Python are both reasonable depending on whether your source engine has a convenient hash function and whether the compute cost on the source is acceptable.

// ---

== By Corridor
#ecl-warning("Transactional to columnar")[No special considerations. Columnar destinations accept new columns without issue. If you're using 0404, `_extracted_at` is the dedup ordering key -- make sure it's populated on every row.]

#ecl-info("Transactional to transactional")[Same approach. One advantage: if you need to add metadata columns to an existing destination table retroactively, `ALTER TABLE ADD COLUMN` is cheap and instant on most transactional engines. On columnar engines it's also cheap, but backfilling the column for historical rows is more expensive.]

// ---

== Related Patterns
- 0404 -- `_extracted_at` as the dedup ordering key
- 0208 -- `_source_hash` enables hash-based change detection
- 0614 -- `_batch_id` for source-destination row count reconciliation
- 0301 -- why `updated_at` is unreliable and `_extracted_at` is your safety net

// ---

= Synthetic Keys
<synthetic-keys>
#quote(block: true)[
#strong[One-liner:] No PK, composite PK, unstable PK -- when the source doesn't give you a reliable key for MERGE, build one from immutable business characteristics.
]

// ---

== When You Need One
<when-you-need-one>
A MERGE needs a key to match source rows against destination rows. If the source gives you a clean, stable, single-column PK, use it and move on. The problems start when the source doesn't cooperate:

#strong[No PK at all.] Some tables genuinely have no primary key -- log tables, staging tables, tables where the DBA forgot. Without a key, MERGE has nothing to match on and you're stuck with full replace or append-only.

#strong[Composite PK of 5+ columns.] The table technically has a key, but it's `(company_code, fiscal_year, document_type, document_number, line_number)`. A MERGE matching on 5 columns works but is unwieldy, slow on some engines, and painful to debug when something doesn't match.

#strong[Recycled auto-increment.] The PK is an `id` that looks stable, but when rows are deleted the ids get reused. Row 42 today is a different entity than row 42 last week. Your MERGE overwrites the new row with the old row's data because the key matches on the wrong entity.

#strong[NULLs in key columns.] `NULL != NULL` in every SQL engine. A MERGE with NULL in the key column silently misses the row on every run -- the match condition never evaluates to true, so the row gets re-inserted as a "new" row every time. You end up with duplicates that accumulate run after run.

// ---

== Building the Key
<building-the-key>
The principle: hash immutable business characteristics -- attributes that identify the entity and won't change over its lifetime. The hash becomes `_source_key`, a single column the MERGE matches on.

#strong[What's immutable.] Natural business identifiers: order number + line number, customer code + document type, SKU + warehouse code. These describe #emph[what] the entity is, not #emph[what happened] to it. If someone corrects the entity later, these columns stay the same.

#strong[What's not immutable.] Amounts, prices, dates, status fields -- anything that can be corrected or updated after the row is created. If you include `total` in the hash and someone corrects an invoice amount, the hash changes and the MERGE treats the corrected row as a new entity instead of an update to the existing one.

```sql
-- source: transactional
-- Build a synthetic key from immutable business identifiers
SELECT
    MD5(CONCAT(
        COALESCE(company_code, '__NULL__'), '|',
        COALESCE(document_type, '__NULL__'), '|',
        COALESCE(document_number::TEXT, '__NULL__'), '|',
        COALESCE(line_number::TEXT, '__NULL__')
    )) AS _source_key,
    *
FROM invoice_lines;
```

=== Concatenation vs Hash
<concatenation-vs-hash>
Two approaches, with a clear winner for most cases:

#strong[Concatenation.] `CONCAT(order_id, '-', line_number)` → `'1001-3'`. Readable and debuggable -- you can look at the key and know which entity it represents. The problem is fragility: if the delimiter (`-`) appears in the data, `CONCAT('100', '-', '13')` and `CONCAT('1001', '-', '3')` are distinguishable, but `CONCAT('10-0', '-', '13')` isn't what you expected. Column ordering matters too -- reversing the concat order produces different keys for the same entity across pipeline versions.

#strong[Hash.] `MD5(CONCAT(col1, '|', col2, '|', col3))` → `'a3f2b8c1...'`. Opaque but safe. The output is always the same length, the delimiter problem effectively disappears (collisions from delimiter confusion are astronomically unlikely in the hash space), and column ordering is consistent as long as the pipeline code doesn't change. The downside is that you can't read the key and know which entity it represents -- debugging requires recomputing the hash from the source columns.

For most pipelines, hash wins. You look at the key rarely; the pipeline matches on it constantly.

// ---

== NULL in Key Columns
<null-in-key-columns>
Most hash functions return NULL if any input is NULL. A row with `company_code = 'ACME'` and `document_number = NULL` produces `MD5(CONCAT('ACME', '|', NULL))` → NULL. Every row with a NULL in the same position produces the same NULL hash, and the MERGE treats them all as the same entity -- or worse, misses them entirely because `NULL != NULL` in the match condition.

COALESCE every column to a sentinel before hashing:

```sql
-- source: transactional
MD5(CONCAT(
    COALESCE(company_code, '__NULL__'), '|',
    COALESCE(document_number::TEXT, '__NULL__')
))
```

The sentinel (`'__NULL__'`) must be something that can't appear in real data. `'__NULL__'` works because no business column will contain that literal string. A shorter sentinel like `''` (empty string) is dangerous because empty strings #emph[do] appear in real data and you'd be conflating NULL with empty -- the exact problem 0504 warns against.

Document which columns participate in the key. Downstream consumers, reconciliation queries, and future pipeline maintainers need to know how `_source_key` is built so they can recompute it when debugging.

// ---

== Hash Function Choice
<conforming-hash-function>
#strong[MD5.] 128-bit output, fast on every engine. The standard choice for synthetic keys. This isn't cryptography -- you're not protecting against adversarial collisions, you're generating unique identifiers for MERGE matching. MD5's known cryptographic weaknesses are irrelevant here.

#strong[SHA-256.] 256-bit output, slower. The only reason to use it over MD5 is if regulatory or audit requirements mandate a specific hash function. Some compliance frameworks specify SHA-256 for anything labeled "hash" regardless of context -- easier to comply than to argue.

=== When 128 Bits Isn't Enough
<when-128-bits-isnt-enough>
The birthday paradox determines collision probability: with a 128-bit hash, you'd need roughly $2^64$ (\~18 quintillion) rows before a 50% chance of any two rows colliding. At 1 billion rows, the probability of a single collision is approximately $frac(1, 3.4 times 10^20)$ -- for practical purposes, zero. You'll hit every other failure mode in your pipeline long before you hit an MD5 collision on a synthetic key.

If you're hashing across multiple tables and worry about cross-table collisions, include the table name in the hash input: `MD5(CONCAT('invoice_lines', '|', col1, '|', col2))`. This is more about hygiene than probability.

// ---

== By Corridor
<by-corridor-1>
#ecl-warning("Transactional to columnar")[Columnar destinations don't enforce UNIQUE, so a bad synthetic key doesn't produce an error -- it produces silent duplicates. A key built from mutable columns, or a key that doesn't COALESCE NULLs, will generate multiple rows for the same entity with no warning from the engine. The dedup is entirely your responsibility, and the only way to catch it is to monitor for duplicate `_source_key` values after load.]

#ecl-info("Transactional to transactional")[Transactional destinations can enforce UNIQUE on `_source_key`. Add a unique index or constraint on the column, and a collision or a badly constructed key gets rejected at the database level with an explicit error. This is a genuine safety net that columnar destinations can't offer -- use it.]

// ---

== Related Patterns
<related-patterns-1>
- 0403 -- the MERGE that needs a reliable key to match on
- 0208 -- hash-based change detection uses similar mechanics but hashes all columns (mutable included) to detect changes, while synthetic keys hash only immutable columns to identify entities
- 0106 -- "PKs are unique and stable" is often a soft rule
- 0501 -- `_source_hash` hashes the full row for change detection; `_source_key` hashes immutable columns for identity. Different purpose, similar mechanics

// ---

= Type Casting and Normalization
<type-casting-and-normalization>
#quote(block: true)[
#strong[One-liner:] Every engine has its own type system and they don't agree on anything. Cast explicitly at extraction or let the loader guess wrong.
]

// ---

== The Playbook
<the-playbook-1>
Cast explicitly in the extraction query, not at the destination. The source knows its type system better than whatever the loader infers, and implicit casts hide precision loss that you won't notice until someone in accounting finds a discrepancy six months later.

That said, not all precision loss is negative. Very few tables need nanosecond precision -- second or microsecond is fine for most business data. The goal is to be deliberate about what you lose and what you keep, not to preserve every bit of precision across every column. A `DATETIME2(7)` truncated to `TIMESTAMP` (microseconds) is almost certainly fine. A `NUMERIC(18,6)` silently cast to `FLOAT64` is almost certainly not.

// ---

== The Mapping Table
<the-mapping-table>
The central reference for type conforming across engines. Every source-destination pair has a mapping, and the dangerous ones are the implicit casts that look harmless.

#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto,auto,auto,auto,auto,),
    table.header([Source type], [Source engine], [Destination type], [Destination engine], [Notes],),
    table.hline(),
    [`DATETIME2(7)`], [SQL Server], [`TIMESTAMP`], [BigQuery], [Nanosecond → microsecond truncation. Rarely matters],
    [`DATETIME`], [MySQL], [`TIMESTAMP`], [BigQuery], [Second precision. Fine for most business data],
    [`NUMERIC(18,6)`], [PostgreSQL], [`NUMERIC(38,9)`], [BigQuery], [Safe. Explicit DDL required -- loader defaults to `FLOAT64`],
    [`NUMERIC(18,6)`], [PostgreSQL], [`FLOAT64`], [BigQuery], [#strong[Dangerous.] Implicit cast loses decimal precision],
    [`MONEY`], [SQL Server], [`NUMERIC(19,4)`], [any], [`MONEY` is a fixed-point type with 4 decimal places. Cast to NUMERIC explicitly],
    [`BIT`], [SQL Server], [`BOOLEAN`], [BigQuery], [Works if all values are 0/1. See Boolean section below],
    [`TINYINT(1)`], [MySQL], [`BOOLEAN`], [BigQuery], [MySQL's pseudo-boolean. May contain values other than 0/1],
    [`NVARCHAR(MAX)`], [SQL Server / HANA], [`STRING`], [BigQuery], [No length limit in BigQuery STRING. Safe],
    [`TEXT`], [PostgreSQL], [`STRING`], [BigQuery], [PostgreSQL `TEXT` is unlimited. BigQuery `STRING` has a 10MB per-value limit. Rarely hit in practice],
    [`JSONB`], [PostgreSQL], [`STRING` or `JSON`], [BigQuery / Snowflake], [See @nested-data-and-json],
    [`TIMESTAMP WITH TIME ZONE`], [PostgreSQL], [`TIMESTAMP`], [BigQuery], [BigQuery `TIMESTAMP` is always UTC. See @timezone-conforming],
  )]
  , kind: table
  )

#ecl-warning("Let the mapping table drive DDL")[If you define the destination DDL explicitly (rather than letting the loader infer it), the type mapping becomes the source of truth for your schema. New table? Look up each source column in the mapping, generate the DDL. This is mechanical work that belongs in a utility function, not in your head.]

// ---

== Dangerous Implicit Casts
<dangerous-implicit-casts>
Three categories of implicit cast that loaders get wrong regularly:

=== Numeric Precision
<numeric-precision>
The most financially dangerous cast. `NUMERIC(18,6)` in PostgreSQL is exact -- it stores 18 digits with 6 decimal places using fixed-point arithmetic. `FLOAT64` in BigQuery is a 64-bit IEEE 754 floating point that can represent \~15-17 significant digits but rounds everything else.

```sql
-- What you expect:
SELECT CAST(123456789.123456 AS NUMERIC(18,6));
-- 123456789.123456

-- What FLOAT64 gives you:
SELECT CAST(123456789.123456 AS FLOAT64);
-- 123456789.12345600128173828125
```

Multiply that rounding error by a million invoice lines and the aggregate diverges from the source. Business people are surprisingly tolerant of float precision errors as long as it doesn't affect the big numbers -- a `ROUND()` downstream can be risky but worthwhile when you can't avoid `FLOAT64`.

#strong[The fix by engine:]

#figure(
  align(center)[#table(
    columns: 2,
    align: (auto,auto,),
    table.header([Destination], [Use instead of FLOAT64],),
    table.hline(),
    [BigQuery], [`NUMERIC(38,9)` or `BIGNUMERIC(76,38)`],
    [Snowflake], [`NUMBER(18,6)`],
    [ClickHouse], [`Decimal(18,6)`],
    [Redshift], [`DECIMAL(18,6)`],
  )]
  , kind: table
  )

Explicit type in the destination DDL -- never let the loader infer the numeric type. Most loaders default to `FLOAT64` because it's the safest generic choice (it accepts everything), and that's exactly the problem.

Replicating with full precision is worth trying but don't get married to it. Some source engines have types that don't map cleanly to any destination type (SQL Server `MONEY` with implicit currency rounding, Oracle `NUMBER` without scale), and chasing exact precision across every column can burn more time than it's worth for columns where nobody cares about the sixth decimal.

=== Timestamp Precision
<timestamp-precision>
`DATETIME2(7)` in SQL Server stores 100-nanosecond precision. `TIMESTAMP` in BigQuery stores microsecond precision. The cast truncates 1 digit of precision, which sounds alarming until you realize that virtually no business process generates or consumes nanosecond-precision timestamps. If your ERP writes `2026-03-15 14:30:00.1234567` and the destination stores `2026-03-15 14:30:00.123456`, nobody will notice -- and if they do, the conversation is about why the source has nanosecond precision, not about why the destination doesn't.

MySQL's `DATETIME` defaults to second precision. That's coarser, but it's what the source has -- landing it as a higher-precision type at the destination doesn't add information that wasn't there.

=== String Length
<string-length>
PostgreSQL `TEXT` is unlimited. BigQuery `STRING` has a 10MB per-value limit. SQL Server `NVARCHAR(MAX)` can hold 2GB. These limits rarely matter for business data, but they matter for columns that store embedded documents, base64-encoded blobs, or serialized objects. If a value exceeds the destination's limit, the load fails silently or truncates -- neither is acceptable. Check the max value length of suspicious columns before committing to a type mapping.

// ---

== Boolean Representations
<boolean-representations>
The source stores boolean-like values in at least six different ways depending on the engine and the application layer:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Representation], [Source engine / system], [Notes],),
    table.hline(),
    [`BIT` (0/1)], [SQL Server], [True boolean. Safe to cast],
    [`TINYINT(1)`], [MySQL], [Pseudo-boolean. May contain 2, 3, or -1],
    [`BOOLEAN`], [PostgreSQL], [True boolean],
    [`'Y'` / `'N'`], [Various ORMs, SAP B1 (English)], [String. Not a boolean at the engine level],
    [`'S'` / `'N'`], [SAP B1 (Spanish/Portuguese install)], [Same table, different literal depending on install language],
    [`1` / `0` as `INTEGER`], [Legacy systems], [Integer semantics, boolean intent],
  )]
  , kind: table
  )

The ECL layer #emph[can] cast to the destination's native boolean, but only with good justification. "The destination has a `BOOL` type" isn't sufficient reason to reinterpret a `'Y'`/`'N'` string column -- the source stored a string, and reflecting the source faithfully means landing a string. Cast to `BOOLEAN` when the source type is already a boolean (`BIT`, `BOOLEAN`) and the destination has a native equivalent. Leave string representations as strings.

#ecl-warning("Three-state logic")[Some boolean-looking columns actually carry three states: true, false, and unknown (or not applicable). A `BOOLEAN` can't represent this. If the source column has NULL alongside `'Y'`/`'N'`, and NULL has a distinct business meaning ("not yet evaluated" vs.~"evaluated as no"), casting to `BOOLEAN` with COALESCE destroys that distinction. Keep the source type.]

// ---

== Decimal Precision
<decimal-precision>
`NUMERIC(18,6)` in PostgreSQL is exact. `FLOAT64` in BigQuery is not. This section covers the mechanics and the pragmatics.

#strong[Where it hurts:] financial data, unit prices, exchange rates. Multiplied by millions of rows, even tiny rounding errors accumulate into visible discrepancies in aggregate reports. An invoice total that's off by \$0.00001 per line becomes \$10 off on a million-line summary -- and accounting will find it.

#strong[Where it doesn't hurt:] quantities, counts, percentages, scores. If the column is an integer disguised as a decimal (`quantity = 5.000000`), `FLOAT64` is fine. If the column has meaningful decimal places but nobody aggregates it across millions of rows, `FLOAT64` is probably fine. The damage is proportional to row count × aggregation.

#strong[The pragmatic approach:] explicit `NUMERIC` in the DDL for financial columns, `FLOAT64` for everything else unless proven otherwise. Monitor aggregate differences between source and destination on the critical columns (0614) and escalate if the divergence exceeds an acceptable threshold.

// ---

== Engine-Specific Traps
<engine-specific-traps>
A few combinations that produce surprising behavior:

#strong[SQL Server `BIT` vs PostgreSQL `BOOLEAN` vs MySQL `TINYINT(1)`.] SQL Server's `BIT` is a true boolean (0 or 1, nothing else). MySQL's `TINYINT(1)` is an integer that the driver #emph[displays] as boolean but happily stores 2, 127, or -1. PostgreSQL's `BOOLEAN` is a true boolean. If you're extracting from MySQL and the column has values outside 0/1, a cast to `BOOLEAN` fails or silently coerces -- check the actual value distribution before casting.

#strong[SAP HANA `NVARCHAR` vs `VARCHAR`.] HANA defaults to `NVARCHAR` (Unicode) for most string columns. When extracting to a destination that distinguishes between Unicode and non-Unicode strings (SQL Server, MySQL), you need to match the encoding or risk truncation on characters outside the ASCII range. When extracting to BigQuery or Snowflake (UTF-8 everywhere), this distinction vanishes.

#strong[Schema evolution interaction.] A new column appears in the source with a type your cast map doesn't cover. If your extraction uses `SELECT *`, the column arrives with whatever type the loader infers -- which might be wrong. If your extraction uses an explicit column list, the column is silently dropped. Both are problems. See 0403's schema evolution section for the detect → decide → apply workflow.

// ---

== By Corridor
<by-corridor-2>
#ecl-warning("Transactional to columnar")[The widest type gap. Type systems are fundamentally different -- transactional engines have dozens of specific types (`MONEY`, `SMALLINT`, `NCHAR(10)`, `DATETIME2(7)`) that columnar engines collapse into a handful (`INT64`, `FLOAT64`, `STRING`, `TIMESTAMP`). Explicit casting is mandatory because the loader's inference maps everything to the broadest compatible type, which is almost always `FLOAT64` for numbers and `STRING` for text. Define your destination DDL explicitly for every table.]

#ecl-info("Transactional to transactional")[Narrower gap, but dialect differences still bite. PostgreSQL `BOOLEAN` vs MySQL `TINYINT(1)`, SQL Server `DATETIME2` vs PostgreSQL `TIMESTAMP`, MySQL `UNSIGNED INT` vs PostgreSQL (no unsigned types). If source and destination run the same engine, the type mapping is nearly 1:1 and explicit casting is rarely needed.]

// ---

== Related Patterns
<related-patterns-2>
- 0104 -- full type mapping tables per engine
- 0403 -- schema evolution and how new types interact with MERGE
- 0102 -- type casting as a conforming operation, not transformation
- 0505 -- timestamp timezone handling, adjacent to timestamp precision
- 0614 -- monitoring precision drift on financial columns

// ---

= Null Handling
<null-handling>
#quote(block: true)[
#strong[One-liner:] NULL means NULL. Reflect the source as-is -- don't COALESCE, don't mix representations, don't solve downstream's problems in the ECL layer.
]

// ---

== The Playbook
<the-playbook-2>
ECL is about reflecting the source as faithfully as possible. If the source has NULL, land NULL. If the source has empty string, land empty string. If the source has `'N/A'`, land `'N/A'`. These are three different values with potentially three different meanings, and converting one to the other is a business decision -- it belongs downstream, not in the conforming layer.

The temptation to "clean up" NULLs at extraction is strong, especially when you know downstream consumers will struggle with them. Resist it. A COALESCE in the extraction query looks harmless, but it makes an irreversible choice about what NULL means for every consumer of the table, and that choice may be wrong for some of them. A NULL `email` might mean "not provided" for the marketing team and "not applicable" for the billing team -- collapsing it to `''` destroys the distinction for both.

// ---

== The Rule: Don't Mix, Don't Match
<the-rule-dont-mix-dont-match>
The worst outcome isn't NULLs in the destination -- it's inconsistent representations of "nothing" across tables, columns, or even rows within the same column.

The source might use any combination of:

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Representation], [Where you find it],),
    table.hline(),
    [`NULL`], [Standard SQL databases, ORMs that use NULL semantics],
    [`''` (empty string)], [Legacy systems, some ORMs that default to empty string on form fields],
    [`'N/A'` / `'NONE'` / `'-'`], [Manual data entry, ERP display defaults, CSV exports],
    [`0`], [Numeric columns where "no value" was entered as zero],
  )]
  , kind: table
  )

If different source tables use different representations, document the inconsistency but don't normalize them to a single representation at the ECL level. Table A uses NULL for "no email" and table B uses `''` for "no email" -- that's the source's problem, and it's worth documenting, but converting one to match the other is a transformation decision. The downstream team gets to decide how to unify them because they understand the business context better than the pipeline does.

#ecl-warning("Avoid COALESCE in the conforming layer")[`COALESCE(email, '')` in the extraction query looks like cleanup. What it actually does: permanently destroys the distinction between "this field was never populated" (NULL) and "this field was explicitly set to empty" (empty string). If that distinction matters to even one consumer, you've lost it for all of them. The only justified COALESCE at the ECL level is in synthetic key hashing (see 0502), where NULL would corrupt the hash output -- and that's infrastructure, not business logic.]

// ---

== When NULLs Matter at the ECL Level
<when-nulls-matter-at-the-ecl-level>
NULLs don't need fixing in the ECL layer, but they do need #emph[awareness] -- three places where NULL behavior affects the pipeline itself, not downstream consumption:

#strong[NULL in synthetic key columns.] Most hash functions return NULL if any input is NULL, so a row with a NULL key column produces a NULL `_source_key` and the MERGE can't match it. COALESCE to a sentinel before hashing -- see 0502. This is the one place where COALESCE is justified because it's protecting pipeline mechanics, not making a business decision.

#strong[NULL in cursor columns.] A NULL `updated_at` makes the row invisible to incremental extraction -- `WHERE updated_at >= :last_run` never evaluates to true for NULL values. This is an extraction problem, not a null handling problem, and 0310 covers the strategies.

#strong[NULL in partition columns.] If you partition `orders` by `order_date` and some rows have `order_date = NULL`, those rows land in a `__NULL__` partition (BigQuery), a default partition (Snowflake), or fail the insert (ClickHouse, depending on config). None of these outcomes are what you want, but the fix belongs in the extraction query (filter or assign a sentinel partition value) -- not in a blanket COALESCE policy.

// ---

== Downstream Consequences
<downstream-consequences>
These are real, and you should document them -- but they're not your problem to fix in the ECL layer.

#strong[`GROUP BY` behavior varies per engine.] BigQuery and PostgreSQL group NULLs together (all NULL values in one group). Some engines don't. An analyst who writes `GROUP BY status` and gets a NULL group isn't looking at a bug -- they're looking at data that has NULLs in the status column, which is what the source has.

#strong[`COUNT(column)` vs `COUNT(*)`.] `COUNT(*)` counts all rows. `COUNT(status)` excludes rows where `status IS NULL`. Analysts who don't know this will report incorrect counts and blame the data. Document the NULL rate per column if it's significant, but don't COALESCE to inflate the count.

#strong[Aggregation with NULLs.] `SUM(amount)` ignores NULLs. `AVG(amount)` ignores NULLs in both numerator and denominator. Both of these are correct SQL behavior, but consumers who expect NULLs to be treated as zeros will get different results than they expect. Again: document, don't fix.

#ecl-tip("Surface NULL rates in quality checks")[A table where `email` is 90% NULL is useful information for consumers. Surface it through data contracts or quality checks so downstream teams know what they're working with -- but don't change the data to make the numbers look cleaner.]

// ---

== By Corridor
<by-corridor-3>
#ecl-warning("Transactional to columnar")[Columnar destinations are permissive with NULLs -- BigQuery, Snowflake, ClickHouse, and Redshift all accept NULL in any column regardless of the DDL. There's no NOT NULL enforcement to worry about, so NULLs from the source land without friction. The downstream behavior differences (GROUP BY, COUNT) are the consumer's responsibility.]

#ecl-info("Transactional to transactional")[Transactional destinations _can_ enforce NOT NULL. If the destination schema has NOT NULL constraints and the source has NULLs, the load fails -- and that's a schema mismatch to resolve by adjusting the destination DDL, not a reason to COALESCE in the extraction. If you genuinely need NOT NULL at the destination (for FK integrity or application requirements), make that a conscious decision and handle the NULLs explicitly in a documented transformation step, not silently in the ECL layer.]

// ---

== Related Patterns
<related-patterns-3>
- 0502 -- the one case where COALESCE before hashing is justified
- 0310 -- NULL `updated_at` as an extraction problem
- 0102 -- null handling as conforming, not transforming
- 0609 -- surfacing NULL rates through quality checks

// ---

= Timezone Conforming
<timezone-conforming>
#quote(block: true)[
#strong[One-liner:] TZ stays TZ, naive stays naive. Don't make timezone decisions that aren't in the source data -- but know what you're landing.
]

// ---

== The Playbook
<the-playbook-3>
The rule follows the same principle as 0504: reflect the source. If the source stores timezone-aware timestamps, land timezone-aware. If the source stores naive timestamps, land them as datetime -- not as timestamp with a timezone you guessed. Converting naive to UTC without being certain of the source timezone is worse than landing naive, because a wrong UTC conversion looks correct in the destination and silently shifts every row by however many hours you got wrong.

Most transactional sources store naive timestamps. The application knows what timezone it means, but the column doesn't say -- and often nobody at the source team documented it either. That's the source's data quality problem. Your job is to land what the source gives you, not to retroactively assign timezone semantics that weren't there.

// ---

== Naive vs Aware
<naive-vs-aware>
Two fundamentally different types that look similar in query results but behave differently everywhere else:

#strong[Naive] (`TIMESTAMP WITHOUT TIME ZONE`, `DATETIME`, `DATETIME2`): the value `2026-03-15 14:30:00` with no timezone attached. The source application assumes a timezone -- usually the server's local time, sometimes the user's timezone, sometimes something else entirely -- but the column itself carries no indication of which one.

#strong[Aware] (`TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ`): the value `2026-03-15 14:30:00+00:00` with an explicit offset or stored as UTC internally. The timezone is part of the data. PostgreSQL's `TIMESTAMPTZ` stores everything as UTC and converts to the session timezone on display; BigQuery's `TIMESTAMP` is always UTC.

The conforming decision:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Source has], [Land as], [Why],),
    table.hline(),
    [Aware (TIMESTAMPTZ)], [Aware (TIMESTAMP / TIMESTAMPTZ)], [The timezone is part of the data -- preserve it],
    [Naive (DATETIME)], [Datetime / naive equivalent], [Don't add timezone info that isn't there],
    [Naive, but you know the timezone with certainty], [Convert to aware, document it], [Only if the source team confirms and it won't change],
  )]
  , kind: table
  )

#ecl-warning("Not every destination supports naive timestamps")[BigQuery's `TIMESTAMP` is always UTC -- there's no naive mode. If you land a naive `14:30:00` as a BigQuery `TIMESTAMP`, the engine treats it as `14:30:00 UTC`, which may be wrong. Use BigQuery `DATETIME` (no timezone) for naive values. Snowflake has both `TIMESTAMP_NTZ` (naive) and `TIMESTAMP_TZ` (aware). Know which one you're targeting.]

// ---

== Discovering the Source Timezone
<discovering-the-source-timezone>
When you need to know what timezone a naive timestamp represents -- for documentation, for downstream, or because you're deciding whether to convert -- here's the investigation order:

#strong[Ask the source team.] "What timezone does your application write timestamps in?" is a 5-minute conversation that saves weeks of debugging. Most teams know the answer even if they never documented it.

#strong[Check the database server timezone.] `SHOW timezone` (PostgreSQL), `SELECT @@global.time_zone` (MySQL), `SELECT SYSDATETIMEOFFSET()` (SQL Server). Many applications inherit the server's timezone for naive timestamps.

#strong[Look at timestamps around DST transitions.] If you see a gap at 2:00-3:00 AM on the spring-forward date, the source writes in a timezone that observes DST. If you see two clusters of timestamps at 1:00-2:00 AM on the fall-back date, same conclusion. If timestamps flow smoothly across both transitions, the source writes in UTC or a non-DST timezone.

#strong[Multinational ERPs.] Each company or branch may write in its own local timezone -- same column, different timezone per row, no indicator column. This is genuinely bad source data quality. If the source doesn't provide a timezone indicator per row, avoid assigning timezones in the ECL layer. Land naive and let downstream handle it with whatever business context they have about which company operates in which timezone.

// ---

== DST Traps
<dst-traps>
Daylight saving transitions create two specific hazards for timestamp data:

#strong[Spring forward (the hour that doesn't exist).] Clocks jump from 2:00 AM to 3:00 AM. A timestamp like `2026-03-08 02:30:00 America/New_York` refers to a moment that never existed. Some engines reject it, some silently shift it to 3:30 AM, some store it as-is. If the source wrote it, it's probably from a system that doesn't validate timestamps -- land it as-is and document that the value is technically invalid.

#strong[Fall back (the hour that repeats).] Clocks fall from 2:00 AM back to 1:00 AM. A timestamp like `2026-11-01 01:30:00 America/New_York` could refer to two different instants -- the first 1:30 AM or the second 1:30 AM. Without an offset, there's no way to distinguish them.

Both of these are reasons to prefer landing naive timestamps as naive rather than converting to UTC at extraction time. A conversion during the ambiguous fall-back hour has a 50% chance of being wrong, and you won't know which rows are affected. A stateless extraction window (0303) helps here -- the overlap naturally re-extracts the ambiguous rows on the next run, and if the source eventually clarifies (some applications write a second-pass correction), the later extraction picks it up.

// ---

== Downstream Boundary Effects
<downstream-boundary-effects>
The most visible consequence of timezone handling isn't in the pipeline -- it's in the business reports that consume the data.

When someone downstream writes `SUM(amount) GROUP BY TRUNC(sale_date, MONTH)`, sales near the month boundary can land in the wrong bucket depending on how the timestamp is interpreted. A sale at `2026-03-31 23:30:00` in the source's local timezone is `2026-04-01 02:30:00 UTC`. If the analyst's report truncates a UTC timestamp, March's revenue is short and April's is inflated. Multiply this across every month boundary and the numbers never match the source system's own reports.

This matters more than partition alignment. A row in the wrong partition is an internal cost issue -- a query scans one extra partition. A row in the wrong month in a revenue report gets escalated to the CFO. Document the timezone assumption clearly so downstream teams can adjust their queries accordingly.

#ecl-tip("Document the timezone assumption per table")[Add a comment to the destination DDL or a row in a metadata table: "`orders.created_at` is naive, assumed `America/Santiago` based on source team confirmation (2026-03-14)." When the assumption is wrong -- and eventually it will be, because someone changes the server timezone or adds a branch in a different country -- at least you'll know what was assumed and when.]

// ---

== By Corridor
<by-corridor-4>
#ecl-warning("Transactional to columnar")[BigQuery and Snowflake handle timezone-aware timestamps well, but only if you give them the right data. BigQuery `TIMESTAMP` = always UTC; use `DATETIME` for naive values. Snowflake has `TIMESTAMP_NTZ` (naive), `TIMESTAMP_LTZ` (session-local), and `TIMESTAMP_TZ` (explicit offset) -- pick the one that matches what the source actually stores. Landing a naive value as an aware type silently assigns a wrong timezone with no error and no warning.]

#ecl-info("Transactional to transactional")[If source is naive PostgreSQL and destination is naive PostgreSQL, no conversion needed -- the naive value transfers as-is. Document the assumption but don't add complexity. If the destination is a different engine (PostgreSQL to MySQL), check whether the naive type behavior differs -- PostgreSQL's `TIMESTAMP WITHOUT TIME ZONE` and MySQL's `DATETIME` are equivalent in practice, but SQL Server's `DATETIME2` has different precision (see 0503).]

// ---

== Related Patterns
<related-patterns-4>
- 0503 -- DATETIME2 → TIMESTAMP casting and precision truncation
- 0504 -- same principle: reflect the source, don't add information that isn't there
- 0202 -- partition boundaries and timezone alignment
- 0105 -- "timestamps have timezones" as a common lie
- 0303 -- overlap windows help with DST ambiguity

// ---

= Charset and Encoding
<charset-and-encoding>
#quote(block: true)[
#strong[One-liner:] Latin-1 source, UTF-8 destination. Let the library handle the conversion -- don't do it by hand.
]

// ---

== The Playbook
<the-playbook-4>
Charset encoding is one of those conforming operations that should be invisible. The right approach is to declare the source encoding explicitly on the connection, let the driver handle the conversion to UTF-8, and move on. Don't write manual byte-level conversion code -- every database driver and library (SQLAlchemy, JDBC, ODBC) has solved this problem already, and their solution is better tested than yours will be.

The only thing you need to get right is the declaration. If you tell the driver the source is UTF-8 and it's actually Latin-1, the conversion produces mojibake silently. If you tell it Latin-1 and it's actually Windows-1252, you lose a handful of characters (curly quotes, em dashes, ellipsis) that exist in Windows-1252 but not in Latin-1. Get the encoding right on the connection string and the rest takes care of itself.

// ---

== Where It Happens
<where-it-happens>
#strong[Legacy ERPs.] SAP (various modules), AS/400, older Oracle installations. These systems predate the UTF-8 consensus and store data in Latin-1, Windows-1252, or vendor-specific encodings. SAP HANA itself is UTF-8, but data migrated from older SAP systems may carry encoding artifacts from the original system.

#strong[CSV exports.] The encoding header is either missing, wrong, or "it depends on which machine exported it." A CSV that claims UTF-8 but was actually exported from Excel on a Windows machine is Windows-1252. Parquet doesn't have this problem -- it's always UTF-8 internally -- which is one of many reasons to prefer Parquet over CSV for data exchange when you have the choice.

#strong[Older OLTP systems.] Any transactional database deployed before \~2010 has a reasonable chance of running a non-UTF-8 encoding. The older the system, the higher the chance -- and the more likely it is that nobody remembers what encoding was configured at install time.

// ---

== Detection
Encoding problems are invisible until they're not. The data loads successfully, the row counts match, and everything looks fine -- until someone searches for a customer named "Muñoz" and finds "Mu?oz" or "MuÃ±oz" instead.

#strong[Replacement characters.] Rows with `?` or `\ufffd` (the Unicode replacement character) in text columns. These appear when the driver encounters a byte sequence that's invalid in the declared encoding and substitutes a placeholder instead of failing. If you see them, the encoding declaration is wrong.

#strong[Mojibake.] Multi-byte UTF-8 sequences interpreted as single-byte Latin-1 characters. `ñ` becomes `Ã±`, `ü` becomes `Ã¼`, `é` becomes `Ã©`. This happens when the data is actually UTF-8 but the connection declares Latin-1 (or vice versa). The characters are still there -- just misinterpreted -- and the fix is correcting the encoding declaration, not the data.

#strong[The canary columns.] Names with `ñ`, `ü`, `ç`, accented characters, or any non-ASCII content. If the accented characters look wrong in the destination, the encoding is wrong. Spot-check these columns after every new source connection setup.

```sql
-- destination: any
-- Quick canary check after load
SELECT customer_name
FROM customers
WHERE customer_name LIKE '%ñ%'
   OR customer_name LIKE '%ü%'
   OR customer_name LIKE '%ç%'
LIMIT 10;
```

If this returns rows with clean characters, the encoding is working. If it returns mojibake or replacement characters, fix the connection encoding before loading anything else.

// ---

== The Fix
<the-fix>
#strong[Declare the encoding on the connection.] Every driver has a parameter for this:

```python
# SQLAlchemy -- Latin-1 source
engine = create_engine(
    "mssql+pyodbc://user:pass@host/db",
    connect_args={"charset": "latin1"}
)

# SQLAlchemy -- Windows-1252 source (common for Excel-origin data)
engine = create_engine(
    "mssql+pyodbc://user:pass@host/db",
    connect_args={"charset": "cp1252"}
)
```

The driver decodes from the source encoding on read and encodes to UTF-8 for your pipeline. No manual byte manipulation needed.

#strong[Validate after load.] Run the canary check above on the first load and after any connection configuration change. Encoding problems are deterministic -- if the canary passes, every row is fine. If it fails, every non-ASCII row is affected.

#strong[CSV-specific.] When the encoding isn't declared in the file, try `chardet` or `cchardet` (Python libraries) to detect it from the byte content. These aren't 100% accurate but they're better than guessing. Once detected, pass the encoding explicitly to your CSV reader: `pandas.read_csv(path, encoding='cp1252')`.

// ---

== Collation Traps
<collation-traps>
Collation is related to encoding but distinct: encoding determines #emph[how bytes map to characters];, collation determines #emph[how characters compare and sort];. A correct encoding with a mismatched collation produces data that loads correctly but behaves differently in queries.

#strong[Case sensitivity.] PostgreSQL respects the table or column `COLLATE` setting -- `WHERE name = 'García'` might or might not match `'GARCÍA'` depending on the collation. BigQuery is always case-sensitive in string comparisons, with no collation configuration. A JOIN that works on a case-insensitive source fails on BigQuery because `'garcia' != 'García'`.

#strong[Accent sensitivity.] A source with accent-insensitive collation treats `café` and `cafe` as equal. A destination with binary collation (the default on most columnar engines) treats them as different values. A JOIN on a text column that "always worked" on the source returns fewer rows on the destination, and the missing rows are the ones with accented characters.

#strong[What to do about it.] Document the source collation for text columns used in JOINs or filters. If a collation mismatch causes incorrect query results at the destination, the fix belongs downstream (a `LOWER()` or `COLLATE` clause in the consumer's query), not in the ECL layer. Conforming doesn't change how strings compare -- it makes sure the bytes arrive correctly.

// ---

== Schema Naming
<conforming-schema-naming>
Related but distinct concern: the characters in table and column #emph[names];, not in the data. This is about safety and consistency across engines, not about renaming `OACT` to `chart_of_accounts`.

#strong[The problem.] SQL Server allows `[Emojis 👽]` as a column name. PostgreSQL allows `"@Table"` with quotes. SAP tables are named `OACT`, `OINV`, `INV1`. These identifiers may contain spaces, special characters, brackets, or characters that are reserved words in the destination engine. A column named `order` in the source breaks every query on the destination unless quoted -- and nobody quotes consistently.

#strong[What the ECL layer should do.] Normalize identifiers for #emph[safety];: lowercase, replace spaces with underscores, strip characters that require quoting on the destination engine. This isn't semantic renaming (`OACT` → `chart_of_accounts`) -- it's making sure the identifier doesn't break SQL on the other side. `[Order Lines]` → `order_lines`, `@Status` → `status`, `Column Name With Spaces` → `column_name_with_spaces`.

This deserves its own full treatment -- see 0707 for the complete naming convention discussion, including when to rename vs.~preserve, schema prefixes, and how to handle identifiers that are reserved words on the destination.

// ---

== By Corridor
<by-corridor-5>
#ecl-warning("Transactional to columnar")[Usually Latin-1 or Windows-1252 to UTF-8, one direction. BigQuery, Snowflake, ClickHouse, and Redshift are all UTF-8 natively. The driver handles the conversion as long as the source encoding is declared correctly. Collation mismatches are more common here because columnar engines default to binary (case-sensitive, accent-sensitive) comparison, while many transactional sources run case-insensitive collations.]

#ecl-info("Transactional to transactional")[Can be UTF-8 to UTF-8 with no encoding conversion needed, but collation differences between engines still bite. PostgreSQL's default collation depends on the OS locale at `initdb` time. MySQL's default depends on the server config and can vary per table. Moving data between them without checking collation equivalence leads to subtle query behavior differences that don't show up until someone reports a missing JOIN match.]

// ---

== Related Patterns
<related-patterns-5>
- 0103 -- source encoding as an extraction gotcha
- 0707 -- table and column naming conventions at the destination
- 0503 -- NVARCHAR vs VARCHAR as a type casting concern

// ---

= Nested Data and JSON
<nested-data-and-json>
#quote(block: true)[
#strong[One-liner:] JSON column in the source? Land it as-is. Normalizing nested data into relational tables is transformation, not conforming.
]

// ---

== The Playbook
<the-playbook-5>
Prefer landing JSON columns as they are -- `STRING` or the destination's native JSON type (BigQuery `JSON`, Snowflake `VARIANT`, PostgreSQL `JSONB`). The source has a JSON column, the destination gets a JSON column. That's conforming.

Flattening JSON into normalized tables (`order`, `order__details`, `order__details__items`) is closer to ETL than ECL. The C in ECL makes data survive the crossing -- it doesn't restructure it. If you have a strong reason to flatten (a consumer that absolutely cannot work with JSON and there's no downstream layer to do it), document the decision and know that you're stepping outside the conforming boundary. Most of the time, you don't need to.

Avoid the hybrid approach (land raw JSON + flatten to normalized tables) at the ECL layer. Two representations means double the storage, double the schema maintenance, and a synchronization problem when one updates and the other doesn't. One representation is enough -- pick the simpler one and let downstream build the other if they need it.

// ---

== Land As-Is
<land-as-is>
The default. The destination gets what the source has, with no interpretation and no decisions about which fields matter.

```sql
-- source: transactional (postgresql)
SELECT
    order_id,
    customer_id,
    details,  -- JSONB column, landed as-is
    CURRENT_TIMESTAMP AS _extracted_at
FROM orders
WHERE updated_at >= :last_run;
```

The `details` column might contain:

```json
{
  "shipping": {
    "method": "express",
    "address": {"city": "Santiago", "zip": "7500000"}
  },
  "items": [
    {"sku": "A100", "qty": 2, "price": 15.50},
    {"sku": "B200", "qty": 1, "price": 42.00}
  ],
  "notes": "Gift wrap requested"
}
```

Land it as-is. The structure, the nesting, the array of items -- all of it arrives at the destination exactly as the source stores it. No field selection, no type inference on nested values, no decision about whether `shipping.address` should be its own table.

This works well when consumers are data-conscious and comfortable with JSON query syntax. BigQuery's `JSON_EXTRACT_SCALAR(details, '$.shipping.method')`, Snowflake's `details:shipping:method`, PostgreSQL's `details->>'shipping'->'method'` -- all of these give consumers access to every field without the ECL layer making structural decisions on their behalf.

// ---

== Know Your Consumer
<know-your-consumer>
Not all BI tools handle nested data the same way, and that's worth understanding even though it doesn't change the ECL approach.

#strong[Tools that handle JSON well.] Looker, BigQuery BI Engine, Metabase (with JSON path support), any tool where the query author writes SQL. These consumers can reach into the JSON with path expressions and extract what they need.

#strong[Tools that struggle with JSON.] Power BI's handling of nested fields is limited -- it can expand JSON into columns, but the experience is clunky and the performance degrades with deeply nested structures. Some reporting tools expect flat tabular data and have no JSON path support at all.

But here's the thing: the same consumers who can't handle JSON often can't handle joins either. Normalizing the JSON into 5 relational tables and expecting a business analyst to JOIN `order` → `order__details` → `order__details__items` correctly is optimistic. You've traded one problem (they can't query JSON) for another (they can't join tables), and the second problem is arguably worse because wrong joins produce silently incorrect results while failing to query JSON produces an error.

If the consumer truly can't work with JSON, the answer is a downstream transformation -- a view or materialized table that flattens the JSON into the shape the consumer needs. That's a serving concern (0703), not an ECL concern. The ECL layer lands the data; the serving layer shapes it for consumption.

#ecl-warning("Flattening views are cheap and reversible")[A `CREATE VIEW orders_flat AS SELECT order_id, JSON_EXTRACT_SCALAR(details, '$.shipping.method') AS shipping_method, ...` gives the consumer a flat table without modifying the landed data. If the JSON structure changes, you update the view. If a new consumer needs a different shape, you create another view. The raw JSON in the landed table is always the source of truth.]

// ---

== Schema Mutation in JSON
<schema-mutation-in-json>
JSON columns mutate without warning. A new field appears because the application team shipped a feature. A field disappears because someone removed it from the API response. A field that was always a string is now sometimes a number because a third-party integration changed its output format. None of this is visible in the source schema -- the column type is still `JSONB`, the DDL hasn't changed, and your extraction query returns the same column.

This is downstream's problem, not the ECL layer's. Land the JSON as-is and let the consumer or the transformation layer handle schema evolution within the blob. The ECL layer doesn't parse the JSON, so it doesn't break when the JSON changes -- which is exactly the property you want.

The one exception: when schema mutation causes the #emph[load itself] to fail. BigQuery `STRUCT` is schema-on-write -- every row must match the declared field names and types. If the JSON gains a new field that the `STRUCT` definition doesn't include, the load rejects the row. Two options:

#strong[Land as `STRING` instead of `STRUCT`.] The destination stores the raw JSON text with no schema enforcement. Any valid JSON string loads successfully regardless of what fields it contains. Consumers parse the JSON at query time. This is the safest choice for mutating JSON because the schema is the consumer's problem, not the load's problem.

#strong[Use a schema-on-read type.] Snowflake `VARIANT` accepts arbitrary JSON without a predefined schema. PostgreSQL `JSONB` does the same. These types give you native JSON query syntax without the rigidity of `STRUCT`. If your destination supports schema-on-read, prefer it over `STRING` for the better query ergonomics.

If you must use a typed `STRUCT` (because the destination requires it or because query performance on `STRING` is unacceptable), a full replace (0401) with an updated `STRUCT` definition handles the schema change cleanly -- drop and rebuild the table with the new field included.

// ---

== By Corridor
<by-corridor-6>
#ecl-info("Transactional to columnar")[Native JSON support varies significantly. #strong[BigQuery]: `JSON` type (schema-on-read, recommended) or `STRUCT`/`REPEATED` (typed, schema-on-write). Use `JSON` for mutating data, `STRUCT` only when the schema is genuinely stable and you need the query performance. Landing as `STRING` is always safe. #strong[Snowflake]: `VARIANT` is schema-on-read and handles arbitrary JSON natively. The natural choice -- flexible, queryable, doesn't break on schema changes. #strong[ClickHouse]: `JSON` type (experimental in recent versions) or `String`. ClickHouse's JSON support is less mature -- `String` with `JSONExtract\*` functions is the safe choice. #strong[Redshift]: `SUPER` type accepts semi-structured data. Queryable with `PartiQL` syntax.]

#ecl-info("Transactional to transactional")[Usually straightforward. #strong[PostgreSQL to PostgreSQL]: `JSONB` to `JSONB`. Native, queryable, indexed with GIN indexes. Zero conversion needed. #strong[MySQL to MySQL / PostgreSQL]: MySQL `JSON` to PostgreSQL `JSONB`. Both accept arbitrary JSON. The query syntax differs (`->` vs `->>` semantics) but the data transfers as-is. Both engines accept arbitrary JSON without schema definition, so schema mutation within the JSON is never a load problem.]

// ---

== Related Patterns
<related-patterns-6>
- 0102 -- the line between conforming and transforming applies directly here
- 0403 -- schema evolution and how JSON columns interact with MERGE
- 0401 -- full replace as a clean way to handle STRUCT schema changes
- 0703 -- flattening views for consumers who can't query JSON

// ---
