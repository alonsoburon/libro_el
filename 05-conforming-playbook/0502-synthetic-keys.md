---
title: "Synthetic Keys"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Synthetic Keys

> **One-liner:** No PK, composite PK, unstable PK -- when the source doesn't give you a reliable key for MERGE, build one from immutable business characteristics.

---

## When You Need One

A [[04-load-strategies/0403-merge-upsert|MERGE]] needs a key to match source rows against destination rows. If the source gives you a clean, stable, single-column PK, use it and move on. The problems start when the source doesn't cooperate:

**No PK at all.** Some tables genuinely have no primary key -- log tables, staging tables, tables where the DBA forgot. Without a key, MERGE has nothing to match on and you're stuck with full replace or append-only.

**Composite PK of 5+ columns.** The table technically has a key, but it's `(company_code, fiscal_year, document_type, document_number, line_number)`. A MERGE matching on 5 columns works but is unwieldy, slow on some engines, and painful to debug when something doesn't match.

**Recycled auto-increment.** The PK is an `id` that looks stable, but when rows are deleted the ids get reused. Row 42 today is a different entity than row 42 last week. Your MERGE overwrites the new row with the old row's data because the key matches on the wrong entity.

**NULLs in key columns.** `NULL != NULL` in every SQL engine. A MERGE with NULL in the key column silently misses the row on every run -- the match condition never evaluates to true, so the row gets re-inserted as a "new" row every time. You end up with duplicates that accumulate run after run.

---

## Building the Key

The principle: hash immutable business characteristics -- attributes that identify the entity and won't change over its lifetime. The hash becomes `_source_key`, a single column the MERGE matches on.

**What's immutable.** Natural business identifiers: order number + line number, customer code + document type, SKU + warehouse code. These describe *what* the entity is, not *what happened* to it. If someone corrects the entity later, these columns stay the same.

**What's not immutable.** Amounts, prices, dates, status fields -- anything that can be corrected or updated after the row is created. If you include `total` in the hash and someone corrects an invoice amount, the hash changes and the MERGE treats the corrected row as a new entity instead of an update to the existing one.

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

### Concatenation vs Hash

Two approaches, with a clear winner for most cases:

**Concatenation.** `CONCAT(order_id, '-', line_number)` → `'1001-3'`. Readable and debuggable -- you can look at the key and know which entity it represents. The problem is fragility: if the delimiter (`-`) appears in the data, `CONCAT('100', '-', '13')` and `CONCAT('1001', '-', '3')` are distinguishable, but `CONCAT('10-0', '-', '13')` isn't what you expected. Column ordering matters too -- reversing the concat order produces different keys for the same entity across pipeline versions.

**Hash.** `MD5(CONCAT(col1, '|', col2, '|', col3))` → `'a3f2b8c1...'`. Opaque but safe. The output is always the same length, the delimiter problem effectively disappears (collisions from delimiter confusion are astronomically unlikely in the hash space), and column ordering is consistent as long as the pipeline code doesn't change. The downside is that you can't read the key and know which entity it represents -- debugging requires recomputing the hash from the source columns.

For most pipelines, hash wins. You look at the key rarely; the pipeline matches on it constantly.

---

## NULL in Key Columns

Most hash functions return NULL if any input is NULL. A row with `company_code = 'ACME'` and `document_number = NULL` produces `MD5(CONCAT('ACME', '|', NULL))` → NULL. Every row with a NULL in the same position produces the same NULL hash, and the MERGE treats them all as the same entity -- or worse, misses them entirely because `NULL != NULL` in the match condition.

COALESCE every column to a sentinel before hashing:

```sql
-- source: transactional
MD5(CONCAT(
    COALESCE(company_code, '__NULL__'), '|',
    COALESCE(document_number::TEXT, '__NULL__')
))
```

The sentinel (`'__NULL__'`) must be something that can't appear in real data. `'__NULL__'` works because no business column will contain that literal string. A shorter sentinel like `''` (empty string) is dangerous because empty strings *do* appear in real data and you'd be conflating NULL with empty -- the exact problem [[05-conforming-playbook/0504-null-handling|0504]] warns against.

Document which columns participate in the key. Downstream consumers, reconciliation queries, and future pipeline maintainers need to know how `_source_key` is built so they can recompute it when debugging.

---

## Hash Function Choice

**MD5.** 128-bit output, fast on every engine. The standard choice for synthetic keys. This isn't cryptography -- you're not protecting against adversarial collisions, you're generating unique identifiers for MERGE matching. MD5's known cryptographic weaknesses are irrelevant here.

**SHA-256.** 256-bit output, slower. The only reason to use it over MD5 is if regulatory or audit requirements mandate a specific hash function. Some compliance frameworks specify SHA-256 for anything labeled "hash" regardless of context -- easier to comply than to argue.

### When 128 Bits Isn't Enough

The birthday paradox determines collision probability: with a 128-bit hash, you'd need roughly $2^{64}$ (~18 quintillion) rows before a 50% chance of any two rows colliding. At 1 billion rows, the probability of a single collision is approximately $\frac{1}{3.4 \times 10^{20}}$ -- for practical purposes, zero. You'll hit every other failure mode in your pipeline long before you hit an MD5 collision on a synthetic key.

If you're hashing across multiple tables and worry about cross-table collisions, include the table name in the hash input: `MD5(CONCAT('invoice_lines', '|', col1, '|', col2))`. This is more about hygiene than probability.

---

## By Corridor

> [!example]- Transactional → Columnar
> Columnar destinations don't enforce UNIQUE, so a bad synthetic key doesn't produce an error -- it produces silent duplicates. A key built from mutable columns, or a key that doesn't COALESCE NULLs, will generate multiple rows for the same entity with no warning from the engine. The dedup is entirely your responsibility, and the only way to catch it is to monitor for duplicate `_source_key` values after load.

> [!example]- Transactional → Transactional
> Transactional destinations can enforce UNIQUE on `_source_key`. Add a unique index or constraint on the column, and a collision or a badly constructed key gets rejected at the database level with an explicit error. This is a genuine safety net that columnar destinations can't offer -- use it.

---

## Related Patterns

- [[04-load-strategies/0403-merge-upsert|0403]] -- the MERGE that needs a reliable key to match on
- [[02-full-replace-patterns/0209-hash-based-change-detection|0209]] -- hash-based change detection uses similar mechanics but hashes all columns (mutable included) to detect changes, while synthetic keys hash only immutable columns to identify entities
- [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]] -- "PKs are unique and stable" is often a soft rule
- [[05-conforming-playbook/0501-metadata-column-injection|0501]] -- `_source_hash` hashes the full row for change detection; `_source_key` hashes immutable columns for identity. Different purpose, similar mechanics
