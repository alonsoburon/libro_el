---
title: "Charset and Encoding"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Charset and Encoding

> **One-liner:** Latin-1 source, UTF-8 destination. Let the library handle the conversion -- don't do it by hand.

---

## The Playbook

Charset encoding is one of those conforming operations that should be invisible. The right approach is to declare the source encoding explicitly on the connection, let the driver handle the conversion to UTF-8, and move on. Don't write manual byte-level conversion code -- every database driver and library (SQLAlchemy, JDBC, ODBC) has solved this problem already, and their solution is better tested than yours will be.

The only thing you need to get right is the declaration. If you tell the driver the source is UTF-8 and it's actually Latin-1, the conversion produces mojibake silently. If you tell it Latin-1 and it's actually Windows-1252, you lose a handful of characters (curly quotes, em dashes, ellipsis) that exist in Windows-1252 but not in Latin-1. Get the encoding right on the connection string and the rest takes care of itself.

---

## Where It Happens

**Legacy ERPs.** SAP (various modules), AS/400, older Oracle installations. These systems predate the UTF-8 consensus and store data in Latin-1, Windows-1252, or vendor-specific encodings. SAP HANA itself is UTF-8, but data migrated from older SAP systems may carry encoding artifacts from the original system.

**CSV exports.** The encoding header is either missing, wrong, or "it depends on which machine exported it." A CSV that claims UTF-8 but was actually exported from Excel on a Windows machine is Windows-1252. Parquet doesn't have this problem -- it's always UTF-8 internally -- which is one of many reasons to prefer Parquet over CSV for data exchange when you have the choice.

**Older OLTP systems.** Any transactional database deployed before ~2010 has a reasonable chance of running a non-UTF-8 encoding. The older the system, the higher the chance -- and the more likely it is that nobody remembers what encoding was configured at install time.

---

## Detection

Encoding problems are invisible until they're not. The data loads successfully, the row counts match, and everything looks fine -- until someone searches for a customer named "Muñoz" and finds "Mu?oz" or "MuÃ±oz" instead.

**Replacement characters.** Rows with `?` or `\ufffd` (the Unicode replacement character) in text columns. These appear when the driver encounters a byte sequence that's invalid in the declared encoding and substitutes a placeholder instead of failing. If you see them, the encoding declaration is wrong.

**Mojibake.** Multi-byte UTF-8 sequences interpreted as single-byte Latin-1 characters. `ñ` becomes `Ã±`, `ü` becomes `Ã¼`, `é` becomes `Ã©`. This happens when the data is actually UTF-8 but the connection declares Latin-1 (or vice versa). The characters are still there -- just misinterpreted -- and the fix is correcting the encoding declaration, not the data.

**The canary columns.** Names with `ñ`, `ü`, `ç`, accented characters, or any non-ASCII content. If the accented characters look wrong in the destination, the encoding is wrong. Spot-check these columns after every new source connection setup.

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

---

## The Fix

**Declare the encoding on the connection.** Every driver has a parameter for this:

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

**Validate after load.** Run the canary check above on the first load and after any connection configuration change. Encoding problems are deterministic -- if the canary passes, every row is fine. If it fails, every non-ASCII row is affected.

**CSV-specific.** When the encoding isn't declared in the file, try `chardet` or `cchardet` (Python libraries) to detect it from the byte content. These aren't 100% accurate but they're better than guessing. Once detected, pass the encoding explicitly to your CSV reader: `pandas.read_csv(path, encoding='cp1252')`.

---

## Collation Traps

Collation is related to encoding but distinct: encoding determines *how bytes map to characters*, collation determines *how characters compare and sort*. A correct encoding with a mismatched collation produces data that loads correctly but behaves differently in queries.

**Case sensitivity.** PostgreSQL respects the table or column `COLLATE` setting -- `WHERE name = 'García'` might or might not match `'GARCÍA'` depending on the collation. BigQuery is always case-sensitive in string comparisons, with no collation configuration. A JOIN that works on a case-insensitive source fails on BigQuery because `'garcia' != 'García'`.

**Accent sensitivity.** A source with accent-insensitive collation treats `café` and `cafe` as equal. A destination with binary collation (the default on most columnar engines) treats them as different values. A JOIN on a text column that "always worked" on the source returns fewer rows on the destination, and the missing rows are the ones with accented characters.

**What to do about it.** Document the source collation for text columns used in JOINs or filters. If a collation mismatch causes incorrect query results at the destination, the fix belongs downstream (a `LOWER()` or `COLLATE` clause in the consumer's query), not in the ECL layer. Conforming doesn't change how strings compare -- it makes sure the bytes arrive correctly.

---

## Schema Naming

Related but distinct concern: the characters in table and column *names*, not in the data. This is about safety and consistency across engines, not about renaming `OACT` to `chart_of_accounts`.

**The problem.** SQL Server allows `[Emojis 👽]` as a column name. PostgreSQL allows `"@Table"` with quotes. SAP tables are named `OACT`, `OINV`, `INV1`. These identifiers may contain spaces, special characters, brackets, or characters that are reserved words in the destination engine. A column named `order` in the source breaks every query on the destination unless quoted -- and nobody quotes consistently.

**What the ECL layer should do.** Normalize identifiers for *safety*: lowercase, replace spaces with underscores, strip characters that require quoting on the destination engine. This isn't semantic renaming (`OACT` → `chart_of_accounts`) -- it's making sure the identifier doesn't break SQL on the other side. `[Order Lines]` → `order_lines`, `@Status` → `status`, `Column Name With Spaces` → `column_name_with_spaces`.

This deserves its own full treatment -- see [[07-serving-the-destination/0707-schema-naming-conventions|0707]] for the complete naming convention discussion, including when to rename vs. preserve, schema prefixes, and how to handle identifiers that are reserved words on the destination.

---

## By Corridor

> [!example]- Transactional → Columnar
> Usually Latin-1 or Windows-1252 → UTF-8, one direction. BigQuery, Snowflake, ClickHouse, and Redshift are all UTF-8 natively. The driver handles the conversion as long as the source encoding is declared correctly. Collation mismatches are more common here because columnar engines default to binary (case-sensitive, accent-sensitive) comparison, while many transactional sources run case-insensitive collations.

> [!example]- Transactional → Transactional
> Can be UTF-8 → UTF-8 with no encoding conversion needed, but collation differences between engines still bite. PostgreSQL's default collation depends on the OS locale at `initdb` time. MySQL's default depends on the server config and can vary per table. Moving data between them without checking collation equivalence leads to subtle query behavior differences that don't show up until someone reports a missing JOIN match.

---

## Related Patterns

- [[01-foundations-and-archetypes/0103-transactional-sources|0103]] -- source encoding as an extraction gotcha
- [[07-serving-the-destination/0707-schema-naming-conventions|0707]] -- table and column naming conventions at the destination
- [[05-conforming-playbook/0503-type-casting-normalization|0503]] -- NVARCHAR vs VARCHAR as a type casting concern
