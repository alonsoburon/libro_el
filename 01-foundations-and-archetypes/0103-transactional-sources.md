---
title: Transactional Sources
aliases: []
tags:
  - pattern/foundations
  - chapter/part-1
status: first_iteration
created: 2026-03-06
updated: 2026-03-06
---

# Transactional Sources

> **One-liner:** Row-oriented, mutable, ACID. The terrain you're extracting from most of the time.

## What Makes a Source Transactional

These databases were built to handle one row at a time, fast. INSERT a customer, UPDATE an order status, DELETE a cancelled invoice. They store data row by row, keep it consistent through transactions and locks, and index everything for quick point lookups.

This is great for the applications that sit on top of them. It's less great for you, because what you need to do -- pull thousands or millions of rows in bulk -- is the opposite of what they were optimized for. You're running full table scans on a system designed for `WHERE id = 42`.

The key thing to internalize: these sources are **mutable**. A row you extracted yesterday might look different today. Or it might be gone. The database won't send you a notification about it. You have to go looking.

## The Engines

You'll run into a handful of these in the wild. They all speak SQL, but they all speak it differently.

**PostgreSQL.** The open source workhorse. Rich type system, `updated_at` triggers are common but never guaranteed. Some teams add them religiously, some don't bother. Watch out for TOAST compression on large text/JSON columns -- it can make extraction slower than you'd expect because the data isn't stored inline with the row.

**MySQL.** Everywhere. If you've worked with a web application's database, you've probably worked with MySQL. The classic trap here is `utf8` vs `utf8mb4`: MySQL's `utf8` is actually 3-byte UTF-8, which means it can't store emoji or certain CJK characters. `utf8mb4` is real UTF-8. If you're extracting text columns coded as `utf8`, you might be getting truncated data and not know it. Also, InnoDB's `DATETIME` has no timezone information at all.

**SQL Server.** The enterprise standard. You'll see it behind most .NET applications and a lot of corporate ERPs. `DATETIME2` gives you up to 100-nanosecond precision, which sounds great until you try to land it in BigQuery's microsecond `TIMESTAMP` and realize you're truncating. Licensing and access are the real pain: getting read access to a production SQL Server often involves procurement, security reviews, VPN configs, and a DBA who has 47 other priorities before your extraction project.

**SAP HANA.** Column-oriented under the hood, but the applications on top of it (SAP B1, S/4HANA) treat it transactionally. Proprietary SQL dialect with its own quirks. Limited tooling for extraction compared to the others. Often sits behind thousands of auto-generated tables you're not supposed to query directly -- and in the case of S/4HANA, legally might not be allowed to. If you're extracting from SAP, you're already in a special kind of hell and you know it.

| Engine     | Timezone trap                    | Encoding trap              | Key gotcha                               |
| ---------- | -------------------------------- | -------------------------- | ---------------------------------------- |
| PostgreSQL | `TIMESTAMP` vs `TIMESTAMPTZ`     | Usually UTF-8, but check   | TOAST on large columns                   |
| MySQL      | `DATETIME` has no TZ at all      | `utf8` != real UTF-8       | `utf8mb4` migration state                |
| SQL Server | `DATETIME2` nanosecond precision | Latin-1 legacy common      | Access/licensing friction                |
| SAP HANA   | Varies by SAP module             | Depends on client codepage | Legally restricted access to some tables |

## What They All Share (That Matters for ECL)

Despite the differences, the fundamentals are the same from an extraction perspective:

They all support `SELECT ... WHERE ...` for pulling data incrementally. That's your primary tool. Every batch extraction pattern in this book starts with a query against the source.

They all have *some* mechanism for detecting changes -- `updated_at` columns, triggers, row versions -- but none of them make it easy or consistent. Every engine does it differently, every application uses it differently (or doesn't use it at all), and you can't count on any of it being reliable until you've verified it yourself.

They all have schemas that mutate. Columns get added, types get changed, tables get renamed. The application team ships a release, and suddenly your `products` table has 3 new columns you've never seen.

```sql
-- last week your extraction query was:
SELECT product_id, name, price, category FROM products;

-- after Friday's deploy:
SELECT product_id, name, price, category FROM products;
-- ERROR: column "category" renamed to "product_category"
-- also: new columns "weight_kg", "is_hazardous", "supplier_id"
```

Your pipeline needs to handle this gracefully or it *will* break on a Friday night.

> [!tip] `SELECT *` is valid for extraction
> Contrary to what every SQL best practices guide tells you, `SELECT *` is a good default for ECL. You're cloning the table, not building a report. New column added? It lands automatically. Type changed? Your type dictionary handles it. But renames and deletes **must** fail your pipeline. If `category` becomes `product_category`, your destination still has `category` receiving no new data, and that's unacceptable. Schema relaxing means: always allow additions, handle type changes, never silently accept renames or deletions. More on this in [[01-foundations-and-archetypes/0105-the-lies-sources-tell|0105-the-lies-sources-tell]].

And they all have data quality issues that the application layer "handles" but the database doesn't enforce. These are the [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|soft rules]] -- the things a stakeholder tells you are "always" true, but the schema doesn't guarantee.

## What Will Bite You

**Locks during extraction.** Your `SELECT` could be reading millions of rows while some poor bastard is trying to create a new invoice. In SQL Server you can use `NOLOCK` to avoid blocking, but now you're reading dirty data -- rows mid-transaction, half-updated. To be clear, I **don't** recommend using NOLOCK.

**NOLOCK (dirty reads, no blocking)**
```sql
-- engine: sqlserver
SELECT order_id, status, updated_at
FROM orders WITH (NOLOCK)
WHERE updated_at >= @last_extraction
```

**Default (clean reads, blocks writers)**
```sql
-- engine: sqlserver
SELECT order_id, status, updated_at
FROM orders
WHERE updated_at >= @last_extraction
-- writers wait until this finishes
```

Use a read replica? Now you have replication lag and you might miss rows that were committed seconds before your query ran. There's no free lunch here; you pick your trade-off and document it. (Also, do you really trust the people making the replica at source?)

**No reliable `updated_at`.** Some tables don't have one. Some have one that only fires on UPDATE, not INSERT. Some have one that the application sets manually and sometimes forgets. It may work 99.9% of the time, that still gets you chewed out when it doesn't.

```sql
-- engine: postgresql
-- what you expect:
SELECT order_id, created_at, updated_at FROM orders WHERE order_id IN (1001, 1002);
```

| order_id | created_at          | updated_at          |
| -------- | ------------------- | ------------------- |
| 1001     | 2026-01-15 09:00:00 | 2026-02-20 14:30:00 |
| 1002     | 2026-03-01 11:00:00 | NULL                |

Order 1002 was just created. The trigger only fires on UPDATE, so `updated_at` is NULL. Your pipeline with `WHERE updated_at >= @last_run` will never see it.

You'll build your pipeline trusting `updated_at`, and three months later discover that 2% of rows were silently missed because the column wasn't being maintained. See [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]].

**Hard deletes.** The row was there yesterday. Today it's gone. The source won't tell you.

```sql
-- yesterday's extraction got 4 invoices:
SELECT invoice_id FROM invoices WHERE status = 'open';
```

| invoice_id |
|---|
| 5001 |
| 5002 |
| 5003 |
| 5004 |

```sql
-- today's extraction gets 3:
SELECT invoice_id FROM invoices WHERE status = 'open';
```

| invoice_id |
|---|
| 5001 |
| 5002 |
| 5004 |

Where's 5003? Deleted. No tombstone, no audit log, no `deleted_at` flag. Your destination still has it. Now your data says there are 4 open invoices when there are 3. Detecting hard deletes in batch extraction is one of the hardest problems in this book. See [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]].

**Connection limits and DBA etiquette.** You're a guest on someone else's production system. Open too many connections, run queries during peak hours, or full-scan their biggest table while the month-end close is running, and the DBA will shut you down. Rightfully.

```sql
-- your "quick extraction" at 10am:
SELECT * FROM order_lines
WHERE updated_at >= '2026-01-01'
-- 12 million rows, no index on updated_at
-- full table scan, 4 minutes, 100% CPU
-- meanwhile 200 users can't save orders
```

You need to know the source system's capacity, its busy hours, and its tolerance for your workload. See [[06-operating-the-pipeline/0607-source-system-etiquette|0607-source-system-etiquette]].

**Encoding traps.** The `customers` table has a `name` column. It's `VARCHAR(100)` in Latin-1. You didn't know it was Latin-1 because nobody told you and the metadata just says `VARCHAR`.

```sql
-- engine: sqlserver
-- what the source has:
SELECT customer_id, name FROM customers WHERE customer_id = 42;
```

| customer_id | name |
|---|---|
| 42 | José Muñoz |

```sql
-- what lands in BigQuery after a naive load:
SELECT customer_id, name FROM customers WHERE customer_id = 42;
```

| customer_id | name |
|---|---|
| 42 | Jos? Mu?oz |

Every `ñ`, `ü`, `é` silently replaced with `?`. Or worse, the load fails entirely and you don't know why until you dig into the byte encoding. Especially common with older ERP systems and legacy OLTP sources. See [[05-conforming-playbook/0506-charset-encoding|0506-charset-encoding]].
