---
title: Hard Rules, Soft Rules
aliases: []
tags:
  - pattern/foundations
  - chapter/part-1
status: draft
created: 2026-03-06
updated: 2026-03-06
---

# Hard Rules, Soft Rules

> **One-liner:** If the database enforces it, it's hard. If a stakeholder told you it's always true, it's soft -- and your pipeline must survive it being wrong.

Every source system comes with two layers of truth: what the database actually enforces, and what the business believes is true. These are not the same thing. Your pipeline has to know the difference, because one is a guarantee and the other is a hope (and a pain).

## The Distinction

A **hard rule** is enforced by the system. A foreign key constraint, a unique index, a NOT NULL column, a CHECK constraint. The database rejects violations at write time -- the bad data never lands. You can build on hard rules unconditionally. If `order_id` has a unique index, your merge key is solid. If `customer_id` is a NOT NULL foreign key into `customers`, every order line has a customer. The system guarantees it.

A **soft rule** is a business expectation with no enforcement behind it. "Quantities are always positive." "Orders go from pending to confirmed to shipped." "Only open invoices get deleted." These are descriptions of how the application is *supposed* to work, told to you by people who have never seen them violated -- because violations happen in prod, on the weekend, through a back-office script nobody documented.

The danger with soft rules is that they feel like hard rules. They hold for months. Your pipeline depends on them and nothing breaks. Then one day a developer runs a bulk update that bypasses the application layer, or support manually resets a status, or someone writes a cleanup script and forgets a WHERE clause. The soft rule breaks, and your pipeline either crashes, silently corrupts data, or both.

## How to Tell Them Apart

Query the source before you build:

```sql
-- source: transactional
-- PostgreSQL: enumerate what the database actually enforces on orders
SELECT
    tc.constraint_type,
    tc.constraint_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'orders'
ORDER BY tc.constraint_type, kcu.column_name;
```

| constraint_type | constraint_name | column_name |
|---|---|---|
| PRIMARY KEY | orders_pkey | id |
| FOREIGN KEY | orders_customer_id_fkey | customer_id |
| NOT NULL | (column constraint) | created_at |

What's on this list is hard. Everything else -- every verbal description, every README, every data dictionary entry -- is soft until proven otherwise. The tell is simple: "this column is always X" with no corresponding constraint in the output above is a soft rule.

> [!tip] Treat data dictionaries as soft
> A data dictionary that describes expected values (`status` is one of `pending`, `confirmed`, `shipped`) is documentation, not enforcement. Unless there's a CHECK constraint or an enum type backing it, the column will accept anything the application sends. Validate against the data, not against the dictionary.

## The Soft Rules in the Domain Model

These are all real-world patterns disguised as fictional tables. Every one of them will eventually be violated:

| Table | Soft Rule | How It Breaks |
|---|---|---|
| `orders` | "Always has at least one line" | Empty order saved by a UI bug, or created programmatically before lines are added |
| `orders` | "Status goes `pending` → `confirmed` → `shipped`" | Support team manually resets a status; migration script backdates records |
| `order_lines` | "Quantities are always positive" | Return entered as `-1`; bulk correction script uses negative values |
| `invoices` | "Only open invoices get deleted" | Year-end cleanup script deletes incorrectly posted invoices |
| `invoice_lines` | "Line status always matches header status" | One line disputed while the rest are approved; partial cancellation |
| `customers` | "Emails are unique" | Duplicate registration; customer service merges accounts manually |

None of these have a constraint in the DDL. All of them are described as "always true" by the people who built the system.

## What to Do When a Soft Rule Breaks

**Load the data. Don't drop rows.** A row that violates a soft rule is still a row that exists in the source. If you drop it, downstream teams are missing data and they don't know why. Dirty data is visible and fixable. Missing data is invisible and dangerous.

**Surface the violation.** Log it. Flag affected rows with a metadata column so consumers can filter or investigate:

```sql
-- source: columnar
-- Flag order_lines with negative quantities at load time
SELECT
    *,
    CASE WHEN quantity < 0 THEN TRUE ELSE FALSE END AS _flag_negative_quantity
FROM stg_order_lines;
```

**Don't fix it in the pipeline.** Coalescing a negative quantity to zero, skipping orders with no lines, or normalizing a status that skipped a step are all transformations that change business data. That belongs downstream, in the hands of whoever owns the business logic. Your job is to clone faithfully and report honestly.

> [!danger] Silent correction is the worst outcome
> Fixing soft rule violations in the pipeline hides the root cause. The source system keeps producing bad data, the pipeline keeps silently correcting it, and nobody ever fixes the application. Six months later, someone queries the source directly and finds data that doesn't match the destination. Now you have a trust problem and an archaeology project.

See [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] for how to formalize soft rule monitoring into data contracts with alerting.

## Soft Rules and Load Strategy

This is where soft rules cause the most damage in practice: incremental extraction.

The most common failure mode in incremental loading is building a cursor on `updated_at` when `updated_at` is a soft rule. The assumption is that **every** write to a row bumps `updated_at`. It's "always" true. Until it isn't.

**Open orders that don't move.** An order sitting in `pending` for three weeks never gets touched by the application. Its `updated_at` doesn't change. Your incremental extraction ignores it. Then a bulk migration script updates the `customer_id` on a set of orders and doesn't touch `updated_at`. Your pipeline never sees the change.

**Header timestamps without line propagation.** An invoice header gets `updated_at` bumped when it's confirmed. The individual `invoice_lines` have no timestamp of their own and no trigger to update when the header changes. Your incremental extraction on `invoice_lines` misses every status change that came through the header.

**Bulk scripts that bypass the ORM.** A price update runs directly against the `products` table via a SQL script. The ORM's `before_update` hook -- which sets `updated_at` -- never fires. Your pipeline sees no changes. The prices in the destination are stale.

The mitigations all involve some form of lookback -- accepting that `updated_at` isn't perfectly reliable and building in a safety net:

- Reprocess all rows with `updated_at` in the last $n$ days on every run, not just since the last checkpoint
- Reprocess all open/pending records unconditionally, regardless of timestamp -- their status can change without bumping `updated_at`
- Run a full replace of the current year once a week/day to catch anything the cursor missed

Full replace sidesteps all of this. A table that gets fully replaced every run doesn't care whether `updated_at` is reliable -- the whole thing comes fresh. This is another reason to default to full replace and earn incremental complexity only when the table is genuinely too large or too slow to reload. See [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]].

See [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] for lookback window patterns, and [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] for building incrementals that survive unreliable cursors.

## Related Patterns

- [[01-foundations-and-archetypes/0105-the-lies-sources-tell|0105-the-lies-sources-tell]]
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]]
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]]
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]]
- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]]
