---
title: "Timestamp Extraction Foundations"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: draft
created: 2026-03-13
updated: 2026-03-13
---

# Timestamp Extraction Foundations

> **One-liner:** `updated_at` is the obvious signal for incremental extraction -- and it's exactly as reliable as your application team's discipline.

## The Problem

Incremental extraction needs a signal: which rows changed since the last run? `updated_at` is the obvious answer -- it's on most tables, queryable, and cheap to filter. The difficulty is that it's maintained by the application layer, not the database. That means it works only if every write path remembers to update it -- triggers, ORMs, admin scripts, bulk imports. In practice, at least one always forgets.

Two patterns build on this signal: [[03-incremental-patterns/0302-cursor-based-extraction|0302]] tracks a high-water mark between runs; [[03-incremental-patterns/0303-stateless-window-extraction|0303]] always re-extracts a fixed trailing window. Both fail the same way when the signal is wrong.

---

## When `updated_at` Lies

**Trigger fires on UPDATE only, not INSERT.** A newly inserted row sits there with `updated_at = NULL` -- invisible to your cursor, no error, no warning.

```sql
-- source: transactional
SELECT order_id, created_at, updated_at
FROM orders
WHERE order_id IN (1001, 1002);
```

| order_id | created_at | updated_at |
|---|---|---|
| 1001 | 2026-01-15 09:00:00 | 2026-02-20 14:30:00 |
| 1002 | 2026-03-01 11:00:00 | NULL |

Order 1002 was just inserted. `updated_at` is NULL. Invisible to any `updated_at`-based filter. See [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]].

**Bulk operations bypass triggers.** Imports, backfills, admin scripts that write directly to the table -- they skip the trigger layer entirely and land rows with stale or null `updated_at`. The person running the script rarely knows your trigger exists.

**Application sets `updated_at` manually.** Some ORMs or legacy apps manage the field in code rather than via trigger. A buggy deploy, a migration script, or a data correction sets it to a past date. Rows change; the signal doesn't.

**Clock skew.** Source DB clock and extractor clock disagree by a few seconds. Rows updated in that gap fall outside the extraction window.

---

## Validating Before You Trust It

- **Does it populate on INSERT?** Query the most recently inserted rows and check whether `updated_at` is NULL or equals `created_at`.
- **Does a bulk operation update it?** Ask the source team directly. "Do your import scripts update `updated_at`?" is a question worth a 5-minute call.
- **What's the column precision?** `DATETIME` in MySQL defaults to second-level precision. `TIMESTAMP(6)` in PostgreSQL is microsecond. Second-level precision means two rows updated in the same second are indistinguishable at the boundary.
- **Is it indexed?** An unindexed `updated_at` on a 50M-row table will full-scan the source on every run.

If any of these is wrong, document it explicitly and decide whether the failure mode is acceptable given your periodic full replace cadence.

---

## The Periodic Full Replace

Both cursor-based and stateless window extraction freeze everything outside their active range. Hard deletes are invisible to both. Bulk operations that bypassed `updated_at` are invisible to both.

A periodic full replace resets all of it. How often do you see corrections that backdate past your cursor window? That's your full-replace cadence:

| Cadence | When it makes sense |
|---|---|
| Weekly | Most corrections land within days; source is well-maintained |
| Monthly | Occasional retroactive corrections; ERP with formal period closes |
| Quarterly | Stable source with rare manual edits |

If a full table reload is too expensive, scope the full replace to a rolling window of recent partitions -- see [[02-full-replace-patterns/0204-scoped-full-replace|0204-scoped-full-replace]].

---

## Related Patterns

- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- track a high-water mark; extract only what changed since the last run
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- extract a fixed trailing window every run; no cursor, no state
- [[03-incremental-patterns/0310-create-vs-update-separation|0310-create-vs-update-separation]] -- when the trigger fires on UPDATE only and INSERT rows are invisible
- [[02-full-replace-patterns/0201-full-scan-strategies|0201-full-scan-strategies]] -- when the table is small enough that incremental complexity isn't worth it
