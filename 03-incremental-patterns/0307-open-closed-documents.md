---
title: "Open/Closed Documents"
aliases: []
tags:
  - pattern/incremental
  - chapter/part-3
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Open/Closed Documents

> **One-liner:** Mutable drafts vs immutable posted documents. Extraction strategy should differ based on document lifecycle state.

See [[03-incremental-patterns/0304-cursor-from-another-table|0304]] for when the header cursor is enough. This pattern picks up where 0304's "when the header cursor lies" leaves off.

---

## The Problem

`invoices` are mutable while open -- status changes, lines get added or removed, amounts are adjusted. Once posted or closed, they're frozen. Treating both sides the same either wastes resources (re-extracting millions of immutable rows) or misses changes (a cursor can't see mutations on open documents that didn't update the header timestamp).

The business lifecycle itself is the scoping mechanism. Open documents need full re-extraction because anything can change. Closed documents are safe to extract once and never revisit.

---

## The Split

Two extraction strategies for one table:

- **Open documents:** re-extract the full set on every run. They're mutable -- lines change, statuses shift, amounts adjust. The only way to be sure you have the current state is to pull it again.
- **Closed documents:** extract only the recently closed. Once posted, a closed invoice is frozen. In many jurisdictions, modifying a closed invoice is illegal -- this is one of the rare cases where a soft rule ("we never edit closed invoices") is backed by a hard rule (the law). See [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]].

---

## The Combination Query

Two queries against the **source**, combined into one extraction:

```sql
-- source: transactional
-- Open side: full set of currently open documents
SELECT *
FROM invoices
WHERE status = 'open';
```

```sql
-- source: transactional
-- Closed side: recently closed only
SELECT *
FROM invoices
WHERE status = 'closed'
  AND updated_at >= :last_run;
```

UNION the results and load. See [[04-load-strategies/0403-merge-upsert|0403]] for load options.

The open set covers all mutations and line changes -- everything the header cursor in [[03-incremental-patterns/0304-cursor-from-another-table|0304]] couldn't see. The closed set is cheap because closed documents don't change.

The **destination** still has documents that were open last run but have since closed or been deleted at the source. The open-side extract no longer includes them. The closed-side cursor catches transitions (the document appears with `status = 'closed'` and a recent `updated_at`). Deletes need [[03-incremental-patterns/0306-hard-delete-detection|0306]].

---

## Extending to Detail Tables

The same split applies to `invoice_lines`: re-extract all lines for open invoices, cursor-only for closed.

```sql
-- source: transactional
-- All lines for open invoices
SELECT il.*
FROM invoice_lines il
JOIN invoices i ON il.invoice_id = i.invoice_id
WHERE i.status = 'open';
```

```sql
-- source: transactional
-- Lines for recently closed invoices only
SELECT il.*
FROM invoice_lines il
JOIN invoices i ON il.invoice_id = i.invoice_id
WHERE i.status = 'closed'
  AND i.updated_at >= :last_run;
```

The line extraction query joins to the header's status, not just its timestamp. This is the answer to [[03-incremental-patterns/0304-cursor-from-another-table|0304]]'s blind spot: open documents get full line coverage regardless of whether the header's `updated_at` fired.

> [!warning] Detail-level status doesn't always match the header
> `invoice_lines` can have their own `status` -- a line marked `disputed` on an otherwise open invoice, or a line already `approved` while the header is still `open`. The split here is on the **header's** lifecycle, not the line's. An open invoice with a mix of approved and disputed lines is still in the open set and gets fully re-extracted. If the line status changes independently after the header closes, neither side of this pattern sees it -- that's [[03-incremental-patterns/0308-detail-without-timestamp|0308]] territory.

---

## The Transition Moment

A document closes between runs. Two scenarios:

**`updated_at` fires on status change.** The closed-side cursor captures it. The document appears in the closed-side extract with its final state. Clean.

**`updated_at` doesn't fire on status change.** The open-side extract had the document in the previous run (it was still open then). The next run's open set won't include it anymore -- and the closed-side cursor won't pick it up either (no `updated_at` change). The document falls out of both sides. The destination keeps the last open-side version -- with `status = 'open'` permanently. The actual `status = 'closed'` transition never syncs. Any modifications between the last open-side extract and the close are also lost. The periodic full replace is the only thing that corrects both problems.

> [!warning] The dangerous edge case
> A document closes AND a line gets hard-deleted in the same window. The open-side extract from the previous run had the line. The closed-side cursor picks up the header (if `updated_at` fired) but the deleted line is gone from the source. The destination keeps the stale line. Either accept this gap until the periodic full replace, or run a line-level reconciliation on recently transitioned documents.

---

## Reopening

"Closed documents don't reopen" -- check the legal framework before assuming this is a soft rule. In most jurisdictions, reopening a posted invoice is illegal; the correct process is to issue a credit note or return document. If the system enforces this, reopening is not a concern for the pipeline.

When it does happen (support manually reopens one, or the system allows it), a reopened document appears in the open set on the next run -- caught naturally.

The gap is between close and reopen: the document was in neither set (closed cursor already passed it, open set didn't include it yet). The stateless window approach from [[03-incremental-patterns/0303-stateless-window-extraction|0303]] absorbs this if the window covers the gap. If the reopen happens within days and the window is 7 days, the document is already covered.

---

## Hard Deletes on Open Documents

Open `invoices` get hard-deleted regularly -- the domain model case.

The open-side extract from the **source** gives you the current set of open IDs. The **destination** has the previous set, which includes documents deleted since the last run. The diff between destination open IDs and source open IDs reveals candidates -- but that diff also includes documents that transitioned to closed. Filter out the newly closed (they appear in the closed-side extract) to isolate the actual deletes.

```sql
-- destination: columnar
-- IDs in destination marked as open, minus source open IDs, minus newly closed
SELECT d.invoice_id
FROM invoices d
WHERE d.status = 'open'
  AND d.invoice_id NOT IN (SELECT invoice_id FROM _stg_source_open_ids)
  AND d.invoice_id NOT IN (SELECT invoice_id FROM _stg_source_closed_recent);
```

Closed documents that get hard-deleted -- the soft rule violation from [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106]] -- need the general mechanism from [[03-incremental-patterns/0306-hard-delete-detection|0306]].

---

## The Cost Equation

The cost is relative to the alternative. The ratio of open to total matters more than the absolute number: 50,000 open invoices is 0.05% of a 100-million-row table -- a fraction of a full replace. The same 50,000 against a 60,000-row table is 83% -- at that point, a full replace is simpler.

In systems with long-lived open documents -- consulting invoices open for months, construction contracts open for years -- the open set grows and the cost advantage over a scoped full replace ([[02-full-replace-patterns/0204-scoped-full-replace|0204]]) shrinks. Evaluate case by case.

---

## By Corridor

> [!example]- Transactional → Columnar (e.g. any source → BigQuery)
> Both queries run on the source as indexed scans (`status` should be indexed, or at least selective enough). The open set is small relative to the table, so the source cost is low. The destination load cost depends on the load strategy -- see [[04-load-strategies/0403-merge-upsert|0403]]. The delete detection query runs entirely in the destination and is cheap (single-column scans).

> [!example]- Transactional → Transactional (e.g. any source → PostgreSQL)
> Cheap on both sides. The open-side extract is a small indexed scan. The delete detection diff can run as a single query joining source and destination if both are accessible from the same connection, or via staging tables if they're not.

---

## Related Patterns

- [[03-incremental-patterns/0304-cursor-from-another-table|0304-cursor-from-another-table]] -- the simpler pattern that teases this one; this is where its blind spots get resolved
- [[03-incremental-patterns/0306-hard-delete-detection|0306-hard-delete-detection]] -- open documents are the ones most likely to get hard-deleted; closed-side deletes need this
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the cursor for the closed-document side
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- absorbs the reopen gap if the window is wide enough
- [[03-incremental-patterns/0308-detail-without-timestamp|0308-detail-without-timestamp]] -- when even the open/closed split can't cover detail changes
- [[01-foundations-and-archetypes/0106-hard-rules-soft-rules|0106-hard-rules-soft-rules]] -- "closed invoices are immutable" is backed by law in most jurisdictions
- [[02-full-replace-patterns/0204-scoped-full-replace|0204-scoped-full-replace]] -- the alternative when the open set grows too large
