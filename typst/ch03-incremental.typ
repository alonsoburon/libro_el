#import "theme.typ": palette, ecl-tip, ecl-warning, ecl-danger, ecl-info
= Timestamp Extraction Foundations
<timestamp-extraction-foundations>
#quote(block: true)[
#strong[One-liner:] `updated_at` is the obvious signal for incremental extraction -- and it's exactly as reliable as your application team's discipline.
]

== The Problem
Incremental extraction needs a signal: which rows changed since the last run? `updated_at` is the obvious answer -- it's on most tables, queryable, and cheap to filter. The difficulty is that it's maintained by the application layer, not the database. That means it works only if every write path remembers to update it -- triggers, ORMs, admin scripts, bulk imports. In practice, at least one always forgets.

Two patterns build on this signal: 0302 tracks a high-water mark between runs; 0303 always re-extracts a fixed trailing window. Both fail the same way when the signal is wrong.

// ---

== When `updated_at` Lies
<when-updated_at-lies>
#strong[Trigger fires on UPDATE only, not INSERT.] A newly inserted row sits there with `updated_at = NULL` -- invisible to your cursor, no error, no warning.

```sql
-- source: transactional
SELECT order_id, created_at, updated_at
FROM orders
WHERE order_id IN (1001, 1002);
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto,auto,auto,),
    table.header([order\_id], [created\_at], [updated\_at],),
    table.hline(),
    [1001], [2026-01-15 09:00:00], [2026-02-20 14:30:00],
    [1002], [2026-03-01 11:00:00], [NULL],
  )]
  , kind: table
  )

Order 1002 was just inserted. `updated_at` is NULL. Invisible to any `updated_at`-based filter. See @create-vs-update-separation.

#strong[Bulk operations bypass triggers.] Imports, backfills, admin scripts that write directly to the table -- they skip the trigger layer entirely and land rows with stale or null `updated_at`. The person running the script rarely knows your trigger exists.

#strong[Application sets `updated_at` manually.] Some ORMs or legacy apps manage the field in code rather than via trigger. A buggy deploy, a migration script, or a data correction sets it to a past date. Rows change; the signal doesn't.

#strong[Clock skew.] Source DB clock and extractor clock disagree by a few seconds. Rows updated in that gap fall outside the extraction window.

// ---

== Validating Before You Trust It
<validating-before-you-trust-it>
- #strong[Does it populate on INSERT?] Query the most recently inserted rows and check whether `updated_at` is NULL or equals `created_at`.
- #strong[Does a bulk operation update it?] Ask the source team directly. "Do your import scripts update `updated_at`?" is a question worth a 5-minute call.
- #strong[What's the column precision?] `DATETIME` in MySQL defaults to second-level precision. `TIMESTAMP(6)` in PostgreSQL is microsecond. Second-level precision means two rows updated in the same second are indistinguishable at the boundary.
- #strong[Is it indexed?] An unindexed `updated_at` on a 50M-row table will full-scan the source on every run.

If any of these is wrong, document it explicitly and decide whether the failure mode is acceptable given your periodic full replace cadence.

// ---

== The Periodic Full Replace
<the-periodic-full-replace>
Both cursor-based and stateless window extraction freeze everything outside their active range. Hard deletes are invisible to both. Bulk operations that bypassed `updated_at` are invisible to both.

A periodic full replace resets all of it. How often do you see corrections that backdate past your cursor window? That's your full-replace cadence:

#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Cadence], [When it makes sense],),
    table.hline(),
    [Weekly], [Most corrections land within days; source is well-maintained],
    [Monthly], [Occasional retroactive corrections; ERP with formal period closes],
    [Quarterly], [Stable source with rare manual edits],
  )]
  , kind: table
  )

If a full table reload is too expensive, scope the full replace to a rolling window of recent partitions -- see @scoped-full-replace.

// ---

// ---

= Cursor-Based Timestamp Extraction
<cursor-based-timestamp-extraction>
#quote(block: true)[
#strong[One-liner:] Track a cursor -- the high-water mark of the last successful run. Each run extracts only rows updated after that point.
]

See 0301 for when `updated_at` lies, how to validate it, and when to run a periodic full replace.

// ---

== How It Works
<how-it-works>
```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= :last_run;
```

After a confirmed successful load, advance the cursor to the current timestamp. On the next run, use the new value.

=== Where to Store the Cursor
<where-to-store-the-cursor>
#strong[Option 1: `MAX(updated_at)` from the destination.]

```sql
-- source: columnar
SELECT MAX(updated_at) AS last_run
FROM orders;
```

Simple, zero extra infrastructure, self-contained. The cursor lives in the data.

The risk: it's tied to what's actually in the destination. If the destination is rebuilt, truncated, or has rows with stale timestamps from a bad load, the max is wrong. A cursor that's too low causes re-extraction (harmless, upsert handles it). A cursor that's too high skips rows permanently.

#strong[Option 2: External state store.] Orchestrator metadata, a dedicated state table, a key-value store. Survives destination rebuilds and is decoupled from data quality.

The risk: more moving parts. If a load partially succeeds and the cursor advances anyway, you have a permanent gap.

Both are valid. `MAX` from destination is the simpler default. External state earns its overhead when destination rebuilds are a real operational scenario.

#ecl-warning("Advance cursor after confirmed load")[A partial load followed by a cursor advance is a permanent gap. The rows in the failed batch will never be re-extracted. Treat cursor advancement as the final step of the pipeline, gated on load confirmation -- not something that happens at the start of the next run.]

=== Boundary Handling
<boundary-handling>
Always use `>=` not `>`. A missed row has no recovery path; a duplicate row is handled by the destination's upsert.

Add a small buffer on the lower bound (5--30 seconds) to absorb clock skew between source and extractor. The overlap mechanism is the same as @late-arriving-data -- just measured in seconds instead of hours.

// ---

== By Corridor
#ecl-info("Transactional to columnar corridor")[PostgreSQL `TIMESTAMPTZ` maps cleanly to BigQuery `TIMESTAMP`. MySQL `DATETIME` has no timezone and second-level precision -- the buffer compensates. A cursor limits the extracted row count but doesn't eliminate the destination load cost -- see @merge-upsert for the MERGE cost anatomy.]

#ecl-warning("Transactional to transactional corridor")[`MAX(updated_at)` from the destination is cheap -- a simple indexed column scan. The buffer overlap produces duplicates; the destination upsert handles them (see @merge-upsert).]

// ---

// ---

= Stateless Window Extraction
<stateless-window-extraction>
#quote(block: true)[
#strong[One-liner:] Extract a fixed trailing window on every run. No cursor, no state between runs. This is how I run most of my incremental tables.
]

See 0301 for when `updated_at` lies, how to validate it, and when to run a periodic full replace.

// ---

== How It Works
<how-it-works-1>
Skip the cursor entirely. Every run extracts a fixed trailing window regardless of what happened last run.

```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days';
```

No state to manage, no cursor to advance, no orchestrator metadata. Run it twice and get the same result. A failed run leaves nothing behind -- the next run picks up from the same window.

A window measured in days absorbs any clock skew between source and extractor. No buffer needed.

// ---

== Why I Default to This
<why-i-default-to-this>
A cursor-based pipeline can fail in ways that are hard to debug: partial loads that advance the cursor, destination rebuilds that reset the high-water mark, orchestrator metadata that gets out of sync. All of these produce permanent gaps that are invisible until someone notices the counts are off.

A stateless window can't have any of these problems. There's no state to corrupt. Re-run it, get the same result. Retry after failure -- just run again. Backfill a date range -- change the window bounds. Two runs overlap -- upsert or dedup handles it. Every property you want from a pipeline (stateless, idempotent, safe to retry, safe to parallelize) comes for free.

The tradeoff: you always process the full window even when almost nothing changed. For small-to-moderate tables with indexed `updated_at`, that cost is almost always less than the engineering cost of managing cursor state across thousands of tables.

#ecl-warning("Match window to correction lag")[How far back can a correction or late-arriving row realistically land? If support can reopen a 3-day-old order, cover at least 4 days. If the source team runs 2-week backfills, cover that. Query cost comes second.]

// ---

== When a Cursor Earns Its Overhead
<when-a-cursor-earns-its-overhead>
Don't use a stateless window when:

- You're running hourly or more on large tables -- a 7-day window running 24 times a day reprocesses those 7 days 24 times. The MERGE cost multiplies directly.
- The table is big enough that even an indexed `updated_at` scan on the window is expensive on the source.
- Mutation rate is high and concentrated in recent rows -- a cursor extracts only the delta, which might be 0.1% of the window.

If you're running daily or less, or the table is small-to-moderate, the stateless window wins on simplicity every time.

// ---

== Window Size x Run Frequency
<window-size-x-run-frequency>
This is the knob that matters. A 7-day window running daily costs X. The same window running hourly costs 24X -- both in source query cost and destination load cost (see @merge-upsert for the cost anatomy).

Size the window for correctness (it must cover your correction lag). Then set run frequency for cost. If the cost is too high, the answer is usually to run less often -- not to shrink the window below what correctness requires.

== Align Windows to Partition Boundaries
<align-windows-to-partition-boundaries>
If the destination is partitioned by date, align the window to complete days. A 7-day window that spans 8 calendar days touches 8 partitions instead of 7. See @merge-upsert for why partition alignment matters in columnar engines.

== Multiple Windows
<multiple-windows>
For tables where freshness matters AND corrections land late:

- Narrow window (1 day) running hourly -- gives sub-hour latency for recent changes
- Wide window (30 days) running nightly -- catches retroactive edits and slow-arriving rows
- Periodic full replace -- catches everything outside both windows

Three tiers, no cursor state anywhere, each tier sized independently.

// ---

== When There's No Timestamp At All
<when-theres-no-timestamp-at-all>
The examples above assume the source has an `updated_at` or similar timestamp to scope the window. Some sources don't -- the data is correct as of whenever you query it, figures get revised and disputes get resolved, but there's no column that tells you when. No `updated_at`, no row version, no mechanism to tell you what changed.

The extraction is still a stateless window, but scoped by a business date instead of a modification timestamp:

```sql
-- source: transactional
SELECT *
FROM invoices
WHERE invoice_date >= CURRENT_DATE - 90;
```

Every source that rewrites history has a horizon -- the furthest back a correction can reach. "Sales figures finalize after 60 days." "Invoices can be disputed within 90 days." That horizon defines your window size. If the business says 60 days, extract 90. The stated horizon is a soft rule (0106) -- verify it against actual data before trusting it.

Rows outside the mutable window are immutable by definition. They stay in the destination untouched between runs. Only the window gets re-extracted.

The key difference from a timestamp-based window: you're extracting #emph[every] row in the window, not just rows that changed. A 7-day `updated_at` window returns only rows modified in the last 7 days. A 90-day business-date window returns all 90 days of rows regardless of whether they changed. The append volume is higher, but there's no filtering assumption to get wrong -- you're guaranteed to capture every correction within the horizon.

Load with append-and-materialize (0404) to keep the per-run cost near zero. At intra-day frequency, this is significantly cheaper than a scoped full replace (0204) which would require a partition rewrite on every run.

// ---

== By Corridor
<by-corridor-1>
#ecl-info("Transactional to columnar corridor")[The source query is cheap (indexed `updated_at` scan). The load cost is where window size and run frequency multiply -- see @merge-upsert and @cost-monitoring. MySQL `DATETIME` second-level precision is a non-issue with a window measured in days.]

#ecl-warning("Transactional to transactional corridor")[Cheap on both sides. The source query is the same indexed scan. Load cost scales with batch size, not table size -- high-frequency runs are viable here. See @merge-upsert for the upsert mechanics.]

// ---

// ---

= Cursor from Another Table
<cursor-from-another-table>
#quote(block: true)[
#strong[One-liner:] When a detail table has no `updated_at`, borrow the header's timestamp to scope the extraction.
]

See 0301 for the shared `updated_at` reliability concerns.

// ---

== The Problem
<the-problem-1>
Some detail tables like `order_lines` and `invoice_lines` carry no timestamp of their own -- the only `updated_at` lives on the header.

Re-extracting all lines on every run works until the table crosses a few million rows and your source DBA starts asking questions. You need to scope.

// ---

== The Pattern
Use the header's cursor to figure out which detail rows to pull. Two ways to write it -- which one is better depends on your source engine and how the query planner handles it.

#strong[Subquery filter:]

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
WHERE ol.order_id IN (
  SELECT o.order_id
  FROM orders o
  WHERE o.updated_at >= :last_run
);
```

Works well when the subquery returns a small set of IDs. Most transactional engines turn this into a semi-join and it's fast.

#strong[Direct join:]

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
JOIN orders o ON ol.order_id = o.order_id
WHERE o.updated_at >= :last_run;
```

Simpler to read. Can be faster when both tables are indexed on the join key. Check your EXPLAIN -- some planners pull unnecessary header columns into the execution plan even with `SELECT ol.*`.

Both pull all lines for every order that changed. Some of those lines didn't actually change. That's fine -- the upsert handles duplicates, and you have no way to know which specific lines changed anyway.

// ---

== Cascading Joins
<cascading-joins>
One hop is straightforward. Two hops gets expensive. Three hops -- stop and reconsider.

```sql
-- source: transactional
SELECT sl.*
FROM shipment_lines sl
JOIN shipments s ON sl.shipment_id = s.shipment_id
JOIN orders o ON s.order_id = o.order_id
WHERE o.updated_at >= :last_run;
```

Each join multiplies the row count and the assumptions. You're trusting two foreign key relationships and two intermediate tables to be correct and up to date. At three hops, the scoped full replace in @scoped-full-replace is almost certainly simpler, cheaper, and more reliable.

// ---

== The False Economy of Re-extracting All Lines
<the-false-economy-of-re-extracting-all-lines>
Teams often feel guilty about pulling all lines for changed orders. Don't.

If an order has 5 lines on average and 200 orders changed since the last run, you're extracting 1,000 rows. The upsert handles the unchanged ones. The source barely notices.

This stops being fine when the combination of line count per header and header change rate produces batches large enough to stress the source or the destination MERGE. But that threshold is relative -- a wholesale distributor with 100+ lines per document also generates proportionally more data everywhere else, which means their infrastructure budget already accounts for heavier workloads. The absolute cost goes up; the cost relative to the operation stays similar.

Until re-extraction is actually causing problems -- slow runs, source contention, MERGE cost spikes -- it's the right default. Simple, correct, and the overhead scales with the business.

// ---

== When the Header Cursor Lies Too
<when-the-header-cursor-lies-too>
Everything above assumes that when a detail row changes, the header's `updated_at` fires. Two categories of failure break this assumption, and they require different responses.

=== The header doesn't know the line changed
<the-header-doesnt-know-the-line-changed>
`invoice_lines.status` changes from `approved` to `disputed` -- the invoice header's `updated_at` never fires. An admin script reprices 10,000 order lines without touching the header. In SAP B1, the header `UpdateDate` is a DATE field with no time component, though with a stateless window measured in days (0303) this particular issue is absorbed.

The common thread: the line mutated, the header didn't, and the cursor is blind to it.

=== The line disappears entirely
<the-line-disappears-entirely>
`invoice_lines` get hard-deleted independently of the header -- not just via cascade. In SAP B1, removing a single line triggers a delete+reinsert of ALL surviving lines with new `LineNum` values. No tombstone, no change log entry. The cursor has nothing to detect because the row is gone and the header may not have registered the event. See @hard-delete-detection.

When this happens, you have two good options:

+ #strong[Accept the blind spot.] The periodic full replace catches everything the cursor misses. If your SLA tolerates the lag, this is the cheapest approach and the one I use most often.

+ #strong[Split by document lifecycle.] Extract all open documents from the source (they're mutable, re-extract everything), combine with only the recently modified closed documents (they're frozen, cursor is reliable). This gives you full coverage of the mutable set without re-extracting the entire table -- but the combination logic is nontrivial, especially when documents transition between open and closed between runs, or when lines get hard-deleted from open documents. @openclosed-documents covers the full pattern.

For detail tables where even these approaches aren't enough, see @detail-without-timestamp.

// ---

== By Corridor
<by-corridor-2>
#ecl-warning("Transactional to columnar corridor")[The join runs on the source, so extraction cost is a source-side index scan. Wide detail tables (many columns per line) amplify the load cost even for moderate row counts -- see @merge-upsert and @cost-monitoring.]

#ecl-info("Transactional to transactional corridor")[Cheap on both sides. Extraction is the same index scan. The composite key (`order_id, line_num`) must be indexed on the destination for the upsert to perform -- see @merge-upsert.]

// ---

// ---

= Sequential ID Cursor
<sequential-id-cursor>
#quote(block: true)[
#strong[One-liner:] No `updated_at` anywhere, but the PK is monotonically increasing. `WHERE id > :last_id` detects inserts only -- updates are invisible by design.
]

// ---

== The Problem
<the-problem-2>
`events` has no `updated_at` and no `created_at`. `inventory_movements` doesn't either. What they do have is an auto-incrementing primary key that grows with every insert. That's enough to build a cursor on -- with an explicit tradeoff.

// ---

== The Pattern
<the-pattern-1>
```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id > :last_id;
```

After a confirmed successful load, set `:last_id` to the `MAX(event_id)` from the extracted batch. On the next run, pick up where you left off.

// ---

== The Tradeoff You Accept
<the-tradeoff-you-accept>
This cursor detects inserts only. An existing row that gets modified will never be re-extracted. You accept this when:

- The table is append-only in practice -- `events` and `inventory_movements` in the domain model are designed this way
- Updates are rare enough that the periodic full replace catches them

Before committing to this pattern, check the table's actual behavior against what the source team claims. "Events are never updated" is likely a soft rule (0106). If nothing in the schema enforces immutability, someone will eventually run an UPDATE on it -- a bulk correction, a backfill, an admin fix. Your pipeline won't notice.

// ---

== Gap Safety
<gap-safety>
Sequences produce gaps all the time -- rolled-back transactions, failed inserts, reserved-but-unused IDs. Gaps are harmless here: `WHERE id > :last_id` skips the gap and picks up the next real row. No false positives, no missed rows.

The dangerous case is the opposite: a row inserted with an ID #emph[lower] than `:last_id`. This happens with:

- Manually set IDs (bulk imports that override the sequence)
- Sequences with `CACHE` in multi-session environments -- IDs are allocated in blocks and committed out of order
- Restored backups that reset the sequence counter

#ecl-warning("Out-of-order inserts are permanent misses")[A row with `id = 500` inserted after the cursor has passed `id = 600` will never be extracted. The periodic full replace is the only safety net.]

If you suspect out-of-order inserts are happening (multi-session `CACHE` is the usual cause), add a small overlap buffer the same way 0302 handles clock skew:

```sql
-- source: transactional
SELECT *
FROM events
WHERE event_id >= :last_id - 100;
```

The overlap re-extracts, at a minimum, the last 100 IDs on every run. The upsert handles duplicates. Size the buffer to your worst observed out-of-order gap -- 100 covers most `CACHE` configurations.

Hard deletes are invisible too, same as with any cursor -- see @hard-delete-detection.

// ---

== Composite Keys
<composite-keys>
When the primary key is a composite (`order_id + line_num`, `warehouse_id + sku`), there's no natural ordering to build a cursor on. This pattern doesn't apply. See @cursor-from-another-table for borrowing a timestamp from a related table, or @detail-without-timestamp when no timestamp is available anywhere in the relationship.

// ---

== By Corridor
<by-corridor-3>
#ecl-info("Transactional to columnar corridor")[For truly append-only sources, the extraction is a simple indexed range scan. The load can use pure APPEND instead of MERGE -- see @append-only-load. If the table turns out to have occasional updates (the soft rule breaks), a periodic full replace catches them.]

#ecl-warning("Transactional to transactional corridor")[Same indexed range scan on the source. The load strategy depends on whether the source is truly immutable -- see @append-only-load for append-only and @merge-upsert for upsert.]

// ---

// ---

= Hard Delete Detection
<hard-delete-detection>
#quote(block: true)[
#strong[One-liner:] The row was there yesterday, today it's gone. A cursor never sees a deleted row -- you need a separate mechanism.
]

// ---

== The Problem
<the-problem-3>
Every extraction pattern in this chapter -- cursors, stateless windows, borrowed timestamps -- detects rows that changed. A hard delete leaves nothing behind to detect. The row is gone from the source, still present in the destination, and every cursor-based run confirms zero about its absence. The count drifts silently.

// ---

== When the Source Cooperates
<when-the-source-cooperates>
Two mechanisms make delete detection trivial:

#strong[Soft-delete columns] (`is_active`, `deleted_at`) -- the source marks the row as deleted instead of removing it. The normal cursor captures the flag change like any other update. This is the clean solution, but it's rarer than you'd expect. Many transactional systems -- especially ERPs -- hard-delete without ceremony.

#strong[Tombstone tables] -- the source writes a record to a separate `deletes` or `audit_log` table when a row is removed. Extract from both tables: the main table for current state, the tombstone table for delete events. Common in CDC-adjacent systems, rare in application databases.

When neither exists -- and for most tables in most sources, neither does -- you need a detection mechanism that works from the outside.

// ---

== When It Doesn't
<when-it-doesnt>
=== Full ID Comparison
<full-id-comparison>
Extract the full set of IDs from the source. Extract the full set of IDs from the destination. Compare them -- either in the orchestrator, or by landing the source IDs into a staging table in the destination and running the diff there.

```sql
-- destination (after landing source IDs into staging)
SELECT d.invoice_id
FROM invoices d
LEFT JOIN _stg_invoice_ids s ON d.invoice_id = s.invoice_id
WHERE s.invoice_id IS NULL;
```

The rows returned exist in the destination but not in the source -- candidates for deletion.

The source-side cost is the expensive part: a full `SELECT id FROM table` on a large transactional table hits every row. Schedule it outside business hours. The destination side is cheap -- columnar engines scan a single key column efficiently regardless of table size.

For small-to-medium tables, this is the simplest and most reliable approach. For large tables, use count reconciliation as a cheaper first pass.

=== Count Reconciliation
<count-reconciliation>
Compare `COUNT(*)` between source and destination. If the counts match, no deletes happened (or inserts and deletes balanced out -- rare but possible). If they diverge, something changed.

```sql
-- source: transactional
SELECT COUNT(*) FROM invoices;

-- destination: columnar
SELECT COUNT(*) FROM invoices;
```

This detects drift but doesn't identify which rows. Two useful responses:

- #strong[Trigger a full replace] when counts diverge -- simple, correct, and often the cheapest response for moderate tables
- #strong[Trigger a full ID comparison] -- when a full replace is too expensive, use the count mismatch as a signal to run the heavier detection

Run the source count immediately after the load completes -- the closer in time the two counts are, the less chance of a concurrent insert or delete skewing the comparison. The direction of the mismatch tells you something:

- #strong[Destination \> source:] deletes happened at the source since the last full sync
- #strong[Destination \< source:] inserts landed at the source that the extraction missed (cursor gap, late-arriving data, simple delay between extraction and loading)

For partitioned tables, compare counts per partition (`GROUP BY date_partition`) to narrow the scope before running a full ID comparison on only the divergent partitions.

#ecl-warning("Count reconciliation as a gate")[Run `COUNT(\*)` on every incremental extraction as a cheap health check. It adds seconds to the run and catches drift early -- before it accumulates into a reconciliation problem. See @reconciliation-patterns.]

// ---

== Propagation
<propagation>
Once you've identified deleted IDs, the question is what to do with them in the destination. This is a load concern -- see @merge-upsert for the mechanics.

Three options, in order of preference:

+ #strong[Soft-delete in destination.] Set `_is_deleted = true` and `_deleted_at = CURRENT_TIMESTAMP`. The row stays queryable, downstream consumers can filter on the flag, and the delete is reversible if the source was wrong. If downstream consumers are technical (analysts, dbt models), this is the default.

+ #strong[Hard-delete in destination.] `DELETE FROM destination WHERE id IN (...)`. Matches the source exactly. Simpler for non-technical consumers who don't understand why "deleted" rows still appear in their reports.

+ #strong[Move to a `_deleted` table.] `INSERT INTO invoices_deleted SELECT *, CURRENT_TIMESTAMP AS _deleted_at FROM invoices WHERE id IN (...); DELETE FROM invoices WHERE id IN (...)`. Only when governance or audit requirements demand a record of what was deleted and when. Adds operational complexity.

// ---

== `invoices` / `invoice_lines`
<invoices-invoice_lines>
The domain model case: open `invoices` get hard-deleted regularly. `invoice_lines` get hard-deleted independently of their header -- not just via cascade. This creates two detection scopes:

- #strong[Header deletes:] compare `invoice_id` sets between source and destination. The open/closed split from 0307 helps -- the open-side full extract naturally reveals missing headers.
- #strong[Line deletes:] for each header that still exists, compare `line_num` sets. A header that hasn't changed can still have lines removed underneath it -- the header cursor from 0304 is blind to this.

In SAP B1, removing a single `invoice_line` triggers a delete+reinsert of ALL surviving lines with new `LineNum` values. The old line numbers are gone, the new ones look like fresh inserts. A full ID comparison catches this while a cursor never will.

// ---

== By Corridor
<by-corridor-4>
#ecl-info("Transactional to columnar corridor")[The source-side `SELECT id` is the bottleneck -- a full table scan on a transactional engine. The destination-side comparison is cheap (single-column scan). Land source IDs into a staging table and run the diff in the destination to avoid pulling large ID sets through the orchestrator. For propagation, soft-delete is a metadata update on the destination -- see @merge-upsert.]

#ecl-warning("Transactional to transactional corridor")[Both sides are cheap for ID extraction if the primary key is indexed (it always is). The comparison can run in either system. `DELETE FROM destination WHERE id IN (...)` is a natural fit here -- transactional engines handle point deletes efficiently.]

// ---

// ---

= Open/Closed Documents
<openclosed-documents>
#quote(block: true)[
#strong[One-liner:] Mutable drafts vs immutable posted documents. Extraction strategy should differ based on document lifecycle state.
]

See 0304 for when the header cursor is enough. This pattern picks up where 0304's "when the header cursor lies" leaves off.

// ---

== The Problem
<the-problem-4>
`invoices` are mutable while open -- status changes, lines get added or removed, amounts are adjusted. Once posted or closed, they're frozen. Treating both sides the same either wastes resources (re-extracting millions of immutable rows) or misses changes (a cursor can't see mutations on open documents that didn't update the header timestamp).

The business lifecycle itself is the scoping mechanism. Open documents need full re-extraction because anything can change. Closed documents are safe to extract once and never revisit.

// ---

== The Split
<the-split>
Two extraction strategies for one table:

- #strong[Open documents:] re-extract the full set on every run. They're mutable -- lines change, statuses shift, amounts adjust. The only way to be sure you have the current state is to pull it again.
- #strong[Closed documents:] extract only the recently closed. Once posted, a closed invoice is frozen. In many jurisdictions, modifying a closed invoice is illegal -- this is one of the rare cases where a soft rule ("we never edit closed invoices") is backed by a hard rule (the law). See 0106.

// ---

== The Combination Query
<the-combination-query>
Two queries against the #strong[source];, combined into one extraction:

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

UNION the results and load. See 0403 for load options.

The open set covers all mutations and line changes -- everything the header cursor in 0304 couldn't see. The closed set is cheap because closed documents don't change.

The #strong[destination] still has documents that were open last run but have since closed or been deleted at the source. The open-side extract no longer includes them. The closed-side cursor catches transitions (the document appears with `status = 'closed'` and a recent `updated_at`). Deletes need 0306.

// ---

== Extending to Detail Tables
<extending-to-detail-tables>
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

The line extraction query joins to the header's status, not just its timestamp. This is the answer to 0304's blind spot: open documents get full line coverage regardless of whether the header's `updated_at` fired.

#ecl-warning("Line status can diverge from header")[`invoice_lines` can have their own `status` -- a line marked `disputed` on an otherwise open invoice, or a line already `approved` while the header is still `open`. The split here is on the #strong[header's] lifecycle, not the line's. An open invoice with a mix of approved and disputed lines is still in the open set and gets fully re-extracted. If the line status changes independently after the header closes, neither side of this pattern sees it -- that's 0308 territory.]

// ---

== The Transition Moment
<the-transition-moment>
A document closes between runs. Two scenarios:

#strong[`updated_at` fires on status change.] The closed-side cursor captures it. The document appears in the closed-side extract with its final state. Clean.

#strong[`updated_at` doesn't fire on status change.] The open-side extract had the document in the previous run (it was still open then). The next run's open set won't include it anymore -- and the closed-side cursor won't pick it up either (no `updated_at` change). The document falls out of both sides. The destination keeps the last open-side version -- with `status = 'open'` permanently. The actual `status = 'closed'` transition never syncs. Any modifications between the last open-side extract and the close are also lost. The periodic full replace is the only thing that corrects both problems.

#ecl-warning("Close plus hard delete in one window")[A document closes AND a line gets hard-deleted in the same window. The open-side extract from the previous run had the line. The closed-side cursor picks up the header (if `updated_at` fired) but the deleted line is gone from the source. The destination keeps the stale line. Either accept this gap until the periodic full replace, or run a line-level reconciliation on recently transitioned documents.]

// ---

== Reopening
<reopening>
"Closed documents don't reopen" -- check the legal framework before assuming this is a soft rule. In most jurisdictions, reopening a posted invoice is illegal; the correct process is to issue a credit note or return document. If the system enforces this, reopening is not a concern for the pipeline.

When it does happen (support manually reopens one, or the system allows it), a reopened document appears in the open set on the next run -- caught naturally.

The gap is between close and reopen: the document was in neither set (closed cursor already passed it, open set didn't include it yet). The stateless window approach from 0303 absorbs this if the window covers the gap. If the reopen happens within days and the window is 7 days, the document is already covered.

// ---

== Hard Deletes on Open Documents
<hard-deletes-on-open-documents>
Open `invoices` get hard-deleted regularly -- the domain model case.

The open-side extract from the #strong[source] gives you the current set of open IDs. The #strong[destination] has the previous set, which includes documents deleted since the last run. The diff between destination open IDs and source open IDs reveals candidates -- but that diff also includes documents that transitioned to closed. Filter out the newly closed (they appear in the closed-side extract) to isolate the actual deletes.

```sql
-- destination: columnar
-- IDs in destination marked as open, minus source open IDs, minus newly closed
SELECT d.invoice_id
FROM invoices d
WHERE d.status = 'open'
  AND d.invoice_id NOT IN (SELECT invoice_id FROM _stg_source_open_ids)
  AND d.invoice_id NOT IN (SELECT invoice_id FROM _stg_source_closed_recent);
```

Closed documents that get hard-deleted -- the soft rule violation from 0106 -- need the general mechanism from 0306.

// ---

== The Cost Equation
<the-cost-equation>
The cost is relative to the alternative. The ratio of open to total matters more than the absolute number: 50,000 open invoices is 0.05% of a 100-million-row table -- a fraction of a full replace. The same 50,000 against a 60,000-row table is 83% -- at that point, a full replace is simpler.

In systems with long-lived open documents -- consulting invoices open for months, construction contracts open for years -- the open set grows and the cost advantage over a scoped full replace (0204) shrinks. Evaluate case by case.

// ---

== By Corridor
<by-corridor-5>
#ecl-warning("Transactional to columnar corridor")[Both queries run on the source as indexed scans (`status` should be indexed, or at least selective enough). The open set is small relative to the table, so the source cost is low. The destination load cost depends on the load strategy -- see 0403. The delete detection query runs entirely in the destination and is cheap (single-column scans).]

#ecl-info("Transactional to transactional corridor")[Cheap on both sides. The open-side extract is a small indexed scan. The delete detection diff can run as a single query joining source and destination if both are accessible from the same connection, or via staging tables if they're not.]

// ---

// ---

= Detail Without Timestamp
<detail-without-timestamp>
#quote(block: true)[
#strong[One-liner:] `order_lines` and `invoice_lines` have no `updated_at`. They depend on the header for change detection -- but what if the detail changes without the header changing?
]

See 0304 for the simpler case where the header cursor is sufficient. This pattern covers what happens when the detail mutates independently of the header.

// ---

== The Problem
<the-problem-5>
0304 extracts detail rows by joining to the header's `updated_at`, which only works when every detail change also touches the header. When it doesn't:

- `invoice_lines.status` changes from `approved` to `disputed` -- the header's `updated_at` never fires
- An admin script reprices 10,000 `order_lines` without touching the header
- A line gets hard-deleted and the header doesn't register the event (see 0306)

The header cursor is blind to all of these because the signal it depends on never fired.

// ---

== The Default: 0304
<the-default-0304>
When independent detail mutations are rare, the 0304 approach is still the right default -- just with the explicit acknowledgment that it only catches detail changes that coincide with header changes, and the periodic full replace catches the rest.

The strategies below apply when that blind spot is too wide.

// ---

== Strategy 1: Computed Column Signals
<strategy-1-computed-column-signals>
Some transactional systems maintain computed columns on the header that change when detail rows mutate -- `PaidToDate`, `DocTotal`, `GrossProfitPercent` in SAP B1, for example. These columns are recalculated by the engine whenever a line is added, removed, or modified, even if `updated_at` doesn't fire.

If such a column exists, use it as a change signal on the header: compare the current value against the last extracted value, and re-extract all detail lines for headers where it differs.

```sql
-- source: transactional
SELECT ol.*
FROM order_lines ol
WHERE ol.order_id IN (
  SELECT o.order_id
  FROM orders o
  WHERE o.doc_total != :last_known_doc_total
     OR o.updated_at >= :last_run
);
```

This turns a header-level computed column into an indirect change detection signal for the detail table, without hashing anything yourself. The limitation is that it only detects changes that affect the computed column -- a line status change that doesn't alter the total remains invisible.

#ecl-warning("Audit computed columns before trusting them")[Verify which detail-level changes actually trigger a recalculation. In SAP B1, `DocTotal` changes when quantities or prices change, but `PaidToDate` only changes on payment linkage. Match the column to the mutations you care about.]

// ---

== Strategy 2: Hash-Based Change Detection
<strategy-2-hash-based-change-detection>
Hash every detail row at the source, compare against stored hashes in the destination, and only extract rows where the hash differs.

```sql
-- source: transactional
SELECT ol.*,
       MD5(CONCAT(ol.order_id, ol.line_num, ol.quantity, ol.unit_price, ol.status)) AS _row_hash
FROM order_lines ol;
```

```sql
-- destination: columnar
-- Compare against stored hashes
SELECT s._row_hash, d._row_hash, s.order_id, s.line_num
FROM _stg_source_hashes s
LEFT JOIN order_lines d ON s.order_id = d.order_id AND s.line_num = d.line_num
WHERE s._row_hash != d._row_hash
   OR d._row_hash IS NULL;
```

This detects every change at the row level -- mutations, inserts, even columns that changed without the header knowing -- but requires extracting and hashing every row from the source on every run. For a detail table with millions of rows, that's a full scan just to compute hashes, and the extraction cost approaches a full replace.

Hash all columns -- the goal is to detect any change, and deciding which columns "matter" is a business decision that breaks the conforming boundary (0102). If a column changed at the source, the destination should reflect it.

See 0208 for the full hash-based pattern, including how to store and compare hashes efficiently.

// ---

== Strategy 3: Accept the Blind Spot
<strategy-3-accept-the-blind-spot>
Some detail changes are invisible to every cursor-based approach, and the periodic full replace from 0301 is the only thing that catches them. If the SLA tolerates the lag between the mutation and the next full replace, this is the cheapest approach -- and the one I use most often.

How often do independent detail mutations happen, and how long can the destination be wrong?

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Mutation frequency], [Full replace cadence], [Verdict],),
    table.hline(),
    [Rare (admin fixes, one-off corrections)], [Weekly], [Accept the blind spot],
    [Occasional (line-level status changes)], [Daily], [Probably fine -- evaluate per table],
    [Frequent (line repricing, bulk updates)], [Any], [Need Strategy 1 or 2],
  )]
  , kind: table
  )

This maps naturally to the tiered freshness model from 0608: the incremental layer handles what the cursor can see, and a slower full replace layer catches everything else -- including detail mutations the cursor missed.

// ---

== Independent Detail Mutations
<independent-detail-mutations>
`invoice_lines.status` can change independently of `invoices.status` -- a line marked `disputed` while the header is still `open`, or a line `approved` while other lines on the same invoice are not. In some systems this is a soft rule violation (0106), in others the detail lifecycle is independent by design. Either way, the extraction problem is the same: the header cursor doesn't see it.

Since the header cursor misses these changes entirely, two responses are worth considering:

#strong[Apply the open/closed split from 0307 independently to the detail table.] If `invoice_lines` has its own status field with a meaningful lifecycle (open/closed, active/disputed), treat the detail table as its own document with its own split. Re-extract all "open" lines (where `status` is still mutable), cursor-only for "closed" lines. This adds complexity but gives full coverage of detail-level mutations without hashing.

#strong[Accept the lag and let the full replace correct it.] If detail-level status changes don't affect downstream consumers until the invoice itself closes, the lag is invisible to the business.

// ---

== By Corridor
<by-corridor-6>
#ecl-info("Transactional to columnar corridor")[Hash-based detection requires landing hashes into a staging table in the destination for comparison -- the cost is a source-side full scan plus a staging load. If that cost approaches a full replace, the full replace is simpler. See 0403 for load cost.]

#ecl-warning("Transactional to transactional corridor")[Hash comparison can run as a cross-database query if both systems are accessible, or via staging tables. Strategy 1 (re-extract all details for changed headers) is the simplest default here -- see 0403 for the upsert mechanics.]

// ---

// ---

= Late-Arriving Data
<late-arriving-data>
#quote(block: true)[
#strong[One-liner:] A row's timestamp predates the extraction window. It was modified retroactively, arrived late from a batch job, or was inserted by a slow-committing transaction.
]

// ---

== The Problem
<the-problem-6>
A row lands in the source with an `updated_at` or `created_at` that's already behind your cursor or outside your window. The extraction ran at 10:00, picked up everything through 09:59, and advanced the cursor. At 10:05, a batch job inserts a row with `updated_at = 08:30`. That row is now permanently behind the cursor and will never be extracted.

This happens through more mechanisms than just "slow transactions":

- #strong[Retroactive corrections.] Support reopens a 3-day-old order and changes the shipping address. The `updated_at` fires with today's date -- fine, the cursor catches it. But in some systems, the correction sets `updated_at` to the original order date, not the correction date, meaning the row changes while the timestamp doesn't move forward.
- #strong[Batch imports.] An overnight job loads yesterday's POS transactions with `created_at = yesterday`. If your cursor already passed yesterday, those rows are invisible.
- #strong[ERP period closes.] Accounting closes March and runs adjustments. The adjustments land with dates in March, but the close happens in April. A daily cursor in April never looks back at March.
- #strong[Slow-committing transactions.] A long-running transaction inserts a row at 09:50 but doesn't commit until 10:10. The `updated_at` is 09:50 (when the INSERT happened), but the row wasn't visible until 10:10 (when the COMMIT happened). If the extraction ran at 10:00, it couldn't see the row -- and the cursor already advanced past 09:50.
- #strong[Async replication lag.] The source is a read replica that's 30 seconds behind the primary. Your extraction reads from the replica, but the cursor advances based on wall-clock time. Rows committed on the primary in those 30 seconds are invisible until the next run -- if the cursor has already moved past them.

In every case, the row's timestamp says it should have been extracted already, but it wasn't visible when the extraction ran.

// ---

== How Far Back Can It Land?
<how-far-back-can-it-land>
The overlap window must cover the worst-case late arrival, and that depends entirely on the source system's behavior:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto,auto,auto,),
    table.header([Source behavior], [Typical lag], [Example],),
    table.hline(),
    [Slow-committing transactions], [Seconds to minutes], [Long-running INSERT that commits after the extraction],
    [Async replication], [Seconds to minutes], [Read replica behind the primary],
    [Batch imports], [Hours], [Overnight POS load with yesterday's timestamps],
    [Retroactive corrections], [Days], [Support editing a week-old order],
    [ERP period closes], [Days to weeks], [Accounting adjustments backdated to the closed period],
    [Cross-system reconciliation], [Weeks], [Finance reconciling invoices from the previous month],
  )]
  , kind: table
  )

#ecl-warning("Measure late arrival, don't guess")[Query the source for rows where `updated_at` predates `created_at` or where `updated_at` is significantly older than the row's actual arrival. Transaction logs, audit tables, or a comparison between `updated_at` and `_extracted_at` over a few weeks will reveal the real distribution. Size the overlap to cover the 99th percentile, not the average.]

// ---

== Overlap Window Sizing
<overlap-window-sizing>
The overlap extends the extraction window backward from the cursor or window start:

```sql
-- source: transactional
-- Cursor-based with overlap
SELECT *
FROM orders
WHERE updated_at >= :last_run - INTERVAL '2 days';
```

```sql
-- source: transactional
-- Stateless window with built-in overlap
SELECT *
FROM orders
WHERE updated_at >= CURRENT_DATE - INTERVAL '9 days';
-- 7 days of intended window + 2 days of overlap
```

The overlap is a correctness parameter, not a performance parameter. Size it for the worst-case late arrival, then evaluate the cost. If the cost is too high, the answer is to shorten the run frequency (run less often, so the overlap is a smaller fraction of total work) or accept the blind spot and let the periodic full replace catch it.

The 0303 pattern has overlap built in by design -- a 7-day window already covers 7 days of late arrivals, with no overlap parameter to configure and no cursor to worry about. This is one of the strongest arguments for defaulting to stateless windows: the window size itself is the overlap, and the late-arriving data problem largely disappears. The only case it doesn't cover is rows that land with timestamps older than the window, which requires either a wider window or the periodic full replace. The 0302 pattern needs the overlap added explicitly to the boundary condition.

How large can a window get? I run a 90-day stateless window on a client's transactions because their back-office team routinely edits orders weeks after the fact, backdates corrections, and re-opens closed periods without notice. A 7-day window missed data constantly; 30 days still wasn't enough. At 90 days the source query is heavier, but the table is indexed on `updated_at` and the alternative -- constant reconciliation and manual fixes -- was more expensive in engineering time.

// ---

== Oracle EBS PRUNE\_DAYS
<oracle-ebs-prune_days>
Oracle BI Applications (OBIA) formalized this pattern as `PRUNE_DAYS` -- a configurable parameter that subtracts N days from the high-water mark on every extraction. The parameter exists because Oracle EBS has long-running concurrent programs (batch jobs) that can take hours to complete, inserting rows with timestamps from when the program started, not when it committed. The concept generalizes beyond Oracle: any system where the gap between "when the row's timestamp says it was created" and "when the row became visible" can be large needs an equivalent parameter.

// ---

== Cost of Overscanning
<cost-of-overscanning>
A wider overlap re-extracts more rows that haven't changed, increasing both source query cost and destination load cost (see @merge-upsert for the load side). The tradeoff is correctness vs.~cost, framed by @purity-vs-freshness: an hours-long overlap adds negligible cost, a days-long overlap is moderate depending on mutation rate, and a weeks-long overlap starts approaching a full replace -- at which point a scoped full replace (0204) may be simpler than a cursor with a massive overlap.

// ---

== Explaining This to Stakeholders
<explaining-this-to-stakeholders>
Late-arriving data is one of the hardest pipeline problems to explain to non-technical stakeholders because the failure is invisible: the data looks correct, the pipeline reports success, and the counts are close enough that nobody notices the missing rows until a reconciliation or audit.

#strong[What stakeholders need to understand:]

"When we extract data incrementally, we ask the source: 'give me everything that changed since the last time I asked.' But some changes arrive with timestamps in the past -- a correction from last week, a batch import with yesterday's dates, an adjustment from a period close. Our pipeline already asked for that time range and moved on. Those rows are invisible until the next full reload."

#strong[The three questions they'll ask:]

+ #strong["Can't you just get everything?"] Yes -- that's a full replace. It's the most correct approach but the slowest and most expensive. I do it periodically as a safety net. The incremental extraction runs between full replaces to keep the data fresh.

+ #strong["How much data are we missing?"] Depends on the table and the source system. For well-behaved transactional tables, almost nothing -- seconds of lag at most. For tables fed by batch jobs or ERP period closes, the gap can be days. I size the overlap window to cover the worst case I've measured, and the periodic full replace catches anything beyond that.

+ #strong["Why can't the data just be right?"] Because "right" has a cost. A 7-day overlap window on a table with 100 million rows re-extracts 7 days of data on every run to catch the rare late arrival. A 30-day overlap re-extracts 30 days. At some point, the cost of absolute correctness exceeds the cost of the occasional missing row. The overlap window is where I draw that line, and the full replace is the safety net behind it.

#ecl-tip("Frame it as a tradeoff")[Stakeholders respond better to \"we chose a 7-day safety margin that catches 99% of late arrivals, with a weekly full reload as a backstop\" than to \"our pipeline might miss some rows.\" Both are true, but the first version communicates a deliberate engineering decision. See @sla-management for how to formalize these guarantees into measurable SLAs.]

// ---

== By Corridor
<by-corridor-7>
#ecl-warning("Transactional to columnar corridor")[The source-side extraction cost scales with the overlap (wider window = more rows scanned on an indexed `updated_at`). The destination-side cost depends on how many partitions the overlap touches -- see @merge-upsert and 0104 for partition rewrite behavior per engine.]

#ecl-info("Transactional to transactional corridor")[Both sides scale with batch size, not table size, so wider overlaps are cheap. A 7-day overlap on a table with 1,000 changes per day re-extracts \~7,000 rows per run -- negligible for a transactional upsert.]

// ---

// ---

= Create vs Update Separation
<create-vs-update-separation>
#quote(block: true)[
#strong[One-liner:] When the trigger fires on UPDATE only and INSERT rows have `updated_at = NULL`, you need two extraction paths.
]

// ---

== The Problem
<the-problem-7>
0301 documents the failure mode: a trigger maintains `updated_at` on UPDATE but not on INSERT, leaving new rows with `updated_at = NULL`. A cursor on `updated_at >= :last_run` catches every modification to existing rows while every new row is permanently invisible.

This happens in `orders` when the trigger was added after the table existed and only wired to the UPDATE event -- a common pattern in legacy systems where the trigger was built for auditing, not extraction. The result is two populations in the same table: rows that have been updated at least once (visible to the cursor) and rows that were inserted but never touched again (invisible).

```sql
-- source: transactional
SELECT order_id, created_at, updated_at
FROM orders
ORDER BY order_id DESC
LIMIT 5;
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto,auto,auto,),
    table.header([order\_id], [created\_at], [updated\_at],),
    table.hline(),
    [1005], [2026-03-14 09:30:00], [NULL],
    [1004], [2026-03-14 08:15:00], [NULL],
    [1003], [2026-03-13 16:00:00], [2026-03-14 10:00:00],
    [1002], [2026-03-12 11:00:00], [NULL],
    [1001], [2026-03-10 09:00:00], [2026-03-13 14:30:00],
  )]
  , kind: table
  )

Orders 1005, 1004, and 1002 were inserted but never updated -- `updated_at` is NULL and the cursor will never see them.

// ---

== Detection
Before building any workaround, confirm the problem exists:

```sql
-- source: transactional
SELECT
  COUNT(*) AS total_rows,
  COUNT(updated_at) AS rows_with_updated_at,
  COUNT(*) - COUNT(updated_at) AS rows_without_updated_at
FROM orders;
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto,auto,auto,),
    table.header([total\_rows], [rows\_with\_updated\_at], [rows\_without\_updated\_at],),
    table.hline(),
    [84,230], [61,507], [22,723],
  )]
  , kind: table
  )

If `rows_without_updated_at` is significant, the trigger is UPDATE-only. A second check confirms the pattern -- recently created rows should have NULLs:

```sql
-- source: transactional
SELECT COUNT(*) AS recent_nulls
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
  AND updated_at IS NULL;
```

A high count here means the problem is ongoing, not a historical artifact from before the trigger was added.

#ecl-warning("Check the trigger definition directly")[In PostgreSQL, `\dS orders` or `SELECT \* FROM information_schema.triggers WHERE event_object_table = 'orders'` shows exactly which events fire the trigger. In MySQL, `SHOW TRIGGERS LIKE 'orders'` does the same. Take some time to check before debugging.]

// ---

== Strategy 1: COALESCE
<strategy-1-coalesce>
The simplest single-query approach -- fall back to `created_at` when `updated_at` is NULL:

```sql
-- source: transactional
SELECT *
FROM orders
WHERE COALESCE(updated_at, created_at) >= :last_run;
```

This works when `created_at` is reliably populated on INSERT (it usually is -- application frameworks and ORMs set it by default). The query captures both populations: updated rows through `updated_at`, and never-updated rows through `created_at`.

This works when `created_at` is reliably populated on INSERT -- application frameworks and ORMs set it by default. The query captures both populations: updated rows through `updated_at`, and never-updated rows through `created_at`.

#strong[Index usage.] `COALESCE(updated_at, created_at)` wraps the columns in a function, which prevents the optimizer from using indexes on either column directly. PostgreSQL supports a functional index on `COALESCE(updated_at, created_at)` that resolves this -- create it if the table is large enough that the full scan matters. MySQL and SQL Server don't support functional indexes in the same way, so the query planner may fall back to a full scan.

#ecl-warning("COALESCE fails when both columns are NULL")[If the table has rows where both `updated_at` and `created_at` are NULL, `COALESCE` returns NULL and those rows vanish from every cursor-based extraction. Check `SELECT COUNT(\*) FROM orders WHERE updated_at IS NULL AND created_at IS NULL` before relying on this approach.]

// ---

== Strategy 2: Dual Cursor
<strategy-2-dual-cursor>
Two separate queries, each optimized for its own population:

#strong[Inserts] -- cursor on `created_at` (or `id > :last_id` if `created_at` is unavailable):

```sql
-- source: transactional
SELECT *
FROM orders
WHERE created_at >= :last_run_created;
```

#strong[Updates] -- cursor on `updated_at`:

```sql
-- source: transactional
SELECT *
FROM orders
WHERE updated_at >= :last_run_updated;
```

UNION the results and load as one batch. Each query uses its own index cleanly -- `created_at` for the insert cursor, `updated_at` for the update cursor -- with no function wrapping and no optimizer guesswork.

The alternative of combining them into a single `WHERE updated_at >= :last_run OR created_at >= :last_run` looks simpler but behaves worse: the OR forces the optimizer to choose between a full scan and a bitmap OR of two index scans, and the plan it picks varies by engine, table size, and statistics freshness. Two queries with a UNION is predictable across engines.

#strong[Cursor management.] Two cursors means two pieces of state to track and advance. If your orchestrator supports per-table metadata, storing both is straightforward. Otherwise, a dedicated state table works:

```sql
-- destination: transactional (state table)
SELECT last_run_updated, last_run_created
FROM _pipeline_state
WHERE table_name = 'orders';
```

#strong[Overlap between the two sets.] A row inserted at 09:00 and updated at 10:30 appears in both queries if `last_run_created` is before 09:00 and `last_run_updated` is before 10:30. The upsert in the destination handles the duplicate -- the second version (the update) overwrites the first (the insert), which is the correct outcome.

#ecl-warning("Fallback insert cursor without created_at")[When `created_at` doesn't exist, use `id > :last_id` for the insert cursor. This is 0305 applied to half the table. The same gap safety rules apply -- sequences with CACHE can produce out-of-order IDs, and a small overlap buffer absorbs them.]

// ---

== Strategy 3: Fix the Source
<strategy-3-fix-the-source>
Add an `AFTER INSERT` trigger that populates `updated_at` with the current timestamp on every INSERT:

```sql
-- source: transactional (PostgreSQL)
CREATE OR REPLACE TRIGGER set_updated_at_on_insert
BEFORE INSERT ON orders
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
```

Then backfill existing NULLs:

```sql
-- source: transactional
UPDATE orders
SET updated_at = created_at
WHERE updated_at IS NULL;
```

After the backfill and trigger are in place, the standard 0302 cursor works for both inserts and updates -- no dual cursor, no COALESCE, no workarounds.

This is the cleanest outcome but requires three things: access to the source database, cooperation from the source team, and confidence that the trigger won't interfere with existing application logic. In practice, adding a trigger to a production table owned by another team is a conversation that can take weeks or never happen. Strategies 1 and 2 exist because Strategy 3 often isn't available.

#ecl-warning("Batch the backfill carefully")[`UPDATE orders SET updated_at = created_at WHERE updated_at IS NULL` on a 50M-row table with 20M NULLs is a heavy write. Run it in batches during off-hours and coordinate with the source team so their monitoring doesn't flag the spike as an incident. The trigger should go live before the backfill starts -- otherwise, rows inserted between the backfill and trigger activation will still have NULLs.]

// ---

== Choosing a Strategy
<choosing-a-strategy>
#figure(
  align(center)[#table(
    columns: (50%, 50%),
    align: (auto,auto,),
    table.header([Situation], [Strategy],),
    table.hline(),
    [`created_at` is reliable, table is small-to-medium], [COALESCE -- simplest, one query, one cursor],
    [`created_at` is reliable, table is large, no functional index], [Dual cursor -- each query hits its own index cleanly],
    [`created_at` is NULL or unreliable], [Dual cursor with `id > :last_id` for the insert side],
    [You have source access and team cooperation], [Fix the source -- eliminates the problem permanently],
  )]
  , kind: table
  )

In all cases, the periodic full replace from 0301 catches anything the workaround misses -- rows where both timestamps are NULL, bulk imports that bypassed both triggers, sequences that created gaps the insert cursor didn't cover.

// ---

== By Corridor
<by-corridor-8>
#ecl-warning("Transactional to columnar corridor")[The dual cursor produces two result sets that get UNIONed before loading. The duplicate rows from the overlap between insert and update cursors are handled by the destination's MERGE -- see @merge-upsert. The COALESCE approach benefits from a functional index on the source side; without one, the extraction query is a full scan on every run.]

#ecl-info("Transactional to transactional corridor")[Both cursors should be cheap indexed range scans on the source. The destination upsert (`ON CONFLICT ... DO UPDATE`) absorbs overlap duplicates naturally -- see @merge-upsert.]

// ---

// ---
