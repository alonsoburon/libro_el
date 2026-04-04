#import "theme.typ": ecl-danger, ecl-info, ecl-tip, ecl-warning, palette
#let p = palette

// ============================================================
// TITLE PAGE
// ============================================================
#page(header: none, footer: none)[
  #v(1fr)
  #align(center)[
    #text(fill: p.fg-bright, size: 36pt, weight: "bold")[Battle-Tested Data Pipelines]
    #v(8pt)
    #text(fill: p.fg-dim, size: 14pt)[The step ELT forgot -- patterns for extraction, conforming, and loading]
    #v(40pt)
    #text(fill: p.fg-subtle, size: 14pt)[Alonso Burón]
  ]
  #v(1fr)
]

// ============================================================
// COPYRIGHT PAGE
// ============================================================
#page(header: none, footer: none)[
  #v(1fr)
  #set text(size: 9pt, fill: p.fg-dim)
  *Battle-Tested Data Pipelines*\
  The step ELT forgot -- patterns for extraction, conforming, and loading

  Copyright © 2026 Alonso Burón. All rights reserved.

  No part of this publication may be reproduced, distributed, or transmitted in any form or by any means without the prior written permission of the author.

  // ISBN placeholder
  // ISBN: 978-X-XXXX-XXXX-X

  First edition, 2026.

  Typeset with Typst. Diagrams by the author.
]

// ============================================================
// DEDICATION
// ============================================================
#page(header: none, footer: none)[
  #v(2fr)
  #align(right)[
    #text(fill: p.fg-quote, size: 12pt, style: "italic")[
      To my lovely feyoncé, whose patience is beaten by no saint, and whose light willed this book into existence.
    ]
  ]
  #v(3fr)
]

// ============================================================
// TABLE OF CONTENTS
// ============================================================
#page(header: none, footer: none)[
  #outline(indent: auto, depth: 2)
]

// ============================================================
// WHAT THIS BOOK COVERS (context first, then motivation)
// ============================================================
== What This Book Covers
<what-this-book-covers>
This book is about the space between source and destination -- the step that ELT skips over and ETL buries inside a monolith. Specifically:

#figure(
  align(center)[#table(
    columns: (54.39%, 45.61%),
    align: (auto, auto),
    table.header([What we cover], [What we don't]),
    table.hline(),
    [Extracting data from transactional systems], [Building dashboards or reports],
    [Conforming types, nulls, timezones, encodings], [Business logic / KPI definitions],
    [Loading into columnar or transactional destinations], [Silver/gold layer transformations],
    [Incremental strategies, full replace, and the huge gray middle],
    [Orchestrator-specific tutorials (Airflow, dbt, etc.)],
    [Failure recovery, idempotency, reconciliation], [Data modeling / star schemas],
    [Protecting destination costs from bad queries], [ML pipelines],
    [Batch extraction patterns], [CDC / real-time streaming / event-driven],
  )],
  kind: table,
)

#ecl-tip(
  "Two corridors, different tradeoffs",
)[Every pattern in this book plays out differently depending on where the data is going. I call these #strong[corridors]: Transactional -> Columnar (e.g. SQL Server -> BigQuery) and Transactional -> Transactional (e.g. PostgreSQL -> PostgreSQL). Same pattern, different trade-offs. I show both.]

#ecl-info(
  "Tool-agnostic patterns, opinionated appendix",
)[The patterns in this book use generic orchestrator language -- "your orchestrator," "a scheduled job," "a downstream dependency" -- because they work regardless of whether you run Dagster, Airflow, Prefect, or cron. The same applies to extractors, loaders, and destination engines. Specific tool recommendations, feature comparisons, and my opinionated picks live in the Appendix (0805--0807).]

= Why This Book
<why-this-book>

One Tuesday morning I woke up to a wall of email alerts -- row count mismatches across dozens of pipelines. I spent the rest of the day fixing them by hand: re-running extractions, reconciling counts against source, patching cursors that had drifted overnight. By the time I was done, I sat back and realized two things. First, the system I had built was complex enough that in a couple of years I wouldn't be able to recreate it without having it written down somewhere. Second, when I went looking for references to categorize what I had built into an existing strategy, I couldn't find any. There were no strategy books for what I was doing.

So I started writing down the patterns I used. Just for future reference, at first. But as I organized them, they split naturally into extraction patterns and loading patterns -- and then I found myself with a collection of small but critical transformations that didn't belong in either category. Type casts, null handling, timezone tagging, key synthesis. They weren't business logic. They weren't the T in ELT. But they were unavoidable, and nobody had a name for them. That was the moment ECL took shape: Extract, Conform, Load. The C names the work that every pipeline does but no framework acknowledges.

In my job, the T belonged to the analysts downstream. My responsibility was to deliver the data exactly as it was in the source, but in a place where they could actually reach it. Conforming was the bridge -- everything the data needed to survive the crossing without changing what it meant.

You might have seen the term EtLT -- Extract, "tiny-t" Load, Transform. It acknowledges that some work happens before the big T, but its focus is still on making downstream transformation easier. This book takes the opposite angle. The C in ECL is about getting massive, often dirty data to the destination as faithfully and efficiently as possible. If it changes business meaning, it doesn't belong here. If it makes the data land correctly, it does.

== Who This Is For

This book is for the engineer who inherited a pile of pipelines that work most of the time -- the ones with no monitoring, no validation, and a cloud bill that spikes because someone scheduled a full refresh every 30 minutes on a table that didn't need it.

It's for the first-time data engineer who's building their first pipeline and assumes everything should be incremental and it should just work. (It won't. This book explains why, and what to do about it.)

It's for the senior who has been doing this for years and needs a framework to teach it to their team, or to finally name the patterns they've been applying by instinct.

This is not a tutorial or a tool guide. This book won't set up your orchestrator from scratch. This is a pattern language -- the decisions, tradeoffs, and failure modes that repeat across every pipeline regardless of stack, and the ways to monitor them, surface them, and fix them.

// ---

= Domain Model
<domain-model>
Every SQL example in this book uses the same fictional schema. Same tables, same columns, same quirks -- so you can focus on the pattern, not on decoding a new schema every chapter.

The tables were chosen because each one represents a distinct extraction challenge. They're not arbitrary.

== Schema
<schema>
#align(center, image("diagrams/domain-model-er.svg", width: 95%))

The three standalone tables -- `events`, `sessions`, and `metrics_daily` -- have no foreign keys into the schema above. `inventory` and `inventory_movements` connect to `products` via `sku_id` but have no `warehouses` table -- `warehouse_id` is a plain integer key. They represent different source archetypes.

== The Tables and Why They're Here
<the-tables-and-why-theyre-here>
#figure(
  align(center)[#table(
    columns: (20%, 80%),
    align: (auto, auto),
    table.header([Table], [ECL challenge]),
    table.hline(),
    [`orders`],
    [Has `updated_at` but it's unreliable -- trigger only fires on UPDATE, not INSERT. The canonical example of a broken cursor.],
    [`order_lines`],
    [Detail table with no timestamp of its own. Must borrow the header's cursor for incremental extraction.],
    [`customers`],
    [Soft-delete via `is_active`. The flag works for normal application flows; back-office scripts bypass it.],
    [`products`],
    [Schema mutates -- new columns appear after deploys, `category` became `product_category` once. The schema drift case.],
    [`invoices`], [Open/closed document pattern. Open invoices get hard-deleted regularly. The hard delete case.],
    [`invoice_lines`],
    [Has its own `status` per line, hard-deleted independently -- not just cascade from the header. Complicates both delete detection and cursor borrowing.],
    [`events`],
    [Append-only, partitioned by date. The simplest extraction pattern. Nothing is ever updated or deleted.],
    [`sessions`],
    [Sessionized clickstream. Late-arriving events mean sessions can close hours after they open, creating the late-arriving data problem.],
    [`metrics_daily`],
    [Pre-aggregated daily metrics, overwritten on recompute. Partition-level replace is the natural fit.],
    [`inventory`],
    [Sparse cross-product of SKU x Warehouse. Most rows are zeros. Filtering zeros loses information -- a zero row and a missing row look identical in the destination.],
    [`inventory_movements`],
    [Append-only log of all stock changes: sales, adjustments, transfers, write-offs. The activity signal for activity-driven extraction. Covers changes that never flow through `order_lines`.],
  )],
  kind: table,
)

== The Soft Rules Baked In
<the-soft-rules-baked-in>
The domain model is designed so that every "always true" business rule is, in fact, a soft rule. None of them have a constraint enforcing them:

- `orders` -- "Always has at least one line." Until a UI bug creates an empty order.
- `orders` -- "Status goes `pending` → `confirmed` → `shipped`." Until support resets one manually.
- `order_lines` -- "Quantities are always positive." Until a return is entered as `-1`.
- `invoices` -- "Only open invoices get deleted." Until a year-end cleanup script runs.
- `invoice_lines` -- "Line status always matches the header." Until one line is disputed.
- `customers` -- "Emails are unique." Until the same customer registers twice and nobody added the unique index.
- `inventory` -- "`on_hand` is always \>= 0." Until a write-off creates a negative balance.
- `inventory_movements` -- "Every stock change creates a movement." Until a bulk import script updates `inventory` directly without logging movements.

When a pattern depends on one of these holding -- or breaking -- it will say so explicitly. See @hard-rules-soft-rules.

// ---
