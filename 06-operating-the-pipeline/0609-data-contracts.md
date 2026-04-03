---
title: "Data Contracts"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-06
updated: 2026-03-28
---

# Data Contracts

> **One-liner:** Schema drift, row counts, null rates, freshness -- what to enforce at the boundary between source and destination.

## The Problem

Source schemas change without notice. A column gets renamed, a type changes from INT to VARCHAR, a new column appears when someone activates an ERP module, an old one disappears after a migration. The source team doesn't know your pipeline exists -- they won't tell you before they deploy a schema migration, and they shouldn't have to. The boundary between their system and yours is your responsibility to defend.

Without a contract, drift propagates silently into the destination -- a dropped column becomes NULLs in downstream queries, a type change produces casting errors that surface three layers deep in a dashboard nobody connects back to the source, and a 90% row count drop looks like a quiet day until someone notices the month-end report is missing most of its data. By then the blast radius is wide and the root cause is buried. A data contract makes these boundaries explicit and checkable.

## What a Data Contract Covers

### Schema Contract

The schema contract defines the expected column names and types -- the fingerprint from [[06-operating-the-pipeline/0601-monitoring-observability|0601]]. It answers three questions when the schema changes:

**New columns** -- accept or reject? The policy is either evolve (add the column to the destination) or freeze (fail the load). Evolve is the right default for almost every table. Source schemas grow -- ERPs add columns when modules are activated, applications add fields as features ship. Freezing a schema that legitimately evolves means a manual intervention every time the source team deploys, which is maintenance you don't want and they won't coordinate with you on. Evolve means one less thing to manage, and downstream consumers shouldn't be doing `SELECT *` against your destination anyway -- an added column doesn't break anything for them unless they wrote their queries wrong.

**Dropped columns** -- no decision gets made without the source system. A column disappearing could be a deliberate removal, a migration gone wrong, or a temporary rollback. Set up tolerances: if the column was created yesterday and disappeared today, it was probably a rollback and you can let it go. If a column that's been there for months vanishes, fail the load and investigate. The tolerance depends on how the downstream uses the column -- a critical join key disappearing is different from an unused description field being cleaned up.

**Type changes** -- fail, cast, or warn. See [[#Type Mapping]] below for how to handle the mapping itself.

### Volume Contract

The volume contract defines the expected row count range per extraction, derived from recent history. A table that normally extracts 450k rows and today extracts 12k likely has a problem -- even if the pipeline reports SUCCESS. The contract surfaces this before the data reaches consumers.

The threshold should come from observed baselines, not assumptions. A simple approach: track the rolling average and standard deviation of row counts over the last 30 runs, and alert when the current run falls outside 2-3 standard deviations. For tables with predictable seasonality (month-end spikes on `invoices`, weekend dips on `orders`), factor the day-of-week or day-of-month into the baseline.

This feeds directly into [[06-operating-the-pipeline/0610-extraction-status-gates|0610]] for inline enforcement -- block the load when the volume looks wrong, rather than discovering the problem downstream.


### Null Contract

The null contract defines expected null rates on key columns. A cursor column like `updated_at` should never be NULL -- if it is, your incremental extraction is blind to those rows. A description column being 40% NULL is probably normal. The contract distinguishes between the two.

The purpose is to protect your pipeline's ability to do its job. A null rate spike on `updated_at` disrupts your extraction; a null rate spike on `customer_name` is the source's problem and downstream's concern. Anything that disrupts your ability to extract and load accurately is alertable. Everything else passes through as-is.

### Freshness Contract

The freshness contract is the SLA from [[06-operating-the-pipeline/0604-sla-management|0604]] expressed as a checkable rule: maximum acceptable staleness per table, measured from the health table's last successful load timestamp. This is the simplest contract to define and the most visible when violated -- a stale table is the one that generates the "why hasn't the dashboard updated" email.

## Enforcement Points

### Pre-Load (Gate)

Check schema, row count, and null rates after extraction but before loading. If the contract is violated, block the load and alert ([[06-operating-the-pipeline/0605-alerting-and-notifications|0605]]). This is the extraction status gate from [[06-operating-the-pipeline/0610-extraction-status-gates|0610]] extended with richer checks.

Pre-load gates are the strongest enforcement point because they prevent bad data from reaching the destination. The cost is that a false positive blocks a load that was actually fine -- which is why baselining matters. A gate based on assumptions ("this column should never be NULL") fires on the first run and trains you to ignore it.

### Post-Load (Validation)

Run checks after the load completes: destination row count vs source, schema matches expected, null rates within bounds. Your orchestrator's post-load check primitives are built for this -- ideally run them as part of the load job so the check and the data it validates stay in sync. At scale, though, the overhead of inline checks on every table may not fit in the schedule window (see [[#The Cost of Checking]]), and running validation on a separate, less frequent cadence becomes the practical tradeoff: you lose immediate detection but keep the pipeline on time.

Post-load validation catches problems that pre-load gates can't see: rows that were lost during the load itself, type coercions that silently truncated values, partition misalignment that put data in the wrong place. The tradeoff is that by the time you detect the problem, the bad data is already in the destination -- you're limiting blast radius rather than preventing damage.

### Continuous (Monitoring)

Schema fingerprint comparison on every run, volume trend tracking over time. This feeds the observability layer from [[06-operating-the-pipeline/0601-monitoring-observability|0601]] and catches slow drift that no single-run check would flag: a table whose row count grows 2% less than expected every week, a column whose null rate creeps from 0.1% to 5% over a quarter.

### The Cost of Checking

Every contract check adds overhead to every run. A schema fingerprint comparison, a row count validation, a null rate scan -- each one might take 10 or 15 seconds on its own, barely noticeable on a single table. Multiply that by 1,000 tables and you've added over 4 hours of load time to your pipeline. The contracts that felt free at 20 tables become a bottleneck at scale.

Contract coverage is a budgeting decision. Not every table needs every check. A critical `orders` table might deserve schema + volume + null rate validation on every run. A 200-row lookup table probably doesn't need anything beyond the run health your orchestrator already provides. Allocate checks where the blast radius of a silent failure justifies the overhead, and leave the rest to the monitoring layer where the cost is amortized across a dashboard glance, not multiplied across every load.

## Schema Evolution Policies

| Policy | Behavior | When to use |
|---|---|---|
| **Evolve** | Accept new columns, add them to destination | Default for most tables -- source schemas grow |
| **Freeze** | Reject any schema change, fail the load | Critical tables where downstream depends on exact schema |

These are the only two valid policies in an ECL context. Some loaders offer `discard_row` and `discard_value` modes that silently drop data when the schema doesn't match -- these are transformation decisions, not conforming ones. If the source sent it, the destination should have it. Either accept the change or reject the load; don't silently drop data. See [[04-load-strategies/0403-merge-upsert|0403]] for the full reasoning.

## Column Naming as a Contract

Your column naming convention -- whether you preserve source names verbatim or normalize to `snake_case` -- is itself a schema contract, and one of the hardest to change after the fact. Changing the convention on a running pipeline means reloading every table and updating every downstream query that references the old names -- a full migration.

The problem gets sharper when you're running multiple pipelines or migrating between systems. A pipeline that loads with source-native names (`@ORDER_VIEW`, `CustomerID`, `línea_factura`) and another that normalizes to `snake_case` produce incompatible destinations. If you plan on running meta-pipelines that handle hundreds of sources, document exactly how you normalize column names and make the convention configurable at two levels: per destination (because consumers expect consistency within the dataset they're querying) and per table (because migrating a source sometimes means fixing individual tables that arrived with a different convention).

This also means you need a documented answer for the edge cases: how do you handle a column named `@ORDER_VIEW` with emoji? A column with spaces? A reserved word? These aren't hypothetical -- ERP systems and legacy databases produce all of them. Your naming contract should handle the full range, not just the clean cases.

## Type Mapping

Type mismatches between source and destination are universal and varied enough that hand-coding each one is a losing strategy. The corridor determines the severity: transactional-to-transactional pairs usually have close type mappings, while transactional-to-columnar pairs (SQL Server to BigQuery, SAP HANA to Snowflake) produce a steady stream of precision loss, overflow risk, and silent truncation.

Numeric precision is the most dangerous category. SQL Server's `DECIMAL(38,12)` mapped to BigQuery's `NUMERIC(29,9)` silently loses precision on values that fit the source but overflow the destination. Financial data with high-precision decimals is exactly the data where this matters most and where the bug is hardest to catch -- the numbers look reasonable until someone reconciles and finds a two-cent discrepancy across a million rows.

The practical approach is to rely on a type-mapping library (SQLAlchemy, your loader's built-in adapters) and override only when you know a specific mapping is wrong for your data. Don't spend time building a comprehensive type-mapping system from scratch -- the libraries have already solved the common cases, and the edge cases are specific enough that a generic solution wouldn't help.

> [!warning] Unusual source-destination pairs
> If you're extracting from a source where no well-tested adapter exists -- a niche ERP, a legacy database with non-standard types, a SaaS API that returns ambiguous JSON types -- you may have no alternative to manual type mapping. Document every mapping decision, test with real data (not just the schema), and watch for silent truncation on the first few runs.

## Anti-Patterns

> [!danger] Don't enforce contracts you haven't baselined
> A contract based on assumptions ("this column should never be NULL") will fire false positives on the first run. Baseline the actual data first: run a profiling pass, measure real null rates and row counts, then set thresholds from observed behavior. A contract that cries wolf on day one trains everyone to ignore it by day three.

> [!danger] Don't freeze schemas on tables that legitimately evolve
> `products` gains a new attribute column every quarter. Freezing its schema means a load failure every quarter and a manual intervention to update the contract. Use evolve for tables with expected growth; freeze only for tables with stable, critical schemas where a column change would genuinely break something important downstream.

> [!danger] Don't discard columns that don't match your schema
> Silently dropping new or unexpected columns breaks the conforming boundary. Wide ERP tables with hundreds of columns are tempting candidates for discard, but the right answer is evolve (accept the column) or [[02-full-replace-patterns/0209-partial-column-loading|0209]] (explicitly declare which columns you extract and document why). Discarding is implicit partial column loading with no documentation -- the worst version of both.

## Tradeoffs

| Pro | Con |
|---|---|
| Schema drift caught before it reaches consumers | Every check adds per-table overhead that compounds at scale |
| Volume anomalies surfaced immediately, not days later | False positives on poorly baselined contracts erode trust |
| Explicit evolve/freeze policy eliminates ambiguity on schema changes | Evolve means downstream must handle new columns; freeze means manual intervention on legitimate changes |
| Type mapping libraries handle the common cases transparently | Edge cases on unusual source-destination pairs still require manual work |

## Related Patterns

- [[06-operating-the-pipeline/0601-monitoring-observability|0601-monitoring-observability]] -- schema fingerprinting and null rate tracking feed contracts
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- contract violations trigger alerts
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610-extraction-status-gates]] -- pre-load gate is the inline enforcement mechanism
- [[06-operating-the-pipeline/0604-sla-management|0604-sla-management]] -- freshness contract is the SLA expressed as a checkable rule
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- volume contract enforcement post-load
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- schema evolution policy reasoning (why discard modes break conforming)
- [[02-full-replace-patterns/0209-partial-column-loading|0209-partial-column-loading]] -- the explicit alternative to silent column discarding
