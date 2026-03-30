---
title: "Extraction Status Gates"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: draft
created: 2026-03-14
updated: 2026-03-28
---

# Extraction Status Gates

> **One-liner:** 0 rows returned successfully is not the same as a silent failure. Gate the load on extraction status before advancing the cursor.

## The Problem

An extraction that returns 0 rows and reports SUCCESS could mean two things: the table genuinely had no changes since the last run, or the source was down, the query timed out silently, or the connection returned an empty result set instead of an error. Without a gate, these two scenarios are indistinguishable -- and the pipeline treats them identically, loading nothing and advancing the cursor past data it never read. For incremental tables, that gap is permanent. For full replace tables, it's worse: the destination gets truncated and replaced with nothing.

This happens more often with APIs than with direct SQL connections, but SQL sources aren't immune. We had a client whose upstream team gave us a "database clone" that periodically truncated its tables before reloading them. If our extraction hit the window between truncate and reload, we'd read 0 rows from a table that should have had hundreds of thousands -- and our full replace would dutifully wipe the destination clean. It happened more than once before we gated it.

## Gate Mechanics

The gate sits between extraction and load. After the extraction query returns, before any data reaches the destination, evaluate whether the result is plausible. The evaluation is per-table -- if 3 out of 200 tables return suspect results, block those 3 and let the other 197 proceed. Gating per-run (blocking everything because one table looks wrong) risks your SLA on every other table, and if the blocked table is a heavy one that can only run overnight, you've lost an entire day of data for tables that were fine.

### What Triggers the Gate

**Zero rows from a table that normally returns data.** The most common trigger. A table that extracted 450k rows yesterday and 0 today deserves scrutiny. A table that routinely returns 0 rows on weekends does not -- the gate needs to know the difference.

**Row count outside the expected range.** Full replace tables should stay within a percentage of their previous row count. A `customers` table that had 50k rows yesterday and has 50,200 today is normal growth; the same table at 5k rows means something upstream went wrong. The threshold depends on the table's volatility -- a `pending_payments` table can legitimately drop by 80% when a batch of payments clears, while `products` is very unlikely to lose half its rows overnight (Also, have they heard of soft deletes?).

**Extraction metadata anomalies.** Query duration of 0ms on a table that normally takes 30 seconds, or bytes transferred far below the expected range. These can signal a connection that returned immediately without actually querying.

### What the Gate Does

When the gate fires:

1. **Blocks the load** -- the extracted data (or lack of it) does not reach the destination. For full replace tables, the destination retains its current data untouched. **For incremental tables, the decision is less clear-cut** -- you may still be getting *some* new data, and a partial update is better than no update at all. Whether to block or load what you got is a case-by-case call based on how wrong the row count looks and how much damage a partial load would cause downstream.
2. **Triggers an alert** ([[06-operating-the-pipeline/0605-alerting-and-notifications|0605]]) with the extraction metadata: expected row count, actual row count, query duration, and which table.
3. **Logs the event** in the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) so the pattern is visible over time -- a table that gates every Monday morning points to a weekend maintenance window nobody told you about.

### Cursor Safety and Stateless Windows

If you're using stateless window extraction ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]), cursor advancement is already a non-issue -- the next run re-reads the same window regardless. The gate still matters for preventing a bad load, but the recovery is automatic: you have the width of your lag window for the upstream problem to be resolved before data actually falls out of scope. The alert fires on day one; upstream has until the lag window closes to fix it.

For cursor-based extraction, a stuck cursor can become a problem if the window between the cursor and "now" grows large enough that re-extraction becomes expensive. A wide enough lag window ([[06-operating-the-pipeline/0608-tiered-freshness|0608]]) mitigates this -- the warm tier's daily pass catches what the hot tier missed, and the cold tier's full replace resets everything. Stateless windows avoid this problem entirely, which is one more reason they've become my preferred approach for most incremental extraction.

## Full Replace Gates

The stakes for full replace tables are higher than for incremental. An incremental extraction that reads 0 rows leaves a gap in the destination; a full replace that reads 0 rows *empties the destination*. The extraction returned nothing, the pipeline replaced the table with nothing, and now consumers are querying an empty table that had 50k rows an hour ago.

Full replace gates check that the extracted row count is within an expected percentage of the previous load's row count. The percentage depends on the table: a `products` dimension that grows by 1% per month should gate on anything below 90-95% of the last load. A `pending_payments` table that legitimately fluctuates as payments clear needs a wider band. The very few tables that can legitimately approach zero (cleared queues, seasonal staging tables) should be explicitly exempted with documentation explaining why -- otherwise the next engineer on call will second-guess the exemption and re-enable the gate.

## Baselines

The gate's accuracy depends entirely on knowing what "normal" looks like for each table. The baseline is a range, not a point -- flag when outside the range, not when different from last run.

A rolling window of the last 30 runs gives you a reasonable baseline for most tables. Track the min, max, and average row count per table, and gate when the current extraction falls below the historical minimum by a configurable margin. For tables with predictable seasonality -- month-end spikes on `invoices`, weekend dips on `orders` -- factor the day-of-week or day-of-month into the baseline so the gate doesn't fire every Saturday.

> [!tip] Start very loose, tighten over time
> A gate that's too tight fires false positives and trains you to ignore it -- the exact same failure mode as over-alerting ([[06-operating-the-pipeline/0605-alerting-and-notifications|0605]]). Start with a generous threshold (block only on 0 rows or >90% drop), observe for a month, then tighten based on the table's actual variance.

## Validating Against Source

When the gate fires, the first question is whether the source actually has the data you expected. A `COUNT(*)` against the source during business hours confirms whether the extraction was wrong (source has data, extraction missed it) or the source is genuinely empty (upstream problem). This validation is manual and delayed -- the gate fires at 3 AM, someone investigates at 9 AM, and the destination sits stale in the meantime. The SLA clock runs during that gap.

If the source confirms the data is there, the extraction failed silently -- re-run it. The truncate-then-reload pattern (source temporarily empty as part of its own load cycle) is a common culprit, and the `COUNT(*)` during business hours distinguishes it from a genuine problem.

If the source is genuinely empty, you have a harder decision with no universal answer:

**Hold the gate** -- the destination keeps its previous data, stale but complete. Consumers see yesterday's numbers, which are wrong but usable. The cost is that you become a silent buffer for upstream's problem: nobody feels the pain, nobody escalates, and the issue can persist for days before anyone outside your team notices.

**Load what you got** -- the destination reflects reality, empty or broken as it is. Consumers see the damage immediately, which hurts but also makes the problem visible to the people who can fix it. A downstream report showing zero revenue generates an escalation in hours; a stale report showing yesterday's revenue generates nothing.

Neither option is always right. Full replace tables almost always deserve a hold -- the destination wipeout is too destructive to let through. Incremental tables with partial data lean toward loading what you got, since some fresh data is better than none and the gap is bounded. For everything in between, the decision depends on the table, the consumer, and how much pain you're willing to absorb on upstream's behalf.

Whichever you choose, make the decision explicit: log it in the health table, include it in the alert, and document the policy per table. A gate that silently holds data without anyone knowing it held is a judgment call that nobody can audit -- and the next engineer on call will make a different judgment if they don't know yours.
## Tradeoffs

| Pro                                                                               | Con                                                                                         |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Prevents silent data loss from empty or truncated extractions                     | Adds per-table overhead (baseline tracking, threshold evaluation)                           |
| Per-table gating protects SLA on unaffected tables                                | Threshold tuning is empirical -- too tight fires constantly, too loose misses real failures |
| Cursor stays safe on incremental tables, destination stays intact on full replace | Stateless windows already mitigate cursor risk, reducing the gate's incremental value       |
| Gated events logged in health table surface recurring upstream patterns           | Volatile tables (cleared queues, seasonal) need explicit exemptions                         |

## Anti-Patterns

> [!danger] Don't gate per-run when you can gate per-table
> Blocking 200 tables because 1 returned 0 rows means your entire pipeline misses its SLA. Gate individually. If your orchestrator doesn't support per-asset gating, this is worth building -- the alternative is choosing between no gate and an all-or-nothing gate that's too disruptive to enable.

> [!danger] Don't gate without a baseline
> A gate that fires on "fewer rows than I expected" without historical data to define "expected" is a guess. Run the pipeline ungated for 30 days, collect baselines, then enable the gate.

## Related Patterns

- [[06-operating-the-pipeline/0609-data-contracts|0609-data-contracts]] -- defines enforcement points (when to check); this pattern defines gate mechanics (what to check)
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- the gate triggers alerts
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- count reconciliation is a post-load version of the same idea
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- cursor advancement gated on confirmed load success
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- stateless windows reduce cursor risk but still benefit from load gating
