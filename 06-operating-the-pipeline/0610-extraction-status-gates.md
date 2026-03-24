---
title: "Extraction Status Gates"
aliases: []
tags:
  - pattern/operational
  - chapter/part-6
status: outline
created: 2026-03-14
updated: 2026-03-14
---

# Extraction Status Gates

> **One-liner:** 0 rows returned successfully is not the same as a silent failure. Gate the load on extraction status before advancing the cursor.

## The Problem
- Extraction returns 0 rows and reports success -- but the source was down, the query timed out silently, or the connection returned an empty result set instead of an error
- The pipeline loads nothing, advances the cursor, and the gap is permanent
- Without a gate, "successful empty extraction" and "failed extraction that looked empty" are indistinguishable

## The Gate
- After extraction, before load: evaluate whether the result is plausible
- 0 rows from a table that normally returns thousands → suspect, gate the load
- 0 rows from a table that's often quiet → normal, proceed

## Baseline Expectations
- Per-table expected row count range (min/max from recent history)
- Tables with known zero-row periods (weekends, holidays, off-hours) need adjusted baselines
- The baseline is a range, not a point -- flag when outside the range, not when different from last run

## What the Gate Does
- Blocks cursor advancement (the cursor stays where it was -- the next run re-extracts the same window)
- Triggers an alert (see [[06-operating-the-pipeline/0605-alerting-and-notifications|0605]])
- Logs the extraction metadata for investigation

## False Positives
- Table legitimately had 0 changes since last run (common for low-activity tables)
- Holiday/weekend: no transactions, 0 rows is correct
- Mitigate with per-table sensitivity and time-aware baselines

## Related Patterns
- [[06-operating-the-pipeline/0605-alerting-and-notifications|0605-alerting-and-notifications]] -- the gate triggers alerts
- [[06-operating-the-pipeline/0614-reconciliation-patterns|0614-reconciliation-patterns]] -- count reconciliation is a post-load version of the same idea
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- cursor advancement gated on confirmed load success
- [[03-incremental-patterns/0302-cursor-based-extraction|0302-cursor-based-extraction]] -- the cursor that must not advance on a suspect extraction
