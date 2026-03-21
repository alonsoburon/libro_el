---
title: "Idempotency"
aliases: []
tags:
  - pattern/foundations
  - chapter/part-1
status: first_iteration
created: 2026-03-06
updated: 2026-03-14
---

# Idempotency

> **One-liner:** If rerunning the pipeline changes the destination, you have a bug.

---

## What It Means

An idempotent pipeline produces the same destination state whether it runs once, twice, or ten times with the same input. No extra rows, no missing rows, no side effects from the previous run bleeding into the next one. The destination after run N+1 is indistinguishable from the destination after run N -- assuming the source didn't change between them.

This sounds obvious until you realize how many pipelines fail it. An append without dedup doubles the data on retry. A cursor that advances before the load confirms creates a permanent gap. A staging table that doesn't get cleaned up causes the next run to load stale data on top of fresh data. Every one of these is a pipeline that works perfectly on the first run and breaks on the second.

---

## Why It's the Foundation

Every other reliability property -- retries, backfills, failure recovery, concurrent runs -- depends on idempotency. If your orchestrator retries a failed run, it needs to know that re-executing the pipeline won't corrupt the destination. If you backfill a date range, you need to know that reprocessing already-loaded data won't create duplicates. If two runs overlap because the first one hung, you need to know the destination survives both.

Without idempotency, retries are dangerous, backfills require manual cleanup, and every failure becomes a unique investigation. With it, the recovery playbook for every failure is the same: run it again.

---

## Full Replace Gets It for Free

A pipeline that drops and reloads the entire table on every run is idempotent by construction. There's no prior state to interfere with, no cursor to manage, no accumulated history that a retry could corrupt. Run it once, run it five times -- the destination is the same because every run rebuilds it from scratch.

This is the strongest argument for [[01-foundations-and-archetypes/0108-purity-vs-freshness|full replace as the default]]. You don't have to think about idempotency because the architecture hands it to you. The moment you move to incremental, you leave this safe zone and take on the burden of proving that your pipeline still produces the same result regardless of how many times it runs, in what order, or after what failures.

---

## Incremental Has to Earn It

Incremental pipelines accumulate state across runs: a cursor position, a set of previously loaded keys, a log of appended rows. That state creates surface area for idempotency violations. The most common ones:

**Cursor advances before load confirms.** The high-water mark moves forward, but the data it points past never made it to the destination. The next run starts from the new position and the gap is permanent (unless a lookback window covers it -- see [[04-load-strategies/0406-reliable-loads|0406]]). Fix: advance the cursor only after the destination confirms the load.

**Append without dedup.** A retry appends the same batch again, and now the destination has two copies of every row. The pipeline "succeeded" both times, but the destination is wrong. Fix: use a dedup mechanism -- `INSERT ... ON CONFLICT` on transactional engines, a `ROW_NUMBER()` dedup view on columnar engines ([[04-load-strategies/0404-append-and-materialize|0404]]).

**Stateful staging that doesn't clean up.** The pipeline writes to a staging table, then loads from it. If the staging table isn't truncated before each run, a retry loads the previous batch plus the new one. Fix: truncate staging at the start of every run, not the end.

**Non-deterministic extraction.** The same query returns different results on different runs -- because the source changed between runs, or because the query uses `NOW()` in a way that shifts the window. This is harder to fix because the source is a moving target. [[03-incremental-patterns/0303-stateless-window-extraction|Stateless window extraction]] helps by anchoring the window to a fixed offset rather than tracking state.

---

## The Test

The simplest way to verify idempotency: run the pipeline, snapshot the destination, run the same pipeline again with the same parameters, compare. If anything changed -- row count, column values, metadata -- the pipeline isn't idempotent and you need to understand why before it goes to production.

> [!tip] Automate the idempotency test for your critical tables
> A scheduled job that runs the pipeline twice on a staging copy and compares the results catches idempotency violations before they hit production. Especially valuable after pipeline changes -- a new column, a modified cursor, a changed load strategy can all break idempotency in ways that a single run won't reveal.

---

## Idempotency by Load Strategy

Each load strategy in Part IV has a different relationship with idempotency:

| Load strategy | Idempotent? | Why |
|---|---|---|
| [[04-load-strategies/0401-full-replace\|Full replace]] | By construction | Every run rebuilds from scratch -- no prior state to interfere |
| [[04-load-strategies/0402-append-only\|Append-only]] | At the table level, no. At the view level, yes | Retries append duplicates, but the dedup view still returns correct state |
| [[04-load-strategies/0403-merge-upsert\|MERGE / upsert]] | Yes | Same key + same data = same result, regardless of how many times it runs |
| [[04-load-strategies/0404-append-and-materialize\|Append and materialize]] | At the view level, yes | Same as append-only: duplicates in the log, correct state in the view |
| [[04-load-strategies/0405-hybrid-append-merge\|Hybrid]] | Yes, if both sides are idempotent | Two destinations means two surfaces to verify |

The table reveals the pattern: full replace and MERGE are unconditionally idempotent. Append-based strategies are idempotent *at the consumer level* (the view), not at the storage level (the log). The distinction matters for storage cost and compaction frequency, but not for correctness -- which is the thing you actually care about.

---

## Statelessness and Idempotency

These two properties are related but distinct. A pipeline is **stateless** if it can run on a fresh machine with no prior context. A pipeline is **idempotent** if running it multiple times produces the same result. You can have one without the other:

- **Stateless + idempotent:** [[03-incremental-patterns/0303-stateless-window-extraction|Stateless window]] with a MERGE load. No state to manage, safe to retry. The sweet spot.
- **Stateful + idempotent:** Cursor-based extraction with a MERGE load. Needs the cursor from a prior run, but retries are safe because MERGE absorbs duplicates.
- **Stateless + not idempotent:** Stateless window with a raw INSERT (no dedup). No state to manage, but retries create duplicates.
- **Stateful + not idempotent:** Cursor-based extraction with a raw INSERT. Needs prior state *and* retries break things. The worst quadrant.

The goal is the top-left quadrant: stateless and idempotent. Full replace lives there naturally. Everything else requires deliberate design to get there -- and [[04-load-strategies/0406-reliable-loads|0406]] covers the mechanics.

---

## Related Patterns

- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]] -- full replace maximizes both purity and idempotency; incremental trades idempotency guarantees for freshness
- [[04-load-strategies/0401-full-replace|0401-full-replace]] -- idempotent by construction
- [[04-load-strategies/0403-merge-upsert|0403-merge-upsert]] -- idempotent by key matching
- [[04-load-strategies/0404-append-and-materialize|0404-append-and-materialize]] -- idempotent at the view level
- [[04-load-strategies/0406-reliable-loads|0406-reliable-loads]] -- the operational mechanics of checkpoint placement, retry, and recovery that depend on idempotency
- [[03-incremental-patterns/0303-stateless-window-extraction|0303-stateless-window-extraction]] -- the extraction pattern that achieves both statelessness and idempotency
