---
title: "Timezone Conforming"
aliases: []
tags:
  - pattern/conforming
  - chapter/part-5
status: draft
created: 2026-03-06
updated: 2026-03-14
---

# Timezone Conforming

> **One-liner:** TZ stays TZ, naive stays naive. Don't make timezone decisions that aren't in the source data -- but know what you're landing.

---

## The Playbook

The rule follows the same principle as [[05-conforming-playbook/0504-null-handling|0504]]: reflect the source. If the source stores timezone-aware timestamps, land timezone-aware. If the source stores naive timestamps, land them as datetime -- not as timestamp with a timezone you guessed. Converting naive to UTC without being certain of the source timezone is worse than landing naive, because a wrong UTC conversion looks correct in the destination and silently shifts every row by however many hours you got wrong.

Most transactional sources store naive timestamps. The application knows what timezone it means, but the column doesn't say -- and often nobody at the source team documented it either. That's the source's data quality problem. Your job is to land what the source gives you, not to retroactively assign timezone semantics that weren't there.

---

## Naive vs Aware

Two fundamentally different types that look similar in query results but behave differently everywhere else:

**Naive** (`TIMESTAMP WITHOUT TIME ZONE`, `DATETIME`, `DATETIME2`): the value `2026-03-15 14:30:00` with no timezone attached. The source application assumes a timezone -- usually the server's local time, sometimes the user's timezone, sometimes something else entirely -- but the column itself carries no indication of which one.

**Aware** (`TIMESTAMP WITH TIME ZONE`, `TIMESTAMPTZ`): the value `2026-03-15 14:30:00+00:00` with an explicit offset or stored as UTC internally. The timezone is part of the data. PostgreSQL's `TIMESTAMPTZ` stores everything as UTC and converts to the session timezone on display; BigQuery's `TIMESTAMP` is always UTC.

The conforming decision:

| Source has | Land as | Why |
|---|---|---|
| Aware (TIMESTAMPTZ) | Aware (TIMESTAMP / TIMESTAMPTZ) | The timezone is part of the data -- preserve it |
| Naive (DATETIME) | Datetime / naive equivalent | Don't add timezone info that isn't there |
| Naive, but you know the timezone with certainty | Convert to aware, document it | Only if the source team confirms and it won't change |

> [!warning] Not every destination supports naive timestamps
> BigQuery's `TIMESTAMP` is always UTC -- there's no naive mode. If you land a naive `14:30:00` as a BigQuery `TIMESTAMP`, the engine treats it as `14:30:00 UTC`, which may be wrong. Use BigQuery `DATETIME` (no timezone) for naive values. Snowflake has both `TIMESTAMP_NTZ` (naive) and `TIMESTAMP_TZ` (aware). Know which one you're targeting.

---

## Discovering the Source Timezone

When you need to know what timezone a naive timestamp represents -- for documentation, for downstream, or because you're deciding whether to convert -- here's the investigation order:

**Ask the source team.** "What timezone does your application write timestamps in?" is a 5-minute conversation that saves weeks of debugging. Most teams know the answer even if they never documented it.

**Check the database server timezone.** `SHOW timezone` (PostgreSQL), `SELECT @@global.time_zone` (MySQL), `SELECT SYSDATETIMEOFFSET()` (SQL Server). Many applications inherit the server's timezone for naive timestamps.

**Look at timestamps around DST transitions.** If you see a gap at 2:00-3:00 AM on the spring-forward date, the source writes in a timezone that observes DST. If you see two clusters of timestamps at 1:00-2:00 AM on the fall-back date, same conclusion. If timestamps flow smoothly across both transitions, the source writes in UTC or a non-DST timezone.

**Multinational ERPs.** Each company or branch may write in its own local timezone -- same column, different timezone per row, no indicator column. This is genuinely bad source data quality. If the source doesn't provide a timezone indicator per row, avoid assigning timezones in the ECL layer. Land naive and let downstream handle it with whatever business context they have about which company operates in which timezone.

---

## DST Traps

Daylight saving transitions create two specific hazards for timestamp data:

**Spring forward (the hour that doesn't exist).** Clocks jump from 2:00 AM to 3:00 AM. A timestamp like `2026-03-08 02:30:00 America/New_York` refers to a moment that never existed. Some engines reject it, some silently shift it to 3:30 AM, some store it as-is. If the source wrote it, it's probably from a system that doesn't validate timestamps -- land it as-is and document that the value is technically invalid.

**Fall back (the hour that repeats).** Clocks fall from 2:00 AM back to 1:00 AM. A timestamp like `2026-11-01 01:30:00 America/New_York` could refer to two different instants -- the first 1:30 AM or the second 1:30 AM. Without an offset, there's no way to distinguish them.

Both of these are reasons to prefer landing naive timestamps as naive rather than converting to UTC at extraction time. A conversion during the ambiguous fall-back hour has a 50% chance of being wrong, and you won't know which rows are affected. A stateless extraction window ([[03-incremental-patterns/0303-stateless-window-extraction|0303]]) helps here -- the overlap naturally re-extracts the ambiguous rows on the next run, and if the source eventually clarifies (some applications write a second-pass correction), the later extraction picks it up.

---

## Downstream Boundary Effects

The most visible consequence of timezone handling isn't in the pipeline -- it's in the business reports that consume the data.

When someone downstream writes `SUM(amount) GROUP BY TRUNC(sale_date, MONTH)`, sales near the month boundary can land in the wrong bucket depending on how the timestamp is interpreted. A sale at `2026-03-31 23:30:00` in the source's local timezone is `2026-04-01 02:30:00 UTC`. If the analyst's report truncates a UTC timestamp, March's revenue is short and April's is inflated. Multiply this across every month boundary and the numbers never match the source system's own reports.

This matters more than partition alignment. A row in the wrong partition is an internal cost issue -- a query scans one extra partition. A row in the wrong month in a revenue report gets escalated to the CFO. Document the timezone assumption clearly so downstream teams can adjust their queries accordingly.

> [!tip] Document the timezone assumption per table
> Add a comment to the destination DDL or a row in a metadata table: "`orders.created_at` is naive, assumed `America/Santiago` based on source team confirmation (2026-03-14)." When the assumption is wrong -- and eventually it will be, because someone changes the server timezone or adds a branch in a different country -- at least you'll know what was assumed and when.

---

## By Corridor

> [!example]- Transactional → Columnar
> BigQuery and Snowflake handle timezone-aware timestamps well, but only if you give them the right data. BigQuery `TIMESTAMP` = always UTC; use `DATETIME` for naive values. Snowflake has `TIMESTAMP_NTZ` (naive), `TIMESTAMP_LTZ` (session-local), and `TIMESTAMP_TZ` (explicit offset) -- pick the one that matches what the source actually stores. Landing a naive value as an aware type silently assigns a wrong timezone with no error and no warning.

> [!example]- Transactional → Transactional
> If source is naive PostgreSQL and destination is naive PostgreSQL, no conversion needed -- the naive value transfers as-is. Document the assumption but don't add complexity. If the destination is a different engine (PostgreSQL → MySQL), check whether the naive type behavior differs -- PostgreSQL's `TIMESTAMP WITHOUT TIME ZONE` and MySQL's `DATETIME` are equivalent in practice, but SQL Server's `DATETIME2` has different precision (see [[05-conforming-playbook/0503-type-casting-normalization|0503]]).

---

## Related Patterns

- [[05-conforming-playbook/0503-type-casting-normalization|0503]] -- DATETIME2 → TIMESTAMP casting and precision truncation
- [[05-conforming-playbook/0504-null-handling|0504]] -- same principle: reflect the source, don't add information that isn't there
- [[02-full-replace-patterns/0202-partition-swap|0202]] -- partition boundaries and timezone alignment
- [[01-foundations-and-archetypes/0105-the-lies-sources-tell|0105]] -- "timestamps have timezones" as a common lie
- [[03-incremental-patterns/0303-stateless-window-extraction|0303]] -- overlap windows help with DST ambiguity
