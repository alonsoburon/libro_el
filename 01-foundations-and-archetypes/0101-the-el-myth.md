---
title: The EL Myth
aliases: []
tags:
  - pattern/foundations
  - chapter/part-1
status: first_iteration
created: 2026-03-06
updated: 2026-03-06
---

# The EL Myth

> **One-liner:** Pure EL doesn't exist. The moment data crosses between systems, you're conforming whether you admit it or not.

## ETL, ELT, and the Pitch That Forgot Something

You've heard of ETL. It's the standard for a reason: it's most useful when the Business layer and the Data layer are handled by the same person. This is hugely common among analysts, who do the vast majority of data consumption. But handling the intricacies of how to query a database without blowing it up with full table scans? That's a skill most of them don't have.

This is one of the reasons most companies, once they reach a certain size, choose to use an OLAP database for analysis while their ERP and internal apps keep using OLTP for ingestion.

> [!info] OLAP vs OLTP
> OLAP (analytical databases like BigQuery, Snowflake, and ClickHouse) stores data in a columnar way, optimized for full-column `SUM()`s and aggregations. OLTP (transactional databases like PostgreSQL, MySQL, and SQL Server) stores data row by row, optimized for inserts and transactional operations.

The ELT framework (Extract, **Load**, Transform) came as a byproduct of this. The pitch: "let's Extract and Load the data raw into our OLAP, then Transform it there." A valid way of thinking -- which sadly forgets how fundamentally different OLTP and OLAP handle things, and how incompatible all SQL dialects really are. I can't simply copy a `DATETIME2` from SQL Server into BigQuery and expect it to behave. I have to cast, handle timezones, normalize dates, inject metadata, and of course -- most of the time I want to update incrementally, which (believe me) can increase complexity ten-fold.

## The Reality

Pure EL doesn't exist. The moment you move data between systems, something has to give. Types need casting, nulls need handling, timestamps need timezones. We call it **conforming**, and it's unavoidable.

So, what we're going to be talking about is ECL: **Extract, Conform, and Load**. The C covers type casting, null handling, timezone normalization, metadata injection, key synthesis. Everything the data needs to land correctly on the other side. If it changes what the data *means*, it belongs downstream.

## What About the T?

If the analysts want to transform afterwards -- aggregate, pivot, build dashboards -- that's their domain. But there's still a chapter in this book for helping them out. Because left unsupervised, an analyst will `SELECT *` on a 3TB events table in Snowflake and then ask you why the bill spiked. We cover how to protect them (and your invoice) in [[07-serving-the-destination/0705-query-patterns-for-analysts|Query patterns for analysts]].

## Related Patterns

- [[00-front-matter/0002-domain-model|0002-domain-model]] -- The shared schema used in every SQL example in this book
- [[01-foundations-and-archetypes/0102-what-is-conforming|0102-what-is-conforming]]
- [[01-foundations-and-archetypes/0108-purity-vs-freshness|0108-purity-vs-freshness]]
- [[07-serving-the-destination/0705-query-patterns-for-analysts|0705-query-patterns-for-analysts]]
