---
title: Preface
aliases: []
tags:
  - front-matter
status: draft
created: 2026-03-06
updated: 2026-03-06
---

# ECL Patterns

### A practical guide to moving data between systems without losing your mind

War stories and patterns to simplify your life when you (or your boss) decide to clone data from a transactional source to an analytical destination -- or between transactional systems. The patterns are about getting the data there correctly, efficiently, and without losing your mind.

## What This Book Covers

This book is about the space between source and destination. Specifically:

| What we cover                                                  | What we don't                                        |
| -------------------------------------------------------------- | ---------------------------------------------------- |
| Extracting data from transactional systems                     | Building dashboards or reports                       |
| Conforming types, nulls, timezones, encodings                  | Business logic / KPI definitions                     |
| Loading into columnar or transactional destinations            | Silver/gold layer transformations                    |
| Incremental strategies, full replace, and the huge gray middle | Orchestrator-specific tutorials (Airflow, dbt, etc.) |
| Failure recovery, idempotency, reconciliation                  | Data modeling / star schemas                         |
| Protecting destination costs from bad queries                  | ML pipelines                                         |
| Batch extraction patterns                                      | CDC / real-time streaming / event-driven             |

> [!tip] The corridors
> Every pattern in this book plays out differently depending on where the data is going. We call these **corridors**: Transactional -> Columnar (e.g. SQL Server -> BigQuery) and Transactional -> Transactional (e.g. PostgreSQL -> PostgreSQL). Same pattern, different trade-offs. We show both.
