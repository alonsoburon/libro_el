# Diagram Index -- for editorial / design handoff

64 figures are embedded in `book.typ`, listed below in reading order. Every SVG lives in `typst/diagrams/`. This folder contains ONLY diagrams the book uses (orphans removed).

## Style spec (match these for any new or revised diagram)

- **Canvas**: `viewBox="0 0 720 H"`, H typically 200-240. Width fixed at 720.
- **Palette (gruvbox light, no other colors)**: bg `#ffffff`, bg-soft `#f9f5d7`, header `#ebdbb2`, lines `#d5c4a1`; text `#3c3836`, muted `#7c6f64`, dim `#928374`, dim-warm `#bdae93`; blue `#458588`, orange `#d65d0e`, green `#79740e`, red `#9d0006`, yellow `#d79921`; accents green-bright `#689d6a`, red-bright `#cc241d`, amber `#b57614`; text-on-tint blue-deep `#076678`, orange-deep `#af3a03`. Fills usually at opacity 0.06-0.30.
- **Font**: `'Segoe UI', system-ui, sans-serif` (monospace allowed for code/SQL/identifiers).
- **Title**: centered at x=360, y=18, font-size 13, fill `#7c6f64`, letter-spacing 0.3.
- **No em-dashes** (use `--`). XML comments must not contain `--`.
- **Recurring idioms** (reuse the closest): nested tiers (`0608`, `0614`); decision-tree with colored terminals (`0610`, `0615`); side-by-side correct/wrong (`0506`, `0704`); multi-table flow with curved arrows (`0207-activity-driven`, `0613`).
- The cover (`cover-art.svg`) is a DRAFT placeholder -- design from scratch.

## Figures in reading order

| # | Diagram | Part | Section |
|---|---|---|---|
| 1 | `diagrams/cover-art.svg` |  |  |
| 2 | `diagrams/domain-model-er.svg` |  | Domain Model -> Schema |
| 3 | `diagrams/ecl-conforming-vs-transforming.svg` | Foundations & Source Archetypes | What Is Conforming -> The Line Between Conforming and Transforming |
| 4 | `diagrams/0104-lies-sources-tell.svg` | Foundations & Source Archetypes | The Lies Sources Tell |
| 5 | `diagrams/schema-drift-detection.svg` | Foundations & Source Archetypes | The Lies Sources Tell -> "The schema is stable" |
| 6 | `diagrams/0105-hard-vs-soft-rules.svg` | Foundations & Source Archetypes | Hard Rules, Soft Rules |
| 7 | `diagrams/ecl-corridors.svg` | Foundations & Source Archetypes | Corridors -> The Two Corridors |
| 8 | `diagrams/0108-purity-freshness.svg` | Foundations & Source Archetypes | Purity vs.~Freshness -> Classifying a Table |
| 9 | `diagrams/0109-stateless-idempotent.svg` | Foundations & Source Archetypes | Idempotency -> Statelessness and Idempotency |
| 10 | `diagrams/0201-replace-strategies.svg` | Full Replace Patterns | Full Scan Strategies -> At the Destination: Replace Strategies |
| 11 | `diagrams/0202-partition-swap.svg` | Full Replace Patterns | Partition Swap |
| 12 | `diagrams/0203-staging-swap.svg` | Full Replace Patterns | Staging Swap |
| 13 | `diagrams/0205-scoped-replace.svg` | Full Replace Patterns | Scoped Full Replace -> The Mechanics |
| 14 | `diagrams/0206-rolling-window.svg` | Full Replace Patterns | Rolling Window Replace -> The Mechanics |
| 15 | `diagrams/0207-sparse-table.svg` | Full Replace Patterns | Sparse Table Extraction -> Zeros vs. Missing |
| 16 | `diagrams/0207-activity-driven.svg` | Full Replace Patterns | Activity-Driven Extraction -> The Mechanics |
| 17 | `diagrams/0208-hash-detection.svg` | Full Replace Patterns | Hash-Based Change Detection -> The Mechanics |
| 18 | `diagrams/0209-partial-columns.svg` | Full Replace Patterns | Partial Column Loading -> Why Exclude Columns? |
| 19 | `diagrams/0301-cursor-blind-spots.svg` | Incremental Extraction Patterns | Timestamp Extraction Foundations -> The Discipline Gap |
| 20 | `diagrams/0302-cursor-mechanics.svg` | Incremental Extraction Patterns | Cursor-Based Timestamp Extraction |
| 21 | `diagrams/0303-stateless-window.svg` | Incremental Extraction Patterns | Stateless Window Extraction |
| 22 | `diagrams/0304-cursor-from-header.svg` | Incremental Extraction Patterns | Cursor from Another Table |
| 23 | `diagrams/0305-sequential-id-cursor.svg` | Incremental Extraction Patterns | Sequential ID Cursor |
| 24 | `diagrams/0306-full-id-comparison.svg` | Incremental Extraction Patterns | Hard Delete Detection |
| 25 | `diagrams/0307-open-closed-split.svg` | Incremental Extraction Patterns | Open/Closed Documents -> The Split |
| 26 | `diagrams/0308-detail-without-timestamp.svg` | Incremental Extraction Patterns | Detail Without Timestamp |
| 27 | `diagrams/0309-late-arriving-data.svg` | Incremental Extraction Patterns | Late-Arriving Data |
| 28 | `diagrams/0310-create-vs-update.svg` | Incremental Extraction Patterns | Create vs Update Separation -> Cursor + NULL Extraction |
| 29 | `diagrams/0401-full-replace-load.svg` | Load Strategies | Full Replace Load -> Scope Alignment |
| 30 | `diagrams/0402-append-only-load.svg` | Load Strategies | Append-Only Load -> The Pattern |
| 31 | `diagrams/0403-merge-mechanics.svg` | Load Strategies | Merge / Upsert -> MERGE Across Engines |
| 32 | `diagrams/0403-merge-cost.svg` | Load Strategies | Merge / Upsert -> Cost Anatomy |
| 33 | `diagrams/0404-log-anatomy.svg` | Load Strategies | Append and Materialize -> The Pattern |
| 34 | `diagrams/0404-compaction.svg` | Load Strategies | Append and Materialize -> Compaction |
| 35 | `diagrams/0405-hybrid-append-merge.svg` | Load Strategies | Hybrid Append-Merge |
| 36 | `diagrams/0406-checkpoint-placement.svg` | Load Strategies | Reliable Loads -> Checkpoint Placement |
| 37 | `diagrams/0406-partial-load-recovery.svg` | Load Strategies | Reliable Loads -> Partial Load Recovery |
| 38 | `diagrams/0501-metadata-injection.svg` | The Conforming Playbook | Metadata Column Injection -> The Playbook |
| 39 | `diagrams/0502-synthetic-key.svg` | The Conforming Playbook | Synthetic Keys -> Building the Key |
| 40 | `diagrams/0503-type-casting.svg` | The Conforming Playbook | Type Casting and Normalization -> The Playbook |
| 41 | `diagrams/0503-type-gap.svg` | The Conforming Playbook | Type Casting and Normalization -> By Corridor |
| 42 | `diagrams/0504-null-handling.svg` | The Conforming Playbook | Null Handling -> The Playbook |
| 43 | `diagrams/0505-timezone-conforming.svg` | The Conforming Playbook | Timezone Conforming -> The Playbook |
| 44 | `diagrams/0506-charset-encoding.svg` | The Conforming Playbook | Charset and Encoding -> The Playbook |
| 45 | `diagrams/0507-nested-json.svg` | The Conforming Playbook | Nested Data and JSON -> Land As-Is |
| 46 | `diagrams/0601-four-layers-observability.svg` | Operating the Pipeline | Monitoring and Observability -> Four Layers of Pipeline Observability |
| 47 | `diagrams/0602-health-table.svg` | Operating the Pipeline | The Health Table -> What You Can't Measure |
| 48 | `diagrams/0604-sla-duration-creep.svg` | Operating the Pipeline | SLA Management -> What Erodes SLAs |
| 49 | `diagrams/0605-alerting-severity.svg` | Operating the Pipeline | Alerting and Notifications -> Severity Calibration |
| 50 | `diagrams/0606-scheduling-dependencies.svg` | Operating the Pipeline | Scheduling and Dependencies -> Frequency vs. Method |
| 51 | `diagrams/0608-tiered-freshness.svg` | Operating the Pipeline | Tiered Freshness -> The Tiers |
| 52 | `diagrams/0609-schema-evolution-policies.svg` | Operating the Pipeline | Data Contracts -> Schema Evolution Policies |
| 53 | `diagrams/0610-extraction-gates.svg` | Operating the Pipeline | Extraction Status Gates -> Gate Mechanics |
| 54 | `diagrams/0611-backfill-types.svg` | Operating the Pipeline | Backfill Strategies -> Reloading Without Downtime |
| 55 | `diagrams/0612-partial-failure-recovery.svg` | Operating the Pipeline | Partial Failure Recovery |
| 56 | `diagrams/0613-duplicate-sources.svg` | Operating the Pipeline | Duplicate Detection -> How Duplicates Arrive |
| 57 | `diagrams/0614-reconciliation-levels.svg` | Operating the Pipeline | Reconciliation Patterns -> Reconciliation Levels |
| 58 | `diagrams/0615-corruption-recovery.svg` | Operating the Pipeline | Recovery from Corruption -> Triage: Assess the Blast Radius |
| 59 | `diagrams/0701-detail-fanout.svg` | Serving the Destination | Don't Pre-Aggregate -> What Consumers Actually Need |
| 60 | `diagrams/0702-partition-pruning.svg` | Serving the Destination | Partitioning, Clustering, and Pruning |
| 61 | `diagrams/0703-view-hierarchy.svg` | Serving the Destination | Pre-Built Views -> Consumer Query Mistakes |
| 62 | `diagrams/0704-analyst-queries.svg` | Serving the Destination | Query Patterns for Analysts -> Current State from Append-Only Tables |
| 63 | `diagrams/0706-point-in-time.svg` | Serving the Destination | Point-in-Time from Events -> Movements to Snapshots |
| 64 | `diagrams/domain-model-er.svg` | Appendix | Domain Model Quick Reference -> Relationships |
