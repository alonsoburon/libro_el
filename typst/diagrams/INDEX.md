# Diagram Index -- for editorial / design handoff

64 figures are embedded in `book.typ`, grouped below by Part in reading order. Every SVG lives in `typst/diagrams/`, which now holds only diagrams the book uses.

## Style spec (match these for any new or revised diagram)

- **Canvas**: `viewBox="0 0 720 H"`, H typically 200-240. Width fixed at 720.
- **Palette (gruvbox light, no other colors)**: bg `#ffffff`, bg-soft `#f9f5d7`, header `#ebdbb2`, lines `#d5c4a1`; text `#3c3836`, muted `#7c6f64`, dim `#928374`, dim-warm `#bdae93`; blue `#458588`, orange `#d65d0e`, green `#79740e`, red `#9d0006`, yellow `#d79921`; accents green-bright `#689d6a`, red-bright `#cc241d`, amber `#b57614`; text-on-tint blue-deep `#076678`, orange-deep `#af3a03`. Fills usually at opacity 0.06-0.30.
- **Font**: `'Segoe UI', system-ui, sans-serif` (monospace allowed for code/SQL/identifiers).
- **Title**: centered at x=360, y=18, font-size 13, fill `#7c6f64`, letter-spacing 0.3.
- **No em-dashes** (use `--`). XML comments must not contain `--`.
- **Recurring idioms** (reuse the closest): nested tiers (`0608`, `0614`); decision-tree with colored terminals (`0610`, `0615`); side-by-side correct/wrong (`0506`, `0704`); multi-table flow with curved arrows (`0207-activity-driven`, `0613`).
- The cover (`cover-art.svg`) is a DRAFT placeholder -- design from scratch.

## Front matter

```
 1  cover-art.svg                       (front matter)
 2  domain-model-er.svg                 Domain Model / Schema
```

## Foundations & Source Archetypes

```
 3  ecl-conforming-vs-transforming.svg  What Is Conforming / The Line Between Conforming and Transforming
 4  0104-lies-sources-tell.svg          The Lies Sources Tell
 5  schema-drift-detection.svg          The Lies Sources Tell / "The schema is stable"
 6  0105-hard-vs-soft-rules.svg         Hard Rules, Soft Rules
 7  ecl-corridors.svg                   Corridors / The Two Corridors
 8  0108-purity-freshness.svg           Purity vs. Freshness / Classifying a Table
 9  0109-stateless-idempotent.svg       Idempotency / Statelessness and Idempotency
```

## Full Replace Patterns

```
10  0201-replace-strategies.svg         Full Scan Strategies / At the Destination: Replace Strategies
11  0202-partition-swap.svg             Partition Swap
12  0203-staging-swap.svg               Staging Swap
13  0205-scoped-replace.svg             Scoped Full Replace / The Mechanics
14  0206-rolling-window.svg             Rolling Window Replace / The Mechanics
15  0207-sparse-table.svg               Sparse Table Extraction / Zeros vs. Missing
16  0207-activity-driven.svg            Activity-Driven Extraction / The Mechanics
17  0208-hash-detection.svg             Hash-Based Change Detection / The Mechanics
18  0209-partial-columns.svg            Partial Column Loading / Why Exclude Columns?
```

## Incremental Extraction Patterns

```
19  0301-cursor-blind-spots.svg         Timestamp Extraction Foundations / The Discipline Gap
20  0302-cursor-mechanics.svg           Cursor-Based Timestamp Extraction
21  0303-stateless-window.svg           Stateless Window Extraction
22  0304-cursor-from-header.svg         Cursor from Another Table
23  0305-sequential-id-cursor.svg       Sequential ID Cursor
24  0306-full-id-comparison.svg         Hard Delete Detection
25  0307-open-closed-split.svg          Open/Closed Documents / The Split
26  0308-detail-without-timestamp.svg   Detail Without Timestamp
27  0309-late-arriving-data.svg         Late-Arriving Data
28  0310-create-vs-update.svg           Create vs Update Separation / Cursor + NULL Extraction
```

## Load Strategies

```
29  0401-full-replace-load.svg          Full Replace Load / Scope Alignment
30  0402-append-only-load.svg           Append-Only Load / The Pattern
31  0403-merge-mechanics.svg            Merge / Upsert / MERGE Across Engines
32  0403-merge-cost.svg                 Merge / Upsert / Cost Anatomy
33  0404-log-anatomy.svg                Append and Materialize / The Pattern
34  0404-compaction.svg                 Append and Materialize / Compaction
35  0405-hybrid-append-merge.svg        Hybrid Append-Merge
36  0406-checkpoint-placement.svg       Reliable Loads / Checkpoint Placement
37  0406-partial-load-recovery.svg      Reliable Loads / Partial Load Recovery
```

## The Conforming Playbook

```
38  0501-metadata-injection.svg         Metadata Column Injection / The Playbook
39  0502-synthetic-key.svg              Synthetic Keys / Building the Key
40  0503-type-casting.svg               Type Casting and Normalization / The Playbook
41  0503-type-gap.svg                   Type Casting and Normalization / By Corridor
42  0504-null-handling.svg              Null Handling / The Playbook
43  0505-timezone-conforming.svg        Timezone Conforming / The Playbook
44  0506-charset-encoding.svg           Charset and Encoding / The Playbook
45  0507-nested-json.svg                Nested Data and JSON / Land As-Is
```

## Operating the Pipeline

```
46  0601-four-layers-observability.svg  Monitoring and Observability / Four Layers of Pipeline Observability
47  0602-health-table.svg               The Health Table / What You Can't Measure
48  0604-sla-duration-creep.svg         SLA Management / What Erodes SLAs
49  0605-alerting-severity.svg          Alerting and Notifications / Severity Calibration
50  0606-scheduling-dependencies.svg    Scheduling and Dependencies / Frequency vs. Method
51  0608-tiered-freshness.svg           Tiered Freshness / The Tiers
52  0609-schema-evolution-policies.svg  Data Contracts / Schema Evolution Policies
53  0610-extraction-gates.svg           Extraction Status Gates / Gate Mechanics
54  0611-backfill-types.svg             Backfill Strategies / Reloading Without Downtime
55  0612-partial-failure-recovery.svg   Partial Failure Recovery
56  0613-duplicate-sources.svg          Duplicate Detection / How Duplicates Arrive
57  0614-reconciliation-levels.svg      Reconciliation Patterns / Reconciliation Levels
58  0615-corruption-recovery.svg        Recovery from Corruption / Triage: Assess the Blast Radius
```

## Serving the Destination

```
59  0701-detail-fanout.svg              Don't Pre-Aggregate / What Consumers Actually Need
60  0702-partition-pruning.svg          Partitioning, Clustering, and Pruning
61  0703-view-hierarchy.svg             Pre-Built Views / Consumer Query Mistakes
62  0704-analyst-queries.svg            Query Patterns for Analysts / Current State from Append-Only Tables
63  0706-point-in-time.svg              Point-in-Time from Events / Movements to Snapshots
```

## Appendix

```
64  domain-model-er.svg                 Domain Model Quick Reference / Relationships
```
