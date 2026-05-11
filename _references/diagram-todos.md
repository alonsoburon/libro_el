# Diagram TODOs -- COMPLETE

**Status (2026-05-11): Done. 22 new diagrams added, book compiles clean, all publish-ready.**

Style invariants: gruvbox light palette (#458588 blue, #d65d0e orange, #79740e green, #9d0006 red, #d79921 yellow, #f9f5d7 bg, #ebdbb2 header, #7c6f64 text), 720px viewBox width, hand-crafted SVG, embedded via `#figure(image("diagrams/XXYY-name.svg", width: 95%))`.

---

## Created (22)

### Part I -- Foundations
- [x] **0104 The Lies Sources Tell** -- 2-column claim vs reality matrix, 6 lies
- [x] **0105 Hard Rules vs Soft Rules** -- two-tier iceberg metaphor (hard band on top, larger soft band below)

### Part II -- Full Replace
- [x] **0201 Replace Strategies** -- DROP vs TRUNCATE vs DELETE comparison matrix with "default" badge on TRUNCATE
- [x] **0202 Partition Swap mechanics** -- before/swap/after flow with engine syntax callouts
- [x] **0203 Staging Swap mechanics** -- 3-step Load/Swap/Drop flow with consumer continuity
- [x] **0207 Activity-Driven Extraction** -- movements log -> DISTINCT pairs -> filtered sparse extraction
  - Also moved existing `0207-sparse-table.svg` to its correct section (2.6 Sparse Table Extraction)

### Part III -- Incremental
- [x] **0308 Detail Without Timestamp** -- parent cursor cascade via FK to detail rows

### Part VI -- Operating
- [x] **0601 Four Layers of Pipeline Observability** -- stacked bands with "earn-its-keep" emphasis on Data Health
- [x] **0602 Health Table** -- pipeline_health schema + three downstream consumers
- [x] **0605 Alerting Severity** -- 3x3 matrix (blast radius x time-to-fix) -> channel
- [x] **0606 Scheduling and Dependencies** -- source pipelines -> tables -> consumers, critical-path highlight
- [x] **0609 Schema Evolution Policies** -- 3x4 matrix (drift type x policy) with anti-pattern marking
- [x] **0610 Extraction Status Gates** -- decision tree resolving zero-row ambiguity
- [x] **0611 Backfill Types** -- timeline showing full reload / targeted partitions / state-reset replay
- [x] **0612 Partial Failure Recovery** -- pipeline stages with per-stage retry-safety callouts
- [x] **0613 Duplicate Sources** -- fan-in of four duplicate origins into destination
- [x] **0614 Reconciliation Levels** -- nested cost-tier diagram (mirrors 0608's idiom)
- [x] **0615 Recovery from Corruption** -- triage flowchart by blast radius
- [x] **0608 hot/warm/cold tiered freshness** -- *(was already done in previous session, kept for reference)*

### Part VII -- Serving
- [x] **0702 Partition Pruning** -- red full-scan vs green pruned-scan side-by-side
- [x] **0703 View Hierarchy** -- 4-layer stack (log / dedup / derived / consumer)
- [x] **0704 Analyst Queries** -- naive SELECT vs dedup-view-correct, side-by-side
- [x] **0706 Point-in-Time from Events** -- movements timeline with as-of reconstructions

---

## Deliberately skipped P3s (6) -- redundant or low-value

Honest filter: not every section needs a diagram. The following would have added visual noise without insight.

| Diagram | Reason for skipping |
|---|---|
| 0102 race condition (Transactional / What Will Bite You) | Redundant with 0301 cursor blind spots (already diagrammed). |
| 0103 bytes-scanned (Columnar / What Will Bite You) | Covered by 0702 partition pruning -- same insight, different framing. |
| 0603 Cost Monitoring | Hard to diagram without becoming a screenshot of a billing console. |
| 0607 Source System Etiquette | Section is already short and pragmatic; a diagram would pad it. |
| 0705 Cost Optimization by Engine | Becomes a table of per-engine knobs -- already a table in the book. |
| 0707 Schema Naming Conventions | Prose carries it; existing examples are explicit enough. |

---

## Visual review workflow

Each new diagram was rendered via `rsvg-convert -w 1080` to PNG and read visually before insertion into `book.typ`. Two of the initial batch needed fixes after review:

- **0601 Four Layers** -- middle column text was overlapping the LAYER sub-labels. Fixed by shortening sub-labels and shifting middle column anchor right.
- **0610 Extraction Gates** -- baseline_zero terminal was too narrow and routing was confusing. Fully redesigned PASS terminals to left, BLOCK terminals to right, horizontal arrows, no zig-zag.
- **0602 Health Table** -- arrow label "INSERT one row / after every table" was overlapping table border. Removed the label (bottom band already explains).

The PNG renders live in `/tmp/diagram-previews/` for re-checking if needed.
