---
title: "Orchestrators"
aliases: []
tags:
  - appendix
status: draft
created: 2026-03-06
updated: 2026-04-02
---

# Orchestrators

The rest of this book uses generic language -- "your orchestrator," "a scheduled job," "a downstream dependency" -- because the patterns work regardless of tooling. This page names names.

## What an Orchestrator Does for ECL

An orchestrator schedules extractions, manages dependencies between them, retries failures, and tracks metadata about each run. Without one, you're writing cron jobs that can't see each other, piping errors to log files nobody reads, and backfilling by SSH-ing into a server and running scripts by hand. The orchestrator replaces all of that with a single control plane where you can observe the state of every table, trigger re-runs for specific date ranges, and get alerted when something drifts.

For ECL specifically, the orchestrator handles the concerns that cut across every pattern in this book: scheduling cadence and dependency ordering ([[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]]), tiered freshness so that critical tables run hourly while reference data runs nightly ([[06-operating-the-pipeline/0608-tiered-freshness|0608]]), backfill execution with isolation from live runs ([[06-operating-the-pipeline/0611-backfill-strategies|0611]]), and -- if it supports per-asset metadata -- automatic population of the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) without explicit INSERT statements in every pipeline.

The three serious options for a Python-based ECL stack today are Dagster, Airflow, and Prefect. Each models the world differently, and the model shapes what's easy and what's painful.

## What to Evaluate

Before comparing tools, know which capabilities actually matter for ECL at scale:

- **Per-table metadata attachment.** Can each extraction run record row counts, durations, schema hashes, and error messages as structured data the orchestrator stores? If yes, the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) populates itself. If no, you're writing INSERT statements into a side table at the end of every task.
- **Partition-aware backfills.** Can you select a date range in the UI and have the orchestrator chunk it into individual runs, track success per chunk, and retry only the failures? Or do you re-run the entire pipeline and hope for the best?
- **Concurrency control per source.** A single source database can only handle so many concurrent connections before extraction starts degrading the application. You need a way to say "max 2 extractions against this PostgreSQL at once" without hardcoding semaphores.
- **Freshness tracking.** The orchestrator should know the difference between "the DAG ran successfully" and "the data in this table is current." Task success and data freshness are separate concerns -- SLA monitoring ([[06-operating-the-pipeline/0604-sla-management|0604]]) needs the latter.
- **Dependency graph across pipelines.** If `order_lines` depends on `orders` being fresh, can the orchestrator express and enforce that without you wiring up polling sensors?

## Feature Comparison

| Concern | Dagster | Airflow | Prefect |
|---|---|---|---|
| Pipeline unit | Software-defined asset (one asset = one table) | DAG containing tasks | Flow containing tasks |
| Scheduling | Schedules + Sensors (event-driven) | Cron-based DAG schedules | Deployment schedules (cron or interval) |
| Data freshness | Freshness policies per asset -- surface violations in UI, trigger alerts | SLA miss callbacks per DAG | `result_ttl` on cached results; no native staleness tracking |
| Data quality | `@asset_check` -- inline assertions after materialization | Custom operators or external tools (Great Expectations, Soda) | Artifacts + assertions; no first-class check framework |
| Health monitoring | Materialization status + custom metadata per asset | DAG/task run state in the Airflow DB | Flow run state in Prefect API |
| Backfill | Partition-based -- select a date range in the UI, orchestrator chunks it | `catchup=True` reruns missed intervals; manual backfill via CLI | Rerun with parameters; no native partition concept |
| Dependency graph | Asset lineage graph -- cross-pipeline, cross-repository | DAG-level dependencies; cross-DAG via `ExternalTaskSensor` | Flow dependencies via `wait_for` or deployments |
| Alerting | Alert policies on asset checks, freshness violations, run failures | Callbacks, SLA miss handlers, email operators | Automations triggered by flow/task state changes |
| Concurrency control | Per-resource concurrency limits (e.g., 2 concurrent extractions per source) | Pool-based concurrency (global or per-pool slots) | Work pool concurrency limits |
| Metadata attachment | First-class -- `context.add_output_metadata({...})` per materialization | Manual -- write to XComs or external tables | Artifacts attached to flow/task runs |
| Dev/prod parity | Resource definitions swap configs per environment | Variables + Connections per environment | Profiles or env-based config |
| Managed offering | Dagster Cloud (newer) | Astronomer, MWAA, Cloud Composer (mature) | Prefect Cloud (solid for small-medium) |
| Learning curve | Moderate -- asset model requires rethinking | Low if DAG-familiar, moderate otherwise | Low -- Python decorators, minimal concepts |

## Dagster

Dagster's core abstraction is the **software-defined asset**: a function that produces a named data artifact, declared in code. For ECL, one asset maps to one destination table -- `orders`, `customers`, `events` -- and the orchestrator tracks when each was last materialized, whether it's fresh, and what metadata was attached to the last run. The asset graph *is* the dependency graph, which means the orchestrator knows that `order_lines` depends on `orders` without you wiring up sensors or trigger rules.

**Why it fits ECL well:**

- **Partitioned assets** let you declare that `events` is partitioned by date, then backfill `2026-01-01` through `2026-03-15` by selecting a range in the UI. The orchestrator chunks the range into individual partition runs, respects concurrency limits, and tracks success/failure per partition. This is the hardest thing to build from scratch, and Dagster gives it to you for free.
- **Asset checks** (`@asset_check`) run inline after materialization -- row count validation, null rate thresholds, schema drift detection. These map directly to [[06-operating-the-pipeline/0609-data-contracts|0609]] and [[06-operating-the-pipeline/0610-extraction-status-gates|0610]].
- **Freshness policies** declare how stale an asset is allowed to be (`freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)`). Violations surface in the UI and trigger alerts -- the [[06-operating-the-pipeline/0604-sla-management|0604]] pattern implemented as configuration, not code.
- **Custom metadata per materialization** (`context.add_output_metadata({"row_count": n, "extract_duration_s": t})`) feeds the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) as a side effect of every run. No explicit INSERT into a health table required -- the orchestrator stores it, and you can query or export it.
- **Resource definitions** abstract connections so the same asset code works across dev, staging, and production by swapping a resource config.
- **Sensors** trigger runs from external events -- a new file in S3, a row count threshold crossed, a webhook from the source system.
- **Concurrency limits per resource** let you cap concurrent extractions against a single source (e.g., 2 concurrent connections to a client's PostgreSQL) without global semaphores or custom locking. Define the limit on the resource, assign assets to it, and the orchestrator queues the rest. At scale -- thousands of tables across dozens of sources -- this is the difference between a controlled pipeline and one that DOS-es its own clients.

> [!tip] Stateless by default
> Dagster's asset model encourages stateless pipelines: each materialization is a self-contained function that reads from the source and writes to the destination, with no persisted cursor state between runs. Incremental cursors ([[03-incremental-patterns/0302-cursor-based-extraction|0302]]) can be managed through Dagster's built-in cursor mechanism or through the destination itself, but the orchestrator doesn't force you to maintain a state store. This aligns with the [[01-foundations-and-archetypes/0109-idempotency|0109]] goal -- the pipeline should produce the same result regardless of how many times you run it.

**Honest caveats:**

- Steeper learning curve than Airflow, especially if you're coming from a DAG/task mental model. The asset abstraction requires rethinking how you structure pipelines.
- Smaller community and fewer pre-built integrations than Airflow's connector ecosystem.
- The asset model works best when each asset is a single table. Complex multi-table operations (extract 5 tables in one API call, then split them) require workarounds with multi-asset functions.
- Dagster Cloud exists but is younger than Airflow's managed offerings (Astronomer, MWAA, Cloud Composer).

## Airflow

Airflow is the incumbent. If you're reading this book at a company with more than 50 engineers, there's a reasonable chance Airflow is already running somewhere. Its model is the **DAG** -- a directed acyclic graph of tasks -- and each task is a unit of work: run a query, call an API, move a file. It's general-purpose by design, which means it does everything but specializes in nothing.

**What works well:**

- Widest connector ecosystem of any orchestrator. If a source system has an API, there's probably an Airflow provider package for it.
- TaskFlow API (2.x) and dynamic DAGs significantly improved the developer experience over the XML-like DAG definitions of 1.x.
- Pool-based concurrency control is straightforward -- define a pool with N slots, assign tasks to it, and Airflow handles queuing.
- `catchup=True` replays missed intervals on deploy, which handles simple time-based backfills without manual intervention.
- Mature managed offerings: Astronomer, AWS MWAA, GCP Cloud Composer all run Airflow with minimal infrastructure management.
- The team already knows it, and that matters more than any feature comparison table.

**Where it gets painful for ECL:**

- No native per-asset metadata attachment. Populating the health table ([[06-operating-the-pipeline/0602-health-table|0602]]) means writing an explicit INSERT at the end of each extraction task -- every task, every DAG, maintained by hand.
- Backfills are DAG-level, not table-level. Backfilling `orders` for March means rerunning the entire DAG for March, including every other table in that DAG -- unless you've structured one DAG per table, which creates its own management overhead.
- Dependency tracking across DAGs requires `ExternalTaskSensor` or the newer dataset-based triggers (2.4+), both of which add wiring that Dagster's asset graph handles implicitly.
- No built-in freshness tracking. SLA miss callbacks exist but fire on task duration, not data age -- "this DAG took too long" and "this table's data is 6 hours stale" are different problems.
- XComs (the inter-task communication mechanism) were not designed for structured metadata. Passing row counts and schema hashes between tasks works but feels bolted on.

> [!tip] DAG structure for ECL on Airflow
> One DAG per source system, with each table as a task within the DAG, tends to be the most manageable structure. One DAG per table creates hundreds of DAGs that overwhelm the scheduler and UI. One monolithic DAG with every table creates a single point of failure where a stuck extraction blocks everything downstream. The per-source-system structure groups tables that share connection limits and scheduling cadence while keeping the blast radius of a failure scoped to one source.

## Prefect

Prefect's pitch is the cleanest developer experience of the three: decorate a Python function with `@flow`, and it's orchestrated. The API is genuinely pleasant to work with, startup is fast, and the managed cloud offering removes infrastructure concerns entirely for small-to-medium deployments.

**What works well:**

- Python-native API with minimal boilerplate -- the gap between "script that works" and "orchestrated pipeline" is smaller than Airflow or Dagster.
- Prefect Cloud handles infrastructure, logging, and scheduling out of the box, which is valuable when you don't have a platform team.
- Automations (trigger actions based on flow/task state changes) provide flexible alerting without callback boilerplate.
- Work pools with concurrency limits give reasonable control over parallel execution.
- Good fit for teams running fewer than ~500 tables where the full asset model is more structure than needed.

**Where it falls short for large-scale ECL:**

- No native partition concept. Backfilling a date range means parameterizing the flow and triggering N runs manually or with a script -- the orchestrator doesn't know that these runs form a logical unit.
- No first-class freshness tracking or asset-level metadata. The health table ([[06-operating-the-pipeline/0602-health-table|0602]]) is entirely your responsibility.
- At scale (thousands of tables), the flow-per-table model generates significant UI clutter without the asset lineage graph that makes Dagster's scale manageable.
- Fewer battle-tested production deployments at the 1000+ table scale compared to Dagster or Airflow.

## Other Tools

A few tools that come up in conversations but don't warrant a full section:

- **Mage** -- Modern orchestrator with a notebook-like UI. Promising for exploratory work but less mature for production ECL at scale. Limited partition/backfill tooling.
- **Kestra** -- Event-driven, YAML-based orchestration. Good for non-Python teams but loses the Python-native advantages that make Dagster and Prefect productive for data engineers.
- **Luigi** -- Spotify's original Python orchestrator. Historically important but effectively superseded by the tools above. No managed offering, minimal community activity.
- **cron + scripts** -- The zero-overhead option. Acceptable for a handful of tables where the only requirement is "run this every night." Falls apart the moment you need retry logic, dependency ordering, backfills, or any visibility beyond `grep`-ing log files.

> [!warning] Don't build your own
> Every team that builds a custom orchestrator eventually rebuilds 60% of Airflow, poorly. The "we just need a simple scheduler" conversation leads to a homegrown system with no UI, no backfill capability, no alerting, and a bus factor of one. Pick a real orchestrator -- even Airflow with its warts -- and spend the engineering time on the pipelines instead.

## Author's Recommendation

For a new ECL project, start with **Dagster**. The asset model maps 1:1 to the "one asset = one destination table" structure that this book is built around, partitioned backfills are the single most painful thing to build from scratch, and inline asset checks plus freshness policies implement half of [[06-operating-the-pipeline/0601-monitoring-observability|Part VI]] as configuration rather than custom code. The asset graph and partition-based backfills have justified the learning curve many times over.

If the team already runs **Airflow** and migration cost exceeds the benefit, stay on Airflow -- especially with 2.x's improvements and a managed provider handling infrastructure. Structure one DAG per source system (not one DAG per table), add explicit health table inserts to each task, and build a thin metadata layer on top of XComs or an external table. It works; it just requires more manual wiring for the patterns in this book. If you're on **Prefect** and running fewer than a few hundred tables with a small team that values developer velocity over operational features, it's a fine choice -- upgrade to Dagster when backfill complexity or table count starts to hurt.

| Scenario | Recommendation |
|---|---|
| New project, any team size | Dagster |
| Existing Airflow installation, migration not justified | Stay on Airflow 2.x |
| Small team, < 500 tables, no platform engineer | Prefect Cloud |
| Non-Python team | Evaluate Kestra or Airflow (wider polyglot support) |
| "We just need cron" | You don't -- but if you truly have < 10 tables with no dependencies, cron is fine until it isn't |

## Related Patterns

- [[06-operating-the-pipeline/0602-health-table|0602]] -- The health table that orchestrators populate
- [[06-operating-the-pipeline/0604-sla-management|0604]] -- Freshness policies and SLA monitoring
- [[06-operating-the-pipeline/0606-scheduling-and-dependencies|0606]] -- Scheduling cadence and dependency ordering
- [[06-operating-the-pipeline/0608-tiered-freshness|0608]] -- Tiered freshness by table criticality
- [[06-operating-the-pipeline/0609-data-contracts|0609]] -- Data quality checks and schema contracts
- [[06-operating-the-pipeline/0610-extraction-status-gates|0610]] -- Status gates that block downstream on extraction failure
- [[06-operating-the-pipeline/0611-backfill-strategies|0611]] -- Backfill execution patterns
