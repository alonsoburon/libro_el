# CLAUDE.md --Instructions for Claude Code

## Project: ECL Patterns Book

### Core Thesis
Pure EL is a practical myth. The moment data crosses between systems, conforming is unavoidable: type casting, metadata injection, null handling, charset encoding. **ECL (Extract, Conform, Load)** names this reality. The C is everything the data needs to survive the crossing and be usable on the other side: type conforming, metadata injection, key synthesis, null handling. If it changes business logic, it's not conforming, it belongs downstream. This book documents the patterns to do ECL well.

### Author Opinions (apply these when writing content)
- **Full refreshes are often the right answer.** A full replace resets state, eliminates accumulated drift, and lets you operate with less-than-perfect methodology knowing you have periodic clean checkpoints. When in doubt, default to full replace. The more tables you can fully replace, the simpler and more reliable the pipeline. Incremental complexity should be earned, not assumed.
- **Purity vs. freshness is a real tradeoff.** Perfectly stable data clones often require full replaces everywhere, which means slow load times and low update frequency. Sometimes that has to be sacrificed to give the business more immediate data. The right point on that tradeoff depends on the table, the consumer, and the SLA -- not on a universal default. This book documents both ends and the space between.
- **Stateless and idempotent is the goal.** Every pipeline run should produce the same result regardless of how many times it's been run before. No side effects from prior runs, no accumulated state that needs to be managed. Full replace achieves this naturally. Incremental is a departure from it -- necessary sometimes, but always at a cost. Getting to stateless/idempotent is the hard part; that's what most of this book is actually about.
- **Partial column loading is a last resort.** Extracting only a subset of columns is useful when the source table is too wide, too slow, or has columns your pipeline can't handle (PII, BLOBs, irrelevant noise). But clients and consumers don't understand it -- they assume the destination has all the source columns. Use it when necessary, document it explicitly, and never do it silently.

### Software Ecosystem

The author runs ~6500 tables in production on Dagster. Orchestrators are a first-class concern in this book, not an afterthought. When patterns touch scheduling, dependency management, observability, or alerting, reference the orchestrator layer explicitly.

#### Orchestrators
Dagster is the primary reference for the author's context, but **book content must use generic orchestrator language** ("your orchestrator", "a downstream task", "a scheduled job"). Specific tool names (Dagster, Airflow, Prefect) are reserved for the software recommendations section in the Appendix (0805-0807).

**Exception -- late-chapter hints**: From Part VI onward, patterns may name specific tools when describing a concrete capability that differentiates them (e.g., "Dagster lets you attach custom metadata per table after each run"). Frame these as teasers that point the reader toward the Appendix, not as prescriptions. The pattern must still work generically; the name-drop highlights a feature worth looking for in whatever orchestrator/tool you choose.

| Concern | Dagster | Airflow | Prefect |
|---|---|---|---|
| Pipeline unit | Software-defined asset | DAG / Task | Flow / Task |
| Scheduling | Schedules + Sensors | DAG schedule | Deployment schedule |
| Data freshness | Freshness policies on assets | SLA miss callbacks | `result_ttl` |
| Data quality | Asset checks (`@asset_check`) | Custom operators | Artifacts + assertions |
| Health monitoring | Asset health / materialization status | DAG run state | Flow run state |
| Backfill | Partition-based backfill UI | Backfill / catchup | Rerun with parameters |
| Dependency graph | Asset lineage graph | DAG dependencies | Flow dependencies |
| Alerting | Alert policies on asset checks / runs | Callbacks, SLAs | Automations |

Relevant Dagster features to reference when appropriate:
- **Asset checks**: inline data quality assertions that run after each materialization (row count, null rate, freshness, schema drift)
- **Freshness policies**: declare how stale an asset is allowed to be; Dagster surfaces violations in the UI and can alert
- **Partitioned assets**: natural fit for date-partitioned tables -- backfill a partition, re-run a date range, inspect per-partition status
- **Sensors**: trigger runs from external events (new file in S3, row count threshold crossed)
- **Resource definitions**: abstract source connections so the same asset works across dev/staging/prod

#### Approved Library Exceptions
Some libraries are low-level infrastructure (not pipeline tools) and can be named in content:
- **SQLAlchemy**: the standard Python database connectivity layer. Covers every source in the book (PostgreSQL, MySQL, SQL Server, SAP HANA) with a unified API. No comparable alternative exists for multi-database extraction. Reference freely in extraction code examples.

#### Loaders and Extractors
The book is tool-agnostic for loaders/extractors -- patterns apply regardless of whether you use dlt, Airbyte, Fivetran, SQLMesh, or custom Python. When a tool is relevant as an example (schema policy behavior, load format defaults, type inference), name it explicitly but frame it as "how tool X handles this," not as a prescription.

Relevant tools to reference by concern:
- **dlt**: schema evolution (`evolve`/`freeze`/`discard_row`), automatic type inference, incremental cursor state
- **Airbyte / Fivetran**: managed connectors, normalization step, sync frequency limits
- **SQLMesh / dbt**: downstream transformation layer -- referenced when explaining what "belongs downstream" vs. in the C

### Scope
- **Batch processing only.** CDC, real-time streaming, and event-driven architectures are out of scope. This book is about scheduled extractions that run in batches.

### Voice & Tone
- **Technical but human.** Write like a senior engineer explaining to a peer over coffee, not like a textbook. But don't overdo the personality -- no comedy, no "you know this table" filler. Direct and confident without performing.
- **Opinionated.** Take stances. Say "don't do this" when something is bad. Use "we" naturally.
- **Fast to scan.** Every pattern should be consultable in under 2 minutes. Use callouts, not walls of text.
- **No filler.** If a sentence doesn't teach something, cut it.
- **No unjustified generalizations.** Don't write "most tables" or "some systems" unless you can back it up. Use the domain model tables or name specific systems.
- **No "it's not A, it's B" constructions.** This is a dead giveaway of AI writing. Don't negate then correct. Just state what it is.
- **No staccato sequences.** Avoid chains of short declarative sentences ("X does this. Y does that. Z fails."). Combine related ideas into longer, flowing sentences using conjunctions, relative clauses, or dashes. One well-constructed sentence beats three choppy ones. The "A happened, B didn't" / "A changed; B didn't" construction is a specific variant of this -- merge the two halves into one sentence that explains the relationship.
- **No dangling summary sentences.** A single sentence sitting alone as its own paragraph after a section, list, or codeblock -- acting as a "punchline" or "takeaway" -- is an AI writing pattern. Integrate the conclusion into the preceding paragraph, or remove it if the content already speaks for itself.
- **Punctuation palette.** Use: `/ ~ * - _ . : , ; ? ! | -> <- <->` and `--` (double hyphen). Never use `—` (em dash). The `--` is a deliberate choice: this is a SQL book, and `--` is a SQL comment. It does the same job as an em dash but belongs here.
- **Progressive complexity.** Start each pattern with the simple version -- the default that works for the common case. Tease the harder problems as you go, naming the pattern where they're resolved but not explaining it yet. The most complex patterns (0310, 0311, 0309) earn their weight because earlier patterns have already shown why the simple approach breaks down. The reader should arrive at a complex pattern already understanding why it exists.

### Writing Conventions

#### Markdown
- Use Obsidian-flavored markdown (wikilinks `[[]]`, callouts `> [!tip]`, tags)
- Frontmatter YAML on every content file (see `_templates/`)
- Internal links use wikilinks: `[[pattern-name]]` not `[pattern-name](pattern-name.md)`

#### SQL Codeblocks
- Always use ` ```sql ` fencing with illustrative data from the domain model. Use whichever table best fits the pattern (`invoices` for hard deletes, `events` for append-only, `orders`/`order_lines` for header-detail, etc.)
- Use `-- source: transactional` or `-- source: columnar` comments to clarify context
- Keep examples short (< 30 lines). If longer, split into steps.
- Use ANSI SQL when possible. When dialect matters, show both with tabs:

```
> [!example]- BigQuery
> ```sql
> SELECT * FROM `project.dataset.table`
> ```

> [!example]- PostgreSQL / MySQL
> ```sql
> SELECT * FROM schema.table
> ```
```

#### Comparing Approaches
- Use two sequential codeblocks with bold titles above each. Side-by-side will be handled in LaTeX at publish time.
- Do NOT use HTML divs, obsidian-columns, or horizontal-blocks for SQL comparisons (nesting codeblocks breaks rendering)

#### Query + Result Tables
- For showing what a query returns, put the query in a codeblock and the result in a markdown table immediately after:

```
```sql
SELECT id, updated_at FROM orders WHERE id IN (1, 2);
```

| id | updated_at |
|---|---|
| 1 | 2026-03-01 14:00:00 |
| 2 | NULL |
```

#### Diagrams
- Use Mermaid fenced blocks (` ```mermaid `)
- Prefer flowchart TD for data flows, erDiagram for schemas, sequenceDiagram for process interactions
- Keep diagrams simple, max 10-12 nodes. Split complex flows into multiple diagrams.
- Use consistent node naming: `src_*` for sources, `stg_*` for staging, `tgt_*` for targets
- Line breaks inside node labels: use `<br>` (HTML), never `\n`. Mermaid does not support `\n`.

#### Corridors in Pattern Files
- Most patterns apply to both corridors (Transactional → Columnar and Transactional → Transactional).
- Where implementation differs, note it explicitly under a `### By Corridor` section using the collapsible `> [!example]-` callout format.
- When there is no "By Corridor" section, the pattern applies to both corridors as written.

#### Callouts (Obsidian syntax)
- `> [!warning]` --Gotchas and traps
- `> [!tip]` --Practical advice
- `> [!danger]` --Anti-patterns (things that will break)
- `> [!info]` --Context or background
- `> [!example]` --Collapsible examples

### File Organization
- Pattern files live in their chapter folder
- Each pattern follows `_templates/pattern-template.md`
- Filenames: `kebab-case.md`
- Tags use format: `#pattern/category` (e.g., `#pattern/incremental`, `#pattern/extraction`)

### Author Workflow
1. **Human writes**: Core opinions, real-world war stories, the "why"
2. **Claude structures**: Fills in template sections, writes SQL examples, creates diagrams, formats consistently
3. **Human reviews**: Edits tone, adds nuance, approves

### Domain Model (for illustrative SQL)
All SQL examples use this shared fictional domain. The goal is always to clone the data exactly from source to destination --the patterns are about how to get it there correctly.

| Table | Description |
|---|---|
| `orders` | Has `updated_at` but it's unreliable |
| `order_lines` | Detail table, no own timestamp |
| `customers` | Soft-delete via `is_active` |
| `products` | Schema mutates (new columns appear) |
| `invoices` | Open/closed document pattern. Open invoices get hard-deleted regularly |
| `invoice_lines` | Detail table for invoices, no own timestamp. Has own `status` per line. Hard-deleted independently (not just cascade) |
| `events` | Append-only, partitioned by date |
| `sessions` | Sessionized clickstream |
| `metrics_daily` | Pre-aggregated, overwritten daily |
| `inventory` | Sparse cross-product (SKU x Warehouse). Most rows are zeros. Zero row vs missing row is ambiguous in destination. |
| `inventory_movements` | Append-only log of all stock changes (sales, adjustments, transfers, write-offs). Activity signal for 0207/0208. |

#### Corridors
- **Transactional → Columnar**: e.g. PostgreSQL → BigQuery (primary)
- **Transactional → Transactional**: e.g. PostgreSQL → PostgreSQL

### Key Terminology
- **EL**: Extract-Load with zero transformation (theoretical ideal)
- **ECL**: Extract, Conform, Load --the C handles type casting, metadata columns, null coalescing, key synthesis. Conforming ≠ transforming: if it changes business logic, it doesn't belong here.
- **Source of cursor**: The field or mechanism used to detect new/changed rows
- **Metadata columns**: Fields injected during extraction (`_extracted_at`, `_source_hash`, `_batch_id`)
- **Open document**: A record that can still be modified (e.g., draft invoice)
- **Closed document**: A record that is immutable (e.g., posted invoice)
- **Hard rule**: A constraint actually enforced by the system (FK, unique index, NOT NULL). If the database enforces it, it's hard.
- **Soft rule**: Something a stakeholder tells you is "always" true but the system doesn't enforce. Business expectations, not data guarantees. Your pipeline must survive soft rules being wrong. Examples from the domain:
  - `orders` --"An order always has at least one line." Until someone creates an empty order and saves it.
  - `orders` --"Orders go from `pending` → `confirmed` → `shipped`." Until support manually sets one back to `pending`.
  - `order_lines` --"Quantities are always positive." Until a return is entered as `-1`.
  - `invoices` --"Only open invoices get deleted." Until someone deletes a closed one through a back-office script.
  - `invoice_lines` --"A line's status always matches the header's status." Until one line is disputed while the rest are approved.
  - `customers` --"Emails are unique." Until a customer registers twice with the same email and nobody enforced a unique index.
