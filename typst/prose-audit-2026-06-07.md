# AI-Writing Audit Report: ECL Patterns Book

## 1. Summary

**Findings by severity (book prose, excluding false positives):**

| Severity | Count |
|---|---|
| High | 17 |
| Medium | 43 |
| Low | 24 |

**Findings by rule (after dropping false positives flagged in the raw data):**

| Rule | Count | Notes |
|---|---|---|
| staccato | 38 | Dominant pattern; clusters in Ch02, Ch06, and the Appendix engine-quirks list |
| dangling-summary | 27 | Second most common; lone punchline paragraphs after lists/codeblocks |
| rule-of-three | 13 | Mostly "Three X..." count-announcements; several are low-confidence |
| negative-parallelism | 11 | "it's not A, it's B" / "X, not Y" constructions |
| significance-inflation | 8 | "most important", "matters most", vague long-term tails |
| copula-inflation | 3 | "was the bridge", "workhorse" |
| ai-vocab | 2 | "intricacies", "showcase" |
| weasel | 3 | "the engineering community", "probably", "experts"-style |
| other / elegant-variation | 5 | "bleeding money", glossary cursor conflation, etc. |

**Dropped as false positives** (per the auditors' own notes): lines 98, 504-area, 4860, 5088, 4941, 6281, 7242, 7777, 7965, 8209, 8237, 8351 (where flagged as non-violations), plus the literal-character "underscore" duplications.

**Overall assessment:** The book is **mostly clean and reads like genuine senior-engineer prose.** There is no purple AI vocabulary problem, no "delve/leverage/seamless" infestation, and the opinionated voice the style guide demands comes through. The real systemic issue is **rhythm, not vocabulary**: short declarative sentences chained in twos and threes (staccato), and lone summary sentences hanging after lists and code blocks (dangling-summary). Together these two rules account for ~65 of the ~84 real findings. Both are mechanical to fix -- merge with a dash or semicolon, or fold the punchline into the preceding sentence. The worst single concentration is the **Appendix "Engine Quirks" section (lines 8572-8578)**, which is built entirely from dash-delimited fragments and needs structural reformatting into proper bullet lists. Fix the staccato and dangling-summary clusters and the book will read as fully human-authored.

## 2. Term-consistency analysis

This is strong overall -- the book is **largely consistent**, which is impressive across nine chunks. A controlled vocabulary is clearly being maintained for the core concepts (`_extracted_at`, `_batch_id`, `_source_hash`, `append-and-materialize`, `full replace`, `conforming` vs `transforming`). The drifts below are the only genuine ones.

| Concept | Variants found | Standardize on | Notes |
|---|---|---|---|
| **Change-detection field** | `cursor` (all chunks), `high-water mark` (I-b, III, IV-b, VI, VIII) | **`cursor`** | The glossary (L8727) actively *equates* the two, defining "Cursor" as "A high-water mark". This is the most important fix: per the book's own terminology, `cursor` is the primary term ("source of cursor"). Standardize on `cursor` in prose; mention `high-water mark` once in the glossary as "sometimes called", then stop using it interchangeably. |
| **Full reload** | `full replace` (dominant), `full refresh` (front-matter, I-a), `full table replacement` (IV-a), `full table replace` | **`full replace`** | "full refresh" appears only in early chunks; CLAUDE.md uses both "full replace" and "full refreshes". Pick `full replace` for the strategy noun and reserve "full refresh" only where quoting the author's opinion verbatim. |
| **Synthetic key** | `synthetic key`, `surrogate key` (IV-a), `natural key` vs `surrogate key`, `_source_key` | **`synthetic key`** (+ `_source_key` for the column) | IV-a introduces `surrogate key` alongside `synthetic key` in the same chunk. These are the same concept here -- collapse to `synthetic key`. `natural key` is legitimately distinct (the real business key) and should stay. |
| **MERGE key** | `merge key` (I-b, IV-a), `MERGE key` (IV-a), `match key` | **`merge key`** | IV-a uses both `merge key` and `MERGE key` (capitalized) in one chunk. Reserve uppercase `MERGE` for the SQL statement only; the key is `merge key`. |
| **Dedup mechanism** | `dedup view`, `append-and-materialize log`, `append log`, `orders_log`, `*_raw` | (no change needed) | These are distinct layers, not drift -- the *view* vs the *log* distinction is load-bearing and correctly maintained. L7341 even gives the naming rule. Keep. |
| **Tiers** | `hot/warm/cold tier` (consistent) | (no change) | Fully consistent across VI-a, VI-b, VIII. Good. |

Everything else (`staging table`, `partition swap`/`staging swap`, `idempotent`/`stateless`, `mojibake`, `conforming layer`/`ECL layer`) is used consistently or with deliberate, meaningful distinction. **No action needed beyond the four drifts above.**

## 3. High & medium findings

### staccato (high/medium)

- [ ] L8572 [VIII Appendix a] "BigQuery DML concurrency... Flood it and statements fail outright -..." -> **Restructure all four engine-quirk blocks (L8572-8578) into proper bullet lists with full sentences.** This is the single worst cluster in the book.
- [ ] L8574 [VIII Appendix a] "Snowflake ... PRIMARY KEY and UNIQUE constraints are not enforced --..." -> Same: convert dash-fragments to declarative sentences in a bullet list.
- [ ] L8576 [VIII Appendix a] "ClickHouse ... ALTER TABLE ... UPDATE and ... DELETE are async --..." -> Same; also fold the stranded fragments "Duplicates coexist..." and "ORDER BY is fixed at creation".
- [ ] L8578 [VIII Appendix a] "Redshift ... COPY from S3 is the only performant bulk load..." -> Same bullet-list restructure.
- [ ] L3038 [III Incremental a] "...confirms zero about its absence. The count drifts silently." -> "...confirms zero about its absence -- the count drifts silently."
- [ ] L5318 [V Conforming b] "A new field appears... A field disappears... A field that was always a string is now..." -> "Fields appear when the application ships a feature, disappear when someone removes them from the API response, and change type when a third-party integration decides a value that was always a string is now sometimes a number."
- [ ] L292 [I Foundations a] "This is great for the applications... It's less great for you" -> merge per suggestion (see also negative-parallelism L294, same locus -- fix together).
- [ ] L300 [I Foundations a] "You'll run into a handful of these in the wild. They all speak SQL, but they all speak it differently." -> "...in the wild -- they all speak SQL, just differently."
- [ ] L710 [I Foundations a] "Some are true. Most are 'true until they're not.'" -> "Some are true; most are 'true until they're not.'"
- [ ] L892 [I Foundations b] "Data is clean in demos. In production, it's a negotiation." -> "Data is clean in demos -- in production it's a negotiation."
- [ ] L1259 [I Foundations b] "...a requirement. When someone says 'real time,' they usually mean 'faster than it is now.'" -> merge into one sentence (also self-explaining restatement).
- [ ] L1282 [I Foundations b] "An append without dedup doubles... A cursor that advances... A staging table that doesn't get cleaned up..." -> join the three failure modes with semicolons.
- [ ] L1562 [II Full Replace a] "...the job fails. Staging is never loaded. No partition is replaced. The data in production stays exactly as it was." -> "...the job fails -- staging is never loaded, no partition is replaced, and production stays exactly as it was."
- [ ] L1645 [II Full Replace a] "...N destination operations. The orchestrator sees one job succeed or fail -- not thirty." -> merge into one sentence with em-dash-substitute clause.
- [ ] L1648 [II Full Replace a] "The extraction reruns cleanly... The staging load reruns... The partition operations rerun... Rerun it and move on." -> collapse to one flowing sentence per suggestion.
- [ ] L1684 [II Full Replace a] "Consumers see complete data throughout. Rollback is dropping the staging table..." -> "...throughout, and rollback is simply dropping the staging table without touching production."
- [ ] L1991 [II Full Replace b] "The cold run is cheap and slow. The warm run is the core... The hot run is fast..." -> join with semicolons.
- [ ] L2149 [II Full Replace b] "Simple. The source still scans the full table --..." -> drop the one-word opener; "The filter is straightforward -- the source still scans..."
- [ ] L2876 [III Incremental a] "One hop is straightforward. Two hops gets expensive. Three hops -- stop and reconsider." -> "One hop is straightforward; two hops gets expensive; at three hops, stop and reconsider."
- [ ] L3125 [III Incremental b] "The old line numbers are gone, the new ones look like fresh inserts." -> state the causal relationship explicitly (see suggestion).
- [ ] L3231 [III Incremental b] "The closed-side cursor captures it. ...with its final state. Clean." -> drop "Clean."; merge into one sentence.
- [ ] L3779 [IV Load Strategies a] "No key matching, no partition rewriting, no update logic. This also makes this load strategy the most naive and fragile." -> "...no update logic -- which also makes it the most fragile load strategy in the book."
- [ ] L3960 [IV Load Strategies a] "Add a column... update three places... Multiply by however many tables..." -> join into one sentence.
- [ ] L4239 [IV Load Strategies b] "...aren't constantly consuming it. More 'on demand' than 'live'." -> absorb the fragment: "...want current data on demand, not a live feed they're constantly polling."
- [ ] L4357 [IV Load Strategies b] "The two writes must be treated as a single pipeline unit. If the append... fails, consumers see different versions..." -> join with em-dash-substitute.
- [ ] L4466 [IV Load Strategies b] "A pipeline that fails silently is worse... Your orchestrator can tell you... You find out Monday morning..." -> merge the escalation into one sentence.
- [ ] L5098 [V Conforming b] "A row in the wrong partition is an internal cost issue... A row in the wrong month in a revenue report gets escalated to the CFO." -> "A row in the wrong partition costs one extra partition scan; a row in the wrong month... gets escalated to the CFO -- the stakes differ by an order of magnitude."
- [ ] L5088 [V Conforming a] -> **DROP (false positive, auditor confirmed: one well-formed sentence).**
- [ ] L5920 [VI Operating a] "...alert fatigue, and alert fatigue produces ignored alerts, and ignored alerts produce missed failures." -> collapse the ladder per suggestion.
- [ ] L5918 [VI Operating b] "Zero rows extracted successfully, schema changed upstream, row counts drifting apart..." -> tighten the three fragments per suggestion.
- [ ] L6354 [VI Operating b] "A column gets renamed, a type changes... a new column appears... an old one disappears..." -> consolidate the four-item list.
- [ ] L6356 [VI Operating b] "a dropped column becomes NULLs... a type change produces casting errors... a 90% row count drop looks like a quiet day..." -> wrap with "drift propagates silently:" lead per suggestion.
- [ ] L6408 [VI Operating a] "Multiply that by 1,000 tables... The contracts that felt free at 20 tables become a bottleneck at scale." -> merge.
- [ ] L6976 [VI Operating c] "Every date-based partition... is silently wrong. You discover it when someone notices March 5th orders showing up in May..." -> reorder and merge per suggestion.
- [ ] L7309 [VII Serving a] "...zero read-time overhead. Powerful for dashboards... but the logic is baked into the write path..." -> join the fragment with em-dash-substitute.
- [ ] L7489 [VII Serving a] "Cost optimization is engine-specific. What saves money on BigQuery... is irrelevant on Snowflake..." -> "Cost optimization is engine-specific: what saves money on BigQuery... is irrelevant on Snowflake, which bills by warehouse time."
- [ ] L7333 [VII Serving a] "They didn't design the schema, they don't know what append-and-materialize means, and they will SELECT * on a 3TB table..." -> "...and left to their own devices they will SELECT * on a 3TB table."
- [ ] L7893 [VII Serving b] "erp_prod.orders reads cleaner... Use connection__schema from day one and..." -> join with em-dash-substitute.
- [ ] L8163 [VIII Appendix a] "The load is a pure append -- no per-row cost, no partition rewrite." -> "The load is a pure append: no per-row cost and no partition rewrite."
- [ ] L8294 [VIII Appendix a] "Atomic, metadata-only. Grants follow the table object, not the name --..." -> "The swap is atomic and metadata-only: grants follow the table object rather than the name, so..."
- [ ] L8361 [VIII Appendix a] "Delete by the declared range, not by what's in staging. If Saturday had rows last run..." -> merge per suggestion.
- [ ] L8438 [VIII Appendix a] "Sort keys and dist keys are changeable... Sort key serves the role of... Dist key controls how data distributes..." -> combine the three.
- [ ] L8471 [VIII Appendix a] "Same result, different syntax. QUALIFY filters directly... Engines that don't support it need the subquery wrapper." -> combine.
- [ ] L9207 [VIII Appendix b] "Each connector handles authentication... Available as a cloud service or self-hosted via Docker." -> "...incremental state; Airbyte is available as a cloud service or self-hosted via Docker."
- [ ] L9395 [VIII Appendix b] "DuckDB is an embedded columnar engine... MotherDuck adds a cloud layer on top:..." -> join with em-dash-substitute.
- [ ] **DROP false positives:** L2317 (trailing-sentence cut, low), L8209/L8237 (auditor confirmed clean sentences).

### dangling-summary (high/medium)

- [ ] L109 [front-matter] "That was the moment ECL took shape: Extract, Conform, Load." -> fold into prior sentence per suggestion.
- [ ] L740 [I Foundations a] "What the loader does in response to that diff is your schema policy... When it fails, alert it --..." -> fold the cross-references into the preceding paragraph / figure caption.
- [ ] L1350 [I Foundations b] "The distinction matters for storage cost and compaction frequency, but not for correctness -- which is the thing you actually care about." -> fold into preceding sentence.
- [ ] L1517 [II Full Replace a] "Rows outside the target range are never touched. The rest of the table stays exactly as it was." -> drop the second sentence (restatement) or merge with em-dash.
- [ ] L2311 [II Full Replace b] "This is a last resort, not a first choice. Reach for it when there is genuinely no cursor..." -> fold into preceding paragraph.
- [ ] L2443 [II Full Replace b] "The danger is the silence around the exclusion." -> cut; the next paragraph makes the point directly.
- [ ] L2447 [II Full Replace b] "This is how a pipeline correctness problem becomes a business trust problem. The consumer makes a decision based on a gap they didn't know existed." -> fold the conclusion into the scenario sentence.
- [ ] L3050 [III Incremental a] "When neither exists -- and for most tables in most sources, neither does -- you need a detection mechanism that works from the outside." -> absorb into the tombstone paragraph.
- [ ] L3557 [III Incremental b] "Use the Dual Cursor approach below instead." -> fold into preceding paragraph.
- [ ] L4183 [IV Load Strategies b] "Consumers query orders and see the current state. The view abstracts the log entirely." -> drop the second sentence or merge.
- [ ] L5153 [V Conforming a] "Encoding problems are invisible until they're not." -> merge into the first substantive sentence.
- [ ] L5172 [V Conforming a] "If this returns rows with clean characters, the encoding is working. If it returns mojibake..." -> fold into the lead-in before the code block.
- [ ] L5921 [VI Operating a] "Your pipelines should be loud, so that you can rest comfortably when there is silence." -> integrate into the preceding calibration sentence.
- [ ] L6295 [VI Operating b] "About two-thirds of tables in a typical pipeline never need this split... Reserve tiered extraction for the tables where size or cost forces the compromise." -> fold the second sentence into a tier paragraph or cut (already stated in "One Size Fits None").
- [ ] L6410 [VI Operating b] "Allocate checks where the blast radius of a silent failure justifies the overhead..." -> fold into the preceding paragraph.
- [ ] L6557 [VI Operating a] "A gate that silently holds data without anyone knowing it held is a judgment call that nobody can audit --..." -> integrate into preceding "document the policy per table" sentence.
- [ ] L6776 [VI Operating c] "One episode of duplicates, even if you fix it in an hour, can make consumers question every number..." -> fold into the preceding sentence.
- [ ] L7020 [VI Operating c] "This is one of the operational arguments for defaulting to stateless: recovery is simpler because there's less state to manage." -> remove (restates prior sentence) or fold.
- [ ] L7051 [VI Operating c] "A pipeline that's producing correct data going forward while three months of bad data sits in the destination is a pipeline that's still wrong --..." -> remove or fold into the "fix the code AND rebuild the range" sentence.
- [ ] L7232 [VII Serving a] "None of this is in the pipeline. It's what you build after the data lands, as a service to the people who consume it." -> fold into preceding sentence.
- [ ] L7341 [VII Serving a] "If you're not sure which is which, the one with fewer rows is the view." -> integrate into the naming-convention sentence.
- [ ] L7409 [VII Serving a] "...skip every partition before January 2026. Always include it." -> "...skip every partition before January 2026, so always include it."
- [ ] L7751 [VII Serving b] "When they disagree, the source has unlogged changes -- that's a source data quality problem, not a pipeline problem." -> merge into preceding sentence (also negative-parallelism -- fix together).
- [ ] L7926 [VII Serving b] "The semantic rename belongs here, not in the ECL layer." -> integrate into the preceding gold-layer paragraph.
- [ ] L8280 [VIII Appendix a] "Keeps exactly one row per key regardless of age -- safe with any extraction strategy. All version history is gone, but every current row survives." -> fold into the compaction lead-in; cut the second sentence.
- [ ] L9019 [VIII Appendix b] "The asset graph and partition-based backfills have justified the learning curve many times over." -> fold into the Dagster recommendation sentence.
- [ ] **Lower-priority (medium but placeholder context):** L8622, L8628 [VIII Appendix a] -- standalone prose under TODO diagram comments; revisit when the diagrams land rather than rewriting now.

### negative-parallelism (high/medium)

- [ ] L294 [I Foundations a] "This is great for the applications that sit on top of them. It's less great for you" -> "The applications on top love this design; you don't, because what you need -- bulk-scanning millions of rows -- is the opposite of what they were optimized for."
- [ ] L1448 [II Full Replace a] "Full replace is not 'DELETE everything, INSERT everything.'" -> "The right approach is a staging swap or partition-level replace, not a bare DELETE + INSERT cycle." (Reframe to lead with the positive.)
- [ ] L5253 [V Conforming a] "The C in ECL makes data survive the crossing -- it doesn't restructure it." -> "The C in ECL is about survivability, not restructuring."
- [ ] L2305 [II Full Replace b] "You either miss changes or you load everything every run." -> "The standard incremental approach fails silently: changes go missing, or you fall back to loading everything every run."
- [ ] L5306 [V Conforming a] "You've traded one problem (they can't query JSON) for another (they can't join tables)..." -> recast per suggestion (moves the complexity; wrong join fails silently where JSON syntax error fails loudly).
- [ ] L5094 [V Conforming b] "isn't in the pipeline -- it's in the business reports that consume the data" -> "The most visible consequence of timezone handling shows up in business reports, not in the pipeline itself."
- [ ] L7708 [VII Serving b] "immune to compaction -- the history is the table, not a side effect of a retention window" -> "...the SCD2 table is the history, with no dependency on a retention window."
- [ ] L7661 [VII Serving b] "Not every table has a natural event log." -> "Some tables have no natural event log."
- [ ] L7688 [VII Serving b] "Not all state changes produce events." -> "Some state changes leave no events."
- [ ] L7751 [VII Serving b] -> see dangling-summary above; the rewrite also removes the "not a pipeline problem" negation.

### rule-of-three (medium)

- [ ] L712 [I Foundations a] "developers who built the application... stakeholders who describe what should be true... systems that were designed when the data volume was a hundredth..." -> cut the weak third item; "...developers who built the application without extraction in mind and stakeholders who describe what should be true rather than what is."
- [ ] L2740 [III Incremental a] "stateless, idempotent, safe to retry, safe to parallelize" -> drop "safe to parallelize" (overlaps "safe to retry"): "stateless, idempotent, safe to retry."
- [ ] L4543 [V Conforming a] "Three use cases earn the column:" -> drop the count heading or merge Debugging + Reconciliation.
- [ ] L6041 [VI Operating b] "three questions dominate your scheduling decisions --..." -> drop the "three questions" frame; lead with "scheduling forces concrete decisions:".
- [ ] L6265 [VI Operating b] "The model is three zones, each with its own cadence and extraction method." -> drop the count: "The model splits the table by time range -- recent (hot), current period (warm), historical (cold)..."

### significance-inflation (medium)

- [ ] L926 [I Foundations b] "This is where @hard-rules-soft-rules becomes critical." -> "See @hard-rules-soft-rules for the full breakdown of what the database actually enforces."
- [ ] L4388 [IV Load Strategies b] "This is the single most important property for reliability" -> state what it does: "A load is idempotent if running it twice with the same batch leaves the destination unchanged..."
- [ ] L6444 [VI Operating b] "Financial data with high-precision decimals is exactly the data where this matters most..." -> "Financial data with high-precision decimals is where precision loss is hardest to detect..."

### copula-inflation / ai-vocab / weasel (medium)

- [ ] L111 [front-matter] "Conforming was the bridge --..." -> "Conforming covered everything the data needed to survive the crossing without changing what it meant."
- [ ] L3907 [IV Load Strategies a] "The workhorse of incremental loading" -> "The standard incremental load -- and the most expensive operation in columnar engines."
- [ ] L200 [I Foundations a] "handling the intricacies of how to query a database" -> "knowing how to query a database without blowing it up with full table scans."
- [ ] L8810 [VIII Appendix b] "Broken cursor showcase" -> "Broken cursor example."
- [ ] L1375 [II Full Replace a] "The engineering community has overcomplicated data pipelines by defaulting to incremental" -> "Most pipelines default to incremental when the table doesn't need it."
- [ ] L8974 [VIII Appendix b] "if a source system has an API, there's probably an Airflow provider package for it" -> "most source systems have an Airflow provider package."

### elegant-variation (high)

- [ ] L8727 [VIII Appendix a] glossary "Cursor -- A high-water mark..." -> redefine to disambiguate: "Cursor -- The field or mechanism used to detect new or changed rows since the last run, typically MAX(updated_at) or MAX(id). Sometimes called a high-water mark; this book uses 'cursor' throughout." (Anchors the term-consistency fix in §2.)

## 4. Low findings (opportunistic)

- L244 [I Found a] three-sentence staccato ending "That's conforming." -> merge.
- L326 [I Found a] dangling intro before bullet list -> cut or make substantive.
- L756 [I Found b] "It's also the most commonly broken one." -> merge into one sentence.
- L936 [I Found b] "(and a pain)" -> cut the filler aside.
- L1068 / L1088 [I Found b] near-duplicate "wrong mental model" sentences -> keep one; drop "bleeding money" colloquialism at L1068.
- L1174 [I Found b] "A table refreshed every 15 minutes is fresh. A table refreshed nightly is stale..." -> semicolon merge.
- L1375 [II FR a] "Extract every row, replace the destination, done. No cursor state... It resets the world..." -> merge.
- L1395 [II FR a] "The simplest pipeline... No state, no checkpoints, no drift." -> merge with em-dash.
- L1503 [II FR a] "Options:" fragment -> "When that happens, scope the scan..."
- L1585 [II FR a] "Safe to retry." fragment -> fold into prior sentence.
- L1840 [II FR a] "Production never changed." punchline -> merge.
- L2317 [II FR b] trailing "The hash is a fingerprint..." -> cut (restates).
- L3046 [III Inc a] "the clean solution" -> "the simplest approach."
- L3419 [III Inc b] "correctness parameter, not a performance parameter" -> "a correctness parameter: size it for worst-case late arrival..."
- L4033 [IV LS a] "The difference is not subtle:" -> merge with prior sentence.
- L4241 [IV LS b] forward-pointing compaction sentence -> move into preceding paragraph.
- L4503 [V Conf a] "cost/benefit ratio" padding -> "Three metadata columns, each with a different purpose."
- L4685 [V Conf a] "For most pipelines, hash wins..." verdict -> append to Hash paragraph.
- L4808 [V Conf a] "Three categories of implicit cast..." -> drop the count.
- L5294 [V Conf a] "This works well when consumers are data-conscious..." -> fold into preceding paragraph.
- L5336 [V Conf b] "The natural choice -- flexible, queryable..." fragment -> fold into VARIANT sentence.
- L5957 [VI Op b] "that's a dashboard item, not a notification" -> borderline; leave unless tightening.
- L6645 [VI Op c] "It's also far simpler to reason about --..." -> merge into preceding sentence.
- L6879 [VI Op c] "removes the duplicate problem structurally" -> lead with mechanism.
- L6976 [VI Op a] "nothing fails, nothing alerts." -> recast as one clause.
- L7115 [VII Serv a] three-consumer trio, third shifts dimension -> cut to two genuine cases.
- L7234 [VII Serv a] "over the life of the table" tail -> cut.
- L7445 [VII Serv a] two-sentence partition-gap -> join.
- L7617 [VII Serv b] "right now, not any point in the past" -> "reflects only the current state."
- L7693 [VII Serv b] "only the changed rows, not the entire table" -> drop "not the entire table."
- L7857 [VII Serv b] "at the worst possible moment" -> cut the drama.
- L8312 / L8349 / L8370 [VIII App a] short paired sentences after code blocks -> em-dash merges.
- L9219 [VIII App b] Fivetran "Fully managed, zero code, zero infrastructure." fragment -> merge.
- L9375 [VIII App b] Redshift "legacy choice... moved ahead" verdict -> plain comparison.
- L2427 [II FR b] "Three situations justify it" -> low; qualify the rarer third or leave.

**Drop (false positives, auditor-confirmed):** L98, L504, L4860, L4941, L5088, L6155, L6281, L7242, L7777, L7965, L8209, L8237, L8576-region clean fragments where noted, L9375 if author intends the verdict.

## 5. War stories

Five new callouts audited; four have real issues, all the **dangling-summary moral-of-the-story** pattern that the style guide explicitly bans for war stories:

- [ ] L774 [high] "The cursor was doing its job perfectly against a signal that simply wasn't being maintained." -> fold into the preceding sentence + the @periodic-full-replace cross-ref.
- [ ] L2507 [high] "The trap in this pattern is real and we've watched it catch other teams; the only reliable defense..." -> remove (preceding sentence already states the defense), or integrate the "footnote in pipeline code" detail.
- [ ] L2507 [medium, rule-of-three] "in the table description, in the docs, in the conversation..." -> collapse first two (same thing): "in the documentation and in the conversation with whoever consumes it."
- [ ] L2631 [medium, rule-of-three] "stateless, idempotent, immune to whatever the source did or didn't track" -> "stateless and idempotent regardless of whatever the source did or didn't track."
- [ ] L5106 [high] timezone double-shift coda "The lesson is to warn loudly about exactly which side does the conversion..." -> weave the lesson into the narrative instead of appending a moral+resolution paragraph (see suggestion).

The war-story voice is otherwise good -- concrete, first-person, no filler. The only systemic tic is ending each story with a tidy moral. Cut the morals; the stories already teach.

## 6. CLAUDE.md

Two of these are **factual contradictions** that will actively misdirect an agent and should be fixed first:

- [ ] L241 [high] Publishing workflow says "write prose in Obsidian → import into Typst" but Book Status (L221) says the vault is legacy and new content goes straight to Typst. -> "Workflow: write prose directly in `typst/book.typ` → embed SVG charts → `typst compile` → PDF. (The Obsidian vault is legacy; do not add new content there.)"
- [ ] L91 [medium] "The most complex patterns (0310, 0311, 0309)" -- **0311 does not exist** (Ch03 caps at 0310). -> "(0309, 0310)".
- [ ] L39 [medium] Tool name-drops "reserved for the software recommendations section in the Appendix (0805-0807)" -- those numbers no longer exist (Appendix is now SQL Dialect Reference + Decision Flowchart). -> "(the Appendix)".
- [ ] L26 [high] Core Thesis itself violates the negative-parallelism rule: "it's not conforming, it belongs downstream." -> "If it changes business logic, it belongs downstream, not in the C."
- [ ] L82 [medium] Rule heading "No 'it's not A, it's B' constructions" is narrower than its body (which also bans "not only X, but also Y"). -> rename to "**No negative parallelism**".
- [ ] L85 [medium] Banned-vocab list duplicates words (boasts, testament, crucial, pivotal, underscore) already governed by the copula/significance rules. -> remove duplicates; keep each word in its specific rule only.
- [ ] L81 [low] One bullet bundles "unjustified generalizations" + "weasel attributions" (different remedies). -> split into two bullets.
- [ ] L152 [medium] Callouts list includes `[!example]` with no Typst equivalent, and omits `ecl-story` (described in prose but not in the list). -> add `ecl-story` to the list; note `[!example]` is Obsidian-only or document its Typst mapping.
- [ ] L88 [low] "One well-constructed sentence beats three choppy ones." is itself a dangling aphorism inside the anti-staccato rule. -> merge into the surrounding sentence.

## 7. Recommended action order

Cheapest mechanical fixes first; biggest payoff per minute at the top.

1. **CLAUDE.md factual fixes (L241, L91, L39, L26).** Minutes each, and they prevent an agent from following wrong guidance. Do these before any prose work.
2. **Appendix Engine Quirks restructure (L8572-8578).** One contained block, four sub-items, highest-severity staccato cluster -- converts to bullet lists mechanically.
3. **Term-consistency sweep (§2).** Global find/replace with judgment: `high-water mark`→`cursor` (keep one glossary mention), `surrogate key`→`synthetic key`, `MERGE key`→`merge key`, `full refresh`→`full replace` in strategy contexts. Fix the glossary entry (L8727) as the anchor. Do this as one pass before editing individual sentences so you don't re-touch lines.
4. **Negative-parallelism (11 findings).** Each is a one-sentence rewrite; high visibility because it's the most recognizable AI tell. Knock these out in one pass.
5. **Dangling-summary (27 findings).** Mostly "fold into preceding sentence" or "delete." Group by chunk and do chapter-by-chapter. Include the five war-story morals (§5) here -- same operation.
6. **Staccato merges (remaining ~30).** Semicolon/dash joins. Mechanical but numerous; do per chunk alongside the dangling-summary pass since they often sit in the same paragraphs.
7. **Rule-of-three count-drops + significance/copula/vocab/weasel (medium).** Single-word or single-clause edits; mop up in a final pass.
8. **Low findings.** Opportunistic -- fix any you pass while doing 4-7; don't make a dedicated pass.
9. **CLAUDE.md rule-hygiene fixes (L82, L85, L81, L152, L88).** Lowest urgency since they affect future authoring, not current text; batch them last.

**Bottom line:** ~10 high-leverage edits (CLAUDE.md facts + Engine Quirks + term sweep + negative-parallelism) remove the most conspicuous AI signals. The remaining staccato/dangling-summary work is volume, not difficulty -- a couple of focused chapter passes finishes it.