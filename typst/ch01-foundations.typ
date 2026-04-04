#import "theme.typ": palette, ecl-tip, ecl-warning, ecl-danger, ecl-info
= The EL Myth
<the-el-myth>
#quote(block: true)[
  #strong[One-liner:] Pure EL doesn't exist. The moment data crosses between systems, you're conforming whether you admit it or not.
]

== ETL, ELT, and the Pitch That Forgot Something
<etl-elt-and-the-pitch-that-forgot-something>
You've heard of ETL. It's the standard for a reason: it's most useful when the Business layer and the Data layer are handled by the same person. This is hugely common among analysts, who do the vast majority of data consumption. But handling the intricacies of how to query a database without blowing it up with full table scans? That's a skill most of them don't have.

This is one of the reasons most companies, once they reach a certain size, choose to use an OLAP database for analysis while their ERP and internal apps keep using OLTP for ingestion.

#ecl-warning(
  "OLAP vs OLTP",
)[OLAP (analytical databases like BigQuery, Snowflake, and ClickHouse) stores data in a columnar way, optimized for full-column `SUM()`s and aggregations. OLTP (transactional databases like PostgreSQL, MySQL, and SQL Server) stores data row by row, optimized for inserts and transactional operations.]

The ELT framework (Extract, #strong[Load];, Transform) came as a byproduct of this. The pitch: "let's Extract and Load the data raw into our OLAP, then Transform it there." A valid way of thinking -- which sadly forgets how fundamentally different OLTP and OLAP handle things, and how incompatible all SQL dialects really are. I can't simply copy a `DATETIME2` from SQL Server into BigQuery and expect it to behave. I have to cast, handle timezones, normalize dates, inject metadata, and of course -- most of the time I want to update incrementally, which (believe me) can increase complexity ten-fold.

== The Reality
<the-reality>
Pure EL doesn't exist. The moment you move data between systems, something has to give. Types need casting, nulls need handling, timestamps need timezones. I call it #strong[conforming];, and it's unavoidable.

So, what we're going to be talking about is ECL: #strong[Extract, Conform, and Load];. The C covers type casting, null handling, timezone normalization, metadata injection, key synthesis. Everything the data needs to land correctly on the other side. If it changes what the data #emph[means];, it belongs downstream.

== What About the T?
<what-about-the-t>
If the analysts want to transform afterwards -- aggregate, pivot, build dashboards -- that's their domain. But there's still a chapter in this book for helping them out. Because left unsupervised, an analyst will `SELECT *` on a 3TB events table in Snowflake and then ask you why the bill spiked. We cover how to protect them (and your invoice) in @query-patterns-for-analysts.

== Related Patterns
- #link(<domain-model>)[Domain Model] -- The shared schema used in every SQL example in this book
- @what-is-conforming
- @purity-vs-freshness
- @query-patterns-for-analysts

// ---

= What Is Conforming
<what-is-conforming>
#quote(block: true)[
  #strong[One-liner:] Conforming is everything the data needs to survive the crossing. If it changes what the data means, it belongs somewhere else.
]

== The Line Between Conforming and Transforming

To know what should be done by you, you must answer the following question: does this operation change what the data #emph[means];, or does it just make it land correctly?

- Casting a `DATETIME2` to `TIMESTAMP`? Conforming.
- Replacing `NULL` with `''` because BigQuery handles them differently in `GROUP BY`? Conforming.
- Converting `BIT` to `BOOLEAN`? Conforming.

None of these change the business meaning of the data. They just make it survive the crossing, and in counter example: - Calculating `revenue = qty * price`? That's transforming. - Filtering out inactive customers? Transforming. - Joining `orders` with `customers` to denormalize a name? Transforming.

You're adding business meaning that wasn't in the original row.

But here's where it gets interesting. `order_lines` has no `updated_at`. If you want to extract incrementally, you #emph[need] to join with `orders` to borrow its timestamp as your cursor. That join doesn't add business meaning -- it adds extraction metadata. You're not enriching `order_lines` with order data; you're giving yourself a `_cursor_at` so you know what to pull. That's conforming.

#ecl-warning(
  "The join test",
)[If the join adds a column the business cares about, it's transforming. If it adds a column only your pipeline cares about (`_cursor_at`, `_header_updated_at`), it's conforming.]

#figure(
  image("diagrams/ecl-conforming-vs-transforming.svg", width: 90%),
)

#ecl-tip(
  "The join test",
)[If the join adds a column the business cares about, it's transforming. If it adds a column only the pipeline cares about (`_cursor_at`, `_header_updated_at`), it's conforming.]

== The Conforming Checklist
<the-conforming-checklist>
These are the operations that belong in the #strong[C];. Each one gets its own chapter in Part IV, but here's the overview so you know what you're signing up for.

#strong[Type casting.] Every engine has its own type system, and they don't agree on anything. SQL Server's `DATETIME2` has nanosecond precision; BigQuery's `TIMESTAMP` has microseconds. PostgreSQL's `NUMERIC(18,6)` is exact; BigQuery's `FLOAT64` is not. You will lose precision if you don't map these explicitly. See @type-casting-and-normalization.

#strong[Null handling.] `NULL`, empty string, `0`, `'N/A'` -- sources use all of them, and they're not the same thing. The ECL position: reflect the source as-is. If the source has NULL, land NULL. Don't COALESCE to a default value at extraction -- that's a business decision that belongs downstream. See @null-handling.

#strong[Timezone normalization.] Source says `2026-03-15 14:30:00` -- in what timezone? If the column is `DATETIME2` or `TIMESTAMP WITHOUT TIME ZONE`, you're looking at a naive timestamp. The ECL rule: TZ stays TZ, naive stays naive. Don't convert naive to UTC unless you're certain of the source timezone -- guessing wrong silently shifts every row. Know what you're landing and document the assumption. See @timezone-conforming.

#strong[Charset and encoding.] Latin-1 source, UTF-8 destination. Most of the time you won't notice, until a customer name has an `ñ` or an `ü` and your load silently replaces it with `?` or fails entirely. This is especially common with older ERP systems and legacy OLTP sources (SAP, AS/400, Oracle SQL). See @charset-and-encoding.

#strong[Metadata injection.] Every row you land could carry `_extracted_at`, `_batch_id`, and ideally a `_source_hash`. These columns don't exist in the source. You add them during extraction so you can debug, reconcile, and reprocess later. Without them, when something goes wrong (and it will), you have no way to know which batch brought the bad data. But you have to weigh its benefits against the additional processing and eventual delays it could bring. See @metadata-column-injection.

#ecl-warning(
  "Metadata injection has a cost",
)[At scale, hashing every row for `_source_hash` adds compute on the source or in your pipeline. At 800 tables, this can add 20 minutes to an already long extraction window. Evaluate per table: high-value mutable tables earn the overhead; stable config tables usually don't.]

#ecl-warning(
  "Analysts can use your metadata",
)["When was this data pulled into our warehouse?" is a valid question, and `_extracted_at` answers exactly that. Be precise though: `_extracted_at` is when #emph[your pipeline] pulled the row, not when the row was last modified in the source. That's `updated_at` (if it exists). A row updated 3 days ago and extracted today has `_extracted_at = today`. Don't let anyone confuse the two.]

#strong[Key synthesis.] The source table has no primary key. Or it has a composite key that's 5 columns wide. Or worse, it has an `id` that gets recycled when rows are deleted. You need something stable to MERGE on, and if the source doesn't give you one, you build it: hash the business key columns into a `_source_hash` or generate a surrogate. See @synthetic-keys.

#strong[Boolean and decimal precision.] SQL Server `BIT`, MySQL `TINYINT(1)`, SAP B1 `'Y'`/`'N'` (or `'S'`/`'N'` depending on install language), PostgreSQL `BOOLEAN` -- every source has its own way of representing booleans. Similarly, `NUMERIC(18,6)` in PostgreSQL is exact while `FLOAT64` in BigQuery is not, and the rounding errors accumulate across millions of rows. Both of these are type casting concerns covered in @type-casting-and-normalization.

#strong[Nested data / JSON.] The source has a `details` column that's a JSON blob. Land it as-is -- `STRING`, `JSONB`, `VARIANT`, whatever the destination's native JSON type is. Flattening JSON into normalized tables is restructuring the data, which is transformation, not conforming. If a consumer can't query JSON, build a flattening view downstream. See @nested-data-and-json.

// ---

= Transactional Sources
<transactional-sources>
#quote(block: true)[
  #strong[One-liner:] Row-oriented, mutable, ACID. The terrain you're extracting from most of the time.
]

== What Makes a Source Transactional
<what-makes-a-source-transactional>
These databases were built to handle one row at a time, fast. INSERT a customer, UPDATE an order status, DELETE a cancelled invoice. They store data row by row, keep it consistent through transactions and locks, and index everything for quick point lookups.

This is great for the applications that sit on top of them. It's less great for you, because what you need to do -- pull thousands or millions of rows in bulk -- is the opposite of what they were optimized for. You're running full table scans on a system designed for `WHERE id = 42`.

The key thing to internalize: these sources are #strong[mutable];. A row you extracted yesterday might look different today. Or it might be gone. The database won't send you a notification about it. You have to go looking.

== The Engines
<the-engines>
You'll run into a handful of these in the wild. They all speak SQL, but they all speak it differently.

#strong[PostgreSQL.] The open source workhorse. Rich type system, `updated_at` triggers are common but never guaranteed. Some teams add them religiously, some don't bother. Watch out for TOAST compression on large text/JSON columns -- it can make extraction slower than you'd expect because the data isn't stored inline with the row.

#strong[MySQL.] Everywhere. If you've worked with a web application's database, you've probably worked with MySQL. The classic trap here is `utf8` vs `utf8mb4`: MySQL's `utf8` is actually 3-byte UTF-8, which means it can't store emoji or certain CJK characters. `utf8mb4` is real UTF-8. If you're extracting text columns coded as `utf8`, you might be getting truncated data and not know it. Also, InnoDB's `DATETIME` has no timezone information at all.

#strong[SQL Server.] The enterprise standard. You'll see it behind most .NET applications and a lot of corporate ERPs. `DATETIME2` gives you up to 100-nanosecond precision, which sounds great until you try to land it in BigQuery's microsecond `TIMESTAMP` and realize you're truncating. Licensing and access are the real pain: getting read access to a production SQL Server often involves procurement, security reviews, VPN configs, and a DBA who has 47 other priorities before your extraction project.

#strong[SAP HANA.] Column-oriented under the hood, but the applications on top of it (SAP B1, S/4HANA) treat it transactionally. Proprietary SQL dialect with its own quirks. Limited tooling for extraction compared to the others. Often sits behind thousands of auto-generated tables you're not supposed to query directly -- and in the case of S/4HANA, legally might not be allowed to. If you're extracting from SAP, you're already in a special kind of hell and you know it.

#figure(
  align(center)[#table(
    columns: (9.26%, 29.63%, 24.07%, 37.04%),
    align: (auto, auto, auto, auto),
    table.header([Engine], [Timezone trap], [Encoding trap], [Key gotcha]),
    table.hline(),
    [PostgreSQL], [`TIMESTAMP` vs `TIMESTAMPTZ`], [Usually UTF-8, but check], [TOAST on large columns],
    [MySQL], [`DATETIME` has no TZ at all], [`utf8` != real UTF-8], [`utf8mb4` migration state],
    [SQL Server], [`DATETIME2` nanosecond precision], [Latin-1 legacy common], [Access/licensing friction],
    [SAP HANA], [Varies by SAP module], [Depends on client codepage], [Legally restricted access to some tables],
  )],
  kind: table,
)

== What They All Share (That Matters for ECL)
<what-they-all-share-that-matters-for-ecl>
Despite the differences, the fundamentals are the same from an extraction perspective:

They all support `SELECT ... WHERE ...` for pulling data incrementally. That's your primary tool. Every batch extraction pattern in this book starts with a query against the source.

They all have #emph[some] mechanism for detecting changes -- `updated_at` columns, triggers, row versions -- but none of them make it easy or consistent. Every engine does it differently, every application uses it differently (or doesn't use it at all), and you can't count on any of it being reliable until you've verified it yourself.

They all have schemas that mutate. Columns get added, types get changed, tables get renamed. The application team ships a release, and suddenly your `products` table has 3 new columns you've never seen.

```sql
-- last week your extraction query was:
SELECT product_id, name, price, category FROM products;

-- after Friday's deploy:
SELECT product_id, name, price, category FROM products;
-- ERROR: column "category" renamed to "product_category"
-- also: new columns "weight_kg", "is_hazardous", "supplier_id"
```

Your pipeline needs to handle this gracefully or it #emph[will] break on a Friday night.

#ecl-warning(
  "SELECT * is valid for extraction",
)[Contrary to what every SQL best practices guide tells you, `SELECT \*` is a good default for ECL. You're cloning the table, not building a report. New column added? It lands automatically. Type changed? Your type dictionary handles it. But renames and deletes #strong[must] fail your pipeline. If `category` becomes `product_category`, your destination still has `category` receiving no new data, and that's unacceptable. Schema relaxing means: always allow additions, handle type changes, never silently accept renames or deletions. More on this in @the-lies-sources-tell.]

And they all have data quality issues that the application layer "handles" but the database doesn't enforce. These are the soft rules -- the things a stakeholder tells you are "always" true, but the schema doesn't guarantee.

== What Will Bite You
<what-will-bite-you>
#strong[Locks during extraction.] Your `SELECT` could be reading millions of rows while some poor bastard is trying to create a new invoice. In SQL Server you can use `NOLOCK` to avoid blocking, but now you're reading dirty data -- rows mid-transaction, half-updated. To be clear, I #strong[don't] recommend using NOLOCK.

#strong[NOLOCK (dirty reads, no blocking)]

```sql
-- engine: sqlserver
SELECT order_id, status, updated_at
FROM orders WITH (NOLOCK)
WHERE updated_at >= @last_extraction
```

#strong[Default (clean reads, blocks writers)]

```sql
-- engine: sqlserver
SELECT order_id, status, updated_at
FROM orders
WHERE updated_at >= @last_extraction
-- writers wait until this finishes
```

Use a read replica? Now you have replication lag and you might miss rows that were committed seconds before your query ran. There's no free lunch here; you pick your trade-off and document it. (Also, do you really trust the people making the replica at source?)

#strong[No reliable `updated_at`.] Some tables don't have one. Some have one that only fires on UPDATE, not INSERT. Some have one that the application sets manually and sometimes forgets. It may work 99.9% of the time, that still gets you chewed out when it doesn't.

```sql
-- engine: postgresql
-- what you expect:
SELECT order_id, created_at, updated_at FROM orders WHERE order_id IN (1001, 1002);
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto, auto, auto),
    table.header([order\_id], [created\_at], [updated\_at]),
    table.hline(),
    [1001], [2026-01-15 09:00:00], [2026-02-20 14:30:00],
    [1002], [2026-03-01 11:00:00], [NULL],
  )],
  kind: table,
)

Order 1002 was just created. The trigger only fires on UPDATE, so `updated_at` is NULL. Your pipeline with `WHERE updated_at >= @last_run` will never see it.

You'll build your pipeline trusting `updated_at`, and three months later discover that 2% of rows were silently missed because the column wasn't being maintained. See @create-vs-update-separation.

#strong[Hard deletes.] The row was there yesterday. Today it's gone. The source won't tell you.

```sql
-- yesterday's extraction got 4 invoices:
SELECT invoice_id FROM invoices;
```

#figure(
  align(center)[#table(
    columns: 1,
    align: (auto,),
    table.header([invoice\_id]),
    table.hline(),
    [5001],
    [5002],
    [5003],
    [5004],
  )],
  kind: table,
)

```sql
-- today's extraction gets 3:
SELECT invoice_id FROM invoices;
```

#figure(
  align(center)[#table(
    columns: 1,
    align: (auto,),
    table.header([invoice\_id]),
    table.hline(),
    [5001],
    [5002],
    [5004],
  )],
  kind: table,
)

Where's 5003? Deleted. No tombstone, no audit log, no `deleted_at` flag. Your destination still has it. Now your data says there are 4 open invoices when there are 3. Detecting hard deletes in batch extraction is one of the hardest problems in this book. See @hard-delete-detection.

#strong[Connection limits and DBA etiquette.] You're a guest on someone else's production system. Open too many connections, run queries during peak hours, or full-scan their biggest table while the month-end close is running, and the DBA will shut you down. Rightfully.

```sql
-- your "quick extraction" at 10am:
SELECT * FROM order_lines
WHERE updated_at >= '2026-01-01'
-- 12 million rows, no index on updated_at
-- full table scan, 4 minutes, 100% CPU
-- meanwhile 200 users can't save orders
```

You need to know the source system's capacity, its busy hours, and its tolerance for your workload. See @source-system-etiquette.

#strong[Encoding traps.] The `customers` table has a `name` column. It's `VARCHAR(100)` in Latin-1. You didn't know it was Latin-1 because nobody told you and the metadata just says `VARCHAR`.

```sql
-- engine: sqlserver
-- what the source has:
SELECT customer_id, name FROM customers WHERE customer_id = 42;
```

#figure(
  align(center)[#table(
    columns: 2,
    align: (auto, auto),
    table.header([customer\_id], [name]),
    table.hline(),
    [42], [José Muñoz],
  )],
  kind: table,
)

```sql
-- what lands in BigQuery after a naive load:
SELECT customer_id, name FROM customers WHERE customer_id = 42;
```

#figure(
  align(center)[#table(
    columns: 2,
    align: (auto, auto),
    table.header([customer\_id], [name]),
    table.hline(),
    [42], [Jos? Mu?oz],
  )],
  kind: table,
)

Every `ñ`, `ü`, `é` silently replaced with `?`. Or worse, the load fails entirely and you don't know why until you dig into the byte encoding. Especially common with older ERP systems and legacy OLTP sources. See @charset-and-encoding.

// ---

= Columnar Destinations
<columnar-destinations>
#quote(block: true)[
  #strong[One-liner:] Append-optimized, partitioned, cost-per-query. The terrain when you're loading into an analytical engine.
]

== What Makes a Destination Columnar
<what-makes-a-destination-columnar>
These engines store data by column. Every value in `event_date` is packed together, compressed alongside every other `event_date` in the table. Scanning one column across a billion rows is fast. Aggregation -- `SUM`, `COUNT`, `GROUP BY` -- is what they were built for. The consumers sitting on top of them are dashboards, reports, ML pipelines, analysts running ad-hoc queries.

From an ECL perspective, three properties define the landing zone:

#strong[Append-cheap, mutate-expensive.] Inserting new rows is fast and the engines optimize for it. Updating or deleting existing rows is a different story. BigQuery rewrites an entire partition on UPDATE. ClickHouse runs mutations as async background jobs with no completion guarantee. Snowflake handles it better than most, but every MERGE still costs warehouse time. Your load strategy should minimize mutations. Append first, deduplicate or materialize downstream.

#strong[Partitioned by default.] Almost every table worth its storage is partitioned, usually by date. Partition pruning controls both query performance and cost for everyone downstream. If you load data without aligning to the partition scheme, every consumer pays the price on every query. The load engineer decides the partition strategy. Get it wrong and you're taxing every analyst and dashboard for the lifetime of the table.

#strong[Cost lives in the query.] A transactional database costs you connections and CPU. A columnar destination costs you bytes scanned (BigQuery) or seconds of compute time (Snowflake, Redshift). A poorly partitioned table or a missing cluster key means every `SELECT` is more expensive than it needs to be. Your loading decisions have a direct, ongoing cost impact on everyone who reads from your tables.

== The Engines
<the-engines-1>
=== BigQuery
<foundations-bigquery>
Serverless, slot-based. No cluster to manage, no warehouse to size. You load data and Google handles the rest. The pricing model is per-byte-scanned on queries, which means your table design directly affects what consumers pay.

Loading mechanics: `bq load` from cloud storage, streaming inserts, or `LOAD DATA` SQL. Parquet and JSONL are the most common formats, however AVRO is preferred. Streaming inserts are fast but newly streamed rows may be briefly invisible to `EXPORT DATA` and table copies -- typically minutes, in rare cases up to 90 minutes -- and streamed rows can't be modified or deleted until they flush.

DML concurrency is the constraint. BigQuery removed the daily per-table DML limit, but mutating statements (`UPDATE`, `DELETE`, `MERGE`) run at most 2 concurrently per table, with up to 20 queued. Stack too many rapid merges and the queue fills -- additional statements fail outright. Prefer append + deduplicate over in-place mutation.

Partitioning is mandatory for cost control. A table without a partition scheme forces a full scan on every query. `require_partition_filter = true` protects consumers from themselves -- it rejects any query that doesn't include the partition column in the `WHERE` clause.

```sql
-- engine: bigquery
-- table definition with partition, cluster, and cost protection
CREATE TABLE `project.dataset.events` (
  event_id STRING,
  event_type STRING,
  event_date DATE,
  payload JSON
)
PARTITION BY event_date
CLUSTER BY event_type
OPTIONS (require_partition_filter = true);
```

#ecl-warning(
  "JSON columns and Parquet don't mix",
)[BigQuery *cannot* load JSON columns from Parquet files -- the job fails permanently. If your source data has JSON or semi-structured fields, load as JSONL. Or strip the JSON columns from the Parquet and load them separately. There's no workaround on the Parquet path.]

=== Snowflake
<foundations-snowflake>
Warehouse-based compute. You pay for the time the warehouse runs, regardless of bytes scanned. More predictable for budgeting, harder to attribute per-query.

Loading goes through stages: internal (Snowflake-managed) or external (S3, GCS, Azure). `COPY INTO` is the bulk loader and it's fast. Snowpipe automates continuous loading from stage to table.

Snowflake's `VARIANT` type handles semi-structured data natively. JSON, Avro, nested Parquet -- it all lands in `VARIANT` and you query it with `:` path notation. But there's a catch: loading JSON from Parquet files converts `VARIANT` to a string. You need `PARSE_JSON` on the other side to get it back into a queryable structure.

Some loaders implement full replace via `CREATE TABLE ... CLONE` from staging -- a metadata-only operation, fast -- but permissions don't carry over on Snowflake. Ensure `FUTURE GRANTS` are set or your consumers lose access after every replace.

`PRIMARY KEY` and `UNIQUE` constraints exist in Snowflake's DDL but they're #strong[not enforced];. They're metadata hints. If your pipeline relies on the destination rejecting duplicates, Snowflake won't help you. Deduplication is your problem.

```sql
-- engine: snowflake
-- bulk load from stage
COPY INTO events
FROM @my_stage/events/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

=== ClickHouse
<foundations-clickhouse>
Built for speed on append-only analytical workloads. The MergeTree engine family is the backbone -- data lands in parts, and background merges compact them over time.

Fastest engine for raw insert throughput. The trade-off is significant: no ACID guarantees. A query during an active merge might see duplicates. `ReplacingMergeTree` deduplicates by a key, but only during merges -- until the merge runs, duplicates coexist. Queries with `FINAL` force deduplication at read time, at a performance cost.

Mutations (`ALTER TABLE ... UPDATE`, `ALTER TABLE ... DELETE`) are async. You fire the statement and it returns immediately. The actual work happens whenever the merge scheduler gets to it. Consumers querying between the mutation request and the merge see pre-mutation data.

For ECL, ClickHouse works best as an append-only destination. Load everything, let the engine merge, build materialized views for the latest state. Fighting the merge model with frequent updates leads to pain.

```sql
-- engine: clickhouse
-- table definition with merge-based deduplication
CREATE TABLE events (
  event_id String,
  event_type String,
  event_date Date,
  payload String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id);
```

=== Redshift
<foundations-redshift>
PostgreSQL dialect, columnar storage underneath. Sort keys and dist keys determine how data is physically laid out and how queries perform. Get them right and range queries fly. Get them wrong and every query shuffles data across all nodes.

Loading goes through `COPY` from S3. This is the fast path. Row-by-row `INSERT` is painfully slow compared to `COPY` -- orders of magnitude slower on any meaningful volume. If your pipeline isn't using `COPY`, fix that first.

VACUUM is still a thing. Deleted rows don't free space until VACUUM runs. If your pipeline does heavy deletes (hard delete detection, merge patterns), VACUUM becomes an operational concern. Dead rows inflate scan time and storage until they're cleaned up.

Column additions are cheap. Type changes require recreating the table. A `VARCHAR(100)` that should have been `VARCHAR(500)` means a full table rebuild later. Plan your types carefully on initial load.

== Type Mapping: Where Conforming Happens
<type-mapping-where-conforming-happens>
When data crosses from a transactional source to a columnar destination, types don't translate cleanly. This is the core of the C in ECL -- and every engine has its own version of the problem.

#strong[Timestamps.] PostgreSQL's `TIMESTAMP WITHOUT TIME ZONE` landing in BigQuery becomes `TIMESTAMP`, which is always UTC. If the source stored local times without timezone info, BigQuery now treats them as UTC and every downstream consumer gets the wrong time. Snowflake distinguishes `TIMESTAMP_TZ` from `TIMESTAMP_NTZ`, so you have a choice -- but you have to make it explicitly. See @timezone-conforming.

#figure(
  align(center)[#table(
    columns: (20%, 20%, 20%, 20%, 20%),
    align: (auto, auto, auto, auto, auto),
    table.header([Source Type], [BigQuery], [Snowflake], [ClickHouse], [Redshift]),
    table.hline(),
    [`TIMESTAMP` (naive)], [`TIMESTAMP` (forced UTC)], [`TIMESTAMP_NTZ`], [`DateTime`], [`TIMESTAMP`],
    [`TIMESTAMPTZ`], [`TIMESTAMP`], [`TIMESTAMP_TZ`], [`DateTime64` with tz], [`TIMESTAMPTZ`],
    [`DATETIME2(7)` (SQL Server)],
    [Truncated to microseconds],
    [`TIMESTAMP_NTZ(9)`],
    [`DateTime64(7)`],
    [Truncated to microseconds],
  )],
  kind: table,
)

#ecl-warning(
  "BigQuery has no naive datetime",
)[Every `TIMESTAMP` in BigQuery is timezone-aware. If your source has naive timestamps, BigQuery treats them as UTC. If they were actually in `America/Santiago` or `Europe/Berlin`, every value is wrong from the moment it lands. You must conform timezone info during load.]

#strong[Decimals.] `NUMERIC(18,6)` in PostgreSQL has fixed precision. BigQuery's `NUMERIC` supports up to `NUMERIC(38,9)`. Snowflake offers `NUMBER(p,s)` with configurable precision, or `DECFLOAT` for unbound decimals -- but DECFLOAT only works with JSONL and CSV loads. Parquet doesn't support it. If your loader converts to `FLOAT64` anywhere in the pipeline, you lose precision. Financial data loaded as floating point is a bug waiting to surface. See @type-casting-and-normalization.

#strong[JSON and nested data.] JSON columns from a transactional source need special handling at every destination:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Destination], [JSON Handling], [Gotcha]),
    table.hline(),
    [BigQuery], [`JSON` type (native)], [Cannot load from Parquet. Use JSONL.],
    [Snowflake], [`VARIANT` type], [Parquet loads produce string, needs `PARSE_JSON`],
    [ClickHouse], [`String` (parse with JSON functions)], [No native JSON type in older versions],
    [Redshift], [`SUPER` type], [Semi-structured queries use PartiQL syntax],
  )],
  kind: table,
)

See @nested-data-and-json for detailed engine mappings.

== Load Formats
<load-formats>
Parquet is the fastest and most type-safe bulk load format. But BigQuery's JSON column type is not supported when loading from Parquet -- use JSONL or Avro instead. Parquet also produces strings for Snowflake's VARIANT and doesn't support DECFLOAT (Snowflake). Avro is BigQuery's preferred format -- it handles JSON natively, preserves schema, and sidesteps the Parquet-JSON limitation entirely. Snowflake and ClickHouse also support Avro loads, though Snowflake still lands complex types into VARIANT. Redshift supports Avro through `COPY` but has a 4 MB max block size limit -- Avro blocks larger than that fail the job. JSONL is slower but handles everything on every engine. CSV loses type information entirely.

#figure(
  align(center)[#table(
    columns: (12%, 10%, 18%, 28%, 32%),
    align: (auto, auto, auto, auto, auto),
    table.header([Format], [Speed], [Type Safety], [JSON Support], [Gotcha]),
    table.hline(),
    [Parquet],
    [Fast],
    [High (schema embedded)],
    [BigQuery: JSON columns unsupported, use JSONL/Avro. Snowflake: string.],
    [Decimal edge cases per engine],
    [Avro],
    [Fast],
    [High (schema embedded)],
    [BigQuery: native. Snowflake: VARIANT.],
    [Redshift: 4 MB max block size. ClickHouse needs schema registry or embedded schema.],
    [JSONL], [Medium], [Medium (inferred)], [Full support everywhere], [Slower on large volumes],
    [CSV],
    [Varies],
    [Low (everything is string)],
    [Must quote/escape],
    [Type inference can surprise you. Compressed CSV can't be split for parallel load.],
  )],
  kind: table,
)

#ecl-warning(
  "Default format recommendation",
)[BigQuery → Avro. Every other columnar destination with no JSON columns → Parquet (including Redshift, where `COPY` handles Parquet natively with automatic parallel splitting). When you hit edge cases (JSON columns on BigQuery, Snowflake DECFLOAT, Redshift Avro block size limit) → JSONL. #strong[Avoid CSV like the plague] unless your destination gives you no other choice.]

== Loading Strategies
<loading-strategies>
Your write disposition -- replace, append, merge -- determines cost, complexity, and correctness. The full spectrum is in Part IV (@full-replace-load through @hybrid-append-merge). When in doubt, full replace: it resets state, eliminates drift, and makes every run idempotent by construction. Earn the complexity of incremental only when the table justifies it.

== Schema Evolution
<foundations-schema-evolution>
New columns will appear in the source. Your loader needs a schema policy: `evolve` (add new columns automatically) or `freeze` (reject the load). Per-engine behavior for `ALTER TABLE ADD COLUMN`, type widening, and the gotchas of each engine are covered in @sql-dialect-reference. Schema policies as enforceable contracts are in @data-contracts.

#ecl-warning(
  "Naming convention lock-in",
)[Identifier normalization at load time is a conforming operation with permanent consequences. If your loader converts `OrderID` to `order_id` (snake\_case), that's the column name forever. Changing the convention later re-normalizes already-normalized identifiers, causing failures. Choose your naming convention on day one and never look back.]

== What Will Bite You
<what-will-bite-you-1>
#strong[DML concurrency (BigQuery).] BigQuery removed the daily per-table DML limit, but mutating statements run at most 2 concurrently per table with up to 20 queued. Flood it with rapid-fire merges and the queue fills -- additional statements fail outright. Batch your loads and buffer in a staging table to keep the queue clear.

#strong[Partition misalignment.] On BigQuery, every DML statement (`MERGE`, `UPDATE`, `DELETE`) rewrites the entire partition it touches -- not just the affected rows. If your batch contains data spread across 30 dates, that's 30 full partition rewrites per load run. Costs multiply fast. Keep your load batches as aligned to partition boundaries as possible: one run = one partition. If you can't control the source data spread, stage everything first and then execute one DML statement per partition.

#strong[Unique constraints that aren't.] Snowflake and BigQuery both accept `PRIMARY KEY` and `UNIQUE` in DDL but neither enforces them -- they're optimizer hints, not guarantees. ClickHouse has no unique constraint mechanism outside of `ReplacingMergeTree` (which deduplicates eventually, during merges). If your pipeline assumes the destination rejects duplicates, it won't. Deduplication is your responsibility, always. See @duplicate-detection.

#strong[Partition limits.] BigQuery caps date-partitioned tables at 10,000 partitions -- and a single job (load or query) can only touch 4,000 of them. Partition by day on a table running for 27+ years and you hit the first wall. Try to MERGE or load a batch spanning more than 4,000 dates and BigQuery rejects the job outright. The fix is partitioning by month or year instead of day, but that's a table rebuild. ClickHouse has a different version of this: each INSERT creates a new part, and MergeTree can only merge so fast. Flood it with small inserts and you trigger "too many parts" errors, which throttle further writes until the merge scheduler catches up. Batch your inserts; never insert row by row.

// ---

= The Lies Sources Tell
<the-lies-sources-tell>
#quote(block: true)[
  #strong[One-liner:] A catalog of things you'll be told are true about the source, and why your pipeline can't trust any of them.
]

Every source system comes with a set of assumptions handed to you by the team that owns it. Some are true. Most are "true until they're not." This chapter is about the ones that will eventually fail -- and what to do when they do.

The lies aren't usually malicious. They're the product of developers who built the application for a use case that didn't include data extraction, stakeholders who describe what #emph[should be true] instead of #strong[what is];, and systems that were designed when the data volume was a hundredth of what it is today. Your pipeline has to survive all of them.

== "The schema is stable"
<the-schema-is-stable>
The most common lie, and the one with the longest tail. Columns get added because a developer needed a new field. Columns get renamed because a product manager decided `cust_flg` should be `is_customer`. Columns get deleted because someone thought "nobody uses this." None of these come with a heads-up to the data team.

Two principles for surviving schema instability:

#strong[Use `SELECT *` at extraction.] You're cloning, not reporting. If the source adds a column, your extraction should pick it up automatically. A `SELECT id, name, email, ...` query is a time bomb -- the moment the source adds a column you're not listing, it silently disappears from your destination.

The detection should happen at the source, before you touch the destination. On each run, snapshot the source schema from `information_schema` and diff it against the snapshot from the previous run (Cached locally or queried on demand, depending on the frequency of update):

```sql
-- source: transactional
-- Pull current schema snapshot before extraction
SELECT
    column_name,
    data_type,
    ordinal_position,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'customers'
ORDER BY ordinal_position;
```

Your pipeline should store this result after every successful run. On the next run, compare new vs stored: columns present last run but absent now = deletion or rename -- fail immediately, before any data moves. Columns absent last run but present now = addition -- add to the destination and continue.

#figure(
  image("diagrams/schema-drift-detection.svg", width: 85%),
)

What the loader does in response to that diff is your schema policy (`evolve` or `freeze`) -- covered in @columnar-destinations. When it fails, alert it -- see @alerting-and-notifications.

#ecl-warning(
  "Schema auto-detection isn't free",
)[BigQuery's `LOAD DATA` with schema auto-detection and Snowflake's `VARIANT` both absorb new columns. But auto-detection can mistype a column (a column with only `"1"` and `"0"` values gets inferred as `BOOL`). And absorbing new columns silently means you won't notice when the column name changed and you now have two columns -- one dead and one alive -- both tracking the same concept.]

See @data-contracts for schema contract enforcement patterns.

== "`updated_at` is reliable"
<updated_at-is-reliable>
`updated_at` is the most common source of cursor for incremental extraction. It's also the most commonly broken one.

#strong[Only fires on UPDATE, not INSERT.] An `ON UPDATE` trigger or application code that only sets `updated_at` when a row is modified. New rows arrive with `updated_at = NULL`. Your incremental extraction filters `WHERE updated_at > :last_run` and misses every new row forever.

```sql
-- source: transactional
-- Orders with no updated_at -- these will never be picked up by a cursor query
SELECT COUNT(*) AS missing_cursor
FROM orders
WHERE updated_at IS NULL;
```

If this returns anything above zero, your incremental extraction is already incomplete.

#strong[Set by the application, not the database.] Application code that does `UPDATE orders SET ..., updated_at = NOW() WHERE id = :id`. A direct SQL edit from a back-office script, a database migration, or a developer with `psql` open doesn't go through the application layer. Those rows don't get a new `updated_at`. Your pipeline never sees them change.

#strong[The index isn't there.] `updated_at` exists but nobody put an index on it. Your incremental query runs a full table scan on every execution. For a table with 50M rows, that's a multi-second query just to find the 200 rows that changed. On a transactional system under concurrent load, that scan will get you a complaint (or a ban) from the DBA.

#ecl-tip(
  "Verify before you commit",
)[Before building an incremental extraction on a cursor, run three checks: (1) query `WHERE updated_at IS NULL` -- if it returns rows, you need a fallback; (2) run `EXPLAIN` on your cursor query -- confirm it hits the index; (3) if you can -- create a row, wait a minute, then update it and check that `updated_at` changed both times. If any check fails, treat this as an unreliable cursor and plan accordingly.]

See @create-vs-update-separation for the pattern when `updated_at` only fires on update, and @reliable-loads for fallback strategies.

== "Primary keys are unique and stable"
<primary-keys-are-unique-and-stable>
Three different lies bundled into one: that the PK uniquely identifies a row, that the PK stays the same across the row's lifetime, and that every table even has a PK.

#strong[The business doesn't understand unicity.] This is the most common one. You ask "what's the primary key?" and get "order\_id." You build your merge on `order_id`. A week later you start seeing duplicates or losing data. Turns out the table stores one row per `(order_id, line_number)` -- the person you asked thinks about orders, not about how the table is structured. Or it's `(product_id, warehouse_id)` for inventory, but they only ever query their own warehouse so they genuinely never noticed. The people closest to the application rarely think in terms of relational keys. Never trust a verbal description of the PK. Run the duplicate check yourself on the actual data.

```sql
-- source: transactional
-- Verify that the claimed PK actually produces unique rows
SELECT id, COUNT(*) AS occurrences
FROM orders
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;
```

If it returns rows, go back and ask which #emph[combination] of columns is actually unique. Then run the check again on that combination.

#strong[The recycled PK.] Delete a row, insert a new one, and many systems reuse the integer ID. If your destination has the old row and you receive a new row with the same ID, you have a collision -- did the old row change, or is this a completely new entity? In most pipelines you'll upsert it as an update. The old entity's history is gone (which may be exactly what you want, to be fair).

#strong[The key whose semantics changed.] A table that used to have one row per `order_id` gets a `tenant_id` column in a multi-tenant migration. Now uniqueness requires `(tenant_id, order_id)`. The column names didn't change -- the rule for what makes a row unique did. Your pipeline is still merging on `order_id` alone and quietly colliding across tenants.

#ecl-warning(
  "Nullable columns in merge keys",
)[A merge key column that allows NULLs is a silent bug. In SQL, `NULL != NULL` -- two rows where the key column is NULL won't match on a JOIN or MERGE. Your upsert might skip them and insert duplicates instead of updating. Check nullability on every column you plan to use as a merge key before you build on it. See @synthetic-keys.]

#strong[No PK at all.] Some tables were created without a primary key: reporting tables, view-like tables, tables built by BI teams -- or, god help you, by someone in Finance with direct database access. You discover this at extraction time when your upsert pattern has no merge key. Or worse: you don't discover it and insert duplicates on every run.

Run this before you commit to an extraction strategy. If the duplicate check returns rows on a column that was supposed to be unique, your merge key is broken.

#ecl-danger(
  "DDL says PK exists -- does the engine enforce it?",
)[On transactional sources, a `PRIMARY KEY` constraint is enforced -- the engine rejects duplicates. The risk is a DBA who drops it temporarily for a bulk load and forgets to recreate it. `information_schema.table_constraints` tells you what the DDL says. A query for duplicates tells you what the data does. Check both before trusting a PK as your merge key.]

See @synthetic-keys for building stable merge keys when the source can't be trusted.

== "Deletes don't happen" / "We use soft deletes"
<deletes-dont-happen-we-use-soft-deletes>
Every system that "never does hard deletes" does hard deletes. The application layer soft-deletes. The back-office script does a real `DELETE`. The developer debugging a data issue deletes the bad rows directly. The scheduled cleanup job runs every night and deletes pending records older than 90 days, and nobody told the data team.

#strong[Soft deletes that aren't consistently applied.] The `is_active = false` flag works for normal application flows. It doesn't work for the script that directly manipulates `customers` to merge duplicate accounts. Those old accounts just disappear. If your extraction only checks `updated_at`, you'll never see them go.

#strong[The "only open invoices get deleted" rule.] Classic soft rule from the domain model. Posted invoices are supposedly immutable. Then someone runs a year-end cleanup and deletes a batch of incorrectly posted invoices. Your pipeline has them as closed invoices in the destination forever. The discrepancy surfaces at audit time, not at load time; and it's your fault for not noticing.

```sql
-- source: transactional
-- Detect hard deletes by comparing yesterday's extracted IDs to today's source
-- Run on the source before extracting
SELECT COUNT(*) AS rows_in_source_today
FROM invoices;
```

```sql
-- source: columnar
-- Compare to yesterday's destination count for the same scope
SELECT COUNT(DISTINCT invoice_id) AS rows_in_destination_yesterday
FROM stg_invoices
WHERE _extracted_at::DATE = CURRENT_DATE - 1;
```

A drop in the source count with no matching deletes in the destination means hard deletes happened. The only reliable way to detect them is a full count comparison or a full ID set comparison between yesterday's destination and today's source. Incremental extraction on `updated_at` is blind to deletions by design -- deleted rows have no `updated_at` because they no longer exist.

#ecl-warning(
  "Soft delete flags have their own problems",
)[`is_active`, `deleted_at`, `status = 'deleted'` -- they all require that every code path removing a record goes through the application layer and sets the flag. Back-office scripts, direct DB access, bulk operations, and third-party integrations often don't. The flag is only as reliable as every write path that touches the table.]

See @hard-delete-detection for detection and propagation patterns.

== "Timestamps have timezones"
<timestamps-have-timezones>
The source database uses `TIMESTAMP WITHOUT TIME ZONE` (PostgreSQL) or `DATETIME` (MySQL, SQL Server). The application "knows" it's UTC. The column doesn't say so. Your pipeline reads `2026-03-06 14:00:00`, assumes UTC, and writes it to BigQuery as `2026-03-06 14:00:00 UTC`.

Except the application was deployed in Santiago, Chile. The value was stored as local time. That's `2026-03-06 17:00:00 UTC`. Every timestamp in your destination is off by 3 hours. The business has been making decisions on wrong data for two years. Nobody noticed because the relative order of events was correct and the absolute times were never spot-checked.

The daylight saving version is worse: the offset changes twice a year, so the error is inconsistent. Some rows are off by 3 hours, others by 4, depending on when they were written.

```sql
-- source: transactional
-- PostgreSQL: check whether the column has timezone info
SELECT column_name, data_type, datetime_precision
FROM information_schema.columns
WHERE table_name = 'orders'
  AND data_type IN ('timestamp without time zone', 'timestamp with time zone');
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto, auto, auto),
    table.header([column\_name], [data\_type], [datetime\_precision]),
    table.hline(),
    [created\_at], [timestamp without time zone], [6],
    [updated\_at], [timestamp without time zone], [6],
  )],
  kind: table,
)

`timestamp without time zone` means the database is storing local time with no context. You need to know the application's intended timezone before you can conform correctly. There's no way to infer it from the data.

#ecl-danger(
  "BigQuery makes this unforgiving",
)[BigQuery has no naive timestamp type. Every `TIMESTAMP` is UTC. Load a naive timestamp and BigQuery silently treats it as UTC. If it was stored in local time, every value is wrong -- and there's no way to fix it after the fact without knowing the original timezone and reloading.]

See @timezone-conforming for the full timezone conforming playbook.

== "The data is clean"
<the-data-is-clean>
The most optimistic lie. Data is clean in demos. In production, it's a negotiation.

#strong[Orphaned foreign keys.] `order_lines` rows pointing to an `order_id` that no longer exists. This happens after hard deletes (orders deleted, lines left behind), after migrations (data moved between systems with FK constraints disabled), or after application bugs. Your pipeline loads `order_lines` and the JOIN to `orders` returns NULL. Downstream reports show revenue lines with no associated order.

#strong[Duplicate "unique" values.] The `customers` table is keyed on email in the application layer -- but there's no unique index in the database. Two registrations with the same email land as two rows. Your destination has two customer records with the same email. Every report that uses email as a join key gets doubled.

#strong[Constraint violations that went unnoticed.] Negative quantities in `order_lines`. Statuses that skipped steps (`pending` directly to `shipped`). NULL values in columns that are "never null." These pass through extraction without errors because your pipeline only checks what's there -- it doesn't validate against what should be there.

```sql
-- source: transactional
-- Check orphaned order_lines
SELECT COUNT(*) AS orphaned_lines
FROM order_lines ol
LEFT JOIN orders o ON ol.order_id = o.id
WHERE o.id IS NULL;
```

```sql
-- source: transactional
-- Check duplicate customer emails
SELECT email, COUNT(*) AS occurrences
FROM customers
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;
```

Run these as pre-extraction quality checks. They won't always block your pipeline -- sometimes you load the dirty data and let downstream handle it -- but you need to know the contamination level before you decide.

#ecl-warning(
  "Conforming is not cleaning",
)[Your job in ECL is to faithfully clone the source to the destination, not to fix the application's data quality problems. An orphaned foreign key in the source should land as an orphaned foreign key in the destination. If you silently drop those rows, downstream teams are missing data and they don't know it. Load it, flag it, let the business decide what to fix.]

This is where @hard-rules-soft-rules becomes critical. Constraints the database doesn't enforce are soft rules. Your pipeline must survive them being wrong -- but it should also surface when they are wrong, so someone can fix the root cause.

== Related Patterns
<related-patterns-1>
- @hard-rules-soft-rules
- @reliable-loads
- @hard-delete-detection
- @create-vs-update-separation
- @synthetic-keys
- @timezone-conforming
- @data-contracts
- @duplicate-detection

// ---

= Hard Rules, Soft Rules
<hard-rules-soft-rules>
#quote(block: true)[
  #strong[One-liner:] If the database enforces it, it's hard. If a stakeholder told you it's always true, it's soft -- and your pipeline must survive it being wrong.
]

Every source system comes with two layers of truth: what the database actually enforces, and what the business believes is true. These are not the same thing. Your pipeline has to know the difference, because one is a guarantee and the other is a hope (and a pain).

== The Distinction
<the-distinction>
A #strong[hard rule] is enforced by the system. A foreign key constraint, a unique index, a NOT NULL column, a CHECK constraint. The database rejects violations at write time -- the bad data never lands. You can build on hard rules unconditionally. If `order_id` has a unique index, your merge key is solid. If `customer_id` is a NOT NULL foreign key into `customers`, every order line has a customer. The system guarantees it.

A #strong[soft rule] is a business expectation with no enforcement behind it. "Quantities are always positive." "Orders go from pending to confirmed to shipped." "Only open invoices get deleted." These are descriptions of how the application is #emph[supposed] to work, told to you by people who have never seen them violated -- because violations happen in prod, on the weekend, through a back-office script nobody documented.

The danger with soft rules is that they feel like hard rules. They hold for months. Your pipeline depends on them and nothing breaks. Then one day a developer runs a bulk update that bypasses the application layer, or support manually resets a status, or someone writes a cleanup script and forgets a WHERE clause. The soft rule breaks, and your pipeline either crashes, silently corrupts data, or both.

== How to Tell Them Apart
<how-to-tell-them-apart>
Query the source before you build:

```sql
-- source: transactional
-- PostgreSQL: enumerate what the database actually enforces on orders
SELECT
    tc.constraint_type,
    tc.constraint_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'orders'
ORDER BY tc.constraint_type, kcu.column_name;
```

#figure(
  align(center)[#table(
    columns: 3,
    align: (auto, auto, auto),
    table.header([constraint\_type], [constraint\_name], [column\_name]),
    table.hline(),
    [PRIMARY KEY], [orders\_pkey], [id],
    [FOREIGN KEY], [orders\_customer\_id\_fkey], [customer\_id],
    [NOT NULL], [(column constraint)], [created\_at],
  )],
  kind: table,
)

What's on this list is hard. Everything else -- every verbal description, every README, every data dictionary entry -- is soft until proven otherwise. The tell is simple: "this column is always X" with no corresponding constraint in the output above is a soft rule.

#ecl-warning(
  "Treat data dictionaries as soft",
)[A data dictionary that describes expected values (`status` is one of `pending`, `confirmed`, `shipped`) is documentation, not enforcement. Unless there's a CHECK constraint or an enum type backing it, the column will accept anything the application sends. Validate against the data, not against the dictionary.]

== The Soft Rules in the Domain Model
<the-soft-rules-in-the-domain-model>
These are all real-world patterns disguised as fictional tables. Every one of them will eventually be violated:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Table], [Soft Rule], [How It Breaks]),
    table.hline(),
    [`orders`],
    ["Always has at least one line"],
    [Empty order saved by a UI bug, or created programmatically before lines are added],
    [`orders`],
    ["Status goes `pending` → `confirmed` → `shipped`"],
    [Support team manually resets a status; migration script backdates records],
    [`order_lines`],
    ["Quantities are always positive"],
    [Return entered as `-1`; bulk correction script uses negative values],
    [`invoices`], ["Only open invoices get deleted"], [Year-end cleanup script deletes incorrectly posted invoices],
    [`invoice_lines`],
    ["Line status always matches header status"],
    [One line disputed while the rest are approved; partial cancellation],
    [`customers`], ["Emails are unique"], [Duplicate registration; customer service merges accounts manually],
  )],
  kind: table,
)

None of these have a constraint in the DDL. All of them are described as "always true" by the people who built the system.

== What to Do When a Soft Rule Breaks
<what-to-do-when-a-soft-rule-breaks>
#strong[Load the data. Don't drop rows.] A row that violates a soft rule is still a row that exists in the source. If you drop it, downstream teams are missing data and they don't know why. Dirty data is visible and fixable. Missing data is invisible and dangerous.

#strong[Surface the violation.] Log it. Flag affected rows with a metadata column so consumers can filter or investigate:

```sql
-- source: columnar
-- Flag order_lines with negative quantities at load time
SELECT
    *,*
    CASE WHEN quantity < 0 THEN TRUE ELSE FALSE END AS _flag_negative_quantity
FROM stg_order_lines;
```

#strong[Don't fix it in the pipeline.] Coalescing a negative quantity to zero, skipping orders with no lines, or normalizing a status that skipped a step are all transformations that change business data. That belongs downstream, in the hands of whoever owns the business logic. Your job is to clone faithfully and report honestly.

#ecl-danger(
  "Silent correction is the worst outcome",
)[Fixing soft rule violations in the pipeline hides the root cause. The source system keeps producing bad data, the pipeline keeps silently correcting it, and nobody ever fixes the application. Six months later, someone queries the source directly and finds data that doesn't match the destination. Now you have a trust problem and an archaeology project.]

See @data-contracts for how to formalize soft rule monitoring into data contracts with alerting.

== Soft Rules and Load Strategy
<soft-rules-and-load-strategy>
This is where soft rules cause the most damage in practice: incremental extraction.

The most common failure mode in incremental loading is building a cursor on `updated_at` when `updated_at` is a soft rule. The assumption is that #strong[every] write to a row bumps `updated_at`. It's "always" true. Until it isn't.

#strong[Open orders that don't move.] An order sitting in `pending` for three weeks never gets touched by the application. Its `updated_at` doesn't change. Your incremental extraction ignores it. Then a bulk migration script updates the `customer_id` on a set of orders and doesn't touch `updated_at`. Your pipeline never sees the change.

#strong[Header timestamps without line propagation.] An invoice header gets `updated_at` bumped when it's confirmed. The individual `invoice_lines` have no timestamp of their own and no trigger to update when the header changes. Your incremental extraction on `invoice_lines` misses every status change that came through the header.

#strong[Bulk scripts that bypass the ORM.] A price update runs directly against the `products` table via a SQL script. The ORM's `before_update` hook -- which sets `updated_at` -- never fires. Your pipeline sees no changes. The prices in the destination are stale.

The mitigations all involve some form of lookback -- accepting that `updated_at` isn't perfectly reliable and building in a safety net:

- Reprocess all rows with `updated_at` in the last $n$ days on every run, not just since the last checkpoint
- Reprocess all open/pending records unconditionally, regardless of timestamp -- their status can change without bumping `updated_at`
- Run a full replace of the current year once a week/day to catch anything the cursor missed

Full replace sidesteps all of this. A table that gets fully replaced every run doesn't care whether `updated_at` is reliable -- the whole thing comes fresh. This is another reason to default to full replace and earn incremental complexity only when the table is genuinely too large or too slow to reload. See @purity-vs-freshness.

See @cursor-based-timestamp-extraction for lookback window patterns, and @reliable-loads for building incrementals that survive unreliable cursors.

== Related Patterns
<related-patterns-2>
- @the-lies-sources-tell
- @purity-vs-freshness
- @reliable-loads
- @cursor-based-timestamp-extraction
- @data-contracts

// ---

= Corridors
<corridors>
#quote(block: true)[
  #strong[One-liner:] Same pattern, different trade-offs. Where the data goes changes how you implement everything.
]

A corridor is the combination of source type and destination type. The extraction pattern looks the same -- query the source, conform, load -- but the implementation decisions change completely depending on which corridor you're in. Get this wrong early and you'll build a pipeline with the wrong mental model from the start, then spend weeks wondering why your load strategy is bleeding money.

== The Two Corridors
<the-two-corridors>
#strong[Transactional → Columnar.] PostgreSQL to BigQuery. SQL Server to Snowflake. MySQL to ClickHouse. This is the primary corridor this book focuses on and the one you'll work in most often. The source is mutable, row-oriented, ACID. The destination is append-optimized, columnar, cost-per-query. The gap between them is wide: everything from type systems to cost models to mutation semantics is different.

#strong[Transactional → Transactional.] PostgreSQL to PostgreSQL. MySQL to SQL Server. An ERP database to a reporting replica. Same class of engine on both ends, often the same dialect. The gap is narrower but not zero -- still have schema drift, cursor problems, and hard deletes. And you now have a destination that actually enforces FK constraints, rejects duplicates, and supports cheap `UPDATE`/`DELETE`. That changes your load strategy significantly.

We don't cover Columnar → Columnar or Columnar → Transactional. The first is rare and usually handled by the analytical platform itself (BigQuery cross-region replication, Snowflake data sharing). The second is unusual enough to be its own project.

#figure(
  image("diagrams/ecl-corridors.svg", width: 90%),
)

#ecl-info(
  "Corridor effect on load strategies",
)[In T→T, MERGE is cheap and native -- the load strategy spectrum compresses. In T→C, MERGE is the costliest DML and the full spread matters.]

== What Changes at the Crossing
<what-changes-at-the-crossing>
The corridor determines your constraints. Getting this wrong means building a pipeline with the wrong mental model from the start.

#strong[Mutation cost.] In a transactional destination, `UPDATE` and `DELETE` are cheap, indexed, and ACID. You can MERGE freely. In a columnar destination, mutations are expensive -- BigQuery rewrites entire partitions, ClickHouse schedules async jobs, Snowflake burns warehouse time. Your load strategy in T→C should minimize mutations. In T→T, you can lean on `INSERT ON CONFLICT` or `MERGE` without the same cost anxiety.

#strong[Constraint enforcement at the destination.] A transactional destination can enforce `PRIMARY KEY`, `UNIQUE`, and `FOREIGN KEY`. If you try to load a duplicate, the database rejects it. You can rely on this as a safety net. A columnar destination won't. BigQuery and Snowflake accept PK/UNIQUE in DDL but don't enforce them. ClickHouse has no unique constraint outside of `ReplacingMergeTree`'s eventual deduplication. In T→C, deduplication is always your problem. In T→T, you can configure the destination to enforce it.

#strong[Cost model.] Transactional destinations cost CPU and IO -- roughly proportional to the rows you touch. Columnar destinations cost bytes scanned (BigQuery) or compute time (Snowflake, Redshift). A badly written conform step in BigQuery that forces a full table scan on every run doesn't just waste time -- it charges you for it, repeatedly, for the lifetime of the pipeline.

#strong[Type system gap.] The wider the gap between source and destination type systems, the more conforming the C has to do. T→T with the same engine (PostgreSQL → PostgreSQL) has almost no type gap. T→C (PostgreSQL → BigQuery) means navigating timezone coercion, decimal precision, JSON handling, and format compatibility. See @columnar-destinations for the full type mapping.

== Where to Process
<where-to-process>
Every conforming operation -- a CAST, a NULL coalesce, a hash key -- runs #emph[somewhere];. The question is where, and what it costs you.

There are four execution points:

#strong[Source.] The source database does the work inside the extraction query. JOINs for cursor borrowing, CAST for type normalization, CONCAT for synthetic keys.

```sql
-- source: transactional
-- engine: postgresql
-- Conform at source: cast types, synthesize key, inject metadata in the extraction query
SELECT
    order_id::TEXT || '-' || line_number::TEXT AS _source_key,
    order_id,
    line_number,
    quantity::NUMERIC(10,2) AS quantity,
    NOW() AT TIME ZONE 'UTC' AS _extracted_at
FROM order_lines
WHERE updated_at >= :last_run;
```

Free if the source can handle it and you're running at 2am. Dangerous if you're on a busy production ERP at 10am -- you're adding work to someone else's production database, and the DBA will find you.

#strong[Orchestrator / middleware.] Python, Spark, a cloud function. You do the transformation in code between extraction and load. You control it fully, but you're adding infrastructure, memory, and an extra data hop that you pay for. Justified when the source can't express it in SQL, or when volume makes centralized processing necessary.

#strong[Staging area.] Land raw in a staging table or dataset on the destination, then transform there before writing to the final table. Common pattern in BigQuery (stage raw → merge to final). Keeps the extraction fast and simple, but all processing costs are on the destination's meter.

#strong[Destination (directly).] Transform inside the final load query. No staging, no intermediate hop. Works well when the transform is simple and the destination query engine is cheap. In T→T this is often the right call. In T→C, a complex transform in the load SQL can scan more data than necessary and inflate costs.

#ecl-warning(
  "Push work to whoever's idle",
)[Source at 2am? Let it do the work. Production ERP at 10am? Extract raw, process downstream. "Cheapest" isn't just infrastructure cost -- it factors in system load and how much the source team will hate you. In T→C, prefer conforming at the source or orchestrator to avoid expensive destination compute. In T→T, the destination is often the cheapest place to process.]

== Transactional → Columnar
<transactional-columnar>
This is the harder corridor. The full details of the destination are in @columnar-destinations, but the strategic implications for the crossing:

#strong[Append by default.] Mutations are expensive. Your instinct to MERGE every changed row is the wrong default. Append raw, deduplicate or materialize downstream. Reserve MERGE for cases where append genuinely doesn't work.

#strong[Partition alignment is your responsibility.] The destination has no FK constraints, no row-level locks, no automatic partition management. You decide how data is physically laid out. Load in partition-aligned batches or you're paying for your own mess on every downstream query.

#strong[Type conforming happens at the crossing.] Naive timestamps, decimal precision, JSON columns -- all of these need explicit handling before data lands. The destination won't reject bad types gracefully; it'll silently coerce them or fail the job. See @type-casting-and-normalization.

#strong[The cost of mistakes compounds.] A wrong partition strategy, a missing cluster key, an unnecessary full-table scan in your load logic -- these aren't one-time costs. Every downstream query pays for them forever.

== Transactional → Transactional
<transactional-transactional>
The narrower corridor. Same class of engine on both ends, but don't let that make you complacent.

#strong[You can use the destination's constraints.] Configure `PRIMARY KEY` and `UNIQUE` on the destination and let the database enforce them. A duplicate load attempt gets rejected at the database level instead of silently creating bad data. This is a genuine advantage over T→C.

#strong[`INSERT ON CONFLICT` / `MERGE` is cheap.] Unlike columnar engines, a transactional destination handles upserts efficiently. You can run them frequently without cost anxiety. This changes your load strategy -- you can afford to be more aggressive with incremental merges.

#strong[Dialect differences still bite.] The source and destination might both be "SQL" but speak it differently. PostgreSQL's `ON CONFLICT DO UPDATE` is not MySQL's `ON DUPLICATE KEY UPDATE` is not SQL Server's `MERGE`. Function names, string handling, date arithmetic, identifier quoting -- all of these differ. See @sql-dialect-reference for the full comparison.

#strong[You still have all the source problems.] Hard deletes, unreliable `updated_at`, soft rules, schema drift -- none of these go away because the destination is also transactional. You still need to detect deletes, handle cursor failures, and survive schema changes. The destination being "easy" doesn't mean the source got simpler.

#ecl-info(
  "Patterns apply to both corridors",
)[Where the implementation differs, chapters note it explicitly under "By Corridor." When nothing is called out, assume the pattern applies to both.]

== Related Patterns
<related-patterns-3>
- @transactional-sources
- @columnar-destinations
- @type-casting-and-normalization
- @sql-dialect-reference

// ---

= Purity vs.~Freshness
<purity-vs-freshness>
#quote(block: true)[
  #strong[One-liner:] The fundamental tradeoff in batch ECL: perfectly stable data requires full replaces at low frequency. Fresher data requires incremental complexity. The right answer depends on the table, the consumer, and the SLA.
]

Every pipeline decision -- how to extract, how often to run, how to load -- is a position on this tradeoff. Most pipelines take a position without realizing it, and then you inherit someone else's unexamined defaults six months later when something breaks.

== The Two Ends
<the-two-ends>
#strong[Purity] means the destination is an exact clone of the source at a given point in time. No drift, no missed rows, no accumulated damage from soft rule violations or unreliable cursors. A full replace achieves this -- every run resets the world. You pull everything, you replace everything, the destination matches the source exactly as of the extraction timestamp.

#strong[Freshness] means how recently the destination reflects the source. A table refreshed every 15 minutes is fresh. A table refreshed nightly is stale by mid-morning.

The tension between them is structural. Full replace maximizes purity but caps freshness -- you can only refresh as often as a full scan completes. Incremental maximizes freshness but trades purity -- missed rows, unreliable cursors, and accumulated drift are inherent to the approach. Every incremental pipeline carries a purity debt that grows until the next full reset corrects it.

Neither extreme is universally correct. The right point on the spectrum depends on the table, the consumer, and what "fresh enough" actually means.

== Why Full Replace Is the Purity Champion
<why-full-replace-is-the-purity-champion>
A full replace has properties that incremental extraction fundamentally can't match:

#strong[It's stateless and idempotent.] Run it twice, same result. No cursor state persisting between runs, no checkpoint files to manage, no accumulated decisions from prior executions. If something goes wrong, rerun -- the destination will be correct.

#strong[It catches everything.] Hard deletes, retroactive corrections, soft rule violations, schema drift, rows that were missed by a prior incremental -- a full replace picks up all of it because it doesn't rely on the source to signal changes. It reads the current state of every row.

#strong[It has no drift accumulation.] An incremental pipeline that misses a row today still has that wrong row tomorrow, and the day after. A full replace that runs tonight corrects everything that was wrong since the last full replace.

The cost is the freshness ceiling. If a full scan of `orders` takes 3 hours, the freshest you can be with a pure full replace strategy is 3 hours behind -- and that's assuming the scan starts the moment the last one ends. For most tables and most businesses, this is completely acceptable. For a handful, it isn't.

== Why Incremental Carries a Purity Debt
<why-incremental-carries-a-purity-debt>
Incremental extraction is a performance optimization -- a necessary one when the table is too large to scan completely within the schedule window, but an optimization nonetheless, with all the fragility that implies.

The cost is real and often underestimated:

#strong[Cursor reliability is a soft rule.] The assumption that every write to a row bumps `updated_at` is an expectation, not an enforcement. Bulk scripts bypass it. ORM hooks miss it. Back-office tools don't know it exists. Every row that changes without bumping the cursor is a row your pipeline will never see update. See @hard-rules-soft-rules.

#strong[Hard deletes are invisible.] A deleted row leaves no trace for a cursor-based extraction to find. You need a separate delete detection mechanism -- a full ID comparison, a count reconciliation, a tombstone table -- which adds complexity and its own failure modes. @hard-delete-detection

#strong[High frequency has a monetary cost.] 288 extractions per day (every 5 minutes) means 288 load jobs, 288 sets of DML operations on the destination, 288 opportunities for partial failures. On BigQuery, that's 288 jobs counting against your DML quota. On Snowflake, that's warehouse time burning through the day. The cost of freshness is real.

#strong[Drift accumulates silently.] A missed row today is still wrong tomorrow. An incremental that has been running for 6 months with a slightly unreliable cursor has 6 months of accumulated drift that nobody has quantified. The destination looks correct -- it has data -- but it doesn't match the source.

== Classifying a Table
<classifying-a-table>
How you classify the table determines everything that follows. Work through these in order:

#strong[\1. What does the business actually need?] "Real time" almost never means real time. It means "faster than it is now." Press for a concrete number. "I need it every 15 minutes" is different from "I need it with no more than a 15-minute delay" -- and both are different from "I need it when I click refresh." Giving consumers a way to trigger an on-demand extraction can reduce scheduled frequency dramatically, cutting cost without sacrificing the freshness they actually use.

#strong[\2. How long does a full scan take?] Measure it. Don't estimate. A 500k-row table on a production ERP at 2am might scan in 4 minutes. The same table at 10am might take 40. If the scan fits comfortably inside your schedule window, full replace is the answer and the conversation is over.

#strong[\3. Does it have hard deletes?] If yes and the table is small enough for a full replace, use full replace -- it handles deletes automatically. If it's too large: you need incremental plus a separate delete detection strategy, which is significant added complexity.

#strong[\4. Does the source rewrite history?] Retroactive corrections are incompatible with incremental. A pricing table where last quarter's prices get adjusted, an ERP where journal entries get reversed and reposted -- a cursor on `updated_at` misses these entirely. Full replace is the only safe option, regardless of table size.

#strong[\5. Is the cursor reliable?] Verify it before committing. Query for NULL `updated_at` values. Run EXPLAIN on the cursor query and confirm it hits an index. Create a row and update it and confirm the timestamp changes both times. If any check fails, the cursor is a soft rule and your incremental will accumulate drift.

#figure(
  image("diagrams/0108-purity-freshness.svg", width: 80%),
)

#figure(
  align(center)[#table(
    columns: (28%, 16%, 16%, 40%),
    align: (auto, auto, auto, auto),
    table.header([Table type], [Full scan fits window?], [Reliable cursor?], [Recommendation]),
    table.hline(),
    [Dimension / config (`products`)], [Yes], [--], [Full replace every run],
    [Pre-aggregated (`metrics_daily`)], [Yes], [--], [Partition-level replace],
    [Append-only (`events`)], [No], [Yes (immutable)], [Append],
    [Large mutable (`orders`)],
    [No],
    [Yes],
    [Scoped replace (if changes cluster); else A+M or merge by read/write profile],
    [Large mutable, hard deletes (`invoices`)], [No], [Yes], [Scoped replace + delete detection + periodic full],
    [Large mutable, unreliable cursor], [No], [No], [Scoped replace + periodic full],
  )],
  kind: table,
)

== The Hybrid: Periodic Full + Intraday Incremental
<the-hybrid-periodic-full-intraday-incremental>
For tables where you need both purity and freshness -- mutable, large, sub-daily SLA -- the hybrid is the answer. Run a full or scoped replace nightly to reset purity. Run incremental extractions intraday to deliver freshness.

#ecl-warning(
  "The incremental doesn't need perfection",
)[It doesn't need to catch hard deletes. It doesn't need to handle retroactive corrections. It doesn't need a lookback window. The nightly full replace will correct everything the incremental missed. Design the intraday incremental to be fast and simple -- a tight cursor window, no delete detection, no complexity -- because it's not the source of truth. The full replace is.]

This also means the incremental's failure mode is manageable. If it misses a run, the data is stale by one interval until the next incremental or the nightly full. If it accumulates drift, the nightly full resets it. The incremental is a freshness layer on top of a reliable foundation.

See @cursor-based-timestamp-extraction for how to design the intraday incremental to coexist cleanly with the periodic full.

== The SLA Conversation
<the-sla-conversation>
Have this conversation before you build anything.

"We need the data in real time" is a starting position, not a requirement. When someone says "real time," they usually mean "faster than it is now." The real question is: what decision does this data inform, and how stale can it be before that decision is wrong? A sales dashboard reviewed at 9am every morning is not harmed by a nightly full replace that completes at 6am. A fraud detection system that needs to act on transactions within minutes is a genuinely different animal -- and also not a batch problem.

Most business SLAs, when pressed to a concrete number, land somewhere between 30 minutes and daily. And most of the time the actual need is "no more than X delay" rather than "updated every X minutes" -- the distinction matters because on-demand extraction (a sensor or manual trigger in your orchestrator) can satisfy the former without the cost of continuous scheduling.

When the SLA genuinely requires sub-hourly continuous refresh: accept it, build for it, and document the purity cost explicitly. The business is choosing freshness over purity. They should understand that the destination may drift, that hard deletes won't be reflected immediately, and that the pipeline complexity -- and the infrastructure cost -- is higher as a result. That's a valid business decision. Make sure it's a conscious one.

#ecl-tip(
  "Start with full replace",
)[Document why you deviated. The default position is full replace. Every deviation toward incremental should be a documented decision: what made full replace infeasible, what purity tradeoffs were accepted, and what the plan is for correcting drift. If you can't articulate why you need incremental, you probably don't.]

== Related Patterns
<related-patterns-4>
- @hard-rules-soft-rules
- @full-scan-strategies
- @scoped-full-replace
- @cursor-based-timestamp-extraction

// ---

= Idempotency
<idempotency>
#quote(block: true)[
  #strong[One-liner:] If rerunning the pipeline changes the destination, you have a bug.
]

// ---

== What It Means
<what-it-means>
An idempotent pipeline produces the same destination state whether it runs once, twice, or ten times with the same input. No extra rows, no missing rows, no side effects from the previous run bleeding into the next one. The destination after run N+1 is indistinguishable from the destination after run N -- assuming the source didn't change between them.

This sounds obvious until you realize how many pipelines fail it. An append without dedup doubles the data on retry. A cursor that advances before the load confirms creates a permanent gap. A staging table that doesn't get cleaned up causes the next run to load stale data on top of fresh data. Every one of these is a pipeline that works perfectly on the first run and breaks on the second.

// ---

== Why It's the Foundation
<why-its-the-foundation>
Every other reliability property -- retries, backfills, failure recovery, concurrent runs -- depends on idempotency. If your orchestrator retries a failed run, it needs to know that re-executing the pipeline won't corrupt the destination. If you backfill a date range, you need to know that reprocessing already-loaded data won't create duplicates. If two runs overlap because the first one hung, you need to know the destination survives both.

Without idempotency, retries are dangerous, backfills require manual cleanup, and every failure becomes a unique investigation. With it, the recovery playbook for every failure is the same: run it again.

// ---

== Full Replace Gets It for Free
<full-replace-gets-it-for-free>
A pipeline that drops and reloads the entire table on every run is idempotent by construction. There's no prior state to interfere with, no cursor to manage, no accumulated history that a retry could corrupt. Run it once, run it five times -- the destination is the same because every run rebuilds it from scratch.

This is the strongest argument for full replace as the default. You don't have to think about idempotency because the architecture hands it to you. The moment you move to incremental, you leave this safe zone and take on the burden of proving that your pipeline still produces the same result regardless of how many times it runs, in what order, or after what failures.

// ---

== Incremental Has to Earn It
<incremental-has-to-earn-it>
Incremental pipelines accumulate state across runs: a cursor position, a set of previously loaded keys, a log of appended rows. That state creates surface area for idempotency violations. The most common ones:

#strong[Cursor advances before load confirms.] The high-water mark moves forward, but the data it points past never made it to the destination. The next run starts from the new position and the gap is permanent (unless a lookback window covers it -- see 0406). Fix: advance the cursor only after the destination confirms the load.

#strong[Append without dedup.] A retry appends the same batch again, and now the destination has two copies of every row. The pipeline "succeeded" both times, but the destination is wrong. Fix: use a dedup mechanism -- `INSERT ... ON CONFLICT` on transactional engines, a `ROW_NUMBER()` dedup view on columnar engines (0404).

#strong[Stateful staging that doesn't clean up.] The pipeline writes to a staging table, then loads from it. If the staging table isn't truncated before each run, a retry loads the previous batch plus the new one. Fix: truncate staging at the start of every run, not the end.

#strong[Non-deterministic extraction.] The same query returns different results on different runs -- because the source changed between runs, or because the query uses `NOW()` in a way that shifts the window. This is harder to fix because the source is a moving target. Stateless window extraction helps by anchoring the window to a fixed offset rather than tracking state.

// ---

== The Test
<the-test>
The simplest way to verify idempotency: run the pipeline, snapshot the destination, run the same pipeline again with the same parameters, compare. If anything changed -- row count, column values, metadata -- the pipeline isn't idempotent and you need to understand why before it goes to production.

#ecl-tip(
  "Automate the idempotency test",
)[For your critical tables, a scheduled job that runs the pipeline twice on a staging copy and compares the results catches idempotency violations before they hit production. Especially valuable after pipeline changes -- a new column, a modified cursor, a changed load strategy can all break idempotency in ways that a single run won't reveal.]

// ---

== Idempotency by Load Strategy
<idempotency-by-load-strategy>
Each load strategy in Part IV has a different relationship with idempotency:

#figure(
  align(center)[#table(
    columns: (33.33%, 33.33%, 33.33%),
    align: (auto, auto, auto),
    table.header([Load strategy], [Idempotent?], [Why]),
    table.hline(),
    [Full replace], [By construction], [Every run rebuilds from scratch -- no prior state to interfere],
    [Append-only],
    [At the table level, no. At the view level, yes],
    [Retries append duplicates, but the dedup view still returns correct state],
    [MERGE / upsert], [Yes], [Same key + same data = same result, regardless of how many times it runs],
    [Append and materialize],
    [At the view level, yes],
    [Same as append-only: duplicates in the log, correct state in the view],
    [Hybrid], [Yes, if both sides are idempotent], [Two destinations means two surfaces to verify],
  )],
  kind: table,
)

The table reveals the pattern: full replace and MERGE are unconditionally idempotent. Append-based strategies are idempotent #emph[at the consumer level] (the view), not at the storage level (the log). The distinction matters for storage cost and compaction frequency, but not for correctness -- which is the thing you actually care about.

// ---

== Statelessness and Idempotency
<statelessness-and-idempotency>
These two properties are related but distinct. A pipeline is #strong[stateless] if it can run on a fresh machine with no prior context. A pipeline is #strong[idempotent] if running it multiple times produces the same result. You can have one without the other:

#figure(
  image("diagrams/0109-stateless-idempotent.svg", width: 95%),
)

The goal is the top-left quadrant: stateless and idempotent. Full replace lives there naturally. Everything else requires deliberate design to get there -- and @reliable-loads covers the mechanics.

// ---

== Related Patterns
<related-patterns-5>
- @purity-vs-freshness -- full replace maximizes both purity and idempotency; incremental trades idempotency guarantees for freshness
- @full-replace-load -- idempotent by construction
- @merge-upsert -- idempotent by key matching
- @append-and-materialize -- idempotent at the view level
- @reliable-loads -- the operational mechanics of checkpoint placement, retry, and recovery that depend on idempotency
- @stateless-window-extraction -- the extraction pattern that achieves both statelessness and idempotency

// ---
