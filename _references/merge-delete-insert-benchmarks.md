# MERGE vs Delete-Insert Benchmarks

Research notes from fact-checking Chapter 4 (Merge/Upsert, Delete-Insert alternative). Verified 2026-04-07.

## BigQuery

- MERGE limited to 2 concurrent operations per table; additional queue. INSERT has no such limit.
- DELETE of whole partition is metadata-only (0 bytes, 0 slots). Scattered key DELETE rewrites partitions like MERGE.
- MERGE scans target side of join + rewrites every touched partition.
- Fine-grained DML (2025 GA) reduces write amplification but limits to 1 concurrent DML per enabled table.
- Sources:
  - [BigQuery DML docs](https://cloud.google.com/bigquery/docs/data-manipulation-language)
  - [BigQuery DML on partitioned tables](https://cloud.google.com/bigquery/docs/using-dml-with-partitioned-tables)
  - [MERGE optimization (Anas Aslam)](https://medium.com/google-cloud/bigquery-merge-optimization-13fc7147efbf)
  - [DML limits (Kirill Demidov)](https://medium.com/@kirkademidov/merge-in-bigquery-and-dml-limits-how-to-overcome-upsert-restrictions-dc7507d6d997)
  - [DELETE+INSERT vs MERGE](https://medium.com/data-engineers-notes/delete-insert-vs-merge-in-bigquery-d9c39536be33)

## Snowflake

- MERGE rewrites entire micro-partitions containing matched rows.
- Well-clustered table: MERGE can prune 99%+ of micro-partitions.
- Benchmark: 620K rows, 17s single-partition vs 95s across 1992 partitions (4.5x slower, 140x more data written).
- Gen2 warehouses (2025): up to 4.4x DML improvement, ~25% cost reduction.
- Sources:
  - [Select.dev: Snowflake MERGE analysis](https://select.dev/posts/snowflake-merges)
  - [Snowflake Gen2 DML blog](https://www.snowflake.com/en/engineering-blog/dml-performance-snowflake-gen2-warehouses/)

## Redshift

- MERGE is a macro: creates temp table + DELETE + INSERT internally. Always has temp table overhead.
- AWS own docs recommend DELETE+INSERT for upserts.
- REMOVE DUPLICATES mode is faster than WHEN MATCHED/NOT MATCHED.
- Sources:
  - [AWS MERGE docs](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html)
  - [AWS merge by replacing rows](https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html)
  - [Redshift Observatory MERGE white paper](https://www.redshift-observatory.ch/white_papers/downloads/MERGE.html)

## PostgreSQL

- ON CONFLICT DO UPDATE: even no-op upserts generate 2 WAL records (HEAP LOCK + COMMIT). Datadog measured disk writes doubling.
- HOT updates possible when no indexed columns change, but ON CONFLICT often touches the conflict column.
- PostgreSQL 15+ MERGE: ~30% faster than ON CONFLICT for multi-condition operations.
- Sources:
  - [Datadog: Debugging Postgres Performance](https://www.datadoghq.com/blog/engineering/debugging-postgres-performance/)
  - [pganalyze: PG15 MERGE vs ON CONFLICT](https://pganalyze.com/blog/5mins-postgres-15-merge-vs-insert-on-conflict)
  - [Cybertec: UPDATE vs DELETE+INSERT](https://www.cybertec-postgresql.com/en/is-update-the-same-as-delete-insert-in-postgresql/)

## MySQL

- ON DUPLICATE KEY UPDATE acquires exclusive lock on conflicting row. More deadlock-prone than DELETE+INSERT.
- INSERT ON DUPLICATE KEY with SELECT: unsafe for statement-based replication (forces row-based in MIXED mode).
- REPLACE INTO is semantically DELETE+INSERT, changes auto_increment, causes fragmentation.
- Sources:
  - [MySQL InnoDB locks](https://dev.mysql.com/doc/refman/8.4/en/innodb-locks-set.html)
  - [MySQL bug #52020](https://bugs.mysql.com/bug.php?id=52020)
  - [Percona: INSERT ON DUPLICATE KEY disk seeks](https://www.percona.com/blog/why-insert-on-duplicate-key-update-may-be-slow-by-incurring-disk-seeks/)

## SQL Server

- MERGE produces single execution plan for all branches (not tunable independently).
- Residual bugs: race conditions without HOLDLOCK, assertion errors on partitioned tables.
- Hugo Kornelis 2023 update: most bugs fixed, but avoid DELETE action in MERGE and be careful with temporal tables.
- Sources:
  - [Aaron Bertrand: MERGE compilation](https://sqlblog.org/merge)
  - [Hugo Kornelis 2023 update](https://sqlserverfast.com/blog/hugo/2023/09/an-update-on-merge/)
  - [Michael J. Swart: Avoiding MERGE](https://michaeljswart.com/2021/08/what-to-avoid-if-you-want-to-use-merge/)
