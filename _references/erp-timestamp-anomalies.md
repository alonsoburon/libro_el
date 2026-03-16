---
title: "ERP & Source System Timestamp Anomalies"
type: reference
tags:
  - reference/erp
  - reference/extraction
created: 2026-03-13
updated: 2026-03-13
---

# ERP & Source System Timestamp Anomalies

Real-world documented cases where `updated_at`-equivalent fields lie, are missing, or behave unexpectedly. Use as source material for `0105-the-lies-sources-tell`, `0302-cursor-based-extraction`, and related patterns.

---

## SAP Business One

**`UpdateDate` is DATE, not DATETIME -- no intraday precision.**
Every header table (ORDR, OINV, OITM, etc.) stores `UpdateDate` as a SQL `DATE` -- date only, no time component. A pipeline running multiple times per day filtering `WHERE UpdateDate > :last_run` misses everything updated after the first run on the same calendar day. SAP B1 10.0 added `UpdateTS` to some tables, but adoption is inconsistent -- price list updates on OITM were documented as not updating `UpdateTS` even in recent FPs.
- https://community.sap.com/t5/enterprise-resource-planning-q-a/updatedate-in-oitm-table/qaq-p/2824630
- https://community.sap.com/t5/enterprise-resource-planning-q-a/item-price-update-does-not-change-oitm-updatets/qaq-p/12520484

**RDR1 / INV1 child tables have no timestamp at all.**
Sales order lines, invoice lines, and similar child tables carry no `UpdateDate` or `UpdateTS`. The only option is using the parent header's `UpdateDate` -- which forces a full document re-extract whenever anything on the header changes, even if no lines changed.
- https://community.sap.com/t5/enterprise-resource-planning-q-a/rdr1-inv1-transaction-line-update-date/qaq-p/8718308

**Line deletion = physical delete + reinsert of all survivors, line numbers shift.**
When a document line is removed via the DI-API or Service Layer, SAP B1 deletes ALL existing lines for the document and reinserts the survivors with new `LineNum` values. No tombstone, no log entry. A cursor comparing `LineNum` between runs sees every surviving line as changed and deleted lines as simply gone. The ADO1 log table also does not capture a "line updated" event when a document is copied downstream (e.g., order copied to delivery).
- https://community.sap.com/t5/enterprise-resource-planning-q-a/removing-document-lines-using-b1-service-layer/qaq-p/12122206
- https://community.sap.com/t5/enterprise-resource-planning-q-a/removing-sales-order-lines/qaq-p/4258877

---

## SAP ECC / S/4HANA

**Date and time in separate DATS/TIMS fields -- must concatenate to get a timestamp.**
SAP ECC stores posting date (BUDAT, type DATS: `YYYYMMDD`) and posting time (type TIMS: `HHMMSS`) in separate fields. Building a cursor-comparable timestamp requires concatenation, timezone handling, and knowing which field pairs belong together. Getting the midnight boundary wrong produces off-by-one-day errors.
- https://community.sap.com/t5/application-development-and-automation-blog-posts/time-stamps-in-abap/ba-p/13495455

**CDHDR/CDPOS change documents only written if explicitly configured -- not universal.**
ECC's change document mechanism (CDHDR/CDPOS) is the standard audit trail for incremental extraction on tables without their own timestamp. But logging must be explicitly configured per ABAP object type. Many standard tables and all custom tables that weren't configured produce no change history at all.
- https://blogs.sap.com/2017/03/13/extracting-data-from-sap-ecc/

---

## Oracle E-Business Suite (EBS)

**Race condition between long-running concurrent programs and the ETL watermark.**
A concurrent program (internal EBS batch job) that starts before the ETL window opens and commits after the watermark was set produces records whose `LAST_UPDATE_DATE` falls inside the window -- but the ETL's query has already run past them. Oracle's own BI Apps documentation addresses this with the `PRUNE_DAYS` parameter: subtract N days from the watermark before filtering, trading efficiency for correctness.
- https://www.wegobeyond.co.uk/prune-days-on-an-e-business-suite-source-environment/

**Custom concurrent programs that skip WHO columns entirely.**
WHO column population (`LAST_UPDATE_DATE`, `CREATED_BY`, etc.) is an application-layer convention, not a database constraint. PL/SQL programs or bulk operations that skip it write rows with `LAST_UPDATE_DATE = NULL` or a placeholder date. No enforcement at the database level.
- https://docs.oracle.com/cd/E18727_01/doc.121/e12897/T302934T303920.htm

---

## NetSuite

**`lastModifiedDate` not available on all record types.**
Several record types omit `lastmodifieddate` or don't support filtering by it. Documented example: `supportCase` records cannot be filtered by `lastModifiedDate` via the REST API -- the field doesn't exist on that record type in the query layer. Silent empty result set, not an error.
- https://community.oracle.com/netsuite/english/discussion/4504731/unable-to-filter-supportcase-records-using-lastmodifieddate-via-rest-api

**TO_DATE vs TO_TIMESTAMP mismatch in SuiteQL silently returns wrong results.**
Mixing `TO_DATE` and `TO_TIMESTAMP` comparators in the same SuiteQL filter expression triggers implicit type conversion that can return incorrect rows with no warning or error.
- https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/article_0824094533.html

---

## Microsoft Dynamics

**Business Central: `SystemRowVersion` only available from BC21 (2022 Wave 2) onward.**
`SystemRowVersion` (SQL Server `rowversion` equivalent) for watermark-based incremental extraction wasn't accessible from AL code until BC21. Older on-premise NAV installations can't use it. Before BC21, only `SystemModifiedAt` (second-level precision) was available.
- https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/developer/devenv-extract-data
- https://yzhums.com/29936/

**Finance & Operations: Synapse Link output is append-only change files, not current state.**
The officially recommended extraction path for D365 F&O produces append-only CSV/Parquet files representing incremental changes. The consumer must reconstruct current state by replaying them -- not a queryable copy of the source table.

---

## Salesforce

**`SystemModstamp` and `LastModifiedDate` diverge -- picking the wrong one breaks incremental sync.**
`LastModifiedDate` updates when a user or visible automation changes the record. `SystemModstamp` updates for any system process, including roll-up summary field recalculations (async, triggered by child record changes), picklist relabeling, email bounce marking, and internal Salesforce jobs. Using `SystemModstamp` as cursor produces false-positive "changed" records on roll-up recalcs. Using `LastModifiedDate` misses records whose derived values changed. `SystemModstamp` is indexed (query performance); `LastModifiedDate` is not.
- https://help.salesforce.com/s/articleView?id=000387261&language=en_US&type=1
- https://developer.salesforce.com/blogs/engineering/2014/11/force-com-soql-performance-tips-systemmodstamp-vs-lastmodifieddate-2

**Bulk API cursor can advance past uncommitted records.**
Salesforce does not guarantee immediate API visibility for recently committed records. The cursor can advance past records not yet visible in the query layer -- permanently missed in subsequent syncs unless a lookback window is configured. Documented in Airbyte connector.
- https://github.com/airbytehq/airbyte/issues/27146

---

## Cross-Platform

**ORM-managed systems: direct SQL bypasses timestamp hooks.**
Every system that maintains `updated_at` / `write_date` through application-layer hooks (Odoo, Django, Hibernate-based ERPs, Rails apps) is vulnerable to direct SQL operations that bypass the ORM. `ALTER TABLE ... DISABLE TRIGGER ALL` and `SET session_replication_role = 'local'` in PostgreSQL are valid operational steps that silently stop all trigger-based timestamp updates.
- https://www.odoo.com/documentation/18.0/developer/reference/backend/orm.html
- https://www.cybertec-postgresql.com/en/rules-or-triggers-to-log-bulk-updates/

**MySQL: default `DATETIME` precision is 0 -- sub-second writes silently rounded.**
`DATETIME` and `TIMESTAMP` columns default to zero fractional seconds unless declared as `DATETIME(6)`. Values with microsecond precision are rounded on insert with no warning (unless `TIME_TRUNCATE_FRACTIONAL` mode is enabled). A pipeline running more than once per second can produce a rounded timestamp that matches the previous watermark, skipping the row on the next run.
- https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
- https://bugs.mysql.com/bug.php?id=73458
