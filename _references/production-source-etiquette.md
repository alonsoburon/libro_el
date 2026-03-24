---
title: "Source System Etiquette -- Real Constraints from Client Databases"
type: reference
tags:
  - reference/production
  - reference/operations
relevant_chapters:
  - 0607-source-system-etiquette
  - 0108-purity-vs-freshness
  - 0604-sla-management
---

# Source System Etiquette in Production

Source: 35+ clients running SAP Business One, Softland ERP, and other ERPs on SQL Server, SAP HANA, PostgreSQL, MySQL. All source access is READ-ONLY.

## What You Cannot Do

These are hard constraints from client contracts and common sense:

| Forbidden | Why |
|-----------|-----|
| Create triggers | Slows source writes, DBA nightmare |
| Create views | Schema pollution, maintenance burden |
| Create stored procedures | Security risk, DBA resistance |
| Enable Change Tracking / CDC | Requires schema changes, often needs Enterprise Edition |
| Add columns (e.g., rowversion) | Alters source schema, violates read-only contract |
| Write to source tables | Obviously |

## What You Can Request

| Allowed | Impact | Success Rate |
|---------|--------|-------------|
| Read-only indexes | Zero write overhead, improves query speed | ~80% |
| Extended query timeout | No schema change, DBA sets it | ~95% |
| Off-peak scheduling | Operational agreement, no technical change | ~90% |
| Connection pool limits | Prevents overwhelming the source | Built into pipeline |

### The Index Request

The single most impactful thing you can ask for. Example email to client:

> "La tabla ORDR tiene 2M filas y la consulta de registros activos toma 18 minutos. Un indice en DocStatus reduciria esto a segundos. El indice es read-only, no afecta la operacion del sistema. Comando: `CREATE INDEX IX_DocStatus ON dbo.ORDR (DocStatus)`."

Most clients accept this because:
1. It's a standard database optimization
2. It doesn't modify application behavior
3. The DBA can verify it's read-only

### The Timeout Problem

Large table scans on SAP HANA without indexes routinely take 15-20 minutes. Default connection timeouts (30s-5min) are too short.

**Set explicit timeouts per-database before long queries:**

| Database | Command | Value |
|----------|---------|-------|
| SAP HANA | `SET TRANSACTION TIMEOUT 1200000` | 20 min (ms) |
| MSSQL | `SET LOCK_TIMEOUT 1200000` | 20 min (ms) |
| PostgreSQL | `SET statement_timeout = '1200s'` | 20 min |
| MySQL | `SET SESSION max_execution_time = 1200000` | 20 min (ms) |

## Concurrency and Source Impact

### Limit Parallel Extractions

Each extraction opens a database connection and runs a SELECT. Running 50 tables in parallel means 50 concurrent connections + 50 concurrent table scans. This can:
- Exhaust connection pool on the source
- Cause lock contention on busy tables
- Trigger DBA alerts ("who is running 50 queries simultaneously?")

**Production default:** 2 concurrent extractions per client. Configurable per client.

### Schedule Around Business Hours

Heavy extractions (full refreshes of large tables) during business hours degrade the source system for end users. Schedule them for off-peak:

| Extraction type | Scheduling |
|----------------|-----------|
| Incremental (small window) | Every 15-60 min, including business hours |
| Full refresh (small tables) | Daily, off-peak |
| Full refresh (large tables) | Weekly, weekend/night |
| Active records check | Off-peak (can be very heavy) |

### Connection String Hygiene

Real-world connection strings from client databases are messy:
- Invisible characters (copy-paste from Word, \r\n, tabs)
- Missing drivers (`ODBC Driver 17` vs `ODBC Driver 18`)
- Missing `TrustServerCertificate=yes` for self-signed SSL
- SAP HANA needs `HDBODBC` driver

Always sanitize connection strings at load time:
```python
conn_str = conn_str.strip().replace('\r', '').replace('\n', '').replace('\t', '')
```

## The "Soft Rule" Taxonomy (from Real Clients)

These were all true until they weren't:

| Soft Rule | What Actually Happened |
|-----------|----------------------|
| "UpdateDate always changes on edit" | Bulk import script bypassed the trigger |
| "DocStatus is only O or C" | Custom status 'P' (Pending) appeared |
| "Orders always have lines" | Empty order created and saved by accident |
| "PKs are always integers" | Softland uses float for IDs (1.0, 2.0, ...) |
| "Tables don't get renamed" | SAP upgrade renamed 3 tables |
| "Schema doesn't change between versions" | SAP B1 10.0 added 47 columns to ORDR |
| "Deleted documents stay deleted" | Client restored a backup, resurrecting old docs |
| "Connection string doesn't change" | Client migrated to a new server over the weekend |

Every one of these caused a production incident before we built defenses against it.
