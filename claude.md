# Claude Development Log

## Project: Daily SQL Reports

### Overview
This project generates daily Excel reports from two databases:
- **Microsoft Fabric (SQL Server)** - forecasting/demand analysis
- **PostgreSQL** - daily business dashboard (DBD) reports

---

## Session: 2026-01-30

### Task: Integrate PostgreSQL DBD Queries

**Problem Identified:**
- Two queries (`dbd_total`, `dbd_reg`) in `queries.py` are written for PostgreSQL
- Currently mixed with SQL Server queries in `jobs.py`
- No PostgreSQL connection exists - only SQL Server via `db.py`
- These need to run AFTER all SQL Server reports complete

**PostgreSQL Connection Info:**
- Host: 192.168.108.111
- Port: 5432
- Database: ducp
- User: asattorov
- Schemas: `supply_chains`, `nps`

**Implemented Changes:**
1. Added PostgreSQL connection to `db.py` (using psycopg2)
2. Separated jobs into `MSSQL_JOBS` and `PG_JOBS` in `jobs.py`
3. Modified `main.py` to run SQL Server jobs first, then PostgreSQL jobs sequentially
4. Both dbd queries output to `dbd.xlsx` (different sheets: `total`, `reg`)

**Status:** COMPLETED

---

## Architecture After Changes

```
main.py
├── run_mssql_jobs()          # SQL Server (Microsoft Fabric)
│   ├── system_antipeaks
│   ├── old_peaks
│   ├── new_peaks
│   └── antipeaks
│
└── run_pg_jobs()             # PostgreSQL (DBD)
    ├── dbd_total → dbd.xlsx (sheet: total)
    └── dbd_reg   → dbd.xlsx (sheet: reg)
```

**Execution Order:**
1. SQL Server snapshot check
2. SQL Server jobs (4 reports)
3. PostgreSQL jobs (2 reports, sequential)

---

## Change Log

| Date | File | Change |
|------|------|--------|
| 2026-01-30 | claude.md | Created development log |
| 2026-01-30 | db.py | Added PostgreSQL connection (`get_pg_connection`, `close_pg_connection`) |
| 2026-01-30 | jobs.py | Split `JOBS` into `MSSQL_JOBS` and `PG_JOBS` |
| 2026-01-30 | main.py | Refactored to `run_mssql_jobs()` and `run_pg_jobs()` functions |

