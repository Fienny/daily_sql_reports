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

**Planned Changes:**
1. Create `db_pg.py` for PostgreSQL connection (using psycopg2)
2. Separate jobs into SQL Server jobs and PostgreSQL jobs
3. Modify `main.py` to run SQL Server jobs first, then PostgreSQL jobs
4. Both dbd queries output to `dbd.xlsx` (different sheets: `total`, `reg`)

**Status:** Awaiting user confirmation on approach

---

## Change Log

| Date | File | Change |
|------|------|--------|
| 2026-01-30 | claude.md | Created development log |

