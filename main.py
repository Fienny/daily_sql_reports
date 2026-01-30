from db import get_connection, get_pg_connection, close_pg_connection
from utils import update_existing_excel
from jobs import MSSQL_JOBS, PG_JOBS
from queries import current_snapshot
import pandas as pd


def run_mssql_jobs():
    """Run all SQL Server (Microsoft Fabric) jobs"""
    print("======== STARTING SQL SERVER JOBS ========")

    with get_connection() as conn:
        # INITIAL CURRENT SNAPSHOT CHECK
        print("Checking if the current snapshot is loaded...")
        df = pd.read_sql(current_snapshot, conn)
        if df.iloc[0, 0] == 0:
            print("CURRENT SNAPSHOT CHECK FAILED. STOPPING THE PROGRAM...")
            return False

        print("CURRENT SNAPSHOT CHECK SUCCESS.")

        for name, job in MSSQL_JOBS.items():
            print(f"▶ {name}")

            df = pd.read_sql(job["sql"], conn)

            if "postprocess" in job:
                df = job["postprocess"](df)

            update_existing_excel(
                df=df,
                path=job["output"],
                sheet_name=job["sheet"]
            )

            print(f"✔ Обновлён лист '{job['sheet']}' в {job['output']}")

    print("======== SQL SERVER JOBS COMPLETED ========\n")
    return True


def run_pg_jobs():
    """Run all PostgreSQL (DBD) jobs sequentially"""
    print("======== STARTING POSTGRESQL JOBS ========")

    conn = None
    try:
        conn = get_pg_connection()

        for name, job in PG_JOBS.items():
            print(f"▶ {name}")

            df = pd.read_sql(job["sql"], conn)

            if "postprocess" in job:
                df = job["postprocess"](df)

            update_existing_excel(
                df=df,
                path=job["output"],
                sheet_name=job["sheet"]
            )

            print(f"✔ Обновлён лист '{job['sheet']}' в {job['output']}")

    finally:
        close_pg_connection(conn)

    print("======== POSTGRESQL JOBS COMPLETED ========")


def main():
    # Step 1: Run SQL Server jobs first
    if not run_mssql_jobs():
        return

    # Step 2: Run PostgreSQL jobs after SQL Server jobs complete
    run_pg_jobs()

    print("\n======== ALL JOBS COMPLETED SUCCESSFULLY ========")


if __name__ == "__main__":
    main()