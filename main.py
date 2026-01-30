from db import get_connection
from utils import update_existing_excel
from jobs import JOBS
from queries import current_snapshot
import pandas as pd

def main():
    with get_connection() as conn:
        # INITIAL CURRENT SNAPSHOT CHECK
        print("======== CHECKING IF THE CURRENT SNAPSHOT IS LOADED ========")
        df = pd.read_sql(current_snapshot, conn)
        if df.iloc[0, 0] == 0:
            print("======== CURRENT SNAPSHOT CHECK IS FAILED. STOPPING THE PROGRAM... ========")
            return
        else:
            print("======== CURRENT SNAPSHOT CHECK SUCCESS. STARTING THE PROGRAM... ========")
        for name, job in JOBS.items():
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

if __name__ == "__main__":
    main()