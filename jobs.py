import pandas as pd
from queries import system_antipeaks, old_peaks, new_peaks, antipeaks, dbd_reg, dbd_total
from processors import process_system_antipeaks

today = pd.Timestamp.today().normalize()

BASE_PATH = r"C:\Users\a.sattorov\OneDrive - Marnell Group Limited\ежедневный мониторинг"

# ===========================
# SQL SERVER JOBS (Microsoft Fabric)
# ===========================
MSSQL_JOBS = {
    "system_antipeaks": {
        "sql": system_antipeaks,
        "postprocess": lambda df: process_system_antipeaks(df, today),
        "output": f"{BASE_PATH}\\system_antipeaks.xlsx",
        "sheet": "antipeaks"
    },

    "old_peaks": {
        "sql": old_peaks,
        "output": f"{BASE_PATH}\\old_peaks.xlsx",
        "sheet": "data"
    },

    "new_peaks": {
        "sql": new_peaks,
        "output": f"{BASE_PATH}\\new_peaks.xlsx",
        "sheet": "data"
    },

    "antipeaks": {
        "sql": antipeaks,
        "output": f"{BASE_PATH}\\antipeaks.xlsx",
        "sheet": "antipeaks"   # pivot на другом листе — не трогаем
    }
}

# ===========================
# POSTGRESQL JOBS (DBD Reports)
# Run AFTER all SQL Server jobs
# ===========================
PG_JOBS = {
    "dbd_total": {
        "sql": dbd_total,
        "output": f"{BASE_PATH}\\dbd.xlsx",
        "sheet": "total"
    },

    "dbd_reg": {
        "sql": dbd_reg,
        "output": f"{BASE_PATH}\\dbd.xlsx",
        "sheet": "reg"
    }
}

