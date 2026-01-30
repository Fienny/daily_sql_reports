from pathlib import Path
import pandas as pd

from pathlib import Path
import pandas as pd


def run_query(sql: str, conn) -> pd.DataFrame:
    return pd.read_sql(sql, conn)

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводит forecast_date к формату YYYY-MM-DD,
    если колонка присутствует в DataFrame
    """
    if "forecast_date" in df.columns:
        if df["forecast_date"].notna().any():
            df["forecast_date"] = pd.to_datetime(
                df["forecast_date"]
            ).dt.date
    return df



def update_existing_excel(
    df: pd.DataFrame,
    path: str,
    sheet_name: str
):
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(
            f"Файл не найден (создание запрещено): {path}"
        )

    # normalize dates
    df = normalize_dates(df)

    with pd.ExcelWriter(
        path,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
