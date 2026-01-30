import pandas as pd

def process_system_antipeaks(df: pd.DataFrame, today: pd.Timestamp) -> pd.DataFrame:
    df['forecast_date'] = pd.to_datetime(
        df['forecast_date'].astype(str),
        format='%Y%m%d',
        errors='coerce'
    )

    df = df.sort_values(["material", "forecast_date"])

    df_past = df.loc[
        (df['forecast_date'] < today) & (df['promo_exists'] == 0),
        ['material', 'total_forecast']
    ]

    median_df = (
        df_past.groupby('material')['total_forecast']
        .median()
        .reset_index()
        .rename(columns={'total_forecast': 'median_forecast'})
    )

    df = df.merge(median_df, on='material', how='left')

    df_antipeaks = df[
        (df['promo_exists'] == 0) &
        (df['forecast_date'] >= today) &
        (df['total_forecast'] > 0) &
        (df['total_forecast'] < 0.4 * df['median_forecast']) &
        (df['median_forecast'] > 100)
    ].copy()

    df_antipeaks['ratio'] = (
        df_antipeaks['total_forecast'] / df_antipeaks['median_forecast']
    )

    idx = df_antipeaks.groupby('material')['ratio'].idxmin()
    return df_antipeaks.loc[idx].reset_index(drop=True)
