# scripts/metrics/rolling.py

import pandas as pd


def add_rolling_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Fügt Rolling-Metriken (7/30 Tage) zu einem Tages-DataFrame hinzu.
    Erwartet Spalten:
    - date (YYYY-MM-DD)
    - avg_wpm
    - error_rate
    - avg_latency
    """

    if daily.empty:
        # Nichts zu tun
        daily["rolling_7d_wpm"] = None
        daily["rolling_30d_wpm"] = None
        daily["rolling_7d_error_rate"] = None
        daily["rolling_30d_error_rate"] = None
        daily["rolling_7d_latency"] = None
        return daily

    df = daily.copy()

    # Datum in echte Datumsobjekte konvertieren
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date")

    df["rolling_7d_wpm"] = (
        df["avg_wpm"]
        .rolling(window=7, min_periods=1)
        .mean()
    )

    df["rolling_30d_wpm"] = (
        df["avg_wpm"]
        .rolling(window=30, min_periods=1)
        .mean()
    )

    df["rolling_7d_error_rate"] = (
        df["error_rate"]
        .rolling(window=7, min_periods=1)
        .mean()
    )

    df["rolling_30d_error_rate"] = (
        df["error_rate"]
        .rolling(window=30, min_periods=1)
        .mean()
    )

    df["rolling_7d_latency"] = (
        df["avg_latency"]
        .rolling(window=7, min_periods=1)
        .mean()
    )

    # Datum wieder als String
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    return df
