# scripts/build_metrics.py

import sqlite3
from pathlib import Path

import pandas as pd

from metrics import compute_daily_metrics, compute_key_metrics, get_weak_keys
from metrics.weak_keys import export_weak_keys

DB_PATH = Path("../db/keybr.db")
OUTPUT_DIR = Path("../output")


def write_daily_metrics(conn: sqlite3.Connection, daily_df: pd.DataFrame) -> None:
    """
    Schreibt daily_metrics in die DB.
    Die Tabelle hat laut Schema.sql folgende Spalten:
    - date TEXT PRIMARY KEY
    - total_keystrokes INTEGER
    - avg_wpm REAL
    - avg_accuracy REAL
    - error_rate REAL
    - avg_latency REAL
    - ttfe REAL
    - ttke REAL
    - rolling_7d_wpm REAL
    - rolling_30d_wpm REAL

    Zusätzliche Rolling-Spalten wie rolling_7d_error_rate etc.
    bleiben im DataFrame/CSV, werden aber NICHT in die DB geschrieben.
    """

    cols_for_db = [
        "date",
        "total_keystrokes",
        "avg_wpm",
        "avg_accuracy",
        "error_rate",
        "avg_latency",
        "ttfe",
        # ttke auf Tagesebene lassen wir vorerst leer (None)
        "ttke",
        "rolling_7d_wpm",
        "rolling_30d_wpm",
    ]

    df_db = daily_df.copy()

    # ttke-Spalte auf Tagesebene als Placeholder
    if "ttke" not in df_db.columns:
        df_db["ttke"] = None

    for col in cols_for_db:
        if col not in df_db.columns:
            df_db[col] = None

    df_db = df_db[cols_for_db].copy()

    cur = conn.cursor()
    cur.execute("DELETE FROM daily_metrics;")
    conn.commit()

    df_db.to_sql("daily_metrics", conn, if_exists="append", index=False)


def write_key_metrics(conn: sqlite3.Connection, key_df: pd.DataFrame) -> None:
    """
    Schreibt key_metrics in die DB.
    Laut Schema.sql:
    - key TEXT PRIMARY KEY
    - attempts INTEGER
    - errors INTEGER
    - miss_rate REAL
    - avg_latency REAL
    - last_timestamp TEXT

    Erweiterungen wie ttke und weak_score verbleiben im DataFrame/CSV.
    """

    cols_for_db = [
        "key",
        "attempts",
        "errors",
        "miss_rate",
        "avg_latency",
        "last_timestamp",
    ]

    df_db = key_df.copy()

    for col in cols_for_db:
        if col not in df_db.columns:
            df_db[col] = None

    df_db = df_db[cols_for_db].copy()

    cur = conn.cursor()
    cur.execute("DELETE FROM key_metrics;")
    conn.commit()

    df_db.to_sql("key_metrics", conn, if_exists="append", index=False)


def export_csvs(daily_df: pd.DataFrame, key_df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    daily_path = OUTPUT_DIR / "daily_metrics.csv"
    key_path = OUTPUT_DIR / "key_metrics.csv"

    daily_df.to_csv(daily_path, index=False)
    key_df.to_csv(key_path, index=False)

    print(f"Exported daily metrics to {daily_path}")
    print(f"Exported key metrics to {key_path}")


def main():
    print(f"Connecting to DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        daily_df = compute_daily_metrics(conn)
        key_df = compute_key_metrics(conn)

        print(f"Daily rows: {len(daily_df)}")
        print(f"Key rows:   {len(key_df)}")

        write_daily_metrics(conn, daily_df)
        write_key_metrics(conn, key_df)

        export_csvs(daily_df, key_df)

        # Weak Keys berechnen & exportieren (optional)
        weak_df = get_weak_keys(key_df, min_attempts=200, top_n=20)
        export_weak_keys(weak_df, OUTPUT_DIR)

        print("Metric build finished successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
