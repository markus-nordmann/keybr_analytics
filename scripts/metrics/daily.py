# scripts/metrics/daily.py

import pandas as pd
import sqlite3

from .rolling import add_rolling_metrics


def compute_daily_metrics(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Compute daily aggregated metrics from lessons_raw and keystats_raw.

    Output columns:
    - date
    - num_lessons
    - total_chars
    - total_errors
    - total_keystrokes
    - avg_wpm                (correctly computed)
    - avg_accuracy
    - error_rate
    - avg_latency
    - ttfe
    - rolling_7d_wpm
    - rolling_30d_wpm
    - rolling_7d_error_rate
    - rolling_30d_error_rate
    - rolling_7d_latency
    """

    # 1) Daily metrics from lessons_raw
    # NOTE: Keybr exports "wpm" per lesson, which is the correct metric.
    # "speed" is CPM (characters per minute) and must NOT be averaged as WPM.
    lessons_sql = """
    SELECT
        substr(timeStamp, 1, 10) AS date,
        COUNT(*) AS num_lessons,
        SUM(length) AS total_chars,
        SUM(errors) AS total_errors,
        AVG(speed / 5.0) AS avg_wpm      -- correct WPM calculation
    FROM lessons_raw
    GROUP BY substr(timeStamp, 1, 10)
    ORDER BY date
"""
    lessons_df = pd.read_sql_query(lessons_sql, conn)

    # 2) Daily keystroke and latency metrics
    # total_keystrokes = hitCount + missCount
    keystats_sql = """
        SELECT
            substr(timeStamp, 1, 10) AS date,
            SUM(hitCount + missCount) AS total_keystrokes,
            AVG(timeToType_ms) AS avg_latency
        FROM keystats_raw
        GROUP BY substr(timeStamp, 1, 10)
        ORDER BY date
    """
    keystats_df = pd.read_sql_query(keystats_sql, conn)

    # 3) TTFE per lesson (minimum latency on keys where a miss occurred)
    ttfe_lesson_sql = """
        SELECT
            substr(timeStamp, 1, 10) AS date,
            timeStamp AS lesson_ts,
            MIN(timeToType_ms) AS ttfe_lesson
        FROM keystats_raw
        WHERE missCount > 0
        GROUP BY timeStamp
    """
    ttfe_lesson_df = pd.read_sql_query(ttfe_lesson_sql, conn)

    # 4) TTFE daily average
    ttfe_daily = (
        ttfe_lesson_df
        .groupby("date", as_index=False)["ttfe_lesson"]
        .mean()
        .rename(columns={"ttfe_lesson": "ttfe"})
    )

    # 5) Merge daily components
    daily = (
        lessons_df
        .merge(keystats_df, on="date", how="outer", sort=True)
        .merge(ttfe_daily, on="date", how="left", sort=True)
    )

    # 6) Fill missing values and compute accuracy/error-rate
    daily["total_chars"] = daily["total_chars"].fillna(0)
    daily["total_errors"] = daily["total_errors"].fillna(0)
    daily["total_keystrokes"] = daily["total_keystrokes"].fillna(0)

    daily["error_rate"] = daily.apply(
        lambda row: (row["total_errors"] / row["total_chars"])
        if row["total_chars"] > 0 else None,
        axis=1,
    )

    daily["avg_accuracy"] = daily.apply(
        lambda row: 1.0 - row["error_rate"]
        if row["error_rate"] is not None else None,
        axis=1,
    )

    # 7) Add rolling metrics
    daily = add_rolling_metrics(daily)

    # 8) Final sort
    return daily.sort_values("date")
