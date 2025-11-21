# scripts/metrics/keys.py

import pandas as pd
import sqlite3
import numpy as np


def compute_key_metrics(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Aggregiert Metriken pro Taste aus keystats_raw:

    Basis:
    - attempts  = SUM(hitCount + missCount)
    - errors    = SUM(missCount)
    - miss_rate = errors / attempts
    - avg_latency = AVG(timeToType_ms)
    - last_timestamp = MAX(timeStamp)

    Erweiterungen (nur im DataFrame / CSV):
    - ttke      = MIN(timeToType_ms WHERE missCount > 0)
    - weak_score = gewichtete Kombination aus miss_rate, normierter Latenz und Rarity-Penalty
    """

    # Basis-Metriken
    base_sql = """
        SELECT
            key,
            SUM(hitCount + missCount) AS attempts,
            SUM(missCount) AS errors,
            AVG(timeToType_ms) AS avg_latency,
            MAX(timeStamp) AS last_timestamp
        FROM keystats_raw
        WHERE key IS NOT NULL AND key <> ''
        GROUP BY key
        ORDER BY key
    """
    base_df = pd.read_sql_query(base_sql, conn)

    # TTKE pro Key: minimaler timeToType_ms bei Fehlertasten
    ttke_sql = """
        SELECT
            key,
            MIN(timeToType_ms) AS ttke
        FROM keystats_raw
        WHERE key IS NOT NULL AND key <> '' AND missCount > 0
        GROUP BY key
    """
    ttke_df = pd.read_sql_query(ttke_sql, conn)

    df = base_df.merge(ttke_df, on="key", how="left")

    # Miss-Rate
    df["miss_rate"] = df.apply(
        lambda row: (row["errors"] / row["attempts"])
        if row["attempts"] and row["attempts"] > 0
        else 0.0,
        axis=1,
    )

    # Weak-Score berechnen
    df["weak_score"] = _compute_weak_scores(df)

    # Spalten sortieren
    # Hinweis: DB hat nur key, attempts, errors, miss_rate, avg_latency, last_timestamp
    return df


def _compute_weak_scores(df: pd.DataFrame) -> pd.Series:
    """
    Berechnet einen Weak-Score pro Taste aus:
    - miss_rate      (0..1)
    - norm_avg_latency (0..1, Skala aus 95. Perzentil)
    - rarity_penalty (0 oder 1, bei wenigen Versuchen)

    weak_score = 0.6 * miss_rate + 0.3 * norm_latency + 0.1 * rarity_penalty
    """

    if df.empty:
        return pd.Series([], dtype=float)

    # Miss-Rate ist bereits in [0,1] begrenzt, clip zur Sicherheit
    miss_rate = df["miss_rate"].clip(lower=0.0, upper=1.0)

    # Normierte Latenz: Teile durch 95. Perzentil, clip auf [0,1]
    lat = df["avg_latency"].fillna(df["avg_latency"].median())
    p95 = np.percentile(lat, 95) if len(lat) > 0 else 1.0
    if p95 <= 0:
        p95 = 1.0
    norm_latency = (lat / p95).clip(lower=0.0, upper=1.0)

    # Rarity-Penalty: wenige Versuche → schlechter Score
    attempts = df["attempts"].fillna(0)
    rarity_penalty = (attempts < 200).astype(float)  # 1.0, wenn weniger als 200 Versuche

    weak_score = 0.6 * miss_rate + 0.3 * norm_latency + 0.1 * rarity_penalty
    return weak_score
