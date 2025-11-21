# scripts/metrics/weak_keys.py

import pandas as pd
from pathlib import Path


def get_weak_keys(key_df: pd.DataFrame, min_attempts: int = 200, top_n: int = 20) -> pd.DataFrame:
    """
    Liefert die schwächsten Tasten nach weak_score, gefiltert nach Mindestversuchen.
    """

    if key_df.empty or "weak_score" not in key_df.columns:
        return pd.DataFrame()

    df = key_df.copy()
    df = df[df["attempts"] >= min_attempts]

    if df.empty:
        return df

    df = df.sort_values("weak_score", ascending=False)
    return df.head(top_n)


def export_weak_keys(weak_df: pd.DataFrame, output_dir: Path) -> None:
    if weak_df.empty:
        return

    output_dir.mkdir(exist_ok=True)
    path = output_dir / "weak_keys.csv"
    weak_df.to_csv(path, index=False)
    print(f"Exported weak keys to {path}")
