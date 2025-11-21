import json
import sqlite3
from pathlib import Path

import pandas as pd

# Base directory of the repo: .../keybr_analytics
ROOT_DIR = Path(__file__).resolve().parents[1]

# Absolute paths so the script works no matter from where it is called
DB_PATH = ROOT_DIR / "db" / "keybr.db"
JSON_PATH = ROOT_DIR / "raw" / "typing-data.json"


def get_last_timestamp(conn: sqlite3.Connection):
    """Read the last lesson timestamp from the DB (for incremental import)."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(timeStamp) FROM lessons_raw;")
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def load_json():
    """Load the full KeyBR JSON export file."""
    if not JSON_PATH.exists():
        raise FileNotFoundError(f"JSON file not found: {JSON_PATH}")

    with JSON_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected top-level JSON array (list of lessons).")

    return data


def filter_new_lessons(all_lessons, last_ts):
    """Filter only lessons that come *after* the last timestamp in the DB."""
    if last_ts is None:
        # First full load from JSON
        return all_lessons

    # ISO-8601 timestamp strings can be compared lexicographically
    return [l for l in all_lessons if l.get("timeStamp") > last_ts]


def lessons_to_dataframe(lessons):
    """Convert lesson objects into a DataFrame matching lessons_raw."""
    rows = []
    for l in lessons:
        rows.append(
            {
                "timeStamp": l.get("timeStamp"),
                "layout": l.get("layout"),
                "textType": l.get("textType"),
                "length": l.get("length"),
                "time_ms": l.get("time"),
                "errors": l.get("errors"),
                "speed": l.get("speed"),
            }
        )
    return pd.DataFrame(rows)


def histogram_to_keystats_dataframe(lessons):
    """Flatten histogram entries into rows for keystats_raw."""
    rows = []
    for l in lessons:
        ts = l.get("timeStamp")
        histogram = l.get("histogram") or []
        for h in histogram:
            code_point = h.get("codePoint")
            try:
                key_char = chr(code_point) if code_point is not None else None
            except (TypeError, ValueError):
                key_char = None

            rows.append(
                {
                    "timeStamp": ts,
                    "codePoint": code_point,
                    "key": key_char,
                    "hitCount": h.get("hitCount"),
                    "missCount": h.get("missCount"),
                    "timeToType_ms": h.get("timeToType"),
                }
            )
    return pd.DataFrame(rows)


def import_new_data():
    print(f"Connecting to DB: {DB_PATH}")
    print(f"Reading JSON from: {JSON_PATH}")

    conn = sqlite3.connect(DB_PATH)

    try:
        last_ts = get_last_timestamp(conn)
        print("Last lesson timestamp in DB:", last_ts)

        all_lessons = load_json()
        print(f"Total lessons in JSON: {len(all_lessons)}")

        new_lessons = filter_new_lessons(all_lessons, last_ts)
        print(f"New lessons to import: {len(new_lessons)}")

        if not new_lessons:
            print("No new lessons found. Nothing to do.")
            return

        # DataFrames for lessons_raw and keystats_raw
        lessons_df = lessons_to_dataframe(new_lessons)
        keystats_df = histogram_to_keystats_dataframe(new_lessons)

        print(f"New lesson rows: {len(lessons_df)}")
        print(f"New keystats rows: {len(keystats_df)}")

        # Write into DB
        lessons_df.to_sql("lessons_raw", conn, if_exists="append", index=False)
        keystats_df.to_sql("keystats_raw", conn, if_exists="append", index=False)

        conn.commit()
        print("Update finished successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    import_new_data()
