import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("../db/keybr.db")

LESSONS_CSV = Path("../raw/lessons.csv")
KEYSTATS_CSV = Path("../raw/keystats.csv")

def import_lessons(cursor):
    print("Importing lessons.csv...")

    df = pd.read_csv(LESSONS_CSV)

    df = df.rename(columns={
        "timeStamp": "timeStamp",
        "layout": "layout",
        "textType": "textType",
        "length": "length",
        "time_ms": "time_ms",
        "errors": "errors",
        "speed": "speed"
    })

    df.to_sql("lessons_raw", cursor.connection, if_exists="append", index=False)
    print(f"Inserted {len(df)} lesson rows.")


def import_keystats(cursor):
    print("Importing keystats.csv...")

    df = pd.read_csv(KEYSTATS_CSV)

    df = df.rename(columns={
        "timeStamp": "timeStamp",
        "codePoint": "codePoint",
        "key": "key",
        "hitCount": "hitCount",
        "missCount": "missCount",
        "timeToType_ms": "timeToType_ms"
    })

    df.to_sql("keystats_raw", cursor.connection, if_exists="append", index=False)
    print(f"Inserted {len(df)} keystats rows.")


def main():
    print("Connecting to DB:", DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    import_lessons(cursor)
    import_keystats(cursor)

    conn.commit()
    conn.close()

    print("Initial import finished successfully.")


if __name__ == "__main__":
    main()
