import sqlite3
from pathlib import Path

DB_PATH = Path("../db/keybr.db")
SCHEMA_PATH = Path("schema.sql")

def init_db():
    print("Creating database at:", DB_PATH)

    DB_PATH.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    with open(SCHEMA_PATH, "r") as f:
        schema = f.read()
        cursor.executescript(schema)

    conn.commit()
    conn.close()

    print("Database initialized successfully.")

if __name__ == "__main__":
    init_db()
