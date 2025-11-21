-- RAW: lessons

CREATE TABLE IF NOT EXISTS lessons_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timeStamp TEXT,
    layout TEXT,
    textType TEXT,
    length INTEGER,
    time_ms INTEGER,
    errors INTEGER,
    speed REAL
);

CREATE INDEX IF NOT EXISTS idx_lessons_timestamp ON lessons_raw(timeStamp);


-- RAW: keystats

CREATE TABLE IF NOT EXISTS keystats_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timeStamp TEXT,
    codePoint INTEGER,
    key TEXT,
    hitCount INTEGER,
    missCount INTEGER,
    timeToType_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_keystats_timestamp ON keystats_raw(timeStamp);
CREATE INDEX IF NOT EXISTS idx_keystats_key ON keystats_raw(key);


-- AGGREGATED: per day

CREATE TABLE IF NOT EXISTS daily_metrics (
    date TEXT PRIMARY KEY,
    total_keystrokes INTEGER,
    avg_wpm REAL,
    avg_accuracy REAL,
    error_rate REAL,
    avg_latency REAL,
    ttfe REAL,
    ttke REAL,
    rolling_7d_wpm REAL,
    rolling_30d_wpm REAL
);


-- AGGREGATED: per key

CREATE TABLE IF NOT EXISTS key_metrics (
    key TEXT PRIMARY KEY,
    attempts INTEGER,
    errors INTEGER,
    miss_rate REAL,
    avg_latency REAL,
    last_timestamp TEXT
);
