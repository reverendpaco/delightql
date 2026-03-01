CREATE TABLE IF NOT EXISTS run (
    id                  INTEGER PRIMARY KEY,
    started_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),
    ball                TEXT NOT NULL,
    ball_mode           TEXT NOT NULL,
    source_fingerprint  TEXT,
    git_rev             TEXT,
    dql_binary_hash     TEXT,
    pm_binary_hash      TEXT,
    fixture             TEXT,
    total               INTEGER NOT NULL DEFAULT 0,
    passed              INTEGER NOT NULL DEFAULT 0,
    failed              INTEGER NOT NULL DEFAULT 0,
    errors              INTEGER NOT NULL DEFAULT 0,
    meh                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS test_result (
    id          INTEGER PRIMARY KEY,
    run_id      INTEGER NOT NULL REFERENCES run(id),
    file        TEXT NOT NULL,
    status      TEXT NOT NULL,
    expected    TEXT,
    actual      TEXT,
    error_msg   TEXT,
    UNIQUE(run_id, file)
);

CREATE INDEX IF NOT EXISTS idx_test_result_status ON test_result(status);
CREATE INDEX IF NOT EXISTS idx_test_result_file ON test_result(file);
CREATE INDEX IF NOT EXISTS idx_test_result_run ON test_result(run_id);
