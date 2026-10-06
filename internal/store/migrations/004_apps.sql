-- Each report replaces presence for one device. Missing apps retain their identity.
CREATE TABLE app_scans (
    node_id TEXT PRIMARY KEY REFERENCES nodes (id),
    session_id TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    failure_code TEXT NOT NULL DEFAULT ''
);
CREATE TABLE apps (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes (id),
    source_key TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    present INTEGER NOT NULL CHECK (present IN (0, 1)),
    definition BLOB NOT NULL,
    UNIQUE (node_id, source_key)
);
