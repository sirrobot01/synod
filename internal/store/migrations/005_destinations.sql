CREATE TABLE destination_revision (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision > 0)
);
INSERT INTO destination_revision VALUES (1, 1);
CREATE TABLE destinations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL COLLATE NOCASE,
    node_id TEXT REFERENCES nodes (id),
    kind TEXT NOT NULL,
    definition BLOB NOT NULL,
    credentials BLOB,
    input_bytes INTEGER NOT NULL,
    initializer_id TEXT REFERENCES operations (id),
    initialized_at INTEGER,
    removed_at INTEGER
);
CREATE UNIQUE INDEX destination_live_name ON destinations (name) WHERE removed_at IS NULL;
CREATE TABLE destination_deliveries (
    node_id TEXT PRIMARY KEY REFERENCES nodes (id),
    session_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    acknowledged_at INTEGER NOT NULL
);
