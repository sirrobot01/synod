CREATE TABLE operations (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES nodes (id),
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 256),
    kind TEXT NOT NULL CHECK (length(kind) BETWEEN 1 AND 64),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'cancelling', 'succeeded', 'failed', 'cancelled')),
    failure_code TEXT NOT NULL DEFAULT '' CHECK (length(failure_code) <= 128),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    finished_at INTEGER,
    CHECK ((status IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)),
    CHECK ((status = 'failed') = (failure_code <> ''))
);
CREATE INDEX operations_recent ON operations (created_at DESC, id);
