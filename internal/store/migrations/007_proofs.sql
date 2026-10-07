-- Keep schedules so an offline agent can report work after settings change.
CREATE TABLE proof_schedules (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    definition BLOB NOT NULL CHECK (length(definition)<=8192 AND json_valid(CAST(definition AS TEXT)))
);
