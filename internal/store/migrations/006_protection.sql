CREATE TABLE protection_defaults (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    installation_id TEXT,
    auto_protect INTEGER NOT NULL CHECK (auto_protect IN (0,1)),
    settings BLOB NOT NULL CHECK (length(settings)<=8192 AND json_valid(CAST(settings AS TEXT)))
);
INSERT INTO protection_defaults VALUES (1,NULL,0,'{"destinations":[],"method":"auto","at":"03:00","time_zone":"UTC","retention":{"daily":7,"weekly":4,"monthly":6}}');

-- Keep old schedules so that an offline agent can deliver a result after settings change.
CREATE TABLE backup_schedules (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL REFERENCES apps(id),
    node_id TEXT NOT NULL REFERENCES nodes(id),
    definition BLOB NOT NULL CHECK (length(definition)<=8192 AND json_valid(CAST(definition AS TEXT)))
);
CREATE TABLE app_protection (
    app_id TEXT PRIMARY KEY REFERENCES apps(id),
    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
    settings BLOB NOT NULL CHECK (length(settings)<=8192 AND json_valid(CAST(settings AS TEXT))),
    schedule_id TEXT REFERENCES backup_schedules(id),
    changed_at INTEGER NOT NULL,
    CHECK ((enabled=1)=(schedule_id IS NOT NULL))
);
CREATE TABLE protection_updates (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL,
    input BLOB NOT NULL CHECK (length(input)<=65536),
    revision INTEGER NOT NULL
);
CREATE TABLE recovery_catalog (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    revision INTEGER NOT NULL,
    envelope BLOB NOT NULL
);
