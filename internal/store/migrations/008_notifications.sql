CREATE TABLE notification_settings (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    revision INTEGER NOT NULL CHECK (revision>0),
    changed_at INTEGER NOT NULL,
    public BLOB NOT NULL CHECK (length(public)<=4096 AND json_valid(CAST(public AS TEXT))),
    credentials BLOB
);
INSERT INTO notification_settings VALUES (1,1,0,'{"configured":false,"overdue_grace_hours":24,"missing_after_hours":168,"full_percent":90}',NULL);

CREATE TABLE notification_updates (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL,
    request_hash BLOB NOT NULL CHECK (length(request_hash)=32),
    revision INTEGER NOT NULL
);

CREATE TABLE notification_alerts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('backup_failed','backup_overdue','drill_failed','destination_full','device_missing','test')),
    node_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    message TEXT NOT NULL CHECK (length(message) BETWEEN 1 AND 4096),
    resource TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','sending','sent','failed','cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 5),
    next_attempt_at INTEGER NOT NULL,
    delivered_at INTEGER,
    failure_code TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX notification_operation ON notification_alerts(operation_id) WHERE kind IN ('backup_failed','drill_failed');
CREATE INDEX notification_pending ON notification_alerts (state,next_attempt_at);
CREATE INDEX notification_recent ON notification_alerts (created_at DESC,id);

-- An active condition emits one alert until it clears.
CREATE TABLE notification_conditions (
    resource TEXT PRIMARY KEY,
    active INTEGER NOT NULL CHECK (active IN (0,1))
);

CREATE TABLE destination_health (
    destination_id TEXT PRIMARY KEY REFERENCES destinations(id),
    node_id TEXT NOT NULL REFERENCES nodes(id),
    observed_at INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL CHECK (total_bytes>0),
    available_bytes INTEGER NOT NULL CHECK (available_bytes>=0 AND available_bytes<=total_bytes)
);

CREATE TABLE notification_tests (
 id TEXT PRIMARY KEY REFERENCES notification_alerts(id),
 actor_id TEXT NOT NULL,
 revision INTEGER NOT NULL
);
