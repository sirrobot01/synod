-- Times are Unix milliseconds in UTC, so SQL compares them as numbers.

CREATE TABLE users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE CHECK (length(username) BETWEEN 1 AND 64),
    password_hash TEXT NOT NULL CHECK (length(password_hash) BETWEEN 1 AND 512),
    role TEXT NOT NULL CHECK (role IN ('admin')),
    failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    locked_until INTEGER,
    disabled_at INTEGER,
    password_changed_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- The verifier is the SHA-256 of the cookie value. The value itself is never stored.
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users (id),
    verifier BLOB NOT NULL UNIQUE CHECK (length(verifier) = 32),
    created_at INTEGER NOT NULL,
    last_used_at INTEGER NOT NULL,
    absolute_expires_at INTEGER NOT NULL,
    revoked_at INTEGER
);
CREATE INDEX sessions_live_user ON sessions (user_id) WHERE revoked_at IS NULL;

-- A token secret is high-entropy random, so SHA-256 is the correct verifier.
CREATE TABLE api_tokens (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 256),
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 64),
    verifier BLOB NOT NULL CHECK (length(verifier) = 32),
    role TEXT NOT NULL CHECK (role IN ('admin')),
    created_at INTEGER NOT NULL,
    last_used_at INTEGER,
    expires_at INTEGER,
    revoked_at INTEGER
);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 256),
    request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
    action TEXT NOT NULL CHECK (action IN ('enrollment.token.registered', 'node.enrolled', 'node.approved', 'node.certificate.revoked', 'destination.added', 'destination.removed', 'backup.requested')),
    subject_id TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
