CREATE TABLE users (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    username text NOT NULL CHECK (username ~ '^[a-z0-9._-]{1,64}$'),
    password_hash text NOT NULL CHECK (octet_length(password_hash) BETWEEN 1 AND 512),
    role text NOT NULL CHECK (role IN ('admin')),
    failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    locked_until timestamptz,
    disabled_at timestamptz,
    password_changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (installation_id, username)
);

-- The verifier is the SHA-256 of the cookie value. The value itself is never stored,
-- so a reader of this table cannot sign in as anybody.
CREATE TABLE sessions (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    user_id uuid NOT NULL REFERENCES users (id),
    verifier bytea NOT NULL UNIQUE CHECK (octet_length(verifier)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_used_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    absolute_expires_at timestamptz NOT NULL,
    revoked_at timestamptz
);
CREATE INDEX sessions_live_user ON sessions (installation_id, user_id) WHERE revoked_at IS NULL;

-- A token secret is high-entropy random, so SHA-256 is the correct verifier.
-- The token carries its row identity, so verification reads exactly one row.
CREATE TABLE api_tokens (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 64),
    verifier bytea NOT NULL CHECK (octet_length(verifier)=32),
    role text NOT NULL CHECK (role IN ('admin')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_used_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz
);
CREATE INDEX api_tokens_live ON api_tokens (installation_id) WHERE revoked_at IS NULL;
