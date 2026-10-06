-- A node is one enrolled device. Presence lives on the row, because one controller
-- holds at most one live session for each node.
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    public_key_hash BLOB NOT NULL CHECK (length(public_key_hash) = 32),
    enrolled_at INTEGER NOT NULL,
    approved_at INTEGER,
    approved_by TEXT,
    revoked_at INTEGER,
    name TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 253),
    architecture TEXT NOT NULL DEFAULT '',
    runtime_version TEXT NOT NULL DEFAULT '',
    agent_version TEXT NOT NULL DEFAULT '',
    destination_address TEXT NOT NULL DEFAULT '',
    session_id TEXT,
    connected_at INTEGER,
    last_seen_at INTEGER,
    heartbeat_sequence INTEGER NOT NULL DEFAULT 0 CHECK (heartbeat_sequence >= 0),
    CHECK ((approved_at IS NULL) = (approved_by IS NULL))
);

CREATE TABLE node_certificates (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes (id),
    serial TEXT NOT NULL UNIQUE,
    der_hash BLOB NOT NULL CHECK (length(der_hash) = 32),
    certificate_pem TEXT NOT NULL CHECK (length(certificate_pem) BETWEEN 1 AND 65536),
    ca_certificate_pem TEXT NOT NULL CHECK (length(ca_certificate_pem) BETWEEN 1 AND 65536),
    not_before INTEGER NOT NULL,
    not_after INTEGER NOT NULL CHECK (not_after > not_before),
    revoked_at INTEGER
);

CREATE TABLE enrollment_tokens (
    id TEXT PRIMARY KEY,
    node_id TEXT,
    actor_id TEXT NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 256),
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    token_verifier BLOB NOT NULL UNIQUE CHECK (length(token_verifier) = 32),
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL CHECK (expires_at > created_at),
    revoked_at INTEGER,
    consumed_at INTEGER,
    enrollment_request_id TEXT,
    csr_hash BLOB CHECK (csr_hash IS NULL OR length(csr_hash) = 32),
    certificate_id TEXT REFERENCES node_certificates (id),
    UNIQUE (actor_id, idempotency_key),
    CHECK ((consumed_at IS NULL) = (enrollment_request_id IS NULL)
       AND (consumed_at IS NULL) = (csr_hash IS NULL)
       AND (consumed_at IS NULL) = (certificate_id IS NULL)),
    CHECK (consumed_at IS NULL OR node_id IS NOT NULL)
);
