CREATE TABLE nodes (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    public_key_hash bytea NOT NULL CHECK (octet_length(public_key_hash)=32),
    enrolled_at timestamptz NOT NULL,
    revoked_at timestamptz,
    UNIQUE (installation_id,id)
);

CREATE TABLE node_certificates (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    node_id uuid NOT NULL,
    serial text NOT NULL,
    der_hash bytea NOT NULL CHECK (octet_length(der_hash)=32),
    certificate_pem bytea NOT NULL CHECK (octet_length(certificate_pem) BETWEEN 1 AND 65536),
    ca_certificate_pem bytea NOT NULL CHECK (octet_length(ca_certificate_pem) BETWEEN 1 AND 65536),
    not_before timestamptz NOT NULL,
    not_after timestamptz NOT NULL CHECK (not_after>not_before),
    revoked_at timestamptz,
    UNIQUE (installation_id,id),
    UNIQUE (installation_id,serial),
    UNIQUE (installation_id,node_id,id),
    FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);

CREATE TABLE enrollment_tokens (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    node_id uuid,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    token_verifier bytea NOT NULL CHECK (octet_length(token_verifier)=32),
    ttl_seconds integer NOT NULL CHECK (ttl_seconds BETWEEN 60 AND 604800),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at>created_at),
    revoked_at timestamptz,
    consumed_at timestamptz,
    enrollment_request_id uuid,
    csr_hash bytea CHECK (csr_hash IS NULL OR octet_length(csr_hash)=32),
    certificate_id uuid,
    UNIQUE (installation_id,id),
    UNIQUE (installation_id,actor_id,idempotency_key),
    UNIQUE (installation_id,token_verifier),
    FOREIGN KEY (installation_id,node_id,certificate_id) REFERENCES node_certificates (installation_id,node_id,id),
    CHECK (num_nonnulls(consumed_at,enrollment_request_id,csr_hash,certificate_id) IN (0,4) AND (consumed_at IS NULL OR node_id IS NOT NULL))
);

-- These references require the enrollment tables created by this migration.
ALTER TABLE audit_events ADD FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id);
ALTER TABLE audit_events ADD FOREIGN KEY (installation_id,node_id,node_certificate_id) REFERENCES node_certificates (installation_id,node_id,id);
ALTER TABLE audit_events ADD FOREIGN KEY (installation_id,enrollment_token_id) REFERENCES enrollment_tokens (installation_id,id);
