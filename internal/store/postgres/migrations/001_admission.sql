CREATE TABLE operations (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    app_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    api_operation text NOT NULL CHECK (api_operation IN ('app.create','app.stop','app.start','app.delete','app.move','app.failover','app.rejoin')),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    hash_version integer NOT NULL CHECK (hash_version = 1),
    kind text NOT NULL CHECK (kind IN ('app.deploy','app.stop','app.start','app.delete','app.move','app.failover','app.rejoin')),
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    status text NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'cancelling', 'succeeded', 'failed', 'cancelled')),
    plan_version integer NOT NULL CHECK (plan_version >= 0),
    owner_id text,
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 0),
    claim_expires_at timestamptz,
    attempts integer NOT NULL CHECK (attempts >= 0),
    next_attempt_at timestamptz,
    cancellation_requested_at timestamptz,
    finished_at timestamptz,
    failure_code text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (installation_id, app_id, id),
    CHECK ((owner_id IS NULL) = (claim_expires_at IS NULL))
);
CREATE UNIQUE INDEX operations_active_app ON operations (installation_id, app_id)
    WHERE status IN ('pending', 'running', 'waiting', 'cancelling');

CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    installation_id uuid NOT NULL,
    app_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind = 'operation.ready'),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    created_at timestamptz NOT NULL,
    delivered_at timestamptz,
    FOREIGN KEY (installation_id, app_id, operation_id) REFERENCES operations (installation_id, app_id, id),
    UNIQUE (operation_id, kind)
);
CREATE INDEX outbox_pending ON outbox (installation_id, id) WHERE delivered_at IS NULL;

-- A committed outbox row wakes a listening relay. The payload names the installation.
-- NOTIFY is transactional, and a lost notification only delays a scan.
CREATE FUNCTION outbox_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('synod_outbox', NEW.installation_id::text);
    RETURN NULL;
END;
$$;
CREATE TRIGGER outbox_notify AFTER INSERT ON outbox
    FOR EACH ROW EXECUTE FUNCTION outbox_notify();

CREATE TABLE audit_events (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    app_id uuid,
    operation_id uuid,
    node_id uuid,
    node_certificate_id uuid,
    enrollment_token_id uuid,
    credential_id uuid,
    credential_version bigint CHECK (credential_version > 0),
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    request_id text NOT NULL CHECK (request_id ~ '^[!-~]{1,128}$'),
    action text NOT NULL CHECK (action IN ('enrollment.token.registered','node.enrolled','node.certificate.revoked','credential.reveal.accepted')),
    outcome text NOT NULL CHECK (outcome = 'accepted'),
    created_at timestamptz NOT NULL,
    FOREIGN KEY (installation_id, app_id, operation_id) REFERENCES operations (installation_id, app_id, id),
    UNIQUE (operation_id, action),
    CHECK (num_nonnulls(app_id,node_id,enrollment_token_id)=1),
    CHECK ((app_id IS NOT NULL)=(operation_id IS NOT NULL OR credential_id IS NOT NULL)),
    CHECK ((credential_id IS NULL)=(credential_version IS NULL)),
    CHECK ((action='credential.reveal.accepted')=(credential_id IS NOT NULL)),
    CHECK (credential_id IS NULL OR operation_id IS NULL),
    CHECK (node_certificate_id IS NULL OR node_id IS NOT NULL),
    CHECK ((action IN ('node.enrolled','node.certificate.revoked'))=(node_certificate_id IS NOT NULL))
);
