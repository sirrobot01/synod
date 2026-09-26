CREATE TABLE clusters (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    name text NOT NULL CHECK (name ~ '^[a-z][a-z0-9-]{0,62}$'),
    engine text NOT NULL CHECK (engine IN ('postgres', 'redis')),
    spec_version integer NOT NULL CHECK (spec_version = 1),
    spec jsonb NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    desired_lifecycle text NOT NULL CHECK (desired_lifecycle IN ('running', 'stopped', 'deleted')),
    -- How many replicas the cluster should run beside its primary. A scale moves this
    -- the way a stop moves the desired lifecycle. Redis allows one. A Patroni cluster runs
    -- desired_replicas+1 members, three to five.
    desired_replicas integer NOT NULL DEFAULT 0 CHECK (desired_replicas BETWEEN 0 AND 4),
    observed_revision bigint NOT NULL CHECK (observed_revision >= 0),
    observed_lifecycle text NOT NULL CHECK (observed_lifecycle IN ('provisioning', 'running', 'stopped', 'deleting', 'deleted', 'unknown')),
    execution_epoch bigint NOT NULL CHECK (execution_epoch >= 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz,
    UNIQUE (installation_id, id),
    CHECK ((spec->>'name') IS NOT DISTINCT FROM name),
    CHECK ((spec->>'engine') IS NOT DISTINCT FROM engine),
    CHECK ((spec->>'schema_version') IS NOT DISTINCT FROM spec_version::text)
);
CREATE UNIQUE INDEX clusters_active_name ON clusters (installation_id, name) WHERE deleted_at IS NULL;

CREATE INDEX clusters_listing ON clusters (installation_id, created_at DESC, id DESC);

CREATE TABLE operations (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    api_operation text NOT NULL CHECK (api_operation IN ('clusters.create.v1','clusters.stop.v1','clusters.start.v1','clusters.delete.v1','clusters.scale.v1','clusters.recover_setup.v1','clusters.rebuild_member.v1','clusters.renew_certificates.v1','clusters.switchover.v1')),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    hash_version integer NOT NULL CHECK (hash_version = 1),
    kind text NOT NULL CHECK (kind IN ('cluster.create','cluster.stop','cluster.start','cluster.delete','cluster.scale','cluster.recover_setup','cluster.rebuild_member','cluster.renew_certificates','cluster.switchover')),
    retry_of uuid,
    action_plan_token uuid,
    -- A switchover names the member that becomes the leader. Nothing else names one here.
    target_member_id uuid,
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
    UNIQUE (installation_id, cluster_id, id),
    FOREIGN KEY (installation_id, cluster_id) REFERENCES clusters (installation_id, id),
    FOREIGN KEY (installation_id, cluster_id, retry_of) REFERENCES operations (installation_id, cluster_id, id),
    CHECK (retry_of IS NULL OR (retry_of <> id AND kind = 'cluster.scale')),
    CHECK ((kind = 'cluster.switchover') = (target_member_id IS NOT NULL)),
    CHECK ((owner_id IS NULL) = (claim_expires_at IS NULL))
);
CREATE UNIQUE INDEX operations_active_cluster ON operations (installation_id, cluster_id)
    WHERE status IN ('pending', 'running', 'waiting', 'cancelling');

CREATE TABLE idempotency_results (
    installation_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    api_operation text NOT NULL CHECK (api_operation IN ('clusters.create.v1','clusters.stop.v1','clusters.start.v1','clusters.delete.v1','clusters.scale.v1','clusters.recover_setup.v1','clusters.rebuild_member.v1','clusters.renew_certificates.v1','clusters.switchover.v1')),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    hash_version integer NOT NULL CHECK (hash_version = 1),
    cluster_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    accepted_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, actor_id, api_operation, idempotency_key),
    FOREIGN KEY (installation_id, cluster_id, operation_id)
        REFERENCES operations (installation_id, cluster_id, id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind = 'operation.ready'),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    created_at timestamptz NOT NULL,
    delivered_at timestamptz,
    FOREIGN KEY (installation_id, cluster_id, operation_id) REFERENCES operations (installation_id, cluster_id, id),
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
    cluster_id uuid,
    operation_id uuid,
    node_id uuid,
    node_certificate_id uuid,
    enrollment_token_id uuid,
    credential_id uuid,
    credential_version bigint CHECK (credential_version > 0),
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    request_id text NOT NULL CHECK (request_id ~ '^[!-~]{1,128}$'),
    action text NOT NULL CHECK (action IN ('cluster.create.accepted','cluster.stop.accepted','cluster.start.accepted','cluster.delete.accepted','cluster.scale.accepted','cluster.recover_setup.accepted','cluster.rebuild_member.accepted','cluster.renew_certificates.accepted','cluster.switchover.accepted','enrollment.token.registered','node.enrolled','node.certificate.revoked','credential.reveal.accepted')),
    outcome text NOT NULL CHECK (outcome = 'accepted'),
    created_at timestamptz NOT NULL,
    FOREIGN KEY (installation_id, cluster_id, operation_id) REFERENCES operations (installation_id, cluster_id, id),
    UNIQUE (operation_id, action),
    CHECK (num_nonnulls(cluster_id,node_id,enrollment_token_id)=1),
    CHECK ((cluster_id IS NOT NULL)=(operation_id IS NOT NULL OR credential_id IS NOT NULL)),
    CHECK ((credential_id IS NULL)=(credential_version IS NULL)),
    CHECK ((action='credential.reveal.accepted')=(credential_id IS NOT NULL)),
    CHECK (credential_id IS NULL OR operation_id IS NULL),
    CHECK (node_certificate_id IS NULL OR node_id IS NOT NULL),
    CHECK ((action IN ('node.enrolled','node.certificate.revoked'))=(node_certificate_id IS NOT NULL))
);
