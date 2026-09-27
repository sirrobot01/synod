-- Applications and copies. Product brief:
-- Synod pools mixed devices and runs any container across them in three tiers.
-- Apps are defined by name, tier, copies, device preferences, service container, and router.

CREATE TABLE apps (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    name text NOT NULL CHECK (name ~ '^[a-z][a-z0-9-]{0,62}$'),
    tier integer NOT NULL CHECK (tier IN (1, 2, 3)),
    spec_version integer NOT NULL CHECK (spec_version = 1),
    spec jsonb NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    execution_epoch bigint NOT NULL DEFAULT 0 CHECK (execution_epoch >= 0),
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    desired_lifecycle text NOT NULL CHECK (desired_lifecycle IN ('running', 'stopped', 'deleted')),
    observed_revision bigint NOT NULL CHECK (observed_revision >= 0),
    observed_lifecycle text NOT NULL CHECK (observed_lifecycle IN ('provisioning', 'running', 'stopped', 'deleting', 'deleted', 'unknown')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz,
    UNIQUE (installation_id, id),
    CHECK ((spec->>'name') IS NOT DISTINCT FROM name),
    CHECK ((spec->>'tier')::integer = tier),
    CHECK ((spec->>'schema_version')::integer = spec_version)
);
CREATE UNIQUE INDEX apps_active_name ON apps (installation_id, name) WHERE deleted_at IS NULL;

CREATE TABLE app_copies (
    id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    app_id uuid NOT NULL,
    node_id uuid NOT NULL,
    copy_index integer NOT NULL CHECK (copy_index >= 0),
    host_port integer NOT NULL CHECK (host_port >= 0 AND host_port <= 65535),
    desired boolean NOT NULL DEFAULT true,
    desired_revision bigint NOT NULL DEFAULT 1 CHECK (desired_revision > 0),
    health text NOT NULL DEFAULT 'unknown' CHECK (health IN ('unknown','starting','healthy','unhealthy')),
    observed_at timestamptz,
    observed_session_id uuid,
    observed_state text NOT NULL CHECK (observed_state IN ('pending', 'running', 'unhealthy', 'stopped', 'failed')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (installation_id, id),

    FOREIGN KEY (installation_id, app_id) REFERENCES apps (installation_id, id) ON DELETE CASCADE,
    FOREIGN KEY (installation_id, node_id) REFERENCES nodes (installation_id, id)
);
CREATE UNIQUE INDEX app_copies_active_index ON app_copies (installation_id,app_id,copy_index) WHERE desired;
CREATE INDEX app_copies_node ON app_copies (installation_id, node_id);

-- Operations is created in migration 001. Apps does not exist until this migration.
-- Add the reference here because its target is created later than operations.
ALTER TABLE operations ADD FOREIGN KEY (installation_id,app_id) REFERENCES apps (installation_id,id);

-- App admission records the retry identity for durable operation admission.
-- The actor and request identify accepted work without storing environment values twice.
CREATE TABLE app_admissions (
    api_operation text NOT NULL CHECK (api_operation IN ('app.create','app.stop','app.start','app.delete')),
    installation_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[!-~]{1,128}$'),
    request_id text NOT NULL CHECK (request_id ~ '^[!-~]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    app_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    accepted_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, actor_id, api_operation, idempotency_key),
    FOREIGN KEY (installation_id,app_id,operation_id) REFERENCES operations (installation_id,cluster_id,id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (installation_id, app_id) REFERENCES apps (installation_id, id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE agent_routes (
 installation_id uuid NOT NULL,
 node_id uuid NOT NULL,
 session_id uuid NOT NULL,
 revision bigint NOT NULL CHECK (revision > 0),
 table_bytes bytea NOT NULL CHECK (octet_length(table_bytes) BETWEEN 1 AND 1048576),
 acknowledged_at timestamptz,
 failure_code text NOT NULL DEFAULT '',
 PRIMARY KEY (installation_id,node_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);
