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
    writer_epoch bigint NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    writer_node_id uuid,
    writer_last_grant_at timestamptz,
    failover_loss_window_seconds double precision CHECK (failover_loss_window_seconds >= 0 AND failover_loss_window_seconds < 'Infinity'::double precision),
    CHECK ((writer_epoch = 0 AND writer_node_id IS NULL AND writer_last_grant_at IS NULL)
        OR (tier IN (1, 2) AND writer_epoch > 0 AND writer_node_id IS NOT NULL)),
    FOREIGN KEY (installation_id, writer_node_id) REFERENCES nodes (installation_id, id),
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
    role text NOT NULL DEFAULT 'parallel' CHECK (role IN ('parallel','active','standby')),
    host_port integer NOT NULL CHECK (host_port >= 0 AND host_port <= 65535),
    desired boolean NOT NULL DEFAULT true,
    rebuild_required boolean NOT NULL DEFAULT false,
    rebuild_data_removed boolean NOT NULL DEFAULT false,
    CHECK (NOT rebuild_data_removed OR rebuild_required),
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
CREATE UNIQUE INDEX app_copies_writer ON app_copies (installation_id,app_id) WHERE desired AND role='active';
CREATE UNIQUE INDEX app_copies_writer_node ON app_copies (installation_id,app_id,node_id) WHERE desired AND role IN ('active','standby');
CREATE INDEX app_copies_node ON app_copies (installation_id, node_id);

-- Operations is created in migration 001. Apps does not exist until this migration.
-- Add the reference here because its target is created later than operations.
ALTER TABLE operations ADD FOREIGN KEY (installation_id,app_id) REFERENCES apps (installation_id,id);

-- App admission records the retry identity for durable operation admission.
-- The actor and request identify accepted work without storing environment values twice.
CREATE TABLE app_admissions (
    api_operation text NOT NULL CHECK (api_operation IN ('app.create','app.stop','app.start','app.delete','app.move')),
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
    FOREIGN KEY (installation_id,app_id,operation_id) REFERENCES operations (installation_id,app_id,id) DEFERRABLE INITIALLY DEFERRED,
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

CREATE TABLE agent_replication (
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

CREATE TABLE app_replicas (
 installation_id uuid NOT NULL,
 app_id uuid NOT NULL,
 node_id uuid NOT NULL,
 session_id uuid NOT NULL,
 role text NOT NULL CHECK (role IN ('active','standby')),
 writer_epoch bigint NOT NULL CHECK (writer_epoch > 0),
 sequence bigint NOT NULL CHECK (sequence >= 0),
 completed_at timestamptz,
 last_pause_nanos bigint NOT NULL DEFAULT 0 CHECK(last_pause_nanos>=0),
 max_pause_nanos bigint NOT NULL DEFAULT 0 CHECK(max_pause_nanos>=last_pause_nanos),
 skipped_snapshots bigint NOT NULL DEFAULT 0 CHECK(skipped_snapshots>=0),
 observed_at timestamptz NOT NULL,
 failure_code text NOT NULL DEFAULT '',
 -- Tier 1 only: the engine's replication state and the age of its last applied change.
 state text NOT NULL DEFAULT '' CHECK (state IN ('','copying','streaming','disconnected','drifted','failed')),
 database_hold_id uuid,
 lag_seconds double precision CHECK (lag_seconds IS NULL OR (lag_seconds >= 0 AND lag_seconds < 1e9)),
 PRIMARY KEY (installation_id,app_id,node_id),
 FOREIGN KEY (installation_id,app_id) REFERENCES apps (installation_id,id) ON DELETE CASCADE,
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id),
 CHECK ((sequence=0 AND completed_at IS NULL) OR (sequence>0 AND completed_at IS NOT NULL))
);

-- A move keeps its source identity until final-copy evidence permits promotion.
CREATE TABLE app_moves (
 installation_id uuid NOT NULL,
 app_id uuid NOT NULL,
 operation_id uuid PRIMARY KEY,
 source_node_id uuid NOT NULL,
 target_node_id uuid NOT NULL,
 writer_epoch bigint NOT NULL CHECK(writer_epoch>0),
 target_copy_id uuid REFERENCES app_copies(id),
 promoted_at timestamptz,
 rollback_operation_id uuid UNIQUE,
 CHECK(source_node_id<>target_node_id),
 FOREIGN KEY(installation_id,source_node_id) REFERENCES nodes(installation_id,id),
 FOREIGN KEY(installation_id,target_node_id) REFERENCES nodes(installation_id,id),
 FOREIGN KEY(installation_id,app_id,rollback_operation_id) REFERENCES operations(installation_id,app_id,id),
 FOREIGN KEY(installation_id,app_id,operation_id) REFERENCES operations(installation_id,app_id,id)
);

-- A rejoin rebuilds one standby without waiting for the other devices.
CREATE TABLE app_rejoins (
 installation_id uuid NOT NULL,
 app_id uuid NOT NULL,
 operation_id uuid PRIMARY KEY,
 copy_id uuid NOT NULL REFERENCES app_copies(id),
 writer_epoch bigint NOT NULL CHECK(writer_epoch>0),
 remove_data boolean NOT NULL,
 FOREIGN KEY(installation_id,app_id,operation_id) REFERENCES operations(installation_id,app_id,id)
);
