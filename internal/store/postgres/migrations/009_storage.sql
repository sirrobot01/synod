-- Volume identity. Specification 33 requires persisting the node, volume, engine
-- identity, path, capacity request, and retention state, and checking the mounted
-- device or volume identity before the engine starts.
--
-- The row is the declaration: what this installation believes the volume is. The
-- agent proves the native object matches it before any engine starts, because an
-- empty mount path is not proof of a new database volume.
CREATE TABLE volume_identities (
 installation_id uuid NOT NULL,
 node_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 -- The volume is a resource, so it shares the resource identity space and can
 -- never collide with a container or a network.
 resource_id uuid NOT NULL,
 engine text NOT NULL CHECK (engine IN ('postgres','redis')),
 engine_version text NOT NULL CHECK (octet_length(engine_version) BETWEEN 1 AND 64),
 -- The path the engine reads inside its container. The host mountpoint belongs to
 -- the agent, which verifies it locally; it never crosses the wire.
 data_path text NOT NULL CHECK (octet_length(data_path) BETWEEN 1 AND 1024 AND data_path LIKE '/%'),
 capacity_request_bytes bigint NOT NULL CHECK (capacity_request_bytes > 0),
 -- Retention outlives the cluster. A delete retains by default, so a row here can
 -- name storage that no running cluster refers to any more.
 retention text NOT NULL CHECK (retention IN ('retained','purge_requested','purged')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (installation_id,resource_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);
CREATE INDEX volume_identities_by_node ON volume_identities (installation_id,node_id);

-- A cluster tombstone. Specification 33 requires keeping the tombstone and the
-- reservations for an offline node until cleanup is proved, so a delete is not
-- finished when the controller says so: it is finished when every node that held
-- part of the cluster has said the runtime objects are gone.
--
-- The row also records what the delete retained. A normal delete keeps data, and an
-- operator needs to find that storage later without reconstructing it from operations.
CREATE TABLE cluster_tombstones (
 installation_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 deleted_by_operation uuid NOT NULL,
 retained_volumes integer NOT NULL CHECK (retained_volumes >= 0),
 -- Backups have a separate retention and deletion policy, so a cluster delete
 -- states what it did about them rather than implying it.
 backup_disposition text NOT NULL CHECK (backup_disposition IN ('retained_under_policy','no_policy')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 -- Null until every named node has acknowledged. An offline node holds it open.
 cleared_at timestamptz,
 PRIMARY KEY (installation_id,cluster_id),
 FOREIGN KEY (installation_id,cluster_id,deleted_by_operation) REFERENCES operations (installation_id,cluster_id,id)
);

-- One row per node that held part of the cluster. acknowledged_at is null while the
-- node has not proved its runtime objects are gone.
CREATE TABLE cluster_tombstone_nodes (
 installation_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 node_id uuid NOT NULL,
 acknowledged_at timestamptz,
 PRIMARY KEY (installation_id,cluster_id,node_id),
 FOREIGN KEY (installation_id,cluster_id) REFERENCES cluster_tombstones (installation_id,cluster_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);
