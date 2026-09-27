-- Cluster intent and its credentials. Specification 07 requires persisting intended
-- database names, roles, and credential versions before dispatch.
--
-- Dispatching first and recording afterwards leaves a credential that reached a host
-- but that the installation cannot name. Nothing can find it, rotate it, or revoke it.
--
-- No plaintext is stored here. The sealed column holds a secret envelope from
-- internal/secret, whose associated data binds it to this installation, this cluster,
-- this credential, and this version, so an envelope cannot be moved to another owner.
CREATE TABLE cluster_credentials (
 installation_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 credential_id uuid NOT NULL,
 -- Version rises on rotation. The row is never updated in place, so an older
 -- version stays readable while a rotation is still reaching hosts.
 version bigint NOT NULL CHECK (version > 0),
 purpose text NOT NULL CHECK (purpose IN ('superuser','application','replication')),
 -- Each password names its account.
 role_name text NOT NULL CHECK (role_name ~ '^[a-z][a-z0-9_]{0,62}$'),
 -- The wrapping key that sealed it. Old keys are retained for recovery.
 key_id uuid NOT NULL,
 format_version integer NOT NULL CHECK (format_version = 1),
 sealed bytea NOT NULL CHECK (octet_length(sealed) BETWEEN 1 AND 1048604),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (installation_id,credential_id,version),
 UNIQUE (installation_id,cluster_id,credential_id,version),
 UNIQUE (installation_id,cluster_id,purpose,version),
 FOREIGN KEY (installation_id,cluster_id) REFERENCES clusters (installation_id,id)
);
CREATE INDEX cluster_credentials_by_cluster ON cluster_credentials (installation_id,cluster_id);

-- Migration 001 creates audit events before this migration creates credentials.
-- Add the reference here to bind each reveal to its cluster and stored version.
ALTER TABLE audit_events ADD FOREIGN KEY (installation_id,cluster_id,credential_id,credential_version)
 REFERENCES cluster_credentials (installation_id,cluster_id,credential_id,version);

-- The logical databases this installation intends the cluster to hold.
--
-- Intent, not observation. A row here says what was asked for; whether it exists on a
-- host is an observation and lives nowhere near this table.
CREATE TABLE cluster_databases (
 installation_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 name text NOT NULL CHECK (name ~ '^[a-z][a-z0-9_]{0,62}$'),
 -- The role that owns it. Specification 07 requires granting application roles only
 -- the privileges that were requested, and ownership of its own database is what a
 -- v0.1 standalone cluster requests.
 owner_role text NOT NULL CHECK (owner_role ~ '^[a-z][a-z0-9_]{0,62}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (installation_id,cluster_id,name),
 FOREIGN KEY (installation_id,cluster_id) REFERENCES clusters (installation_id,id)
);
