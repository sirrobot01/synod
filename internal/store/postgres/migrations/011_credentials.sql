-- Tier 1 database credentials. Specification 07 requires persisting roles and
-- credential versions before dispatch.
--
-- Dispatching first and recording afterwards leaves a credential that reached a host
-- but that the installation cannot name. Nothing can find it, rotate it, or revoke it.
--
-- No plaintext is stored here. The sealed column holds a secret envelope from
-- internal/secret, whose associated data binds it to this installation, this app,
-- this credential, and this version, so an envelope cannot be moved to another owner.
CREATE TABLE app_credentials (
 installation_id uuid NOT NULL,
 app_id uuid NOT NULL,
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
 UNIQUE (installation_id,app_id,credential_id,version),
 UNIQUE (installation_id,app_id,purpose,version)
);
CREATE INDEX app_credentials_by_app ON app_credentials (installation_id,app_id);

-- Migration 001 creates audit events before this migration creates credentials.
-- Add the reference here to bind each reveal to its app and stored version.
ALTER TABLE audit_events ADD FOREIGN KEY (installation_id,app_id,credential_id,credential_version)
 REFERENCES app_credentials (installation_id,app_id,credential_id,version);
