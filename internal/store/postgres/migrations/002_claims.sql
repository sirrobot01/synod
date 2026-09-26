CREATE SEQUENCE cluster_lease_id_seq START WITH 1;

CREATE TABLE cluster_leases (
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    owner_id text NOT NULL,
    lease_id bigint NOT NULL,
    revision bigint NOT NULL CHECK (revision>0),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (installation_id, cluster_id)
);

CREATE TABLE installation_generations (
    installation_id uuid PRIMARY KEY,
    recovery_generation uuid NOT NULL
);
CREATE TABLE execution_claims (
    claim_id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch>0),
    recovery_generation uuid NOT NULL,
    owner_id text NOT NULL,
    lease_id bigint NOT NULL,
    lease_revision bigint NOT NULL CHECK (lease_revision>0),
    acquired_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (installation_id,cluster_id,execution_epoch),
    FOREIGN KEY (installation_id,cluster_id,operation_id) REFERENCES operations (installation_id,cluster_id,id)
);
ALTER TABLE operations ADD COLUMN claim_id uuid REFERENCES execution_claims (claim_id);
ALTER TABLE operations ADD COLUMN recovery_generation uuid;
ALTER TABLE operations ADD COLUMN claim_active boolean NOT NULL DEFAULT false;
ALTER TABLE operations ADD COLUMN lease_id bigint;
ALTER TABLE operations ADD COLUMN lease_revision bigint;
ALTER TABLE operations ADD COLUMN plan_bytes bytea;
ALTER TABLE operations ADD CONSTRAINT operations_claim_identity CHECK (
    (claim_id IS NULL AND recovery_generation IS NULL AND lease_id IS NULL AND lease_revision IS NULL AND NOT claim_active)
    OR (claim_id IS NOT NULL AND recovery_generation IS NOT NULL AND owner_id IS NOT NULL AND lease_id IS NOT NULL AND lease_revision > 0)
);
ALTER TABLE operations ADD CONSTRAINT operations_plan_present CHECK ((plan_version=0)=(plan_bytes IS NULL));
CREATE UNIQUE INDEX operations_claim_id ON operations (claim_id) WHERE claim_id IS NOT NULL;

CREATE TABLE command_identities (
    command_id uuid PRIMARY KEY,
    input_hash bytea NOT NULL CHECK (octet_length(input_hash)=32),
    UNIQUE (command_id,input_hash)
);

CREATE TABLE command_dispatches (
    command_id uuid NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch>0),
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    recovery_generation uuid NOT NULL,
    node_id uuid NOT NULL,
    step_id uuid NOT NULL,
    input_hash bytea NOT NULL CHECK (octet_length(input_hash)=32),
    command_bytes bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (command_id, execution_epoch, recovery_generation),
    FOREIGN KEY (command_id,input_hash) REFERENCES command_identities (command_id,input_hash),
    FOREIGN KEY (installation_id,cluster_id,operation_id) REFERENCES operations (installation_id,cluster_id,id)
);
CREATE INDEX command_dispatches_cluster ON command_dispatches (installation_id,cluster_id,node_id);

CREATE TABLE claim_barriers (
    claim_id uuid NOT NULL REFERENCES execution_claims (claim_id),
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    node_id uuid NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch>0),
    recovery_generation uuid NOT NULL,
    report_bytes bytea,
    reconciled_at timestamptz,
    reconciliation_summary text,
    PRIMARY KEY (claim_id,node_id),
    FOREIGN KEY (installation_id,cluster_id,operation_id) REFERENCES operations (installation_id,cluster_id,id),
    CHECK ((reconciled_at IS NULL)=(reconciliation_summary IS NULL)),
    CHECK (reconciled_at IS NULL OR report_bytes IS NOT NULL)
);

CREATE TABLE operation_progress (
    operation_id uuid NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch>0),
    sequence bigint NOT NULL CHECK (sequence>0),
    state text NOT NULL CHECK (state IN ('running','waiting','cancelling','succeeded','failed','cancelled')),
    reason text NOT NULL,
    unknown_effects boolean NOT NULL CHECK (NOT unknown_effects OR state='waiting'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (operation_id,execution_epoch,sequence),
    FOREIGN KEY (operation_id) REFERENCES operations (id)
);
