-- claim_barriers is created in 002 and agent_sessions is created in 005.
-- The reference must therefore wait for a later migration. It is not a compatibility step.
ALTER TABLE claim_barriers ADD COLUMN session_id uuid REFERENCES agent_sessions (session_id);

CREATE TABLE command_admissions (
    command_id uuid NOT NULL,
    execution_epoch bigint NOT NULL,
    recovery_generation uuid NOT NULL,
    accepted_rank integer NOT NULL DEFAULT 0 CHECK (accepted_rank BETWEEN 0 AND 4),
    accepted_bytes bytea CHECK (octet_length(accepted_bytes) BETWEEN 1 AND 4096),
    accepted_session_id uuid REFERENCES agent_sessions (session_id),
    rejection_bytes bytea CHECK (octet_length(rejection_bytes) BETWEEN 1 AND 4096),
    rejection_session_id uuid REFERENCES agent_sessions (session_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (command_id,execution_epoch,recovery_generation),
    FOREIGN KEY (command_id,execution_epoch,recovery_generation)
        REFERENCES command_dispatches (command_id,execution_epoch,recovery_generation),
    CHECK ((accepted_rank=0)=(accepted_bytes IS NULL)),
    CHECK ((accepted_bytes IS NULL)=(accepted_session_id IS NULL)),
    CHECK ((rejection_bytes IS NULL)=(rejection_session_id IS NULL))
);

CREATE TABLE command_inspections (
    request_id uuid PRIMARY KEY,
    claim_id uuid NOT NULL REFERENCES execution_claims (claim_id),
    installation_id uuid NOT NULL,
    node_id uuid NOT NULL,
    session_id uuid NOT NULL,
    command_id uuid NOT NULL REFERENCES command_identities (command_id),
    request_bytes bytea NOT NULL CHECK (octet_length(request_bytes) BETWEEN 1 AND 1048576),
    result_bytes bytea CHECK (octet_length(result_bytes) BETWEEN 1 AND 1048576),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    received_at timestamptz,
    FOREIGN KEY (installation_id,node_id,session_id) REFERENCES agent_sessions (installation_id,node_id,session_id),
    CHECK ((result_bytes IS NULL)=(received_at IS NULL))
);
CREATE INDEX command_inspections_claim ON command_inspections (claim_id,request_id);

CREATE TABLE command_delivery_attempts (
    attempt_id uuid PRIMARY KEY,
    claim_id uuid NOT NULL REFERENCES execution_claims (claim_id),
    command_id uuid NOT NULL,
    execution_epoch bigint NOT NULL,
    recovery_generation uuid NOT NULL,
    session_id uuid NOT NULL REFERENCES agent_sessions (session_id),
    attempt_count integer NOT NULL CHECK (attempt_count BETWEEN 1 AND 10),
    retry_delay_us bigint NOT NULL CHECK (retry_delay_us BETWEEN 1000 AND 60000000),
    created_at timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    UNIQUE (command_id,attempt_count),
    FOREIGN KEY (command_id,execution_epoch,recovery_generation)
        REFERENCES command_dispatches (command_id,execution_epoch,recovery_generation)
);

CREATE TABLE operation_step_completions (
    operation_id uuid NOT NULL REFERENCES operations (id),
    step_id uuid NOT NULL,
    command_id uuid NOT NULL,
    execution_epoch bigint NOT NULL,
    recovery_generation uuid NOT NULL,
    receipt_hash bytea NOT NULL CHECK (octet_length(receipt_hash)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (operation_id,step_id),
    FOREIGN KEY (command_id,execution_epoch,recovery_generation)
        REFERENCES command_results (command_id,execution_epoch,recovery_generation)
);

CREATE TABLE claim_recovery_checkpoints (
    claim_id uuid PRIMARY KEY REFERENCES execution_claims (claim_id),
    frontier_hash bytea NOT NULL CHECK (octet_length(frontier_hash)=32),
    inspection_ids uuid[] NOT NULL CHECK (cardinality(inspection_ids)<=256),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
