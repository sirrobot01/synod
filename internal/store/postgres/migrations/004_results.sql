CREATE TABLE command_results (
    command_id uuid NOT NULL,
    execution_epoch bigint NOT NULL CHECK (execution_epoch > 0),
    recovery_generation uuid NOT NULL,
    result_bytes bytea NOT NULL CHECK (octet_length(result_bytes) BETWEEN 1 AND 1048576),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (command_id, execution_epoch, recovery_generation),
    FOREIGN KEY (command_id, execution_epoch, recovery_generation)
        REFERENCES command_dispatches (command_id, execution_epoch, recovery_generation)
);
