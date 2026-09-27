CREATE TABLE agent_sessions (
    session_id uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    node_id uuid NOT NULL,
    hello_bytes bytea NOT NULL CHECK (octet_length(hello_bytes) BETWEEN 1 AND 65536),
    connected_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (installation_id, node_id, session_id),
    FOREIGN KEY (installation_id, node_id) REFERENCES nodes (installation_id, id)
);
CREATE TABLE agent_presence (
    installation_id uuid NOT NULL,
    node_id uuid NOT NULL,
    session_id uuid NOT NULL,
    heartbeat_sequence bigint NOT NULL DEFAULT 0 CHECK (heartbeat_sequence >= 0),
    heartbeat_bytes bytea CHECK (octet_length(heartbeat_bytes) BETWEEN 1 AND 262144),
    online_since timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    recovery_required boolean NOT NULL DEFAULT false,
    PRIMARY KEY (installation_id, node_id),
    FOREIGN KEY (installation_id, node_id, session_id)
        REFERENCES agent_sessions (installation_id, node_id, session_id),
    CHECK ((heartbeat_sequence = 0) = (heartbeat_bytes IS NULL))
);
