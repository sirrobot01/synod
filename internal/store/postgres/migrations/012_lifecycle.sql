CREATE TABLE cluster_action_plans (
    plan_token uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    cluster_id uuid NOT NULL,
    cluster_name text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision > 0),
    action text NOT NULL CHECK (action IN ('delete','scale_down','recover_setup')),
    data_disposition text NOT NULL CHECK (data_disposition = 'retain'),
    retained_volumes uuid[] NOT NULL,
    -- Replica removal and setup recovery name one member. Delete names none.
    member_id uuid,
    CHECK ((action IN ('scale_down','recover_setup')) = (member_id IS NOT NULL)),
    resource_id uuid,
    CHECK ((action = 'recover_setup') = (resource_id IS NOT NULL)),
    downtime text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    UNIQUE (installation_id,cluster_id,actor_id,plan_token),
    FOREIGN KEY (installation_id,cluster_id) REFERENCES clusters (installation_id,id)
);
CREATE INDEX cluster_action_plans_expiry ON cluster_action_plans (expires_at);

-- This reference follows operations because this migration creates action plans.
ALTER TABLE operations ADD CONSTRAINT operations_action_plan_fk
    FOREIGN KEY (installation_id,cluster_id,actor_id,action_plan_token)
    REFERENCES cluster_action_plans (installation_id,cluster_id,actor_id,plan_token);
