CREATE TABLE cluster_action_plans (
    plan_token uuid PRIMARY KEY,
    installation_id uuid NOT NULL,
    actor_id text NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    cluster_id uuid NOT NULL,
    cluster_name text NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision > 0),
    action text NOT NULL CHECK (action IN ('delete','scale_down','scale_down_failed','recover_setup','rebuild_replica','rebuild_member')),
    data_disposition text NOT NULL CHECK (data_disposition = 'retain'),
    retained_volumes uuid[] NOT NULL,
    -- Replica removal, setup recovery, and member rebuild name one member. Delete names none.
    member_id uuid,
    CHECK ((action IN ('scale_down','scale_down_failed','recover_setup','rebuild_replica','rebuild_member')) = (member_id IS NOT NULL)),
    resource_id uuid,
    CHECK ((action IN ('recover_setup','scale_down_failed','rebuild_replica','rebuild_member')) = (resource_id IS NOT NULL)),
    downtime text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    UNIQUE (installation_id,cluster_id,actor_id,plan_token),
    FOREIGN KEY (installation_id,cluster_id) REFERENCES clusters (installation_id,id)
);
CREATE INDEX cluster_action_plans_expiry ON cluster_action_plans (expires_at);

-- A completed Patroni delete leaves the cluster's DCS keys and etcd account. The controller
-- removes them after the members are gone, then records completion. A repeat inserts nothing.
CREATE TABLE patroni_dcs_cleanups (
    installation_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    requested_by_operation uuid NOT NULL,
    requested_at timestamptz NOT NULL,
    completed_at timestamptz,
    PRIMARY KEY (installation_id,cluster_id),
    FOREIGN KEY (installation_id,cluster_id) REFERENCES clusters (installation_id,id)
);
CREATE INDEX patroni_dcs_cleanups_pending ON patroni_dcs_cleanups (installation_id) WHERE completed_at IS NULL;

-- This reference follows operations because this migration creates action plans.
ALTER TABLE operations ADD CONSTRAINT operations_action_plan_fk
    FOREIGN KEY (installation_id,cluster_id,actor_id,action_plan_token)
    REFERENCES cluster_action_plans (installation_id,cluster_id,actor_id,plan_token);
