-- Cluster membership. Specification 08 requires a stable primary while replicas are
-- added and removed. This table names each member.
--
-- Synod declares the primary. It does not read the primary from reservation order or
-- from the engine. Reservation order changes when a reservation is released and taken
-- again. The engine reports only the role it believes now. Neither fact survives a
-- restart, a retry, or a partition.
CREATE TABLE cluster_members (
 installation_id uuid NOT NULL,
 cluster_id uuid NOT NULL,
 member_id uuid NOT NULL,
 -- patroni: a member of a Patroni cluster. Patroni elects its primary, so Synod
 -- declares none, and the one-primary index below does not apply.
 role text NOT NULL CHECK (role IN ('primary','replica','patroni')),
 -- The instance resource this member runs as. The member and its capacity share one
 -- identity, so one read answers for both.
 resource_id uuid NOT NULL,
 -- pending: Synod reserved and planned the member.
 -- active: a checked receipt proved the declared role.
 -- residual: a terminal failure left the identity behind. An exact retry resumes it.
 -- A retry must not add a second member, because the data can already exist.
 state text NOT NULL CHECK (state IN ('pending','active','residual')),
 created_operation_id uuid NOT NULL,
 created_revision bigint NOT NULL CHECK (created_revision>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 -- Set when a scale down removes the member. The row stays. A later add receives a
 -- new member identity.
 removed_at timestamptz,
 -- The operation whose plan holds the current container configuration, when it is not
 -- the creating operation. A certificate renewal recreates the container.
 container_operation_id uuid,
 PRIMARY KEY (installation_id,member_id),
 UNIQUE (installation_id,resource_id),
 FOREIGN KEY (installation_id,cluster_id,created_operation_id) REFERENCES operations (installation_id,cluster_id,id),
 FOREIGN KEY (installation_id,cluster_id,container_operation_id) REFERENCES operations (installation_id,cluster_id,id)
);

-- One live primary for each cluster. The database refuses a second one. A writer does
-- not need to remember the rule.
CREATE UNIQUE INDEX cluster_members_primary ON cluster_members (installation_id,cluster_id)
 WHERE role='primary' AND removed_at IS NULL;

CREATE INDEX cluster_members_live ON cluster_members (installation_id,cluster_id)
 WHERE removed_at IS NULL;
