CREATE TABLE node_placement_policies (
 installation_id uuid NOT NULL, node_id uuid NOT NULL,
 revision bigint NOT NULL CHECK (revision>0),
 policy_bytes bytea NOT NULL CHECK (octet_length(policy_bytes) BETWEEN 1 AND 65536),
 PRIMARY KEY (installation_id,node_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);
CREATE TABLE node_capacity (
 installation_id uuid NOT NULL, node_id uuid NOT NULL,
 probe_revision bigint NOT NULL CHECK (probe_revision>0),
 probe_id uuid NOT NULL UNIQUE,
 probe_bytes bytea NOT NULL CHECK (octet_length(probe_bytes) BETWEEN 1 AND 4096),
 report_bytes bytea CHECK (octet_length(report_bytes) BETWEEN 1 AND 262144),
 PRIMARY KEY (installation_id,node_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);
CREATE TABLE resource_allocations (
 id uuid PRIMARY KEY, installation_id uuid NOT NULL, node_id uuid NOT NULL,
 cluster_id uuid NOT NULL, resource_id uuid NOT NULL,
 created_operation_id uuid NOT NULL, created_revision bigint NOT NULL CHECK (created_revision>0),
 allocation_bytes bytea NOT NULL CHECK (octet_length(allocation_bytes) BETWEEN 1 AND 65536),
 released_at timestamptz,
 UNIQUE (installation_id,resource_id),
 UNIQUE (installation_id,cluster_id,node_id,id),
 UNIQUE (installation_id,cluster_id,id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id),
 FOREIGN KEY (installation_id,cluster_id,created_operation_id) REFERENCES operations (installation_id,cluster_id,id)
);
CREATE TABLE allocation_resources (
 installation_id uuid NOT NULL, cluster_id uuid NOT NULL, node_id uuid NOT NULL,
 allocation_id uuid NOT NULL, resource_id uuid NOT NULL,
 PRIMARY KEY (installation_id,resource_id),
 FOREIGN KEY (installation_id,cluster_id,node_id,allocation_id) REFERENCES resource_allocations (installation_id,cluster_id,node_id,id)
);
CREATE TABLE allocation_ports (
 installation_id uuid NOT NULL, cluster_id uuid NOT NULL, node_id uuid NOT NULL,
 allocation_id uuid NOT NULL, address inet NOT NULL,
 transport text NOT NULL CHECK (transport IN ('tcp','udp')),
 port integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
 released_at timestamptz,
 PRIMARY KEY (allocation_id,address,transport,port),
 FOREIGN KEY (installation_id,cluster_id,node_id,allocation_id) REFERENCES resource_allocations (installation_id,cluster_id,node_id,id),
 CHECK (masklen(address)=CASE family(address) WHEN 4 THEN 32 ELSE 128 END)
);
CREATE UNIQUE INDEX allocation_ports_live ON allocation_ports (installation_id,node_id,address,transport,port) WHERE released_at IS NULL;
CREATE TABLE operation_placements (
 installation_id uuid NOT NULL, cluster_id uuid NOT NULL, operation_id uuid NOT NULL,
 request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
 request_bytes bytea NOT NULL CHECK (octet_length(request_bytes) BETWEEN 1 AND 262144),
 decision_bytes bytea NOT NULL CHECK (octet_length(decision_bytes) BETWEEN 1 AND 65536),
 plan_hash bytea NOT NULL CHECK (octet_length(plan_hash)=32),
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (installation_id,operation_id),
 FOREIGN KEY (installation_id,cluster_id,operation_id) REFERENCES operations (installation_id,cluster_id,id)
);
CREATE TABLE operation_allocation_bindings (
 installation_id uuid NOT NULL, cluster_id uuid NOT NULL, operation_id uuid NOT NULL,
 allocation_id uuid NOT NULL,
 PRIMARY KEY (installation_id,operation_id,allocation_id),
 FOREIGN KEY (installation_id,cluster_id,operation_id) REFERENCES operations (installation_id,cluster_id,id),
 FOREIGN KEY (installation_id,cluster_id,allocation_id) REFERENCES resource_allocations (installation_id,cluster_id,id)
);
