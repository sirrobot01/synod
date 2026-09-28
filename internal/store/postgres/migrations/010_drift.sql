-- Drift. Specification 33 requires reporting unowned containers and unexpected
-- changes without deleting them, and requires recovering previously labelled Synod
-- objects rather than treating them as foreign.
--
-- This is a report, not desired state. Nothing in this schema can start, stop, or
-- remove anything, and nothing here is an input to placement or to a plan. It exists
-- so an operator can see what is on a host that Synod did not put there, or did put
-- there and no longer recognises.
CREATE TABLE node_drift (
 installation_id uuid NOT NULL,
 node_id uuid NOT NULL,
 -- The session that observed it. A report from a session that has since gone is
 -- still what was true when it was taken, and its age is visible here.
 session_id uuid NOT NULL,
 observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 -- The host held more drift than one report carries. More than that needs an
 -- operator on the host, not a longer message.
 truncated boolean NOT NULL,
 PRIMARY KEY (installation_id,node_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES nodes (installation_id,id)
);

CREATE TABLE node_drift_items (
 installation_id uuid NOT NULL,
 node_id uuid NOT NULL,
 kind text NOT NULL CHECK (kind IN ('container','volume','network')),
 native_id text NOT NULL CHECK (octet_length(native_id) BETWEEN 1 AND 256),
 name text NOT NULL CHECK (octet_length(name) <= 256),
 state text NOT NULL CHECK (octet_length(state) <= 64),
 -- unowned was never ours. foreign belongs to another installation or node.
 -- mislabelled and changed were ours: the first lost its identity, the second is
 -- no longer the object this node created.
 class text NOT NULL CHECK (class IN ('unowned','foreign','mislabelled','changed')),
 -- Null when the object no longer carries a readable Synod identity. There is no
 -- foreign key: the whole point is that these may name nothing Synod has a record
 -- of, and a report that refuses to store what it saw is not a report.
 app_id uuid,
 resource_id uuid,
 -- A stable code naming what disagrees. It never carries a label value or a path.
 detail text NOT NULL CHECK (octet_length(detail) <= 64),
 PRIMARY KEY (installation_id,node_id,kind,native_id),
 FOREIGN KEY (installation_id,node_id) REFERENCES node_drift (installation_id,node_id) ON DELETE CASCADE
);
