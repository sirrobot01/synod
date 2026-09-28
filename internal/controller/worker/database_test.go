package worker

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestDatabaseStandbyStartUsesItsCurrentAssignment(t *testing.T) {
	for _, scenario := range []string{"ready", "unacknowledged", "old_epoch", "active_assignment", "retired_copy", "another_copy", "tier2", "primary_without_lease"} {
		t.Run(scenario, func(t *testing.T) {
			h := newHarness(t, agentScript{drained: true, admit: true}.answer, func(plan *agentv1.ExecutionPlan) {
				plan.Steps = plan.Steps[1:2]
				plan.Steps[0].DependsOn = nil
			})
			source := uuid.New()
			if _, err := h.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, source, h.claim.InstallationID, make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			tier := 1
			if scenario == "tier2" {
				tier = 2
			}
			if scenario == "primary_without_lease" {
				source = h.node
			}
			// The harness admitted a tier 3 app. This scenario needs a database with a writer.
			if _, err := h.pool.Exec(t.Context(), `DELETE FROM app_copies WHERE installation_id=$2 AND app_id=$1`, h.claim.AppID, h.claim.InstallationID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.pool.Exec(t.Context(), `UPDATE apps SET tier=$3,spec=jsonb_build_object('name',name,'tier',$3::integer,'schema_version',1),writer_epoch=1,writer_node_id=$4
    WHERE id=$1 AND installation_id=$2`, h.claim.AppID, h.claim.InstallationID, tier, source); err != nil {
				t.Fatal(err)
			}
			command := h.plan.Steps[0].Command
			copyID := command.Input.ResourceId
			if scenario == "another_copy" {
				copyID = uuid.NewString()
			}
			if _, err := h.pool.Exec(t.Context(), `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,role,host_port,desired,desired_revision,observed_state,created_at,updated_at)
    VALUES($1,$2,$3,$4,1,'standby',5432,$5,1,'pending',clock_timestamp(),clock_timestamp())`, copyID, h.claim.InstallationID, h.claim.AppID, h.node, scenario != "retired_copy"); err != nil {
				t.Fatal(err)
			}
			if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET kind='app.start' WHERE id=$1`, h.claim.OperationID); err != nil {
				t.Fatal(err)
			}
			h.worker.config.Operation.Kind = "app.start"
			peer, ok := h.transport.Session(h.node)
			if !ok {
				t.Fatal("no agent session")
			}
			assignment := &agentv1.AppReplication{AppId: h.claim.AppID.String(), Epoch: 1, Role: "standby", Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}, PrimaryHost: "192.0.2.1", PrimaryPort: 5432}}
			if scenario == "old_epoch" {
				assignment.Epoch = 2
			}
			if scenario == "active_assignment" {
				assignment.Role = "active"
			}
			encoded, err := proto.Marshal(&agentv1.ReplicationTable{SessionId: peer.SessionID, Revision: 1, Apps: []*agentv1.AppReplication{assignment}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.pool.Exec(t.Context(), `INSERT INTO agent_replication(installation_id,node_id,session_id,revision,table_bytes,acknowledged_at) VALUES($1,$2,$3,1,$4,CASE WHEN $5 THEN clock_timestamp() END)`, h.claim.InstallationID, h.node, peer.SessionID, encoded, scenario != "unacknowledged"); err != nil {
				t.Fatal(err)
			}
			if err := h.worker.Prepare(t.Context()); err != nil {
				t.Fatal(err)
			}
			state, completed, err := h.worker.resume(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := h.worker.checkpoint(t.Context(), state, completed); err != nil {
				t.Fatal(err)
			}
			pass, err := h.worker.dispatch(t.Context(), state, completed)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "ready" {
				if pass.committed != 1 {
					t.Fatalf("standby start did not dispatch: %+v", pass)
				}
				waitFor(t, func() bool {
					return h.count(t, `SELECT count(*) FROM command_admissions WHERE command_id=$1 AND accepted_bytes IS NOT NULL`, command.CommandId) == 1
				}, "standby did not acknowledge start")
			} else if pass.committed != 0 {
				t.Fatal("unproved copy started")
			}
			if h.count(t, `SELECT count(*) FROM apps WHERE id=$1 AND writer_last_grant_at IS NOT NULL`, h.claim.AppID) != 0 {
				t.Fatal("standby received a writer lease")
			}
		})
	}
}
