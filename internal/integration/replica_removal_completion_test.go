package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestReplicaRemovalRetryAfterNativeDeletion(t *testing.T) {
	h := start(t, 30*time.Second)
	h.awaitAgent(t, 30*time.Second)
	created := admit(t, h, "removal-completion-retry")
	if op := h.await(t, created.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("create: %s %s", op.Status, failure(op))
	}
	primary := h.connect(t, created.ClusterID)
	defer primary.Close(context.WithoutCancel(t.Context()))
	if _, err := primary.Exec(t.Context(), `CREATE TABLE retained_rows(value int); INSERT INTO retained_rows VALUES(139)`); err != nil {
		t.Fatal(err)
	}
	added := h.scale(t, created.ClusterID, 1, nil)
	if op := h.await(t, added.OperationID, 4*time.Minute); op.Status != operation.Succeeded {
		t.Fatalf("add: %s %s%s", op.Status, failure(op), h.receipts(t, added.OperationID))
	}
	preview, err := h.store.PreviewClusterScaleDown(t.Context(), h.installationID, created.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	members, err := h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	var target string
	for _, member := range members {
		if member.MemberID == preview.MemberID {
			target = member.ResourceID.String()
		}
	}
	if target == "" {
		t.Fatal("review target absent")
	}
	// The fixture prevents the final metadata transaction, while real commands and receipts continue.
	if _, err := h.pool.Exec(t.Context(), `CREATE FUNCTION refuse_member_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture completion outage'; END $$;
 CREATE TRIGGER refuse_member_completion BEFORE UPDATE OF removed_at ON cluster_members FOR EACH ROW WHEN (NEW.removed_at IS NOT NULL) EXECUTE FUNCTION refuse_member_completion()`); err != nil {
		t.Fatal(err)
	}
	removed := h.scale(t, created.ClusterID, 0, &cluster.ActionConfirmation{PlanToken: preview.PlanToken, Confirm: true, DataDisposition: "retain"})
	deadline := time.Now().Add(90 * time.Second)
	for {
		var proved int
		if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM operation_step_completions WHERE operation_id=$1`, removed.OperationID).Scan(&proved); err != nil {
			t.Fatal(err)
		}
		if proved == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native removal did not finish%s", h.receipts(t, removed.OperationID))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := h.api.ContainerInspect(t.Context(), "synod-instance-"+target, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatalf("native container remains: %v", err)
	}
	var live int
	if err := h.pool.QueryRow(t.Context(), `SELECT count(*) FROM cluster_members WHERE cluster_id=$1 AND removed_at IS NULL`, created.ClusterID).Scan(&live); err != nil || live != 2 {
		t.Fatalf("completion outage lost membership: %d %v", live, err)
	}
	// End this injected failed attempt. An ordinary accepted replay must still return it.
	if _, err := h.pool.Exec(t.Context(), `UPDATE operations SET status='failed',claim_active=false,finished_at=clock_timestamp(),failure_code='fixture_completion_outage' WHERE id=$1`, removed.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(t.Context(), `DROP TRIGGER refuse_member_completion ON cluster_members; DROP FUNCTION refuse_member_completion()`); err != nil {
		t.Fatal(err)
	}
	again, err := h.store.PreviewClusterScaleDown(t.Context(), h.installationID, created.ClusterID, "integration")
	if err != nil {
		t.Fatal(err)
	}
	retry := h.scale(t, created.ClusterID, 0, &cluster.ActionConfirmation{PlanToken: again.PlanToken, Confirm: true, DataDisposition: "retain"})
	if op := h.await(t, retry.OperationID, 4*time.Minute); op.Status != operation.Succeeded || op.RetryOf == nil || *op.RetryOf != removed.OperationID {
		t.Fatalf("retry: %s %s%s", op.Status, failure(op), h.receipts(t, retry.OperationID))
	}
	var encoded []byte
	if err := h.pool.QueryRow(t.Context(), `SELECT plan_bytes FROM operations WHERE id=$1`, retry.OperationID).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	var plan agentv1.ExecutionPlan
	if err := proto.Unmarshal(encoded, &plan); err != nil || len(plan.Steps) != 1 || plan.Steps[0].Command.Input.GetVerifyContainerAbsent() == nil || plan.Steps[0].Command.Input.ResourceId != target {
		t.Fatalf("retry repeated mutation: %v", err)
	}
	members, err = h.store.ReadClusterMembers(t.Context(), h.installationID, created.ClusterID)
	if err != nil || len(members) != 1 || members[0].Role != "primary" {
		t.Fatalf("completed membership: %+v %v", members, err)
	}
	for _, volume := range preview.RetainedVolumes {
		if _, err := h.api.VolumeInspect(t.Context(), "synod-volume-"+volume.String(), client.VolumeInspectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := primary.Exec(t.Context(), `INSERT INTO retained_rows VALUES(140)`); err != nil {
		t.Fatal(err)
	}
	var total int
	if err := primary.QueryRow(t.Context(), `SELECT sum(value)::int FROM retained_rows`).Scan(&total); err != nil || total != 279 {
		t.Fatalf("primary data: %d %v", total, err)
	}
}
