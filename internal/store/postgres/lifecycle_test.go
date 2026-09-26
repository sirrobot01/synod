package postgres

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

// action asks for one lifecycle change under a fresh idempotency key.
func (h *placementHost) action(t *testing.T, clusterID uuid.UUID, apiOperation string, expected int64) (Admission, error) {
	t.Helper()
	request := ClusterActionRequest{
		Identity: RequestIdentity{
			InstallationID: h.id, ActorID: "actor-1",
			APIOperation: apiOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: clusterID, ExpectedRevision: expected,
	}
	if apiOperation == DeleteClusterAPIOperation {
		plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, clusterID, "actor-1")
		if err != nil {
			return Admission{}, err
		}
		request.Confirmation = &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}
	}
	return h.store.AdmitClusterAction(t.Context(), request)
}

// finish marks the cluster's current operation done so another can be admitted.
func (h *placementHost) finish(t *testing.T, operationID uuid.UUID) {
	t.Helper()
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE operations
 SET status='succeeded',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, operationID); err != nil {
		t.Fatal(err)
	}
}

// An action moves desired state and admits the work that carries it out, together.
func TestClusterActionMovesDesiredStateAndAdmitsWork(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	host.finish(t, claim.OperationID)
	for _, step := range []struct {
		apiOperation string
		lifecycle    cluster.DesiredLifecycle
		kind         string
	}{
		{StopClusterAPIOperation, cluster.DesiredStopped, "cluster.stop"},
		{StartClusterAPIOperation, cluster.DesiredRunning, "cluster.start"},
		{DeleteClusterAPIOperation, cluster.DesiredDeleted, "cluster.delete"},
	} {
		t.Run(step.kind, func(t *testing.T) {
			before, err := host.store.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			admission, err := host.action(t, claim.ClusterID, step.apiOperation, before.DesiredRevision)
			if err != nil {
				t.Fatal(err)
			}
			if admission.DesiredRevision != before.DesiredRevision+1 || admission.Replayed {
				t.Fatalf("admission=%+v, want the next revision", admission)
			}
			after, err := host.store.GetCluster(t.Context(), host.id, claim.ClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if after.DesiredLifecycle != step.lifecycle || after.DesiredRevision != admission.DesiredRevision {
				t.Fatalf("cluster=%+v, want %s at revision %d", after, step.lifecycle, admission.DesiredRevision)
			}
			operation, err := host.store.GetOperation(t.Context(), host.id, admission.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if operation.Kind != step.kind || operation.DesiredRevision != admission.DesiredRevision {
				t.Fatalf("operation=%+v, want %s", operation, step.kind)
			}
			var bound bool
			if err := host.store.pool.QueryRow(t.Context(), `SELECT action_plan_token IS NOT NULL FROM operations WHERE id=$1`, admission.OperationID).Scan(&bound); err != nil {
				t.Fatal(err)
			}
			if bound != (step.apiOperation == DeleteClusterAPIOperation) {
				t.Fatalf("action plan binding for %s: %v", step.apiOperation, bound)
			}
			host.finish(t, admission.OperationID)
		})
	}
}

// A repeat under one key is a replay. A repeat that asks for something else is not.
func TestClusterActionReplaysUnderItsKey(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	host.finish(t, claim.OperationID)
	identity := RequestIdentity{
		InstallationID: host.id, ActorID: "actor-1",
		APIOperation: StopClusterAPIOperation, IdempotencyKey: uuid.NewString(),
	}
	request := ClusterActionRequest{Identity: identity, RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: 1}
	first, err := host.store.AdmitClusterAction(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := host.store.AdmitClusterAction(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.OperationID != first.OperationID || second.DesiredRevision != first.DesiredRevision {
		t.Fatalf("second=%+v, want a replay of %+v", second, first)
	}
	// The revision moved once, however many times the request arrived.
	found, err := host.store.GetCluster(t.Context(), host.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if found.DesiredRevision != first.DesiredRevision {
		t.Fatalf("revision=%d, want the one the action moved to", found.DesiredRevision)
	}
	// The same key asking for a different action is a conflict, not a second action.
	changed := request
	changed.Identity.APIOperation = StartClusterAPIOperation
	if _, err := host.store.AdmitClusterAction(t.Context(), changed); err == nil {
		t.Fatal("one key admitted two different actions")
	}
}

// Only one unfinished operation may exist for a cluster, so a stop and a delete
// cannot race each other onto the same host.
func TestClusterActionWaitsForTheOperationInFlight(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	// The create operation is still unfinished.
	if _, err := host.action(t, claim.ClusterID, StopClusterAPIOperation, 1); err == nil {
		t.Fatal("an action was admitted while another operation was running")
	}
	host.finish(t, claim.OperationID)
	if _, err := host.action(t, claim.ClusterID, StopClusterAPIOperation, 1); err != nil {
		t.Fatalf("an action was refused after the operation finished: %v", err)
	}
}

// A caller that names a revision it did not read is acting on a cluster it has not seen.
func TestClusterActionChecksTheRevisionItExpects(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	host.finish(t, claim.OperationID)
	if _, err := host.action(t, claim.ClusterID, StopClusterAPIOperation, 99); err == nil {
		t.Fatal("a stale expected revision was accepted")
	}
	_, err := host.action(t, claim.ClusterID, StopClusterAPIOperation, 0)
	requireCode(t, err, InvalidRequest)
}

func TestClusterActionRefusesAnUnsoundRequest(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	host.finish(t, claim.OperationID)
	sound := ClusterActionRequest{
		Identity: RequestIdentity{
			InstallationID: host.id, ActorID: "actor-1",
			APIOperation: StopClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: 1,
	}
	for name, spoil := range map[string]func(*ClusterActionRequest){
		"an action nobody built":      func(r *ClusterActionRequest) { r.Identity.APIOperation = "clusters.explode.v1" },
		"a cluster that is not there": func(r *ClusterActionRequest) { r.ClusterID = uuid.New() },
		"no idempotency key":          func(r *ClusterActionRequest) { r.Identity.IdempotencyKey = "" },
		"an unprintable key":          func(r *ClusterActionRequest) { r.Identity.IdempotencyKey = "a b" },
		"no actor":                    func(r *ClusterActionRequest) { r.Identity.ActorID = "" },
		"no request id":               func(r *ClusterActionRequest) { r.RequestID = "" },
		"a negative revision":         func(r *ClusterActionRequest) { r.ExpectedRevision = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := sound
			bad.Identity.IdempotencyKey = uuid.NewString()
			spoil(&bad)
			if _, err := host.store.AdmitClusterAction(t.Context(), bad); err == nil {
				t.Fatalf("an unsound action was admitted for %s", name)
			}
		})
	}
}

// Widening a vocabulary column must not narrow it for somebody else.
//
// Migration 012 widened the audit action list for lifecycle operations and dropped
// the enrolment actions migration 003 had added, because PostgreSQL gives both checks
// the same generated name. Nothing failed at migration time; enrolment simply stopped
// working. This asserts the union rather than the newest addition.
func TestAuditActionsKeepEveryVocabularyThatWasAdded(t *testing.T) {
	host := placementFixture(t)
	var definition string
	if err := host.store.pool.QueryRow(t.Context(), `SELECT pg_get_constraintdef(oid)
 FROM pg_constraint WHERE conname='audit_events_action_check'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{
		"cluster.create.accepted", "cluster.stop.accepted", "cluster.start.accepted", "cluster.delete.accepted",
		"enrollment.token.registered", "node.enrolled", "node.certificate.revoked",
	} {
		if !strings.Contains(definition, action) {
			t.Fatalf("the audit action check no longer admits %s: %s", action, definition)
		}
	}
}

// scale asks for one replica count under a fresh idempotency key.
func (h *placementHost) scale(t *testing.T, clusterID uuid.UUID, replicas int, expected int64) (Admission, error) {
	t.Helper()
	return h.store.AdmitClusterAction(t.Context(), ClusterActionRequest{
		Identity: RequestIdentity{
			InstallationID: h.id, ActorID: "actor-1",
			APIOperation: ScaleClusterAPIOperation, IdempotencyKey: uuid.NewString(),
		},
		RequestID: uuid.NewString(), ClusterID: clusterID, ExpectedRevision: expected,
		Replicas: &replicas,
	})
}

// A scale moves the replica count and leaves the lifecycle where it is.
func TestScaleMovesTheReplicaCountOnly(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimRedis(t, "cache")
	host.finish(t, claim.OperationID)
	admitted, err := host.scale(t, claim.ClusterID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	var lifecycle string
	var replicas int
	var kind string
	if err := host.store.pool.QueryRow(t.Context(), `SELECT c.desired_lifecycle,c.desired_replicas,o.kind
 FROM clusters c JOIN operations o ON o.cluster_id=c.id WHERE c.id=$1 AND o.id=$2`,
		claim.ClusterID, admitted.OperationID).Scan(&lifecycle, &replicas, &kind); err != nil {
		t.Fatal(err)
	}
	if lifecycle != "running" {
		t.Fatalf("lifecycle=%q, want the scale to leave it running", lifecycle)
	}
	if replicas != 1 || kind != "cluster.scale" {
		t.Fatalf("replicas=%d kind=%q", replicas, kind)
	}
}

// A scale acts on a running cluster, and it must ask for more than the cluster has.
func TestScaleRefusesWhatItCannotCarryOut(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimRedis(t, "cache")
	host.finish(t, claim.OperationID)

	// The same count is not work.
	if _, err := host.scale(t, claim.ClusterID, 0, 1); err == nil {
		t.Fatal("a scale that changes nothing was admitted")
	}
	// A removal needs a preview and residual handling that this version does not have.
	admitted, err := host.scale(t, claim.ClusterID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	host.finish(t, admitted.OperationID)
	if _, err := host.scale(t, claim.ClusterID, 0, 2); err == nil {
		t.Fatal("a scale down was admitted before it is supported")
	}
	// A stopped cluster has no engine to replicate from.
	stopped, err := host.action(t, claim.ClusterID, StopClusterAPIOperation, 2)
	if err != nil {
		t.Fatal(err)
	}
	host.finish(t, stopped.OperationID)
	if _, err := host.scale(t, claim.ClusterID, 1, 3); err == nil {
		t.Fatal("a stopped cluster was scaled")
	}
}
