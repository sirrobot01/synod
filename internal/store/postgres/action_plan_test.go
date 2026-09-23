package postgres

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestDeletePlanDescribesRetainedDataWithoutAdmittingWork(t *testing.T) {
	h := placementFixture(t)
	claim := h.claimCluster(t, "db")
	h.finish(t, claim.OperationID)
	var expected []uuid.UUID
	for range 3 {
		identity := declared(h, claim.ClusterID)
		if err := h.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, identity.ResourceID)
	}
	slices.SortFunc(expected, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	before, err := h.store.GetCluster(t.Context(), h.id, claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlanToken == uuid.Nil || plan.ClusterID != claim.ClusterID || plan.ClusterName != "db" || plan.Action != "delete" || plan.DataDisposition != "retain" || plan.ExpectedRevision != before.DesiredRevision || plan.Downtime == "" || !slices.Equal(plan.RetainedVolumes, expected) {
		t.Fatalf("incorrect deletion preview: %+v", plan)
	}
	var ttl time.Duration
	var seconds float64
	if err := h.store.pool.QueryRow(t.Context(), `SELECT extract(epoch FROM expires_at-created_at) FROM cluster_action_plans WHERE plan_token=$1`, plan.PlanToken).Scan(&seconds); err != nil {
		t.Fatal(err)
	}
	ttl = time.Duration(seconds) * time.Second
	if ttl != 5*time.Minute {
		t.Fatalf("plan lifetime=%v, want five minutes", ttl)
	}
	after, err := h.store.GetCluster(t.Context(), h.id, claim.ClusterID)
	if err != nil || after.DesiredRevision != before.DesiredRevision || after.DesiredLifecycle != before.DesiredLifecycle {
		t.Fatalf("preview changed cluster state: %v", err)
	}
	var operations, outbox, audit int
	if err := h.store.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM operations), (SELECT count(*) FROM outbox), (SELECT count(*) FROM audit_events WHERE cluster_id=$1)`, claim.ClusterID).Scan(&operations, &outbox, &audit); err != nil {
		t.Fatal(err)
	}
	if operations != 1 || outbox != 1 || audit != 1 {
		t.Fatalf("preview created product work: operations=%d outbox=%d audit=%d", operations, outbox, audit)
	}
}

func TestDeleteAdmissionReplaysAfterLostCommitExpiryAndDeletion(t *testing.T) {
	h := placementFixture(t)
	claim := h.claimCluster(t, "db")
	h.finish(t, claim.OperationID)
	plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	if err != nil || plan.RetainedVolumes == nil || len(plan.RetainedVolumes) != 0 {
		t.Fatalf("empty plan must have an empty volume array: %+v %v", plan, err)
	}
	request := ClusterActionRequest{
		Identity:  RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: DeleteClusterAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: plan.ExpectedRevision,
		Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"},
	}
	owner := New(h.store.pool)
	owner.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	_, err = owner.AdmitClusterAction(t.Context(), request)
	requireCode(t, err, OutcomeUnknown)
	first, err := h.store.AdmitClusterAction(t.Context(), request)
	if err != nil || !first.Replayed {
		t.Fatalf("lost commit replay: %+v %v", first, err)
	}
	h.finish(t, first.OperationID)
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE clusters SET deleted_at=clock_timestamp() WHERE id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE cluster_action_plans SET created_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE plan_token=$1`, plan.PlanToken); err != nil {
		t.Fatal(err)
	}
	replay, err := h.store.AdmitClusterAction(t.Context(), request)
	if err != nil || !replay.Replayed || replay.OperationID != first.OperationID || replay.DesiredRevision != first.DesiredRevision {
		t.Fatalf("expired/deleted replay: %+v %v", replay, err)
	}
	var confirmed uuid.UUID
	if err := h.store.pool.QueryRow(t.Context(), `SELECT action_plan_token FROM operations WHERE id=$1`, replay.OperationID).Scan(&confirmed); err != nil || confirmed != plan.PlanToken {
		t.Fatalf("lost-commit replay changed confirmed plan: %s %v", confirmed, err)
	}
	_, err = h.store.pool.Exec(t.Context(), `DELETE FROM cluster_action_plans WHERE plan_token=$1`, plan.PlanToken)
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "23503" || pgErr.ConstraintName != "operations_action_plan_fk" {
		t.Fatalf("confirmed plan deletion did not hit the foreign key: %v", err)
	}
	for _, changed := range []ClusterActionRequest{
		{Identity: request.Identity, RequestID: request.RequestID, ClusterID: uuid.New(), ExpectedRevision: request.ExpectedRevision, Confirmation: request.Confirmation},
		{Identity: request.Identity, RequestID: request.RequestID, ClusterID: request.ClusterID, ExpectedRevision: request.ExpectedRevision + 1, Confirmation: request.Confirmation},
		{Identity: request.Identity, RequestID: request.RequestID, ClusterID: request.ClusterID, ExpectedRevision: request.ExpectedRevision, Confirmation: &cluster.ActionConfirmation{PlanToken: uuid.New(), Confirm: true, DataDisposition: "retain"}},
		{Identity: request.Identity, RequestID: request.RequestID, ClusterID: request.ClusterID, ExpectedRevision: request.ExpectedRevision, Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: false, DataDisposition: "retain"}},
	} {
		_, err := h.store.AdmitClusterAction(t.Context(), changed)
		requireCode(t, err, IdempotencyConflict)
	}
}

func TestDeleteAdmissionRejectsUnconfirmedAndChangedPlans(t *testing.T) {
	h := placementFixture(t)
	claim := h.claimCluster(t, "db")
	h.finish(t, claim.OperationID)
	other := h.claimCluster(t, "other")
	h.finish(t, other.OperationID)
	for _, tc := range []struct {
		name  string
		alter func(*ClusterActionRequest)
		code  ErrorCode
	}{
		{"missing confirmation", func(r *ClusterActionRequest) { r.Confirmation = nil }, InvalidRequest},
		{"not confirmed", func(r *ClusterActionRequest) { r.Confirmation.Confirm = false }, InvalidRequest},
		{"purge", func(r *ClusterActionRequest) { r.Confirmation.DataDisposition = "purge" }, InvalidRequest},
		{"unknown token", func(r *ClusterActionRequest) { r.Confirmation.PlanToken = uuid.New() }, InvalidRequest},
		{"another actor", func(r *ClusterActionRequest) { r.Identity.ActorID = "actor-2" }, InvalidRequest},
		{"another target", func(r *ClusterActionRequest) { r.ClusterID = other.ClusterID }, InvalidRequest},
		{"another installation", func(r *ClusterActionRequest) { r.Identity.InstallationID = uuid.New() }, NotFound},
		{"stale revision", func(r *ClusterActionRequest) { r.ExpectedRevision++ }, RevisionConflict},
		{"expired", func(r *ClusterActionRequest) {
			if _, err := h.store.pool.Exec(t.Context(), `UPDATE cluster_action_plans SET created_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '1 minute' WHERE plan_token=$1`, r.Confirmation.PlanToken); err != nil {
				t.Fatal(err)
			}
		}, InvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
			if err != nil {
				t.Fatal(err)
			}
			request := ClusterActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: DeleteClusterAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: 1, Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}}
			tc.alter(&request)
			_, err = h.store.AdmitClusterAction(t.Context(), request)
			requireCode(t, err, tc.code)
		})
	}
	found, err := h.store.GetCluster(t.Context(), h.id, claim.ClusterID)
	if err != nil || found.DesiredRevision != 1 || found.DesiredLifecycle != cluster.DesiredRunning {
		t.Fatalf("refused delete changed the cluster: %+v %v", found, err)
	}
}

func TestDeletePreviewRefusesActiveDeletedAndPurgingClusters(t *testing.T) {
	h := placementFixture(t)
	claim := h.claimCluster(t, "db")
	_, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	requireCode(t, err, OperationConflict)
	h.finish(t, claim.OperationID)
	identity := declared(h, claim.ClusterID)
	identity.Retention = PurgeRequested
	if err := h.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	_, err = h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	requireCode(t, err, OperationConflict)
	if err := h.store.SetVolumeRetention(t.Context(), h.id, identity.ResourceID, Purged); err != nil {
		t.Fatal(err)
	}
	plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	if err != nil || len(plan.RetainedVolumes) != 0 {
		t.Fatalf("purged storage still appears in preview: %+v %v", plan, err)
	}
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE clusters SET desired_lifecycle='deleted' WHERE id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	_, err = h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
	requireCode(t, err, NotFound)
}

func TestDeleteAdmissionSerializesConcurrentVolumeChanges(t *testing.T) {
	for _, change := range []string{"new volume", "retention"} {
		t.Run(change, func(t *testing.T) {
			h := placementFixture(t)
			claim := h.claimCluster(t, "db")
			h.finish(t, claim.OperationID)
			identity := declared(h, claim.ClusterID)
			if change == "retention" {
				if err := h.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, claim.ClusterID, "actor-1")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ready, release := make(chan struct{}), make(chan struct{})
			writer := New(h.store.pool)
			writer.commitTx = func(ctx context.Context, tx pgx.Tx) error {
				close(ready)
				select {
				case <-release:
					return tx.Commit(ctx)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			changed := make(chan error, 1)
			go func() {
				if change == "retention" {
					changed <- writer.SetVolumeRetention(ctx, h.id, identity.ResourceID, PurgeRequested)
					return
				}
				changed <- writer.RecordVolumeIdentity(ctx, identity)
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			admitted := make(chan error, 1)
			type previewResult struct {
				plan cluster.ActionPlan
				err  error
			}
			previewed := make(chan previewResult, 1)
			go func() {
				plan, err := h.store.PreviewClusterDelete(ctx, h.id, claim.ClusterID, "actor-1")
				previewed <- previewResult{plan: plan, err: err}
			}()
			go func() {
				_, err := h.store.AdmitClusterAction(ctx, ClusterActionRequest{Identity: RequestIdentity{InstallationID: h.id, ActorID: "actor-1", APIOperation: DeleteClusterAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: plan.ExpectedRevision, Confirmation: &cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"}})
				admitted <- err
			}()
			waitForKeyWait(t, h.store)
			close(release)
			if err := <-changed; err != nil {
				t.Fatal(err)
			}
			requireCode(t, <-admitted, OperationConflict)
			preview := <-previewed
			if change == "retention" {
				requireCode(t, preview.err, OperationConflict)
			} else if preview.err != nil || !slices.Equal(preview.plan.RetainedVolumes, []uuid.UUID{identity.ResourceID}) {
				t.Fatalf("preview missed a concurrent volume write: %+v %v", preview.plan, preview.err)
			}
		})
	}
}

func TestConfirmedActionPlanCannotCrossScope(t *testing.T) {
	h := placementFixture(t)
	claim := h.claimCluster(t, "db")
	h.finish(t, claim.OperationID)
	other := h.claimCluster(t, "other")
	h.finish(t, other.OperationID)
	for _, scope := range []struct {
		name      string
		clusterID uuid.UUID
		actor     string
	}{
		{"actor", claim.ClusterID, "actor-2"},
		{"cluster", other.ClusterID, "actor-1"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			plan, err := h.store.PreviewClusterDelete(t.Context(), h.id, scope.clusterID, scope.actor)
			if err != nil {
				t.Fatal(err)
			}
			_, err = h.store.pool.Exec(t.Context(), `UPDATE operations SET action_plan_token=$1 WHERE id=$2`, plan.PlanToken, claim.OperationID)
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "23503" || pgErr.ConstraintName != "operations_action_plan_fk" {
				t.Fatalf("cross-scope binding did not hit the foreign key: %v", err)
			}
		})
	}
}
