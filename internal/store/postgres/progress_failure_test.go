package postgres

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
)

func TestProgressFailurePersistsAndReleasesMutationSlot(t *testing.T) {
	for _, tc := range []struct {
		name string
		code string
		want string
	}{
		{name: "storage loss", code: "storage_lost", want: "storage_lost"},
		{name: "default", want: "operation_failed"},
		{name: "maximum length", code: strings.Repeat("a", 128), want: strings.Repeat("a", 128)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, claim := claimFixture(t)
			plan := claimPlan(t, claim)
			if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
				t.Fatal(err)
			}
			reconcilePlan(t, s, claim, plan)
			if err := s.ActivateClaim(t.Context(), claim); err != nil {
				t.Fatal(err)
			}
			running := ProgressUpdate{Sequence: 1, Status: operation.Running, Reason: "Check the engine storage."}
			if err := s.UpdateProgress(t.Context(), claim, running); err != nil {
				t.Fatal(err)
			}
			next := ClusterActionRequest{
				Identity:  RequestIdentity{InstallationID: claim.InstallationID, ActorID: "test", APIOperation: StopClusterAPIOperation, IdempotencyKey: uuid.NewString()},
				RequestID: uuid.NewString(), ClusterID: claim.ClusterID, ExpectedRevision: 1,
			}
			_, err := s.AdmitClusterAction(t.Context(), next)
			requireCode(t, err, OperationConflict)

			failed := ProgressUpdate{Sequence: 2, Status: operation.Failed, Reason: "The engine storage is absent.", FailureCode: tc.code}
			if err := s.UpdateProgress(t.Context(), claim, failed); err != nil {
				t.Fatal(err)
			}
			stored, err := s.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != operation.Failed || stored.FailureCode == nil || *stored.FailureCode != tc.want || stored.FinishedAt == nil {
				t.Fatalf("status=%s, code=%v, finished=%v, want a terminal failure with %q", stored.Status, stored.FailureCode, stored.FinishedAt, tc.want)
			}
			var active bool
			if err := s.pool.QueryRow(t.Context(), `SELECT claim_active FROM operations WHERE id=$1`, claim.OperationID).Scan(&active); err != nil || active {
				t.Fatalf("claim active=%v, err=%v, want inactive", active, err)
			}
			if _, err := s.RenewClaim(t.Context(), claim, time.Minute); !errors.Is(err, operation.ErrStaleOwner) {
				t.Fatalf("renewed a terminal claim: %v", err)
			}
			if err := s.UpdateProgress(t.Context(), claim, failed); err != nil {
				t.Fatalf("identical failed progress replay: %v", err)
			}
			if err := s.UpdateProgress(t.Context(), claim, running); err != nil {
				t.Fatalf("prior running progress replay after terminal failure: %v", err)
			}
			changed := failed
			changed.FailureCode = "another_failure"
			if err := s.UpdateProgress(t.Context(), claim, changed); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("altered failure code replay: %v", err)
			}
			var progressCount int
			if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, claim.OperationID).Scan(&progressCount); err != nil || progressCount != 2 {
				t.Fatalf("progress count=%d, err=%v, want two unchanged records", progressCount, err)
			}
			if _, err := s.AdmitClusterAction(t.Context(), next); err != nil {
				t.Fatalf("known failure retained the cluster mutation slot: %v", err)
			}
		})
	}
}

func TestProgressRejectsInvalidFailureCodes(t *testing.T) {
	s, _, claim := claimFixture(t)
	plan := claimPlan(t, claim)
	if err := s.InstallPlan(t.Context(), claim, plan); err != nil {
		t.Fatal(err)
	}
	reconcilePlan(t, s, claim, plan)
	if err := s.ActivateClaim(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		status operation.Status
		code   string
	}{
		{name: "running", status: operation.Running, code: "storage_lost"},
		{name: "waiting", status: operation.Waiting, code: "storage_lost"},
		{name: "succeeded", status: operation.Succeeded, code: "storage_lost"},
		{name: "uppercase", status: operation.Failed, code: "STORAGE_LOST"},
		{name: "space", status: operation.Failed, code: "storage lost"},
		{name: "path", status: operation.Failed, code: "/private/storage"},
		{name: "newline", status: operation.Failed, code: "storage_lost\n"},
		{name: "unicode", status: operation.Failed, code: "storagé_lost"},
		{name: "oversized", status: operation.Failed, code: strings.Repeat("a", 129)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCode(t, s.UpdateProgress(t.Context(), claim, ProgressUpdate{
				Sequence: 1, Status: tc.status, Reason: "Check the reported failure.", FailureCode: tc.code,
			}), InvalidRequest)
		})
	}
	stored, err := s.GetOperation(t.Context(), claim.InstallationID, claim.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != operation.Running || stored.FailureCode != nil || stored.FinishedAt != nil {
		t.Fatal("invalid progress changed the operation")
	}
	var progressCount int
	if err := s.pool.QueryRow(t.Context(), `SELECT count(*) FROM operation_progress WHERE operation_id=$1`, claim.OperationID).Scan(&progressCount); err != nil || progressCount != 0 {
		t.Fatalf("progress count=%d, err=%v, want no invalid records", progressCount, err)
	}
	if err := s.ValidateClaim(t.Context(), claim); err != nil {
		t.Fatalf("invalid progress changed claim authority: %v", err)
	}
}
