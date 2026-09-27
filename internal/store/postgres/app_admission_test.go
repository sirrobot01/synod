package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/app"
)

func TestAppAdmission(t *testing.T) {
	s := testStore(t)
	request := CreateAppRequest{
		Identity: RequestIdentity{InstallationID: uuid.New(), ActorID: "operator", APIOperation: CreateAppAPIOperation, IdempotencyKey: "retry"}, RequestID: "request",
		Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 2, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80, Protocol: "tcp"}}}, Router: app.RouterSpec{Port: 8080, Protocol: "tcp"}},
	}
	const count = 8
	results := make([]app.Admission, count)
	failures := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() { results[i], failures[i] = s.AdmitApp(t.Context(), request) })
	}
	wg.Wait()
	first := results[0]
	fresh := 0
	for i, result := range results {
		if failures[i] != nil {
			t.Fatal(failures[i])
		}
		if result.OperationID == uuid.Nil || result.OperationID != first.OperationID || result.AppID != first.AppID || !result.AcceptedAt.Equal(first.AcceptedAt) || result.DesiredRevision != 1 {
			t.Fatalf("different admission: %+v", result)
		}
		if !result.Replayed {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("new admissions = %d", fresh)
	}
	stored, err := s.GetApp(t.Context(), request.Identity.InstallationID, first.AppID)
	if err != nil || stored.Name != "web" || stored.Spec.Copies != 2 {
		t.Fatalf("stored: %+v %v", stored, err)
	}
	operation, err := s.GetOperation(t.Context(), request.Identity.InstallationID, first.OperationID)
	if err != nil || operation.ClusterID != first.AppID || operation.Kind != "app.deploy" || operation.Status != "pending" {
		t.Fatalf("durable app operation: %+v %v", operation, err)
	}
	generation := uuid.New()
	if err := s.InitializeGeneration(t.Context(), request.Identity.InstallationID, generation); err != nil {
		t.Fatal(err)
	}
	claimRequest := ClaimRequest{ClaimID: uuid.New(), InstallationID: request.Identity.InstallationID, OperationID: first.OperationID, RecoveryGeneration: generation, OwnerID: "app-controller", TTL: time.Minute, LeaseID: 1, LeaseRevision: 1}
	claim, err := s.AcquireClaim(t.Context(), claimRequest)
	if err != nil || claim.ClusterID != first.AppID || claim.ExecutionEpoch != 1 {
		t.Fatalf("app claim: %+v %v", claim, err)
	}
	replayClaim, err := s.AcquireClaim(t.Context(), claimRequest)
	if err != nil || replayClaim.ID != claim.ID || replayClaim.ExecutionEpoch != claim.ExecutionEpoch {
		t.Fatalf("app claim replay: %+v %v", replayClaim, err)
	}
	// A second Store proves the replay does not depend on process memory.
	replay, err := (&Store{pool: s.pool}).AdmitApp(t.Context(), request)
	if err != nil || !replay.Replayed || replay.AppID != first.AppID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	for name, test := range map[string]struct {
		mutate func(*CreateAppRequest)
		code   ErrorCode
	}{
		"changed input":     {func(r *CreateAppRequest) { r.Spec.Copies++ }, IdempotencyConflict},
		"new key same name": {func(r *CreateAppRequest) { r.Identity.IdempotencyKey = "new" }, NameConflict},
		"other actor":       {func(r *CreateAppRequest) { r.Identity.ActorID = "other" }, NameConflict},
		"bad key":           {func(r *CreateAppRequest) { r.Identity.IdempotencyKey = "bad key" }, InvalidRequest},
		"forged platforms":  {func(r *CreateAppRequest) { r.Spec.Service.SupportedArchitectures = []string{"amd64"} }, InvalidRequest},
	} {
		t.Run(name, func(t *testing.T) {
			input := request
			test.mutate(&input)
			_, err := s.AdmitApp(t.Context(), input)
			failure, ok := errors.AsType[*AdmissionError](err)
			if !ok || failure.Code != test.code {
				t.Fatalf("want %s, got %v", test.code, err)
			}
		})
	}
	var admissions int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM app_admissions WHERE installation_id=$1", request.Identity.InstallationID).Scan(&admissions); err != nil || admissions != 1 {
		t.Fatalf("failed requests left rows: %d %v", admissions, err)
	}
	request.Identity.InstallationID = uuid.New()
	other, err := s.AdmitApp(t.Context(), request)
	if err != nil || other.AppID == first.AppID {
		t.Fatalf("installation isolation: %+v %v", other, err)
	}
}

func TestAppAdmissionRecoversLostCommitReply(t *testing.T) {
	s := testStore(t)
	request := CreateAppRequest{Identity: RequestIdentity{InstallationID: uuid.New(), ActorID: "operator", APIOperation: CreateAppAPIOperation, IdempotencyKey: "retry"}, RequestID: "request",
		Spec: app.AppSpec{SchemaVersion: 1, Name: "web", Tier: app.TierParallel, Copies: 1, Service: app.ServiceSpec{Image: "nginx", Ports: []app.PortSpec{{ContainerPort: 80}}}, Router: app.RouterSpec{Port: 8080, Protocol: "tcp"}},
	}
	s.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errors.New("lost reply")
	}
	_, err := s.AdmitApp(t.Context(), request)
	failure, ok := errors.AsType[*AdmissionError](err)
	if !ok || failure.Code != OutcomeUnknown {
		t.Fatalf("lost reply: %v", err)
	}
	s.commitTx = nil
	result, err := s.AdmitApp(t.Context(), request)
	if err != nil || !result.Replayed {
		t.Fatalf("recovery: %+v %v", result, err)
	}
}
