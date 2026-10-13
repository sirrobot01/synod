package restore

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
)

func TestScheduledMaintenanceRunsOnlyForThePlannedOwner(t *testing.T) {
	installation, node, destID := uuid.New(), uuid.New(), uuid.New()
	location := repository.Location{Kind: "local", Path: t.TempDir()}
	credentials := repository.Credentials{Password: "private-maintenance-password"}
	r, err := repository.Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	cache, err := journal.Open(t.Context(), filepath.Join(t.TempDir(), "agent.db"), installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{41}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Add(-47 * time.Hour).Truncate(time.Millisecond)
	schedule := proof.Schedule{ID: uuid.New(), Kind: proof.Maintenance, InstallationID: installation, NodeID: node, DestinationID: destID, CreatedAt: created}
	entry := destination.Entry{Destination: destination.Destination{ID: destID, Name: "maintained", NodeID: node, Location: location, CreatedAt: created}, Credentials: credentials}
	plan := destination.Plan{InstallationID: installation, Revision: 1, Destinations: []destination.Entry{entry}, Ready: []uuid.UUID{destID}, Proofs: []proof.Schedule{schedule}}
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	w := &Worker{Backup: &agentbackup.Worker{Journal: cache, Keyring: ring, InstallationID: installation, NodeID: node}, Directory: t.TempDir(), Apps: t.TempDir()}
	run := func(slot time.Time) (proof.Request, proof.Result) {
		t.Helper()
		request, err := schedule.Request(slot)
		if err != nil {
			t.Fatal(err)
		}
		result, err := w.ExecuteProof(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		status := "succeeded"
		if result.FailureCode != "" {
			status = "failed"
		}
		if (proof.Event{ID: request.ID, Status: status, Request: &request, Result: result}).Validate(request) != nil {
			t.Fatalf("invalid maintenance result: %+v", result)
		}
		return request, result
	}
	if _, result := run(created); result.Maintenance != string(repository.MaintenanceClaimed) || result.DestinationID != destID {
		t.Fatalf("first maintenance: %+v", result)
	}
	request, result := run(created.Add(24 * time.Hour))
	if result.Maintenance != string(repository.MaintenanceHandingOver) {
		t.Fatalf("maintenance during handover: %+v", result)
	}
	if repeated, err := w.ExecuteProof(t.Context(), request); err != nil || !reflect.DeepEqual(repeated, result) {
		t.Fatal("completed maintenance ran again", err)
	}
	if n, err := cache.AdmitScheduled(t.Context(), ring, created.Add(36*time.Hour)); err != nil || n != 0 {
		t.Fatalf("maintenance replayed in one day: %d %v", n, err)
	}
	if n, err := cache.AdmitScheduled(t.Context(), ring, created.Add(48*time.Hour)); err != nil || n != 1 {
		t.Fatalf("next daily maintenance: %d %v", n, err)
	}

	// A newer plan without this device's schedule stops an admitted job.
	plan.Revision, plan.Proofs = 2, nil
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if _, result := run(created.Add(48 * time.Hour)); result.FailureCode != "maintenance_not_owner" || result.Maintenance != "" {
		t.Fatalf("maintenance after ownership moved: %+v", result)
	}
}
