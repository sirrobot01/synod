package journal

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
)

func TestProofOutageCatchUpWaitsForBackupAndSurvivesRestart(t *testing.T) {
	installation, node, target := uuid.New(), uuid.New(), uuid.New()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ring, err := DestinationKeyring(installation, node, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "agent.db")
	j, err := Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	appID := uuid.NewSHA1(node, []byte("compose:proof"))
	check := proof.Schedule{ID: uuid.New(), Kind: proof.Integrity, InstallationID: installation, NodeID: node, DestinationID: target, CreatedAt: now}
	drill := proof.Schedule{ID: uuid.New(), Kind: proof.Drill, InstallationID: installation, NodeID: node, AppID: appID, CreatedAt: now}
	plan := destination.Plan{InstallationID: installation, Revision: 1, Proofs: []proof.Schedule{check, drill}, Destinations: []destination.Entry{{Destination: destination.Destination{ID: target, Name: "proof", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "private-proof-password"}}}}
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if n, err := j.AdmitScheduled(t.Context(), ring, now); err != nil || n != 0 {
		t.Fatalf("proof ran before the first backup: %d %v", n, err)
	}
	job := backup.Request{ID: uuid.New(), InstallationID: installation, NodeID: node, AppID: appID, SourceKey: "compose:proof", DestinationID: target, Method: backup.Stop, RequestedAt: now}
	raw, _ := json.Marshal(job)
	if _, err := j.Admit(t.Context(), job.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := j.Start(t.Context(), job.ID); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(backup.Result{SnapshotID: job.ID, DestinationID: target, StartedAt: now, CompletedAt: now, RepositoryOpened: true})
	if err := j.Complete(t.Context(), job.ID, raw); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(t.Context(), job.ID); err != nil {
		t.Fatal(err)
	}
	later := now.AddDate(0, 2, 0)
	if n, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || n != 2 {
		t.Fatalf("latest proof slots: %d %v", n, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := j.AdmitScheduled(t.Context(), ring, later.AddDate(0, 2, 0)); err != nil || n != 0 {
		t.Fatalf("pending proof replayed after restart: %d %v", n, err)
	}
	records, err := j.Pending(t.Context())
	if err != nil || len(records) != 2 {
		t.Fatal("lost proof jobs", err)
	}
	for _, record := range records {
		var request proof.Request
		if json.Unmarshal(record.Input, &request) != nil || (!check.Matches(request) && !drill.Matches(request)) {
			t.Fatal("job differs from the encrypted schedule")
		}
		if err := j.Start(t.Context(), request.ID); err != nil {
			t.Fatal(err)
		}
		result := proof.Result{DestinationID: target, StartedAt: later, CompletedAt: later.Add(time.Minute)}
		if request.Kind == proof.Drill {
			result.SnapshotID, result.Containers = uuid.New(), 2
		} else {
			result.Read, result.Contents = 1, 20
		}
		raw, _ := json.Marshal(result)
		if err := j.Complete(t.Context(), request.ID, raw); err != nil {
			t.Fatal(err)
		}
		if err := j.Acknowledge(t.Context(), request.ID); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || n != 0 {
		t.Fatalf("acknowledgement replayed proof: %d %v", n, err)
	}
	// A changed schedule must not repeat a proof completed after its current slot.
	plan.Revision++
	plan.Proofs[0].ID, plan.Proofs[1].ID = uuid.New(), uuid.New()
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	if n, err := j.AdmitScheduled(t.Context(), ring, later); err != nil || n != 0 {
		t.Fatalf("success history did not suppress catch-up: %d %v", n, err)
	}
	if n, err := j.AdmitScheduled(t.Context(), ring, later.AddDate(0, 1, 0)); err != nil || n != 2 {
		t.Fatalf("next month did not admit both proofs: %d %v", n, err)
	}
}
