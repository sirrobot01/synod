package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDestinationPersistenceAndAuthorizedDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	installation, keyID := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{42}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	ca := authority(t)
	owner, _ := join(t, s, ca)
	other, _ := join(t, s, ca)
	sessions := map[uuid.UUID]uuid.UUID{owner: uuid.New(), other: uuid.New()}
	for node, session := range sessions {
		if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64", Hostname: node.String()}); err != nil {
			t.Fatal(err)
		}
	}
	entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "Local", NodeID: owner, Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "private-repository-passphrase-that-must-stay-wrapped"}}
	if _, err := s.AddDestination(t.Context(), installation, ring, entry, "test", "request"); code(err) != InvalidRequest {
		t.Fatalf("unapproved owner: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), owner, "test", "approve"); err != nil {
		t.Fatal(err)
	}
	saved, err := s.AddDestination(t.Context(), installation, ring, entry, "test", "request")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.AddDestination(t.Context(), installation, ring, entry, "test", "retry")
	if err != nil || !saved.CreatedAt.Equal(again.CreatedAt) {
		t.Fatalf("retry: %v", err)
	}
	conflict := entry
	conflict.Credentials.Password = "changed"
	if _, err := s.AddDestination(t.Context(), installation, ring, conflict, "test", "retry"); code(err) != IdempotencyConflict {
		t.Fatalf("changed retry: %v", err)
	}
	duplicate := entry
	duplicate.Destination.ID = uuid.New()
	duplicate.Destination.Name = "local"
	if _, err := s.AddDestination(t.Context(), installation, ring, duplicate, "test", "retry"); code(err) != IdempotencyConflict {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := s.DestinationPlan(t.Context(), installation, other, sessions[other], ring); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("unapproved delivery: %v", err)
	}
	if _, err := s.ApproveNode(t.Context(), other, "test", "approve"); err != nil {
		t.Fatal(err)
	}
	remote := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "Remote", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "backups"}}}, Credentials: repository.Credentials{Password: entry.Credentials.Password, AccessKeyID: "private-access-key", SecretAccessKey: "private-provider-key"}}
	if _, err := s.AddDestination(t.Context(), installation, ring, remote, "test", "remote"); err != nil {
		t.Fatal(err)
	}
	for node, session := range sessions {
		plan, err := s.DestinationPlan(t.Context(), installation, node, session, ring)
		want := 1
		if node == owner {
			want = 2
		}
		if err != nil || len(plan.Destinations) != want || plan.Revision != 3 {
			t.Fatalf("plan for %s: %v", node, err)
		}
		for _, d := range plan.Destinations {
			if d.Credentials.Password != entry.Credentials.Password {
				t.Fatal("wrong decrypted password")
			}
		}
		if err := s.AcknowledgeDestinations(t.Context(), node, session, plan.Revision); err != nil {
			t.Fatal(err)
		}
	}
	listing, err := s.ListDestinations(t.Context())
	if err != nil || len(listing.Delivery) != 2 {
		t.Fatalf("list: %v", err)
	}
	for _, d := range listing.Delivery {
		if !d.Current {
			t.Fatal("ack missing")
		}
	}
	raw, _ := json.Marshal(listing)
	if bytes.Contains(raw, []byte("private-")) || bytes.Contains(raw, []byte("credentials")) {
		t.Fatal("public listing leaked credentials")
	}
	// Replacing a connection invalidates the old acknowledgement, even at the same revision.
	old := sessions[owner]
	sessions[owner] = uuid.New()
	if err := s.RecordAgentSession(t.Context(), owner, sessions[owner], &agentv1.Hello{NodeId: owner.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeDestinations(t.Context(), owner, old, 3); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("stale ack: %v", err)
	}
	listing, err = s.ListDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range listing.Delivery {
		if d.NodeID == owner && d.Current {
			t.Fatal("replacement session skipped delivery")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(path + suffix)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(entry.Credentials.Password)) || bytes.Contains(data, []byte(remote.Credentials.SecretAccessKey)) {
			t.Fatal("credential stored in plaintext")
		}
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if plan, err := s.DestinationPlan(t.Context(), installation, owner, sessions[owner], ring); err != nil || len(plan.Destinations) != 2 {
		t.Fatalf("restart: %v", err)
	}
	if _, err := s.DestinationPlan(t.Context(), uuid.New(), owner, sessions[owner], ring); err == nil {
		t.Fatal("wrong installation decrypted credentials")
	}
	if err := s.RemoveDestination(t.Context(), entry.Destination.ID, "test", "remove"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveDestination(t.Context(), entry.Destination.ID, "test", "remove"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddDestination(t.Context(), installation, ring, entry, "test", "retry"); code(err) != IdempotencyConflict {
		t.Fatalf("deleted request recreated destination: %v", err)
	}
	plan, err := s.DestinationPlan(t.Context(), installation, owner, sessions[owner], ring)
	if err != nil || len(plan.Destinations) != 1 || plan.Revision != 4 {
		t.Fatalf("remove plan: %v", err)
	}
	if err := s.AcknowledgeDestinations(t.Context(), owner, sessions[owner], 3); err != nil {
		t.Fatal(err)
	}
	listing, err = s.ListDestinations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range listing.Delivery {
		if d.Current {
			t.Fatal("old revision marked current")
		}
	}
	var sealed []byte
	if err := s.db.QueryRowContext(t.Context(), `SELECT credentials FROM destinations WHERE id=?`, entry.Destination.ID).Scan(&sealed); err != nil || sealed != nil {
		t.Fatal("removed credential remains in active metadata")
	}
}
