package journal

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
)

func TestDestinationCacheRestartConflictAndRemoval(t *testing.T) {
	installation, node := uuid.New(), uuid.New()
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
	plan := destination.Plan{InstallationID: installation, Revision: 2, Destinations: []destination.Entry{{Destination: destination.Destination{ID: uuid.New(), Name: "Home", NodeID: node, Location: repository.Location{Kind: "local", Path: "/backup"}}, Credentials: repository.Credentials{Password: "private-passphrase-not-in-the-journal"}}}}
	if _, err := j.DestinationPlan(t.Context(), ring); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty cache: %v", err)
	}
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	foreign := plan
	foreign.InstallationID = uuid.New()
	foreign.Revision++
	if err := j.SaveDestinationPlan(t.Context(), ring, foreign); !errors.Is(err, ErrConflict) {
		t.Fatalf("foreign installation plan accepted: %v", err)
	}
	if err := j.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	conflict := plan
	conflict.Destinations = nil
	if err := j.SaveDestinationPlan(t.Context(), ring, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed revision: %v", err)
	}
	conflict.Revision = 1
	if err := j.SaveDestinationPlan(t.Context(), ring, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("older revision: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(plan.Destinations[0].Credentials.Password)) {
		t.Fatal("plaintext credential in journal")
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := DestinationKeyring(installation, node, key)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := j.DestinationPlan(t.Context(), reopened)
	if err != nil || saved.Revision != 2 || len(saved.Destinations) != 1 || saved.Destinations[0].Credentials.Password != plan.Destinations[0].Credentials.Password {
		t.Fatalf("reopen: %v", err)
	}
	_, wrong, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongRing, err := DestinationKeyring(installation, node, wrong)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.DestinationPlan(t.Context(), wrongRing); err == nil {
		t.Fatal("wrong enrolled key opened cache")
	}
	if err := j.SaveDestinationPlan(t.Context(), ring, destination.Plan{InstallationID: installation, Revision: 3, Destinations: []destination.Entry{}}); err != nil {
		t.Fatal(err)
	}
	saved, err = j.DestinationPlan(t.Context(), ring)
	if err != nil || saved.Revision != 3 || len(saved.Destinations) != 0 {
		t.Fatalf("removed credentials: %v", err)
	}
}
