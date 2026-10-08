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
		if err != nil || len(plan.Destinations) != want || plan.Revision != 5 {
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
	if err := s.AcknowledgeDestinations(t.Context(), owner, old, 5); !errors.Is(err, ErrStaleSession) {
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
	if err != nil || len(plan.Destinations) != 1 || plan.Revision != 6 {
		t.Fatalf("remove plan: %v", err)
	}
	if err := s.AcknowledgeDestinations(t.Context(), owner, sessions[owner], 5); err != nil {
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

func TestDevicePlansRefreshAddressesAndCertificateAccess(t *testing.T) {
	s, ca := open(t), authority(t)
	owner, _ := join(t, s, ca)
	source, _ := join(t, s, ca)
	pending, _ := join(t, s, ca)
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{23}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	sessions := map[uuid.UUID]uuid.UUID{owner: uuid.New(), source: uuid.New(), pending: uuid.New()}
	for node, session := range sessions {
		hello := &agentv1.Hello{NodeId: node.String(), Architecture: "arm64"}
		if node == owner {
			hello.DestinationAddress = "192.0.2.10:8445"
		}
		if err := s.RecordAgentSession(t.Context(), node, session, hello); err != nil {
			t.Fatal(err)
		}
		if node != pending {
			if _, err := s.ApproveNode(t.Context(), node, "test", "approve"); err != nil {
				t.Fatal(err)
			}
		}
	}
	id := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: id, NodeID: owner, Name: "Device", Location: repository.Location{Kind: "device", Path: "/srv/backup", Device: &repository.DeviceLocation{NodeID: owner, DestinationID: id}}}, Credentials: repository.Credentials{Password: "repository-password"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "test", "add"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.DestinationPlan(t.Context(), ca.InstallationID(), source, sessions[source], ring)
	if err != nil || len(plan.Destinations) != 1 || len(plan.Peers) != 2 {
		t.Fatalf("device plan: %v", err)
	}
	for _, peer := range plan.Peers {
		if peer.NodeID == pending {
			t.Fatal("pending peer received access")
		}
	}
	location, _, err := plan.Connection(id, &repository.DeviceIdentity{InstallationID: ca.InstallationID()})
	if err != nil || location.Device.Endpoint != "https://192.0.2.10:8445" || len(location.Device.CertificateSHA256) != 1 {
		t.Fatalf("device address: %v", err)
	}
	if err := s.AcknowledgeDestinations(t.Context(), source, sessions[source], plan.Revision); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAgentSession(t.Context(), owner, uuid.New(), &agentv1.Hello{NodeId: owner.String(), Architecture: "arm64", DestinationAddress: "192.0.2.11:8445"}); err != nil {
		t.Fatal(err)
	}
	moved, err := s.DestinationPlan(t.Context(), ca.InstallationID(), source, sessions[source], ring)
	if err != nil || moved.Revision != plan.Revision+1 {
		t.Fatalf("address revision: %v", err)
	}
	location, _, err = moved.Connection(id, &repository.DeviceIdentity{InstallationID: ca.InstallationID()})
	if err != nil || location.Device.Endpoint != "https://192.0.2.11:8445" {
		t.Fatalf("new address: %v", err)
	}
	var certificateID uuid.UUID
	if err := s.db.QueryRowContext(t.Context(), `SELECT id FROM node_certificates WHERE node_id=?`, source).Scan(&certificateID); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAgentCertificate(t.Context(), source, certificateID, "test", "revoke"); err != nil {
		t.Fatal(err)
	}
	revoked, err := s.DestinationPlan(t.Context(), ca.InstallationID(), source, sessions[source], ring)
	if err != nil || revoked.Revision != moved.Revision+1 || len(revoked.Peers) != 1 || revoked.Peers[0].NodeID != owner {
		t.Fatalf("certificate revocation did not change plan: %v", err)
	}
}

func TestDestinationUsesKitPasswordUnlessGiven(t *testing.T) {
	s, ca := open(t), authority(t)
	node, _ := join(t, s, ca)
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{61}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	add := func(name, password string) error {
		entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: name, Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: name}}}, Credentials: repository.Credentials{Password: password, AccessKeyID: "key", SecretAccessKey: "secret"}}
		_, err := s.AddDestination(t.Context(), ca.InstallationID(), ring, entry, "admin", name)
		return err
	}
	if err := add("before-kit", ""); code(err) != InvalidRequest {
		t.Fatalf("a destination without any password was accepted: %v", err)
	}
	if err := s.SetRepositoryPassword(t.Context(), ca.InstallationID(), ring, "short"); code(err) != InvalidRequest {
		t.Fatalf("a short kit password was accepted: %v", err)
	}
	const kit = "abcd-efgh-ijkl-mnop-qrst-uvwx-yz23-4567"
	if err := s.SetRepositoryPassword(t.Context(), ca.InstallationID(), ring, kit); err != nil {
		t.Fatal(err)
	}
	if err := add("default", ""); err != nil {
		t.Fatal(err)
	}
	if err := add("custom", "custom-existing-repository-password"); err != nil {
		t.Fatal(err)
	}
	plan, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
	if err != nil {
		t.Fatal(err)
	}
	passwords := map[string]string{}
	for _, entry := range plan.Destinations {
		passwords[entry.Destination.Name] = entry.Credentials.Password
	}
	if passwords["default"] != kit || passwords["custom"] != "custom-existing-repository-password" {
		t.Fatal("destinations did not keep the kit password and the custom password")
	}
	other := uuid.New()
	wrong, err := secret.NewKeyring(other, map[uuid.UUID][]byte{other: bytes.Repeat([]byte{62}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: "wrong-key", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "w"}}}, Credentials: repository.Credentials{AccessKeyID: "key", SecretAccessKey: "secret"}}
	if _, err := s.AddDestination(t.Context(), ca.InstallationID(), wrong, entry, "admin", "wrong"); err == nil {
		t.Fatal("another key ring opened the kit password")
	}
}
