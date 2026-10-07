package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestImportCatalogRequiresFreshTrustAndPreservesSettings(t *testing.T) {
	s, ca := open(t), authority(t)
	node, dest := uuid.New(), uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)
	settings := backup.DefaultSettings()
	settings.Destinations, settings.At = []uuid.UUID{dest}, "05:30"
	catalog := repository.Catalog{Version: 1, InstallationID: ca.InstallationID(), Generation: 42, WrittenAt: now,
		Devices:      []repository.Device{{ID: node, Name: "saved-device", Architecture: "amd64"}},
		Destinations: []repository.CatalogDestination{{ID: dest, NodeID: node, Name: "backup", Location: repository.Location{Kind: "local", Path: "/backup"}, Credentials: repository.Credentials{Password: "private-recovery-password"}, Initialized: true, CreatedAt: now}},
		AutoProtect:  true, Defaults: &settings,
	}
	for _, name := range []string{"enabled", "disabled"} {
		key := "compose:" + name
		app := inventory.App{ID: uuid.NewSHA1(node, []byte(key)), NodeID: node, NodeName: "saved-device", ObservedAt: now, Present: true,
			Definition: &agentv1.DiscoveredApp{Key: key, Name: name, Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: name, Name: name, Image: "alpine"}}}}
		catalog.Apps = append(catalog.Apps, app)
		catalog.Protection = append(catalog.Protection, backup.Selection{AppID: app.ID, Enabled: name == "enabled", Settings: settings})
	}
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{32}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ImportCatalog(t.Context(), catalog, ring); err != nil {
		t.Fatal(err)
	}
	if err := s.ImportCatalog(t.Context(), catalog, ring); code(err) != OperationConflict {
		t.Fatalf("second import: %v", err)
	}
	nodes, err := s.ListNodes(t.Context())
	if err != nil || len(nodes) != 1 || nodes[0].ID != node || nodes[0].ApprovedAt != nil || nodes[0].ConnectedAt != nil {
		t.Fatalf("recovered devices: %+v %v", nodes, err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "too-soon"); code(err) != NodeFactsUnavailable {
		t.Fatalf("approved an unseen recovered device: %v", err)
	}
	plan, err := s.ProtectionPlan(t.Context())
	if err != nil || plan.Revision != 43 || !plan.AutoProtect || len(plan.Apps) != 2 || plan.Defaults.At != "05:30" {
		t.Fatalf("protection plan: %v", err)
	}
	for _, app := range plan.Apps {
		if app.App.Present || app.Enabled != (app.App.Definition.Name == "enabled") || app.Settings.At != "05:30" {
			t.Fatalf("changed saved selection for %s", app.App.ID)
		}
	}
	if _, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, uuid.New(), ring); !errors.Is(err, ErrStaleSession) {
		t.Fatalf("delivered secrets before enrollment: %v", err)
	}
	var sealed []byte
	if err := s.db.QueryRowContext(t.Context(), `SELECT credentials FROM destinations WHERE id=?`, dest).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("private-recovery-password")) {
		t.Fatal("stored plaintext credentials")
	}
	var envelope secret.Envelope
	if json.Unmarshal(sealed, &envelope) != nil {
		t.Fatal("credentials have no envelope")
	}
	if _, err := ring.Open(secret.Binding{InstallationID: ca.InstallationID(), OwnerType: "destination", OwnerID: dest, CredentialID: dest, Version: 1}, envelope); err != nil {
		t.Fatal(err)
	}
	registration := TokenRegistration{ActorID: "admin", RequestID: "join", IdempotencyKey: uuid.NewString(), TokenVerifier: sha256.Sum256([]byte("unbound")), TTLSeconds: 600}
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	request := EnrollmentRequest{NodeID: node, RequestID: uuid.New(), TokenVerifier: registration.TokenVerifier, CSRDER: csr(t)}
	if _, err := s.EnrollAgent(t.Context(), request, ca); code(err) != EnrollmentConflict {
		t.Fatalf("unbound token claimed history: %v", err)
	}
	registration.NodeID, registration.IdempotencyKey, registration.TokenVerifier = node, uuid.NewString(), sha256.Sum256([]byte("bound"))
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	request.TokenVerifier, request.NodeID = registration.TokenVerifier, uuid.New()
	if _, err := s.EnrollAgent(t.Context(), request, ca); code(err) != EnrollmentUnauthorized {
		t.Fatalf("bound token claimed another ID: %v", err)
	}
	request.NodeID = node
	if _, err := s.EnrollAgent(t.Context(), request, ca); err != nil {
		t.Fatal(err)
	}
	if result, err := s.EnrollAgent(t.Context(), request, ca); err != nil || !result.Replayed {
		t.Fatalf("recovery join replay: %v", err)
	}
	registration.IdempotencyKey = uuid.NewString()
	if _, err := s.RegisterEnrollmentToken(t.Context(), registration); code(err) != EnrollmentConflict {
		t.Fatalf("real identity can be overwritten: %v", err)
	}
	session := uuid.New()
	if err := s.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "arm64", Hostname: "new-host"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	private, err := s.DestinationPlan(t.Context(), ca.InstallationID(), node, session, ring)
	if err != nil || len(private.Destinations) != 1 || len(private.Ready) != 1 || len(private.Schedules) != 0 || private.Destinations[0].Credentials.Password != "private-recovery-password" {
		t.Fatalf("recovered plan: %v", err)
	}
	rebuilt, err := repository.DecodeCatalog(private.Catalog)
	if err != nil || rebuilt.Generation <= catalog.Generation || rebuilt.Devices[0].Architecture != "arm64" || len(rebuilt.Protection) != 2 {
		t.Fatalf("new catalog: %v", err)
	}
}

func TestImportCatalogRollsBackAndRejectsRevisionOverflow(t *testing.T) {
	s := open(t)
	node, key := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{42}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	catalog := repository.Catalog{Version: 1, InstallationID: uuid.New(), Generation: 10, WrittenAt: time.Now(), Devices: []repository.Device{{ID: node, Name: "saved", Architecture: "arm64"}}}
	for range 2 {
		catalog.Destinations = append(catalog.Destinations, repository.CatalogDestination{ID: uuid.New(), NodeID: node, Name: "duplicate", Location: repository.Location{Kind: "local", Path: "/backup"}, Credentials: repository.Credentials{Password: "private-password"}})
	}
	if err := s.ImportCatalog(t.Context(), catalog, ring); err == nil {
		t.Fatal("duplicate destination names accepted")
	}
	if nodes, err := s.ListNodes(t.Context()); err != nil || len(nodes) != 0 {
		t.Fatal("failed import left partial devices")
	}
	catalog.Destinations = catalog.Destinations[:1]
	catalog.Generation = math.MaxInt64
	if err := s.ImportCatalog(t.Context(), catalog, ring); code(err) != InvalidRequest {
		t.Fatalf("revision overflow: %v", err)
	}
	catalog.Generation = 10
	if err := s.ImportCatalog(t.Context(), catalog, ring); err != nil {
		t.Fatalf("retry after failed import: %v", err)
	}
}
