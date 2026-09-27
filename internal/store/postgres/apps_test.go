package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
)

func TestAppStoreCRUD(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	installationID := uuid.New()

	// Enroll a test node for foreign key constraint on app_copies
	nodeID := uuid.New()
	publicKeyHash := make([]byte, 32)
	publicKeyHash[0] = 0x42
	_, err := s.pool.Exec(ctx, `INSERT INTO nodes (id, installation_id, public_key_hash, enrolled_at) VALUES ($1, $2, $3, $4)`,
		nodeID, installationID, publicKeyHash, time.Now().UTC())
	if err != nil {
		t.Fatalf("insert test node: %v", err)
	}

	appItem := app.App{
		ID:             uuid.New(),
		InstallationID: installationID,
		Name:           "web-app",
		Tier:           app.TierParallel,
		Spec: app.AppSpec{
			SchemaVersion: 1,
			Name:          "web-app",
			Tier:          app.TierParallel,
			Copies:        2,
			CopiesMode:    app.CopiesModeFixed,
			Service: app.ServiceSpec{
				Image: "traefik/whoami",
				Ports: []app.PortSpec{{ContainerPort: 80, Protocol: "tcp"}},
			},
			Router: app.RouterSpec{
				Protocol: "http",
				Port:     8080,
			},
		},
	}

	created, err := s.CreateApp(ctx, appItem)
	if err != nil {
		t.Fatalf("CreateApp failed: %v", err)
	}
	if created.ID != appItem.ID || created.Name != appItem.Name || created.Tier != app.TierParallel {
		t.Fatalf("created app mismatch: %+v", created)
	}

	// Read by ID
	fetched, err := s.GetApp(ctx, installationID, appItem.ID)
	if err != nil {
		t.Fatalf("GetApp failed: %v", err)
	}
	if fetched.Name != appItem.Name || fetched.Spec.Router.Port != 8080 {
		t.Fatalf("fetched app mismatch: %+v", fetched)
	}

	// Read by Name
	byName, err := s.GetAppByName(ctx, installationID, "web-app")
	if err != nil {
		t.Fatalf("GetAppByName failed: %v", err)
	}
	if byName.ID != appItem.ID {
		t.Fatalf("byName mismatch: %+v", byName)
	}

	// List
	list, err := s.ListApps(ctx, installationID)
	if err != nil {
		t.Fatalf("ListApps failed: %v", err)
	}
	if len(list) != 1 || list[0].ID != appItem.ID {
		t.Fatalf("list apps unexpected: %+v", list)
	}

	// Save App Copies
	copies := []app.Copy{
		{
			ID:            uuid.New(),
			AppID:         appItem.ID,
			NodeID:        nodeID,
			CopyIndex:     0,
			HostPort:      30001,
			ObservedState: "running",
		},
		{
			ID:            uuid.New(),
			AppID:         appItem.ID,
			NodeID:        nodeID,
			CopyIndex:     1,
			HostPort:      30002,
			ObservedState: "pending",
		},
	}
	if err := s.SaveAppCopies(ctx, installationID, appItem.ID, copies); err != nil {
		t.Fatalf("SaveAppCopies failed: %v", err)
	}

	// Get App Copies
	fetchedCopies, err := s.GetAppCopies(ctx, installationID, appItem.ID)
	if err != nil {
		t.Fatalf("GetAppCopies failed: %v", err)
	}
	if len(fetchedCopies) != 2 {
		t.Fatalf("expected 2 copies, got %d", len(fetchedCopies))
	}
	if fetchedCopies[0].HostPort != 30001 || fetchedCopies[1].HostPort != 30002 {
		t.Fatalf("unexpected copies: %+v", fetchedCopies)
	}
}
