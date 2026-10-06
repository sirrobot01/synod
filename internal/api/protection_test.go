package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProtectionAPIConfirmsCurrentPlanAndShowsAgentDelivery(t *testing.T) {
	server, metadata, _ := identityServer(t)
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{27}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	node := enrollTestNode(t, metadata)
	session := uuid.New()
	if err := metadata.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	report := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "notes", Image: "alpine"}}}}}
	if err := metadata.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	target := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: target, Name: "Cloud", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "backups"}}}, Credentials: repository.Credentials{Password: "private-password", AccessKeyID: "private-key", SecretAccessKey: "private-secret"}}
	if _, err := metadata.AddDestination(t.Context(), server.installationID, ring, entry, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	if response := call(t, server, "GET", "/v1/protection", ""); response.Code != 401 {
		t.Fatalf("anonymous plan: %d", response.Code)
	}
	var plan backup.ProtectionPlan
	response := call(t, server, "GET", "/v1/protection", "", withBootstrap)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &plan) != nil || len(plan.Apps) != 1 || plan.Apps[0].Enabled {
		t.Fatalf("initial plan: %d", response.Code)
	}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{target}
	input := backup.ProtectionUpdate{ID: uuid.New(), ExpectedRevision: plan.Revision, All: true, Enabled: true, Settings: settings}
	raw, _ := json.Marshal(input)
	if response := call(t, server, "PUT", "/v1/protection", string(raw)); response.Code != 401 {
		t.Fatalf("anonymous confirmation: %d", response.Code)
	}
	for range 2 {
		response := call(t, server, "PUT", "/v1/protection", string(raw), withBootstrap)
		var confirmation backup.ProtectionConfirmation
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &confirmation) != nil || confirmation.ID != input.ID || confirmation.Revision != input.ExpectedRevision+1 {
			t.Fatalf("confirmation or replay: %d", response.Code)
		}
	}
	input.ID = uuid.New()
	raw, _ = json.Marshal(input)
	response = call(t, server, "PUT", "/v1/protection", string(raw), withBootstrap)
	if response.Code != 409 || !bytes.Contains(response.Body.Bytes(), []byte("protect_plan_changed")) {
		t.Fatalf("stale preview accepted: %d", response.Code)
	}
	response = call(t, server, "GET", "/v1/protection", "", withBootstrap)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &plan) != nil || !plan.AutoProtect || !plan.Apps[0].Enabled || plan.Apps[0].SettingsCurrent || plan.Apps[0].DataLossWindowSeconds != nil {
		t.Fatalf("pending settings or no-copy status is wrong: %d", response.Code)
	}
	if bytes.Contains(response.Body.Bytes(), []byte("private-")) {
		t.Fatal("plan exposes destination credentials")
	}
	if err := metadata.AcknowledgeDestinations(t.Context(), node, session, plan.Revision); err != nil {
		t.Fatal(err)
	}
	response = call(t, server, "GET", "/v1/protection", "", withBootstrap)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &plan) != nil || !plan.Apps[0].SettingsCurrent {
		t.Fatal("acknowledged plan still pending")
	}
	input.ExpectedRevision, input.Enabled = plan.Revision, false
	input.All, input.AppIDs = false, []uuid.UUID{plan.Apps[0].App.ID}
	raw, _ = json.Marshal(input)
	if response := call(t, server, "PUT", "/v1/protection", string(raw), withBootstrap); response.Code != 200 {
		t.Fatalf("opt-out: %d", response.Code)
	}
	response = call(t, server, "GET", "/v1/protection", "", withBootstrap)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &plan) != nil || !plan.AutoProtect || plan.Apps[0].Enabled || plan.Apps[0].SettingsCurrent {
		t.Fatal("opt-out changed auto-protect or claimed delivery too soon")
	}
}
