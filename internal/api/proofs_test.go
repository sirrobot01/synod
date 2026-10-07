package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProofAPIRequiresApprovalAndKeepsRequestsImmutable(t *testing.T) {
	server, metadata, _ := identityServer(t)
	keyID := uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{18}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	server.CredentialKeyring = ring
	node := enrollTestNode(t, metadata)
	session := uuid.New()
	if err := metadata.RecordAgentSession(t.Context(), node, session, &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	report := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:test", Name: "test", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "container", Name: "test", Image: "alpine"}}}}}
	if err := metadata.RecordInventory(t.Context(), node, session, report); err != nil {
		t.Fatal(err)
	}
	destinationID := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: destinationID, Name: "Cloud", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://storage.example", Bucket: "proofs"}}}, Credentials: repository.Credentials{Password: "private-password", AccessKeyID: "private-key", SecretAccessKey: "private-secret"}}
	if _, err := metadata.AddDestination(t.Context(), server.installationID, ring, entry, "admin", "destination"); err != nil {
		t.Fatal(err)
	}
	input := proof.Submission{Kind: proof.Drill, ID: uuid.New(), AppID: uuid.NewSHA1(node, []byte("compose:test")), DestinationID: destinationID, NodeID: node}
	raw, _ := json.Marshal(input)
	if response := call(t, server, "POST", "/v1/proofs", string(raw)); response.Code != 401 {
		t.Fatalf("anonymous proof: %d", response.Code)
	}
	if response := call(t, server, "POST", "/v1/proofs", string(raw), withBootstrap); response.Code != 404 {
		t.Fatalf("unapproved proof: %d", response.Code)
	}
	if _, err := metadata.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		response := call(t, server, "POST", "/v1/proofs", string(raw), withBootstrap)
		var op operation.Operation
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &op) != nil || op.ID != input.ID || op.Status != operation.Pending {
			t.Fatalf("proof submit: %d", response.Code)
		}
		if bytes.Contains(response.Body.Bytes(), []byte("private-")) || bytes.Contains(response.Body.Bytes(), []byte("source_key")) {
			t.Fatal("proof response exposes private input")
		}
	}
	input.SnapshotID = uuid.New()
	changed, _ := json.Marshal(input)
	if response := call(t, server, "POST", "/v1/proofs", string(changed), withBootstrap); response.Code != 409 {
		t.Fatalf("changed request: %d", response.Code)
	}
}
