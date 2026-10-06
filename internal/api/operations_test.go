package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestOperationInspection(t *testing.T) {
	server, metadata, _ := identityServer(t)
	if response := call(t, server, "GET", "/v1/operations", ""); response.Code != 401 {
		t.Fatalf("anonymous list: %d", response.Code)
	}
	if response := call(t, server, "GET", "/v1/operations", "", withBootstrap); response.Code != 200 || strings.TrimSpace(response.Body.String()) != "[]" {
		t.Fatalf("empty list: %d %s", response.Code, response.Body.String())
	}
	node := enrollTestNode(t, metadata)
	if err := metadata.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.ApproveNode(t.Context(), node, "admin", "r"); err != nil {
		t.Fatal(err)
	}
	op, err := metadata.CreateOperation(t.Context(), operation.Operation{ID: uuid.New(), AppID: uuid.New(), NodeID: node, ActorID: "admin", Kind: "backup"})
	if err != nil {
		t.Fatal(err)
	}
	response := call(t, server, "GET", "/v1/operations/"+op.ID.String(), "", withBootstrap)
	var got operation.Operation
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &got) != nil || got.ID != op.ID || got.Status != operation.Pending {
		t.Fatalf("read: %d %s", response.Code, response.Body.String())
	}
	for path, status := range map[string]int{"/v1/operations/invalid": 400, "/v1/operations/" + uuid.NewString(): 404} {
		if response := call(t, server, "GET", path, "", withBootstrap); response.Code != status {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
}
