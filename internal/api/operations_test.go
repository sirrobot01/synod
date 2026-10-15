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

// Activity pages by cursor, hides finished work, and devices can be renamed.
func TestActivityAndDeviceNameRoutes(t *testing.T) {
	server, metadata, _ := identityServer(t)
	node := enrollTestNode(t, metadata)
	if err := metadata.RecordAgentSession(t.Context(), node, uuid.New(), &agentv1.Hello{NodeId: node.String(), Architecture: "amd64", Hostname: "c128ce056ec1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.ApproveNode(t.Context(), node, "admin", "approve"); err != nil {
		t.Fatal(err)
	}
	finished, running := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{finished, running} {
		if _, err := metadata.CreateOperation(t.Context(), operation.Operation{ID: id, AppID: uuid.New(), NodeID: node, ActorID: "admin", Kind: "backup"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := metadata.TransitionOperation(t.Context(), finished, operation.Pending, operation.Cancelled, ""); err != nil {
		t.Fatal(err)
	}
	if rec := call(t, server, "GET", "/v1/operations?limit=0", "", withBootstrap); rec.Code != 400 {
		t.Fatalf("limit 0: %d", rec.Code)
	}
	if rec := call(t, server, "GET", "/v1/operations?limit=1", "", withBootstrap); rec.Code != 200 || strings.Count(rec.Body.String(), `"kind"`) != 1 {
		t.Fatalf("first page: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "DELETE", "/v1/operations/"+running.String(), "", withBootstrap); rec.Code != 409 || !strings.Contains(rec.Body.String(), "operation_active") {
		t.Fatalf("remove running work: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "DELETE", "/v1/operations/"+finished.String(), "", withBootstrap); rec.Code != 204 {
		t.Fatalf("remove finished work: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "GET", "/v1/operations", "", withBootstrap); strings.Contains(rec.Body.String(), finished.String()) {
		t.Fatal("removed work is still listed")
	}
	if rec := call(t, server, "POST", "/v1/operations/actions/clear", "{}", withBootstrap); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"removed":0`) {
		t.Fatalf("clear: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "PATCH", "/v1/nodes/"+node.String(), `{"name":"-bad"}`, withBootstrap); rec.Code != 400 {
		t.Fatalf("bad name: %d %s", rec.Code, rec.Body)
	}
	if rec := call(t, server, "PATCH", "/v1/nodes/"+node.String(), `{"name":"server"}`, withBootstrap); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"name":"server"`) {
		t.Fatalf("rename: %d %s", rec.Code, rec.Body)
	}
}
