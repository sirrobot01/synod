package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestNameLookupIsScopedAndRejectsAmbiguousDevices(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	installation, other := uuid.New(), uuid.New()
	token := strings.Repeat("lookup-token-", 8)
	verifier, err := auth.NewAdminToken(token, "operator")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, verifier, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "operator", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: "create"}, RequestID: "create", Spec: app.AppSpec{SchemaVersion: 1, Name: "photos-db", Tier: app.TierDatabase, Copies: 1, Replication: new(app.DefaultDatabaseReplication()), Database: &app.DatabaseSpec{Engine: "postgres", Version: "17", Databases: []string{"photos"}}, Router: app.RouterSpec{Protocol: "tcp", Port: 5432}}})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for i, node := range nodes {
		owner := installation
		name := "odin"
		if i == 1 {
			owner = other
		}
		if i == 2 {
			name = "server"
		}
		if _, err := pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, node, owner, make([]byte, 32)); err != nil {
			t.Fatal(err)
		}
		policy, _ := json.Marshal(map[string]any{"labels": map[string]string{"synod.name": name}})
		if _, err := pool.Exec(t.Context(), `INSERT INTO node_placement_policies(installation_id,node_id,revision,policy_bytes) VALUES($1,$2,1,$3)`, owner, node, policy); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		path, credential string
		code             int
		want             string
	}{
		{"/v1/apps/by-name/photos-db", "", 401, ""},
		{"/v1/apps/by-name/photos-db", token, 200, created.AppID.String()},
		{"/v1/apps/by-name/missing", token, 404, ""},
		{"/v1/nodes/by-name/odin", token, 200, nodes[0].String()},
		{"/v1/nodes/by-name/missing", token, 404, ""},
	} {
		request := httptest.NewRequest("GET", test.path, nil)
		if test.credential != "" {
			request.Header.Set("Authorization", "Bearer "+test.credential)
		}
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != test.code || (test.want != "" && !strings.Contains(response.Body.String(), test.want)) {
			t.Fatalf("%s: %d %s", test.path, response.Code, response.Body)
		}
	}
	policy := []byte(`{"labels":{"synod.name":"odin"}}`)
	if _, err := pool.Exec(t.Context(), `UPDATE node_placement_policies SET policy_bytes=$2 WHERE node_id=$1`, nodes[2], policy); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "/v1/nodes/by-name/odin", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != 400 {
		t.Fatalf("ambiguous name: %d %s", response.Code, response.Body)
	}
}
