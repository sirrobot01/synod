package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

func TestAppAPIAdmission(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("token-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, verifier, uuid.New(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"schema_version":1,"name":"web","tier":3,"copies":2,"service":{"image":"nginx","ports":[{"container_port":80,"protocol":"tcp"}]},"router":{"protocol":"tcp","port":8080}}`
	var first app.Admission
	for _, test := range []struct {
		name, body, key, credential string
		status                      int
	}{
		{"unauthorized", "invalid", "key", "", 401},
		{"create", body, "key", token, 202},
		{"replay", body, "key", token, 202},
		{"changed", strings.Replace(body, `"copies":2`, `"copies":3`, 1), "key", token, 409},
		{"name", body, "other", token, 409},
		{"missing key", body, "", token, 400},
		{"unknown field", strings.Replace(body, `"name":`, `"secret":"private-input","name":`, 1), "new", token, 400},
		{"second object", body + `{}`, "new", token, 400},
		{"null", `null`, "new", token, 400},
		{"invalid mode", strings.Replace(body, `"copies":2`, `"copies":2,"copies_mode":"bogus"`, 1), "new", token, 400},
		{"forged architecture", strings.Replace(body, `"image":`, `"supported_architectures":["amd64"],"image":`, 1), "new", token, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("POST", "/v1/apps", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			if test.key != "" {
				request.Header.Set("Idempotency-Key", test.key)
			}
			if test.credential != "" {
				request.Header.Set("Authorization", "Bearer "+test.credential)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status %d: %s", response.Code, response.Body)
			}
			if bytes.Contains(response.Body.Bytes(), []byte("private-input")) {
				t.Fatal("private input leaked")
			}
			if response.Code == 202 {
				var result app.Admission
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if first.AppID == uuid.Nil {
					first = result
				} else if result.AppID != first.AppID || !result.Replayed {
					t.Fatalf("bad replay: %+v", result)
				}
				if response.Header().Get("Location") != "/v1/apps/"+first.AppID.String() {
					t.Fatal("bad location")
				}
			}
		})
	}
	request := httptest.NewRequest("GET", "/v1/apps/"+first.AppID.String(), nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var got app.App
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || response.Code != 200 || got.ID != first.AppID || got.ObservedLifecycle != "provisioning" {
		t.Fatalf("read app: %s %v", response.Body, err)
	}
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM apps").Scan(&count); err != nil || count != 1 {
		t.Fatalf("stored apps: %d %v", count, err)
	}
	for index, action := range []string{"stop", "start", "delete"} {
		method, path := http.MethodPost, "/v1/apps/"+first.AppID.String()+"/actions/"+action
		if action == "delete" {
			method, path = http.MethodDelete, "/v1/apps/"+first.AppID.String()
		}
		var accepted app.Admission
		for _, test := range []struct {
			revision, body, token string
			status                int
		}{
			{"1", "{}", "", 401},
			{"", "{}", token, 400},
			{strconv.Itoa(index + 1), "null", token, 400},
			{strconv.Itoa(index + 1), "{}", token, 202},
			{strconv.Itoa(index + 1), "{}", token, 202},
		} {
			r := httptest.NewRequest(method, path, strings.NewReader(test.body))
			r.Header.Set("Authorization", "Bearer "+test.token)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", action)
			if test.revision != "" {
				r.Header.Set("If-Match", strconv.Quote(test.revision))
			}
			w := httptest.NewRecorder()
			server.ServeHTTP(w, r)
			if w.Code != test.status {
				t.Fatalf("%s: %d %s", action, w.Code, w.Body)
			}
			if w.Code == 202 {
				var result app.Admission
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if accepted.OperationID != uuid.Nil && (result.OperationID != accepted.OperationID || !result.Replayed) {
					t.Fatalf("action retry: %+v", result)
				}
				accepted = result
				if w.Header().Get("Location") != "/v1/operations/"+result.OperationID.String() {
					t.Fatal("wrong operation location")
				}
			}
		}
	}
}

func TestAppMoveAPIKeepsTargetAndRetryIdentity(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	installation, source, target := uuid.New(), uuid.New(), uuid.New()
	token := strings.Repeat("move-token-", 8)
	verifier, err := auth.NewAdminToken(token, "operator")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, verifier, installation, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	r := app.DefaultReplication()
	created, err := store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "operator", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: "create"}, RequestID: "create", Spec: app.AppSpec{SchemaVersion: 1, Name: "move-app", Tier: app.TierSingleWriter, Copies: 1, Replication: &r, Service: app.ServiceSpec{Image: "example", Ports: []app.PortSpec{{ContainerPort: 80}}, Mounts: []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []uuid.UUID{source, target} {
		if _, err := pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, node, installation, make([]byte, 32)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(t.Context(), `UPDATE apps SET writer_node_id=$2,writer_epoch=1 WHERE id=$1`, created.AppID, source); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',finished_at=clock_timestamp() WHERE id=$1`, created.OperationID); err != nil {
		t.Fatal(err)
	}
	body := `{"target_node_id":"` + target.String() + `"}`
	var accepted app.Admission
	for _, test := range []struct {
		name, body, key, revision, credential string
		code                                  int
	}{
		{"unauthorized", body, "move", `"1"`, "", 401},
		{"missing target", "{}", "move", `"1"`, token, 400},
		{"bad target", `{"target_node_id":"device"}`, "move", `"1"`, token, 400},
		{"stale", body, "stale", `"2"`, token, 412},
		{"accepted", body, "move", `"1"`, token, 202},
		{"replay", body, "move", `"1"`, token, 202},
		{"changed", `{"target_node_id":"` + source.String() + `"}`, "move", `"1"`, token, 409},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/apps/"+created.AppID.String()+"/move", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", test.key)
			request.Header.Set("If-Match", test.revision)
			if test.credential != "" {
				request.Header.Set("Authorization", "Bearer "+test.credential)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("response %d: %s", response.Code, response.Body.String())
			}
			if test.code == 202 {
				var result app.Admission
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if test.name == "accepted" {
					accepted = result
				} else if !result.Replayed || result.OperationID != accepted.OperationID {
					t.Fatalf("replay changed operation: %+v", result)
				}
			}
		})
	}
}
