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
