package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// A member rebuild preview needs member_id, and no other preview takes one. A standalone
// cluster has no member rebuild, no certificate renewal, and no switchover.
func TestMemberRebuildRoutes(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, verifier, uuid.New(), testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	send := func(path, key, match, body string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, https.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
			request.Header.Set("If-Match", match)
		}
		response, err := https.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, https.URL+"/v1/clusters", strings.NewReader(`{"schema_version":1,"name":"example","engine":"postgres","postgresql":{"version":"17.11","topology":"standalone"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Idempotency-Key", "create-key")
	response, err := https.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var created postgres.Admission
	err = json.NewDecoder(response.Body).Decode(&created)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("create: %d %v", response.StatusCode, err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, created.OperationID); err != nil {
		t.Fatal(err)
	}
	path := "/v1/clusters/" + created.ClusterID.String()
	member := uuid.NewString()
	for body, want := range map[string]int{
		`{"action":"rebuild_member"}`:                              http.StatusBadRequest,
		`{"action":"delete","member_id":"` + member + `"}`:         http.StatusBadRequest,
		`{"action":"rebuild_member","member_id":"` + member + `"}`: http.StatusUnprocessableEntity,
	} {
		if status, data := send(path+"/action-plans", "", "", body); status != want {
			t.Fatalf("preview %s: %d %s", body, status, data)
		}
	}
	match := strconv.Quote(strconv.FormatInt(created.DesiredRevision, 10))
	confirmation := `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`
	if status, data := send(path+"/actions/rebuild-member", "rebuild-key", match, confirmation); status != http.StatusUnprocessableEntity {
		t.Fatalf("standalone member rebuild: %d %s", status, data)
	}
	if status, data := send(path+"/actions/renew-certificates", "renew-key", match, `{}`); status != http.StatusUnprocessableEntity {
		t.Fatalf("standalone certificate renewal: %d %s", status, data)
	}
	// A switchover names its member, and a standalone cluster has none to switch to.
	for body, want := range map[string]int{
		`{}`: http.StatusBadRequest,
		`{"member_id":"` + uuid.Nil.String() + `"}`: http.StatusBadRequest,
		`{"member_id":"` + member + `","x":1}`:      http.StatusBadRequest,
		`{"member_id":"` + member + `"}`:            http.StatusUnprocessableEntity,
	} {
		if status, data := send(path+"/actions/switchover", "switch-"+strconv.Itoa(len(body)), match, body); status != want {
			t.Fatalf("switchover %s: %d %s", body, status, data)
		}
	}
}
