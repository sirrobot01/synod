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
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

// planExpiryBound is the plan lifetime plus room for clock skew. The database computes
// the expiry from its own clock and the test measures it against this process's, so an
// exact bound fails whenever the database is a moment ahead.
const planExpiryBound = 5*time.Minute + 30*time.Second

func TestClusterActionsAreReachable(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	token := strings.Repeat("secret-", 8)
	verifier, err := auth.NewAdminToken(token, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.New()
	server, err := NewServer(store, verifier, installation, testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	https := httptest.NewTLSServer(server)
	defer https.Close()
	send := func(method, path, key, match, body string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, https.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		if match != "" {
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
		if response.StatusCode == http.StatusAccepted {
			var result postgres.Admission
			if json.Unmarshal(data, &result) != nil || response.Header.Get("Location") != "/v1/operations/"+result.OperationID.String() {
				t.Fatalf("accepted response has no operation location: %s", data)
			}
		}
		return response.StatusCode, data
	}
	status, data := send("POST", "/v1/clusters", "create-key", "", `{"schema_version":1,"name":"example","engine":"postgres","postgresql":{"version":"17.11","topology":"standalone"}}`)
	var created postgres.Admission
	if status != http.StatusAccepted || json.Unmarshal(data, &created) != nil {
		t.Fatalf("create: %d %s", status, data)
	}
	path := "/v1/clusters/" + created.ClusterID.String()
	finish := func(id uuid.UUID) {
		t.Helper()
		if _, err := pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	if status, data := send("POST", path+"/action-plans", "", "", `{"action":"delete"}`); status != http.StatusConflict {
		t.Fatalf("preview during work: %d %s", status, data)
	}
	finish(created.OperationID)
	revision := created.DesiredRevision
	var stopped postgres.Admission
	for _, action := range []string{"stop", "start"} {
		status, data := send("POST", path+"/actions/"+action, action+"-key", strconv.Quote(strconv.FormatInt(revision, 10)), `{}`)
		var result postgres.Admission
		if status != http.StatusAccepted || json.Unmarshal(data, &result) != nil || result.Replayed || result.ClusterID != created.ClusterID || result.DesiredRevision != revision+1 {
			t.Fatalf("%s: %d %s", action, status, data)
		}
		if action == "stop" {
			stopped = result
		}
		revision = result.DesiredRevision
		finish(result.OperationID)
	}
	if status, data := send("POST", path+"/actions/stop", "stale-key", `"1"`, `{}`); status != http.StatusPreconditionFailed || !strings.Contains(string(data), `"revision_conflict"`) {
		t.Fatalf("stale revision: %d %s", status, data)
	}
	status, data = send("POST", path+"/action-plans", "", "", `{"action":"delete"}`)
	var plan cluster.ActionPlan
	if status != http.StatusOK || json.Unmarshal(data, &plan) != nil || plan.PlanToken == uuid.Nil || plan.ClusterID != created.ClusterID || plan.ClusterName != "example" || plan.Action != "delete" || plan.ExpectedRevision != revision || plan.DataDisposition != "retain" || plan.RetainedVolumes == nil || plan.Downtime == "" || time.Until(plan.ExpiresAt) <= 0 || time.Until(plan.ExpiresAt) > planExpiryBound {
		t.Fatalf("preview: %d %s", status, data)
	}
	current, err := store.GetCluster(t.Context(), installation, created.ClusterID)
	if err != nil || current.DesiredRevision != revision || current.DesiredLifecycle != "running" {
		t.Fatalf("preview changed cluster: %+v %v", current, err)
	}
	var operations int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM operations WHERE cluster_id=$1`, created.ClusterID).Scan(&operations); err != nil || operations != 3 {
		t.Fatalf("preview created work: count=%d err=%v", operations, err)
	}
	confirmation, err := json.Marshal(cluster.ActionConfirmation{PlanToken: plan.PlanToken, Confirm: true, DataDisposition: "retain"})
	if err != nil {
		t.Fatal(err)
	}
	match := strconv.FormatInt(revision, 10)
	status, data = send("DELETE", path, "delete-key", match, string(confirmation))
	var deleted postgres.Admission
	if status != http.StatusAccepted || json.Unmarshal(data, &deleted) != nil || deleted.Replayed || deleted.DesiredRevision != revision+1 {
		t.Fatalf("delete: %d %s", status, data)
	}
	finish(deleted.OperationID)
	// A lost reply must remain recoverable after the plan expires and deletion settles.
	if _, err := pool.Exec(t.Context(), `UPDATE cluster_action_plans SET created_at=clock_timestamp()-interval '10 minutes',expires_at=clock_timestamp()-interval '5 minutes' WHERE plan_token=$1`, plan.PlanToken); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE clusters SET deleted_at=clock_timestamp(),observed_lifecycle='deleted' WHERE id=$1`, created.ClusterID); err != nil {
		t.Fatal(err)
	}
	status, data = send("DELETE", path, "delete-key", match, string(confirmation))
	var replay postgres.Admission
	if status != http.StatusAccepted || json.Unmarshal(data, &replay) != nil || !replay.Replayed || replay.OperationID != deleted.OperationID || replay.DesiredRevision != deleted.DesiredRevision {
		t.Fatalf("expired delete replay: %d %s", status, data)
	}
	status, data = send("POST", path+"/actions/stop", "stop-key", strconv.FormatInt(created.DesiredRevision, 10), `{}`)
	if status != http.StatusAccepted || json.Unmarshal(data, &replay) != nil || !replay.Replayed || replay.OperationID != stopped.OperationID {
		t.Fatalf("stop replay after deletion: %d %s", status, data)
	}
	if status, data := send("DELETE", path, "delete-key", strconv.FormatInt(revision+1, 10), string(confirmation)); status != http.StatusConflict || !strings.Contains(string(data), `"idempotency_conflict"`) {
		t.Fatalf("changed retry: %d %s", status, data)
	}
	for _, body := range []string{
		strings.Replace(string(confirmation), `"confirm":true`, `"confirm":false`, 1),
		strings.Replace(string(confirmation), `"retain"`, `"purge"`, 1),
		strings.Replace(string(confirmation), plan.PlanToken.String(), uuid.Nil.String(), 1),
	} {
		if status, data := send("DELETE", path, "delete-key", match, body); status != http.StatusConflict || !strings.Contains(string(data), `"idempotency_conflict"`) {
			t.Fatalf("changed confirmation retry: %d %s", status, data)
		}
	}
	if status, data := send("POST", path+"/action-plans", "", "", `{"action":"delete"}`); status != http.StatusNotFound {
		t.Fatalf("deleted preview: %d %s", status, data)
	}
	if status, data := send("POST", path+"/actions/delete", "old-route", match, string(confirmation)); status != http.StatusNotFound {
		t.Fatalf("removed route: %d %s", status, data)
	}
}

func TestClusterActionRequestValidation(t *testing.T) {
	store, _, _ := apiTestStore(t)
	admin, err := auth.NewAdminToken(bootstrapToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(store, admin, uuid.New(), testCatalog(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/clusters/" + uuid.NewString()
	for _, route := range []struct{ method, path, body string }{
		{"POST", path + "/actions/start", `{}`},
		{"POST", path + "/actions/stop", `{}`},
		{"POST", path + "/actions/recover-setup", `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`},
		{"DELETE", path, `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`},
		{"POST", path + "/action-plans", `{"action":"delete"}`},
	} {
		t.Run(route.method+route.path, func(t *testing.T) {
			prepare := func(r *http.Request) {
				withBootstrap(r)
				r.Header.Set("Idempotency-Key", "validation-key")
				r.Header.Set("If-Match", `"1"`)
			}
			for name, body := range map[string]string{"null": "null", "array": "[]", "empty": "", "unknown": `{"unexpected":true}`, "multiple": route.body + ` {}`, "broken": "{"} {
				t.Run(name, func(t *testing.T) {
					rec := call(t, server, route.method, route.path, body, prepare, func(r *http.Request) { r.Header.Set("Content-Type", "application/json") })
					if rec.Code != http.StatusBadRequest {
						t.Fatalf("%d %s", rec.Code, rec.Body)
					}
				})
			}
			if rec := call(t, server, route.method, route.path, route.body); rec.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated: %d %s", rec.Code, rec.Body)
			}
			if rec := call(t, server, route.method, route.path, route.body, prepare, func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+bootstrapToken) }); rec.Code != http.StatusUnauthorized {
				t.Fatalf("duplicate auth: %d %s", rec.Code, rec.Body)
			}
			if strings.HasSuffix(route.path, "action-plans") {
				return
			}
			for _, match := range []string{"", "0", "-1", "*", `W/"1"`, `"1", "2"`, "9223372036854775808", "+1", "01", `"\u0031"`} {
				rec := call(t, server, route.method, route.path, route.body, prepare, func(r *http.Request) { r.Header.Set("If-Match", match) })
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("If-Match %q: %d %s", match, rec.Code, rec.Body)
				}
			}
			for _, header := range []string{"If-Match", "Idempotency-Key"} {
				for _, duplicate := range []bool{false, true} {
					rec := call(t, server, route.method, route.path, route.body, prepare, func(r *http.Request) {
						if duplicate {
							r.Header.Add(header, r.Header.Get(header))
						} else {
							r.Header.Del(header)
						}
					})
					if rec.Code != http.StatusBadRequest {
						t.Fatalf("header=%s duplicate=%v: %d %s", header, duplicate, rec.Code, rec.Body)
					}
				}
			}
		})
	}
	for _, body := range []string{`{}`, `{"confirm":true,"data_disposition":"retain"}`, `{"plan_token":"` + uuid.NewString() + `","confirm":false,"data_disposition":"retain"}`, `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"purge"}`} {
		rec := call(t, server, "DELETE", path, body, withBootstrap, func(r *http.Request) { r.Header.Set("Idempotency-Key", "delete"); r.Header.Set("If-Match", "1") })
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("confirmation %s: %d %s", body, rec.Code, rec.Body)
		}
	}
	for _, body := range []string{`{}`, `{"action":"stop"}`} {
		rec := call(t, server, "POST", path+"/action-plans", body, withBootstrap)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("preview %s: %d %s", body, rec.Code, rec.Body)
		}
	}
}

func TestClusterActionsRequireBrowserOrigin(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	cookie := sessionCookie(t, server, "operator", operatorPassword)
	path := "/v1/clusters/" + uuid.NewString()
	for _, route := range []struct{ method, path, body string }{
		{"POST", path + "/actions/start", `{}`},
		{"POST", path + "/actions/stop", `{}`},
		{"POST", path + "/actions/recover-setup", `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`},
		{"DELETE", path, `{"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`},
		{"POST", path + "/action-plans", `{"action":"delete"}`},
	} {
		headers := func(r *http.Request) { r.Header.Set("If-Match", "1"); r.Header.Set("Idempotency-Key", "origin-test") }
		rec := call(t, server, route.method, route.path, route.body, withCookie(cookie), headers)
		if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), `"cross_origin"`) {
			t.Fatalf("missing origin: %d %s", rec.Code, rec.Body)
		}
		rec = call(t, server, route.method, route.path, route.body, withCookie(cookie), headers, func(r *http.Request) {
			r.Header.Set("Origin", "https://other.example")
			r.Header.Set("Sec-Fetch-Site", "cross-site")
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("cross origin: %d %s", rec.Code, rec.Body)
		}
		rec = call(t, server, route.method, route.path, route.body, withCookie(cookie), headers, sameOriginFetch)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("same origin did not reach resource lookup: %d %s", rec.Code, rec.Body)
		}
	}
}

func TestEveryAdmissionCodeHasAStatus(t *testing.T) {
	for code, want := range map[postgres.ErrorCode]int{
		postgres.EnrollmentUnauthorized: http.StatusUnauthorized,
		postgres.EnrollmentConflict:     http.StatusConflict,
		postgres.InvalidRequest:         http.StatusBadRequest,
		postgres.UnsupportedSpec:        http.StatusUnprocessableEntity,
		postgres.IdempotencyConflict:    http.StatusConflict,
		postgres.NameConflict:           http.StatusConflict,
		postgres.OperationConflict:      http.StatusConflict,
		postgres.RevisionConflict:       http.StatusPreconditionFailed,
		postgres.NotFound:               http.StatusNotFound,
		// Both of these are genuinely "ask again later", so they keep the default.
		postgres.DependencyUnavailable: http.StatusServiceUnavailable,
		postgres.OutcomeUnknown:        http.StatusServiceUnavailable,
	} {
		t.Run(string(code), func(t *testing.T) {
			server := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			recorder := httptest.NewRecorder()
			request := withIdentity(httptest.NewRequest(http.MethodGet, "/v1/clusters", nil), requestIdentity{requestID: "test"})
			server.storeError(recorder, request, &postgres.AdmissionError{Code: code})
			if recorder.Code != want {
				t.Fatalf("status=%d, want %d", recorder.Code, want)
			}
			var body struct {
				Code string `json:"code"`
			}
			if json.Unmarshal(recorder.Body.Bytes(), &body) != nil || body.Code != string(code) {
				t.Fatalf("body=%s, want the code named", recorder.Body)
			}
		})
	}
}

// A scale carries a replica count, so the route must read one and refuse a request
// that leaves it out. An absent count is not a request for zero.
func TestScaleRouteCarriesAReplicaCount(t *testing.T) {
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
	send := func(method, path, key, match, body string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, https.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		if match != "" {
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
	status, data := send("POST", "/v1/clusters", "create-key", "",
		`{"schema_version":1,"name":"cache","engine":"redis","redis":{"version":"8.2.7","topology":"standalone","persistence":"aof"}}`)
	var created postgres.Admission
	if status != http.StatusAccepted || json.Unmarshal(data, &created) != nil {
		t.Fatalf("create: %d %s", status, data)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, created.OperationID); err != nil {
		t.Fatal(err)
	}
	path := "/v1/clusters/" + created.ClusterID.String() + "/actions/scale"
	match := strconv.Quote(strconv.FormatInt(created.DesiredRevision, 10))
	for name, body := range map[string]string{
		"no count":     `{}`,
		"null count":   `{"replicas":null}`,
		"not a number": `{"replicas":"one"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if status, data := send("POST", path, name+"-key", match, body); status != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", status, data)
			}
		})
	}
	// The guards the other actions have apply here too.
	if status, data := send("POST", path, "", match, `{"replicas":1}`); status != http.StatusBadRequest {
		t.Fatalf("missing idempotency key: %d %s", status, data)
	}
	if status, data := send("POST", path, "no-match-key", "", `{"replicas":1}`); status != http.StatusBadRequest {
		t.Fatalf("missing revision: %d %s", status, data)
	}
	status, data = send("POST", path, "scale-key", match, `{"replicas":1}`)
	var admitted postgres.Admission
	if status != http.StatusAccepted || json.Unmarshal(data, &admitted) != nil || admitted.ClusterID != created.ClusterID {
		t.Fatalf("scale: %d %s", status, data)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, admitted.OperationID); err != nil {
		t.Fatal(err)
	}
	// The scale moved the revision, so the one it replaced is now stale.
	if status, data := send("POST", path, "stale-key", match, `{"replicas":1}`); status != http.StatusPreconditionFailed {
		t.Fatalf("a stale revision was accepted: %d %s", status, data)
	}
	// A scale down carries the reviewed plan in the same body as the count. The route
	// must accept those fields: rejecting them as unknown makes every confirmed removal
	// impossible, and the decoder refuses unknown fields.
	body := `{"replicas":0,"plan_token":"` + uuid.NewString() + `","confirm":true,"data_disposition":"retain"}`
	status, data = send("POST", path, "confirmed-key", match, body)
	if strings.Contains(string(data), "known fields") {
		t.Fatalf("the scale route refused a removal confirmation as unknown fields: %d %s", status, data)
	}

	// The cluster reports the count it was asked for, and its one member so far.
	status, data = send("GET", "/v1/clusters/"+created.ClusterID.String(), "", "", "")
	if status != http.StatusOK || !strings.Contains(string(data), `"desired_replicas":1`) {
		t.Fatalf("cluster read: %d %s", status, data)
	}
	// A cluster whose create was admitted but not yet planned holds no member. The
	// route reports that as an empty list rather than as a missing cluster.
	status, data = send("GET", "/v1/clusters/"+created.ClusterID.String()+"/members", "", "", "")
	if status != http.StatusOK || strings.TrimSpace(string(data)) != `{"members":[]}` {
		t.Fatalf("members read: %d %s", status, data)
	}
	// A mistyped identity is not an empty cluster.
	if status, data := send("GET", "/v1/clusters/"+uuid.New().String()+"/members", "", "", ""); status != http.StatusNotFound {
		t.Fatalf("members read for an unknown cluster: %d %s", status, data)
	}
}
