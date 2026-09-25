package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"golang.org/x/time/rate"
)

func TestConnectionInspectionAndAuditedReveal(t *testing.T) {
	store, pool, _ := apiTestStore(t)
	installation, keyID := uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.CreateCluster(t.Context(), postgres.CreateClusterRequest{
		Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "test-admin", APIOperation: postgres.CreateClusterAPIOperation, IdempotencyKey: "connection-fixture"}, RequestID: "connection-fixture",
		Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "connection-fixture", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	password := "private-credential-value-do-not-log"
	credential := postgres.Credential{CredentialID: uuid.New(), Version: 1, Purpose: postgres.SuperuserCredential, RoleName: "synod_admin"}
	binding := secret.Binding{InstallationID: installation, OwnerType: "cluster", OwnerID: admitted.ClusterID, CredentialID: credential.CredentialID, Version: 1}
	credential.Envelope, err = ring.Seal(binding, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordClusterIntent(t.Context(), postgres.ClusterIntent{InstallationID: installation, ClusterID: admitted.ClusterID, Credentials: []postgres.Credential{credential}}); err != nil {
		t.Fatal(err)
	}
	admin, err := auth.NewAdminToken(bootstrapToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "api.log")
	logs, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	makeServer := func(keys *secret.Keyring, limit *rate.Limiter) *httptest.Server {
		t.Helper()
		handler, err := NewServer(store, admin, installation, testCatalog(t), logger)
		if err != nil {
			t.Fatal(err)
		}
		handler.SecretKeyring, handler.credentialRevealLimit = keys, limit
		handler.PatroniCA = []byte("public-native-ca")
		https := httptest.NewTLSServer(handler)
		t.Cleanup(https.Close)
		return https
	}
	https := makeServer(ring, rate.NewLimiter(rate.Inf, 1))
	send := func(server *httptest.Server, method, path, body string, authorize bool) (int, http.Header, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if authorize {
			request.Header.Set("Authorization", "Bearer "+bootstrapToken)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("response permits caching: %d %s", response.StatusCode, data)
		}
		if response.StatusCode != http.StatusOK && (bytes.Contains(data, []byte(password)) || bytes.Contains(data, []byte("private-audit-detail"))) {
			t.Fatal("failure exposed secret material")
		}
		return response.StatusCode, response.Header, data
	}
	path := "/v1/clusters/" + admitted.ClusterID.String()
	revealPath := path + "/credentials/superuser/reveal"
	status, _, data := send(https, "GET", path+"/endpoints", "", true)
	var endpoints cluster.EndpointInspection
	if status != http.StatusOK || json.Unmarshal(data, &endpoints) != nil || endpoints.ClusterID != admitted.ClusterID || endpoints.UnavailableReason != "not_allocated" || endpoints.Endpoints == nil || endpoints.Engine != cluster.PostgreSQL || endpoints.DesiredRevision != 1 || endpoints.WriterURL != "" || endpoints.CACertificatePEM != "" {
		t.Fatalf("endpoints: %d %s", status, data)
	}
	status, _, data = send(https, "GET", path+"/credentials", "", true)
	var inspection cluster.CredentialInspection
	if status != http.StatusOK || json.Unmarshal(data, &inspection) != nil || inspection.ClusterID != admitted.ClusterID || len(inspection.Credentials) != 1 || inspection.Credentials[0].CredentialID != credential.CredentialID || inspection.Credentials[0].Purpose != "superuser" || inspection.Credentials[0].Username != "synod_admin" {
		t.Fatalf("credential metadata: %d %s", status, data)
	}
	if bytes.Contains(data, []byte(password)) || bytes.Contains(data, []byte("password")) || bytes.Contains(data, []byte("sealed")) || bytes.Contains(data, []byte(keyID.String())) {
		t.Fatal("ordinary inspection exposed secret material")
	}
	for _, request := range []struct {
		method, path, body string
		authorized         bool
		want               int
	}{
		{"GET", path + "/endpoints", "", false, 401},
		{"GET", path + "/credentials", "", false, 401},
		{"POST", revealPath, `{}`, false, 401},
		{"GET", revealPath, "", true, 405},
		{"HEAD", revealPath, "", true, 405},
		{"POST", revealPath, `null`, true, 400},
		{"POST", revealPath, `[]`, true, 400},
		{"POST", revealPath, `{"password":"private-credential-value-do-not-log"}`, true, 400},
		{"POST", revealPath, `{} {}`, true, 400},
		{"POST", revealPath, "", true, 400},
		{"POST", path + "/credentials/application/reveal", `{}`, true, 404},
		{"POST", path + "/credentials/invalid/reveal", `{}`, true, 400},
		{"GET", "/v1/clusters/not-an-id/credentials", "", true, 400},
		{"GET", "/v1/clusters/" + uuid.NewString() + "/endpoints", "", true, 404},
	} {
		if status, _, data := send(https, request.method, request.path, request.body, request.authorized); status != request.want {
			t.Fatalf("%s %s: %d want %d %s", request.method, request.path, status, request.want, data)
		}
	}
	var count int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refused request authorized disclosure: count=%d err=%v", count, err)
	}
	status, headers, data := send(https, "POST", revealPath, `{}`, true)
	var revealed cluster.RevealedCredential
	if status != http.StatusOK || json.Unmarshal(data, &revealed) != nil || revealed.Password != password || revealed.Credential.CredentialID != credential.CredentialID || revealed.Credential.ClusterID != admitted.ClusterID || revealed.Credential.Version != 1 {
		t.Fatalf("reveal returned incorrect credential: status=%d", status)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted' AND cluster_id=$1 AND actor_id='test-admin' AND request_id=$2 AND credential_id=$3 AND credential_version=1 AND operation_id IS NULL`, admitted.ClusterID, headers.Get("X-Request-ID"), credential.CredentialID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("response preceded committed audit: count=%d err=%v", count, err)
	}
	if status, _, _ := send(makeServer(nil, rate.NewLimiter(rate.Inf, 1)), "POST", revealPath, `{}`, true); status != 503 {
		t.Fatalf("missing keyring: %d", status)
	}
	wrongKey, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: bytes.Repeat([]byte{9}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if status, _, _ := send(makeServer(wrongKey, rate.NewLimiter(rate.Inf, 1)), "POST", revealPath, `{}`, true); status != 503 {
		t.Fatalf("wrong key: %d", status)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("decryption ran before access audit: count=%d err=%v", count, err)
	}
	// A row copied from another owner must not decrypt with the requested cluster binding.
	otherBinding := binding
	otherBinding.OwnerID = uuid.New()
	moved, err := ring.Seal(otherBinding, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE cluster_credentials SET sealed=$1 WHERE credential_id=$2`, moved.Sealed, credential.CredentialID); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := send(https, "POST", revealPath, `{}`, true); status != 503 {
		t.Fatalf("wrong owner binding: %d", status)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE cluster_credentials SET sealed=$1 WHERE credential_id=$2`, credential.Envelope.Sealed, credential.CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `CREATE FUNCTION refuse_reveal_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='credential.reveal.accepted' THEN RAISE EXCEPTION 'private-audit-detail'; END IF; RETURN NEW; END $$; CREATE TRIGGER refuse_reveal BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION refuse_reveal_audit()`); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := send(https, "POST", revealPath, `{}`, true); status != 503 {
		t.Fatalf("audit failure: %d", status)
	}
	if _, err := pool.Exec(t.Context(), `DROP TRIGGER refuse_reveal ON audit_events`); err != nil {
		t.Fatal(err)
	}
	// A deferred failure reaches the uncertain COMMIT branch without a test-only Store seam.
	if _, err := pool.Exec(t.Context(), `CREATE CONSTRAINT TRIGGER refuse_reveal_commit AFTER INSERT ON audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse_reveal_audit()`); err != nil {
		t.Fatal(err)
	}
	if status, _, data := send(https, "POST", revealPath, `{}`, true); status != 503 || !bytes.Contains(data, []byte(`"code":"outcome_unknown"`)) || bytes.Contains(bytes.ToLower(data), []byte("retry")) || !bytes.Contains(data, []byte("No credential was returned.")) {
		t.Fatalf("uncertain reveal authorization returned unsafe guidance: status=%d", status)
	}
	if _, err := pool.Exec(t.Context(), `DROP TRIGGER refuse_reveal_commit ON audit_events`); err != nil {
		t.Fatal(err)
	}
	limited := makeServer(ring, rate.NewLimiter(0, 1))
	if status, _, _ := send(limited, "POST", revealPath, `{}`, true); status != 200 {
		t.Fatalf("first limited request: %d", status)
	}
	if status, headers, _ := send(limited, "POST", revealPath, `{}`, true); status != 429 || headers.Get("Retry-After") != "1" {
		t.Fatalf("reveal rate limit: %d %v", status, headers)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("failed or rate-limited request added audit: count=%d err=%v", count, err)
	}
	if status, _, _ := send(limited, "GET", path+"/credentials", "", true); status != 200 {
		t.Fatalf("reveal budget blocked metadata read: %d", status)
	}
	var revision int64
	if err := pool.QueryRow(t.Context(), `SELECT desired_revision FROM clusters WHERE id=$1`, admitted.ClusterID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("reveal changed desired state: %d %v", revision, err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM operations WHERE cluster_id=$1`, admitted.ClusterID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("reveal created host work: %d %v", count, err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(logData, []byte(password)) || bytes.Contains(logData, []byte("private-audit-detail")) || bytes.Contains(logData, []byte(keyID.String())) {
		t.Fatal("API logs exposed secret material")
	}
}

func TestCredentialRevealRequiresBrowserOrigin(t *testing.T) {
	server, _, _ := identityServer(t)
	createOperator(t, server, "operator")
	cookie := sessionCookie(t, server, "operator", operatorPassword)
	path := "/v1/clusters/" + uuid.NewString() + "/credentials/superuser/reveal"
	for _, proof := range []func(*http.Request){func(*http.Request) {}, func(r *http.Request) {
		r.Header.Set("Origin", "https://other.example")
		r.Header.Set("Sec-Fetch-Site", "cross-site")
	}} {
		rec := call(t, server, "POST", path, `{}`, withCookie(cookie), proof)
		if rec.Code != http.StatusForbidden || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("unproved origin: %d %s", rec.Code, rec.Body)
		}
	}
	rec := call(t, server, "POST", path, `{}`, withCookie(cookie), sameOriginFetch)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"dependency_unavailable"`) {
		t.Fatalf("proved origin did not reach keyring check: %d %s", rec.Code, rec.Body)
	}
}
