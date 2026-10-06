package api

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store"
	"golang.org/x/time/rate"
)

func TestHTTPSEnrollment(t *testing.T) {
	metadata, pool := apiTestDatabase(t)
	installation, node, requestID := uuid.New(), uuid.New(), uuid.New()
	adminToken := strings.Repeat("admin-secret-", 4)
	admin, err := auth.NewAdminToken(adminToken, "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := pki.CreateCA(installation, time.Now(), 60*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server, err := NewServer(metadata, admin, installation, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server.EnrollmentAuthority = ca
	https := httptest.NewTLSServer(server)
	defer https.Close()
	send := func(path, body, token, idempotency string, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), "POST", https.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if idempotency != "" {
			req.Header.Set("Idempotency-Key", idempotency)
		}
		resp, err := https.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != want {
			t.Fatalf("%s status=%d want=%d: %s", path, resp.StatusCode, want, data)
		}
		if resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store")
		}
		if token != "" && bytes.Contains(data, []byte(token)) {
			t.Fatal("response exposed token")
		}
		return data
	}
	rawToken := make([]byte, 32)
	if _, err := rand.Read(rawToken); err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(rawToken)
	registration, _ := json.Marshal(map[string]any{"node_id": node, "token": token, "ttl_seconds": 600})
	send("/v1/enrollment-tokens", string(registration), token, "register", 401)
	data := send("/v1/enrollment-tokens", string(registration), adminToken, "register", 201)
	var registered store.EnrollmentToken
	if err := json.Unmarshal(data, &registered); err != nil {
		t.Fatal(err)
	}
	data = send("/v1/enrollment-tokens", string(registration), adminToken, "register", 201)
	var replayToken store.EnrollmentToken
	if err := json.Unmarshal(data, &replayToken); err != nil {
		t.Fatal(err)
	}
	if replayToken.ID != registered.ID || !replayToken.Replayed {
		t.Fatal("registration replay changed identity")
	}
	send("/v1/enrollment-tokens", strings.Replace(string(registration), "600", "601", 1), adminToken, "register", 409)
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	enrollment, _ := json.Marshal(map[string]any{"node_id": node, "request_id": requestID, "csr_der": csr})
	send("/v1/enrollments", string(enrollment), "", "", 401)
	send("/v1/enrollments", string(enrollment), adminToken, "", 401)
	send("/v1/enrollments", string(enrollment), base64.RawURLEncoding.EncodeToString(make([]byte, 32)), "", 401)
	send("/v1/enrollments", `{"unknown":"private-input"}`, token, "", 400)
	send("/v1/enrollments", string(enrollment)+strings.Repeat(" ", 65536), token, "", 413)
	data = send("/v1/enrollments", string(enrollment), token, "", 201)
	var first store.EnrollmentResult
	if err := json.Unmarshal(data, &first); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(first.CertificatePEM))
	if block == nil {
		t.Fatal("missing certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.CheckAgentCertificate(t.Context(), node, leaf); err != nil {
		t.Fatal(err)
	}
	if first.CACertificatePEM != string(cert) || first.Replayed {
		t.Fatal("incorrect initial CA or replay flag")
	}
	if _, err := pool.ExecContext(t.Context(), "UPDATE enrollment_tokens SET created_at=created_at-120000, expires_at=created_at-119000 WHERE id=?", registered.ID); err != nil {
		t.Fatal(err)
	}
	data = send("/v1/enrollments", string(enrollment), token, "", 201)
	var replay store.EnrollmentResult
	if err := json.Unmarshal(data, &replay); err != nil {
		t.Fatal(err)
	}
	if replay.CertificatePEM != first.CertificatePEM || replay.CACertificatePEM != first.CACertificatePEM || !replay.Replayed || !replay.AcceptedAt.Equal(first.AcceptedAt) {
		t.Fatal("enrollment replay changed result")
	}
	send("/v1/enrollments", strings.Replace(string(enrollment), requestID.String(), uuid.NewString(), 1), token, "", 409)
	revoke := "/v1/nodes/" + node.String() + "/certificates/" + first.CertificateID.String() + "/actions/revoke"
	send(revoke, "{}", token, "", 401)
	send(revoke, "null", adminToken, "", 400)
	send(revoke, "{}", adminToken, "", 204)
	send(revoke, "{}", adminToken, "", 204)
	if err := metadata.CheckAgentCertificate(t.Context(), node, leaf); err == nil {
		t.Fatal("revoked certificate accepted")
	}
	var audits int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM audit_events WHERE action='node.certificate.revoked'").Scan(&audits); err != nil || audits != 1 {
		t.Fatalf("revocation audits=%d err=%v", audits, err)
	}
	// Expiry before initial consumption rejects the request and leaves no node.
	node2 := uuid.New()
	rawToken[0] ^= 1
	token2 := base64.RawURLEncoding.EncodeToString(rawToken)
	registration2, _ := json.Marshal(map[string]any{"node_id": node2, "token": token2, "ttl_seconds": 60})
	send("/v1/enrollment-tokens", string(registration2), adminToken, "register2", 201)
	noIssuer, err := NewServer(metadata, admin, installation, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	missingCARequest := httptest.NewRequest("POST", "/v1/enrollments", strings.NewReader(strings.Replace(string(enrollment), node.String(), node2.String(), 1)))
	missingCARequest.Header.Set("Authorization", "Bearer "+token2)
	missingCARequest.Header.Set("Content-Type", "application/json")
	missingCAResponse := httptest.NewRecorder()
	noIssuer.ServeHTTP(missingCAResponse, missingCARequest)
	if missingCAResponse.Code != 503 {
		t.Fatalf("issuance without CA: %d %s", missingCAResponse.Code, missingCAResponse.Body)
	}
	var nodeCount int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM nodes WHERE id=?", node2).Scan(&nodeCount); err != nil || nodeCount != 0 {
		t.Fatalf("failed issuance wrote node: %d %v", nodeCount, err)
	}

	if _, err := pool.ExecContext(t.Context(), "UPDATE enrollment_tokens SET created_at=created_at-120000, expires_at=created_at-119000 WHERE node_id=?", node2); err != nil {
		t.Fatal(err)
	}
	send("/v1/enrollments", strings.Replace(string(enrollment), node.String(), node2.String(), 1), token2, "", 401)
	if strings.Contains(logs.String(), token) || strings.Contains(logs.String(), adminToken) || strings.Contains(logs.String(), "private-input") {
		t.Fatal("logs exposed sensitive input")
	}
	// The security protocol can recover committed work without the signing key.
	replayServer, err := NewServer(metadata, admin, installation, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/enrollments", bytes.NewReader(enrollment))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	replayServer.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("replay without CA: %d %s", rec.Code, rec.Body)
	}
}

func TestEnrollmentAuthenticationAndRateLimits(t *testing.T) {
	admin, err := auth.NewAdminToken(strings.Repeat("a", 32), "test-admin")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(apiTestStore(t), admin, uuid.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/enrollment-tokens", "/v1/enrollments?token=" + strings.Repeat("a", 43), "/v1/nodes/" + uuid.NewString() + "/actions/approve"} {
		req := httptest.NewRequest("POST", path, strings.NewReader("invalid"))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Fatalf("unauthenticated %s=%d", path, rec.Code)
		}
	}
	// One path serves unauthenticated enrollment. A variant is a different endpoint,
	// and the router decides that now, not a string comparison in the request path.
	variant := httptest.NewRecorder()
	server.ServeHTTP(variant, httptest.NewRequest("POST", "/v1/enrollments/", strings.NewReader("invalid")))
	if variant.Code != 404 {
		t.Fatalf("a path variant reached a handler: %d %s", variant.Code, variant.Body)
	}
	req := httptest.NewRequest("POST", "/v1/enrollments", strings.NewReader("invalid"))
	req.Header.Add("Authorization", "Bearer "+strings.Repeat("a", 43))
	req.Header.Add("Authorization", "Bearer "+strings.Repeat("a", 43))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatal("accepted duplicate authorization")
	}
	for _, enrollment := range []bool{false, true} {
		if enrollment {
			server.enrollmentLimit = rate.NewLimiter(0, 0)
		} else {
			server.requestLimit = rate.NewLimiter(0, 0)
		}
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/enrollments", nil))
		if rec.Code != 429 || rec.Header().Get("Retry-After") != "1" {
			t.Fatal("missing bounded rate rejection")
		}
		server.requestLimit = rate.NewLimiter(100, 200)
	}
}
