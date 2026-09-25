package cli_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestCredentialRevealDisclosesGuidanceBeforeRequest(t *testing.T) {
	id := uuid.New()
	value := cluster.RevealedCredential{Credential: cluster.CredentialMetadata{CredentialID: uuid.New(), ClusterID: id, Version: 3, Purpose: "superuser", Username: "synod_admin"}, Password: "exact-private-database-password"}
	var diagnostics actionDiagnostics
	var requests atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		diagnostics.Lock()
		text := diagnostics.buffer.String()
		diagnostics.Unlock()
		for _, want := range []string{"password", "Protect stdout", "private terminal", "logs"} {
			if !strings.Contains(text, want) {
				t.Errorf("reveal preceded guidance: %q", want)
			}
		}
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/clusters/"+id.String()+"/credentials/superuser/reveal" || string(body) != "{}" {
			t.Error("reveal sent an incorrect request")
		}
		json.NewEncoder(w).Encode(value)
	}))
	var stdout bytes.Buffer
	// Connection commands keep one JSON document even without the global flag.
	code := cli.Run(t.Context(), append(flags[1:], "credential", "reveal", id.String(), "--purpose", "superuser"), &stdout, &diagnostics)
	var result cli.Result
	decoder := json.NewDecoder(&stdout)
	if err := decoder.Decode(&result); err != nil || code != 0 || result.RevealedCredential == nil || *result.RevealedCredential != value || requests.Load() != 1 {
		t.Fatalf("credential reveal failed: code=%d decode=%v", code, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatal("reveal stdout contained more than one JSON result")
	}
	diagnostics.Lock()
	defer diagnostics.Unlock()
	if strings.Contains(diagnostics.buffer.String(), value.Password) {
		t.Fatal("password leaked to diagnostics")
	}
}

type unavailableDiagnostics struct{}

func (unavailableDiagnostics) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCredentialRevealRequiresPurposeAndWritableGuidance(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
		fail  bool
	}{
		{name: "missing purpose"},
		{name: "empty purpose", flags: []string{"--purpose", ""}},
		{name: "unknown purpose", flags: []string{"--purpose", "admin"}},
		{name: "guidance failure", flags: []string{"--purpose", "application"}, fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
			var stdout, stderr bytes.Buffer
			var diagnostics io.Writer = &stderr
			if test.fail {
				diagnostics = unavailableDiagnostics{}
			}
			args := append(flags[1:], "credential", "reveal", uuid.NewString())
			code := cli.Run(t.Context(), append(args, test.flags...), &stdout, diagnostics)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.RevealedCredential != nil || requests.Load() != 0 {
				t.Fatalf("unconfirmed reveal reached API: code=%d requests=%d", code, requests.Load())
			}
			want := "invalid_request"
			if test.fail {
				want = "local_error"
			}
			if result.Error.Code != want {
				t.Fatalf("failure code=%s, want %s", result.Error.Code, want)
			}
		})
	}
}

func TestConnectionFailuresRemainJSONWithoutOutputFlag(t *testing.T) {
	for _, args := range [][]string{
		{"endpoint", "get", uuid.NewString()},
		{"credential", "list", uuid.NewString()},
		{"credential", "reveal", uuid.NewString(), "--purpose", "application"},
		{"endpoint", "get"},
		{"credential", "reveal", "not-a-uuid", "--purpose", "superuser"},
	} {
		flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":"not_found","message":"private-server-detail"}`)
		}))
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(flags[1:], args...), &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.RevealedCredential != nil || strings.Contains(stdout.String()+stderr.String(), "private-server-detail") {
			t.Fatalf("connection failure lost JSON or leaked detail: command=%v code=%d decode=%v", args, code, err)
		}
	}
}

func TestConnectionInspectionExcludesPasswordsAndWarnsAboutPlaintext(t *testing.T) {
	id := uuid.New()
	for _, action := range []string{"endpoint", "credential"} {
		t.Run(action, func(t *testing.T) {
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("normal inspection tried to reveal a secret")
				}
				if action == "endpoint" {
					if r.URL.Path != "/v1/clusters/"+id.String()+"/endpoints" {
						t.Error("wrong endpoint inspection route")
					}
					json.NewEncoder(w).Encode(cluster.EndpointInspection{ClusterID: id, Engine: cluster.PostgreSQL, DesiredRevision: 4, ObservedRevision: 3, DesiredLifecycle: cluster.DesiredStopped, ObservedLifecycle: cluster.ObservedRunning, Endpoints: []cluster.Endpoint{{ID: uuid.New(), NodeID: uuid.New(), Host: "192.0.2.8", Port: 15432, Transport: "tcp", Database: "app", ClientMode: "direct", AvailabilityProfile: "single_node", TLSMode: "disabled"}}})
					return
				}
				if r.URL.Path != "/v1/clusters/"+id.String()+"/credentials" {
					t.Error("wrong credential inspection route")
				}
				json.NewEncoder(w).Encode(cluster.CredentialInspection{ClusterID: id, Credentials: []cluster.CredentialMetadata{{CredentialID: uuid.New(), ClusterID: id, Version: 1, Purpose: "superuser", Username: "synod_admin"}}})
			}))
			var stdout, stderr bytes.Buffer
			verb := "get"
			if action == "credential" {
				verb = "list"
			}
			code := cli.Run(t.Context(), append(flags[1:], action, verb, id.String()), &stdout, &stderr)
			var result cli.Result
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 0 || result.RevealedCredential != nil || strings.Contains(stdout.String(), "password") {
				t.Fatalf("normal inspect failed: code=%d decode=%v", code, err)
			}
			if action == "endpoint" {
				if result.EndpointInspection == nil || result.EndpointInspection.DesiredRevision != 4 || !strings.Contains(stderr.String(), "plaintext") || !strings.Contains(stderr.String(), "TLS is disabled") || !strings.Contains(stderr.String(), "not a live health check") {
					t.Fatal("endpoint omitted lifecycle metadata or the plaintext warning")
				}
			} else if result.CredentialInspection == nil || len(result.CredentialInspection.Credentials) != 1 || result.CredentialInspection.Credentials[0].Purpose != "superuser" {
				t.Fatal("credential list lost metadata or manufactured an application role")
			}
		})
	}
}

func TestNodeApproveSendsDeclaredClientAddress(t *testing.T) {
	id := uuid.New()
	var requests atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var policy client.NodePolicy
		if err := json.NewDecoder(r.Body).Decode(&policy); err != nil || policy.ClientAddress != "2001:db8::8" || r.Method != http.MethodPut || r.URL.Path != "/v1/nodes/"+id.String()+"/placement-policy" || r.Header.Get("If-Match") != `"0"` {
			t.Error("node policy lost its declared client address")
		}
		json.NewEncoder(w).Encode(client.NodePolicyResult{NodeID: id, Revision: 1})
	}))
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags, "node", "approve", "--node-id", id.String(), "--architecture", "arm64", "--runtime-version", "29.7.2", "--storage-id", "local", "--engine", "redis:8.2.7", "--client-address", "2001:db8::8"), &stdout, &stderr)
	if code != 0 || requests.Load() != 1 {
		t.Fatalf("node approve failed: code=%d diagnostics=%s", code, stderr.String())
	}
}

// Only an endpoint without verify-full TLS earns the plaintext warning.
func TestEndpointWarnsOnlyForPlaintext(t *testing.T) {
	id := uuid.New()
	routed := cluster.EndpointInspection{ClusterID: id, Engine: cluster.PostgreSQL, DesiredRevision: 1, DesiredLifecycle: cluster.DesiredRunning, ObservedLifecycle: cluster.ObservedRunning,
		CACertificatePEM: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"}
	var writers, readers []string
	for index, host := range []string{"192.0.2.10", "192.0.2.11", "2001:db8::12"} {
		resource, node := uuid.New(), uuid.New()
		for position, role := range []string{"writer", "reader"} {
			port := 6000 + 10*index + position
			routed.Endpoints = append(routed.Endpoints, cluster.Endpoint{ID: resource, NodeID: node, Host: host, Port: uint16(port), Transport: "tcp", Database: "app", ClientMode: "routed", AvailabilityProfile: "patroni_routed", TLSMode: "verify-full", Role: role})
			hostPort := net.JoinHostPort(host, strconv.Itoa(port))
			if role == "writer" {
				writers = append(writers, hostPort)
			} else {
				readers = append(readers, hostPort)
			}
		}
	}
	routed.WriterURL = "postgresql://" + strings.Join(writers, ",") + "/app?target_session_attrs=read-write&sslmode=verify-full"
	routed.ReaderURL = "postgresql://" + strings.Join(readers, ",") + "/app?load_balance_hosts=random&sslmode=verify-full"
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(routed) }))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "endpoint", "get", id.String()), &stdout, &stderr); code != 0 {
		t.Fatalf("endpoint get: code=%d err=%s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "plaintext") || !strings.Contains(stderr.String(), "not a live health check") {
		t.Fatalf("routed verify-full endpoint diagnostics: %q", stderr.String())
	}
}
