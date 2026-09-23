package client

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestConnectionReadsUseTypedPasswordFreeMetadata(t *testing.T) {
	id := uuid.New()
	metadata := cluster.CredentialMetadata{CredentialID: uuid.New(), ClusterID: id, Version: 2, Purpose: "superuser", Username: "synod_admin"}
	for _, engine := range []cluster.Engine{cluster.PostgreSQL, cluster.Redis} {
		t.Run(string(engine), func(t *testing.T) {
			database := "app"
			if engine == cluster.Redis {
				database = "0"
			}
			endpoint := cluster.Endpoint{ID: uuid.New(), NodeID: uuid.New(), Host: "2001:db8::8", Port: 16379, Transport: "tcp", Database: database, ClientMode: "direct", AvailabilityProfile: "single_node", TLSMode: "disabled"}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("inspection did not use authenticated GET")
				}
				switch r.URL.Path {
				case "/v1/clusters/" + id.String() + "/endpoints":
					json.NewEncoder(w).Encode(cluster.EndpointInspection{ClusterID: id, Engine: engine, DesiredRevision: 3, ObservedRevision: 2, DesiredLifecycle: cluster.DesiredStopped, ObservedLifecycle: cluster.ObservedRunning, Endpoints: []cluster.Endpoint{endpoint}})
				case "/v1/clusters/" + id.String() + "/credentials":
					json.NewEncoder(w).Encode(struct {
						cluster.CredentialInspection
						Password string `json:"password"`
					}{CredentialInspection: cluster.CredentialInspection{ClusterID: id, Credentials: []cluster.CredentialMetadata{metadata}}, Password: "must-not-reach-normal-output"})
				default:
					t.Error("unexpected connection route")
				}
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 3}
			endpoints, err := api.GetEndpoints(t.Context(), id)
			if err != nil || len(endpoints.Endpoints) != 1 || endpoints.Endpoints[0] != endpoint {
				t.Fatalf("endpoint inspection failed: %v", err)
			}
			credentials, err := api.ListCredentials(t.Context(), id)
			if err != nil || len(credentials.Credentials) != 1 || credentials.Credentials[0] != metadata {
				t.Fatalf("credential inspection failed: %v", err)
			}
			encoded, err := json.Marshal(credentials)
			if err != nil || strings.Contains(string(encoded), "password") || strings.Contains(string(encoded), "must-not-reach") {
				t.Fatal("normal inspection retained secret fields")
			}
		})
	}
}

func TestEndpointInspectionRejectsUnprovenMetadata(t *testing.T) {
	id := uuid.New()
	for _, change := range []string{"target", "nil endpoints", "unknown reason", "revision", "lifecycle", "engine", "missing node", "wildcard", "loopback", "mapped", "wrong database", "wrong profile", "zero port", "two endpoints"} {
		t.Run(change, func(t *testing.T) {
			value := cluster.EndpointInspection{ClusterID: id, Engine: cluster.Redis, DesiredRevision: 1, DesiredLifecycle: cluster.DesiredRunning, ObservedLifecycle: cluster.ObservedProvisioning, Endpoints: []cluster.Endpoint{{ID: uuid.New(), NodeID: uuid.New(), Host: "192.0.2.8", Port: 16379, Transport: "tcp", Database: "0", ClientMode: "direct", AvailabilityProfile: "single_node", TLSMode: "disabled"}}}
			switch change {
			case "target":
				value.ClusterID = uuid.New()
			case "nil endpoints":
				value.Endpoints = nil
				value.UnavailableReason = "not_allocated"
			case "unknown reason":
				value.Endpoints = []cluster.Endpoint{}
				value.UnavailableReason = "healthy"
			case "revision":
				value.DesiredRevision = 0
			case "lifecycle":
				value.ObservedLifecycle = ""
			case "engine":
				value.Engine = "unknown"
			case "missing node":
				value.Endpoints[0].NodeID = uuid.Nil
			case "wildcard":
				value.Endpoints[0].Host = "0.0.0.0"
			case "loopback":
				value.Endpoints[0].Host = "::1"
			case "mapped":
				value.Endpoints[0].Host = "::ffff:192.0.2.8"
			case "wrong database":
				value.Endpoints[0].Database = "app"
			case "wrong profile":
				value.Endpoints[0].AvailabilityProfile = "ha"
			case "zero port":
				value.Endpoints[0].Port = 0
			case "two endpoints":
				value.Endpoints = append(value.Endpoints, value.Endpoints[0])
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(value) }))
			defer server.Close()
			api := &Client{endpoint: server.URL, http: server.Client(), attempts: 1}
			result, err := api.GetEndpoints(t.Context(), id)
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != "invalid_response" || result.ClusterID != uuid.Nil {
				t.Fatalf("unproven endpoint returned: %v", err)
			}
		})
	}
}

func TestCredentialInspectionAndRevealRejectInvalidMetadata(t *testing.T) {
	id := uuid.New()
	for _, change := range []string{"cluster", "credential", "version", "purpose", "username"} {
		t.Run(change, func(t *testing.T) {
			metadata := cluster.CredentialMetadata{CredentialID: uuid.New(), ClusterID: id, Version: 1, Purpose: "application", Username: "synod_app"}
			switch change {
			case "cluster":
				metadata.ClusterID = uuid.New()
			case "credential":
				metadata.CredentialID = uuid.Nil
			case "version":
				metadata.Version = 0
			case "purpose":
				metadata.Purpose = "unknown"
			case "username":
				metadata.Username = ""
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					json.NewEncoder(w).Encode(cluster.RevealedCredential{Credential: metadata, Password: "must-not-return-on-error"})
					return
				}
				json.NewEncoder(w).Encode(cluster.CredentialInspection{ClusterID: id, Credentials: []cluster.CredentialMetadata{metadata}})
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, http: server.Client(), attempts: 1}
			if _, err := api.ListCredentials(t.Context(), id); err == nil {
				t.Fatal("invalid credential metadata was accepted")
			}
			value, err := api.RevealCredential(t.Context(), id, "application")
			if err == nil || value.Password != "" {
				t.Fatal("invalid disclosure returned a password")
			}
		})
	}
}

func TestCredentialRevealDoesNotRetryFailures(t *testing.T) {
	for _, failure := range []string{"disconnect", "rate limit", "unavailable", "unknown authorization", "invalid JSON"} {
		t.Run(failure, func(t *testing.T) {
			id := uuid.New()
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodPost || r.URL.Path != "/v1/clusters/"+id.String()+"/credentials/superuser/reveal" || string(body) != "{}" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Idempotency-Key") != "" {
					t.Error("reveal sent an incorrect request")
				}
				switch failure {
				case "disconnect":
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					connection.Close()
				case "rate limit":
					w.WriteHeader(http.StatusTooManyRequests)
					io.WriteString(w, `{"code":"rate_limited","message":"private-secret"}`)
				case "unavailable":
					w.WriteHeader(http.StatusServiceUnavailable)
					io.WriteString(w, `{"code":"dependency_unavailable","message":"private-secret"}`)
				case "unknown authorization":
					w.WriteHeader(http.StatusServiceUnavailable)
					io.WriteString(w, `{"code":"outcome_unknown","message":"private-secret"}`)
				case "invalid JSON":
					io.WriteString(w, `{"password":"private-secret","credential":`)
				}
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, http: server.Client(), attempts: 5}
			value, err := api.RevealCredential(t.Context(), id, "superuser")
			if err == nil || value.Password != "" || strings.Contains(err.Error(), "private-secret") || requests.Load() != 1 {
				t.Fatalf("failed reveal leaked or repeated: requests=%d", requests.Load())
			}
			if failure == "rate limit" {
				apiError, ok := errors.AsType[*Error](err)
				if !ok || apiError.Code != "rate_limited" {
					t.Fatalf("rate limit code lost: %v", err)
				}
			}
			if failure == "unknown authorization" {
				apiError, ok := errors.AsType[*Error](err)
				if !ok || apiError.Code != "outcome_unknown" || apiError.Message != "Credential access authorization is uncertain. No password was returned." {
					t.Fatalf("unknown authorization has wrong guidance: %v", err)
				}
			}
		})
	}
}

func TestConnectionInspectionAcceptsEmptyProvisioningMetadata(t *testing.T) {
	id := uuid.New()
	for _, reason := range []string{"not_allocated", "client_address_required", "address_not_bound", "ambiguous_allocation"} {
		t.Run(reason, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/credentials") {
					json.NewEncoder(w).Encode(cluster.CredentialInspection{ClusterID: id, Credentials: []cluster.CredentialMetadata{}})
					return
				}
				json.NewEncoder(w).Encode(cluster.EndpointInspection{ClusterID: id, Engine: cluster.PostgreSQL, DesiredRevision: 1, DesiredLifecycle: cluster.DesiredRunning, ObservedLifecycle: cluster.ObservedProvisioning, Endpoints: []cluster.Endpoint{}, UnavailableReason: reason})
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, http: server.Client(), attempts: 1}
			endpoints, err := api.GetEndpoints(t.Context(), id)
			if err != nil || endpoints.Endpoints == nil || len(endpoints.Endpoints) != 0 || endpoints.UnavailableReason != reason {
				t.Fatalf("unavailable endpoints lost their reason: %v", err)
			}
			credentials, err := api.ListCredentials(t.Context(), id)
			if err != nil || credentials.Credentials == nil || len(credentials.Credentials) != 0 {
				t.Fatalf("empty credentials were manufactured or refused: %v", err)
			}
		})
	}
}

func TestCredentialInspectionRejectsInvalidCollections(t *testing.T) {
	id := uuid.New()
	metadata := cluster.CredentialMetadata{CredentialID: uuid.New(), ClusterID: id, Version: 1, Purpose: "superuser", Username: "synod_admin"}
	for _, value := range []cluster.CredentialInspection{
		{ClusterID: uuid.New(), Credentials: []cluster.CredentialMetadata{}},
		{ClusterID: id},
		{ClusterID: id, Credentials: []cluster.CredentialMetadata{metadata, metadata}},
	} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(value) }))
		t.Cleanup(server.Close)
		api := &Client{endpoint: server.URL, http: server.Client(), attempts: 1}
		if _, err := api.ListCredentials(t.Context(), id); err == nil {
			t.Fatal("invalid credential collection was accepted")
		}
	}
}
