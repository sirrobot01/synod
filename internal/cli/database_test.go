package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	appmodel "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/cli"
)

func TestDatabaseCreateSendsATier1App(t *testing.T) {
	var spec appmodel.AppSpec
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/apps" {
			t.Errorf("request sent to %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(appmodel.Admission{AppID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1, AcceptedAt: time.Now().UTC()}); err != nil {
			t.Error(err)
		}
	}))
	args := append(flags, "db", "create", "photos-db", "--version", "17", "--standbys", "2", "--prefer", "server", "--allow", "odin", "--writer-port", "5432", "--reader-port", "5433", "--failover-after", "2m")
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("code %d: %s %s", code, &stdout, &stderr)
	}
	want := appmodel.ReplicationSpec{Standbys: 2, FailoverAfterSeconds: 120, Failback: appmodel.FailbackManual, FailbackStableSeconds: 600}
	if spec.Tier != appmodel.TierDatabase || spec.Replication == nil || *spec.Replication != want || spec.Service.Image != "" {
		t.Fatalf("spec: %+v %+v", spec, spec.Replication)
	}
	if spec.Database == nil || spec.Database.Engine != "postgres" || spec.Database.Version != "17" || len(spec.Database.Databases) != 1 || spec.Database.Databases[0] != "photos_db" {
		t.Fatalf("database: %+v", spec.Database)
	}
	if spec.Router != (appmodel.RouterSpec{Protocol: "tcp", Port: 5432, ReaderPort: 5433}) || spec.DevicePreference.Preferred[0] != "server" || spec.DevicePreference.Allowed[0] != "odin" {
		t.Fatalf("routes or preference: %+v %+v", spec.Router, spec.DevicePreference)
	}
}

func TestDatabaseCreateRefusesBeforeSending(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid database sent to API") }))
	for name, extra := range map[string][]string{
		"four standbys":     {"--standbys", "4"},
		"redis database":    {"--engine", "redis", "--database", "cache"},
		"same ports":        {"--reader-port", "5432"},
		"fractional period": {"--failover-after", "90500ms"},
		"unknown engine":    {"--engine", "mysql"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(append(append([]string{}, flags...), "db", "create", "orders", "--version", "17", "--writer-port", "5432"), extra...)
			if code := cli.Run(t.Context(), args, &stdout, &stderr); code == 0 || !strings.Contains(stderr.String(), "invalid_request") {
				t.Fatalf("result: %d %s %s", code, &stdout, &stderr)
			}
		})
	}
}

func TestDatabaseSwitchoverResolvesNamesAndKeepsRetryIdentity(t *testing.T) {
	id, node, op := uuid.New(), uuid.New(), uuid.New()
	calls := 0
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/apps/by-name/photos-db":
			json.NewEncoder(w).Encode(appmodel.App{ID: id, Name: "photos-db", Tier: appmodel.TierDatabase, DesiredRevision: 12, Status: &appmodel.Status{OperationID: uuid.New()}})
		case "/v1/nodes/by-name/odin":
			json.NewEncoder(w).Encode(map[string]string{"node_id": node.String()})
		case "/v1/apps/" + id.String() + "/move":
			calls++
			var body struct {
				TargetNodeID uuid.UUID `json:"target_node_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TargetNodeID != node || r.Header.Get("If-Match") != `"7"` || r.Header.Get("Idempotency-Key") != "same-key" {
				t.Errorf("request: %v %v %v", body, err, r.Header)
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(appmodel.Admission{AppID: id, OperationID: op, DesiredRevision: 8, AcceptedAt: time.Now(), Replayed: true})
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	var stdout, stderr bytes.Buffer
	args := append(flags, "db", "switchover", "photos-db", "--to", "odin", "--expected-revision", "7", "--idempotency-key", "same-key")
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 || calls != 1 {
		t.Fatalf("code %d calls %d: %s %s", code, calls, &stdout, &stderr)
	}
}

func TestDatabaseSwitchoverRefusesANonDatabase(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/v1/apps/by-name/web" {
			t.Errorf("unexpected action: %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(appmodel.App{ID: uuid.New(), Name: "web", Tier: appmodel.TierParallel, DesiredRevision: 1, Status: &appmodel.Status{OperationID: uuid.New()}})
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "switchover", "web", "--to", "odin"), &stdout, &stderr); code == 0 || !strings.Contains(stderr.String(), "Switchover requires a database") {
		t.Fatalf("code %d: %s %s", code, &stdout, &stderr)
	}
}

func TestDatabaseCredentialsRevealsLoginAndRoutes(t *testing.T) {
	id, credential := uuid.New(), uuid.New()
	node := uuid.New()
	var reveals int
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/by-name/photos-db":
			item := appmodel.App{ID: id, Name: "photos-db", Tier: appmodel.TierDatabase, DesiredRevision: 1,
				Spec:   appmodel.AppSpec{Router: appmodel.RouterSpec{Protocol: "tcp", Port: 5432, ReaderPort: 5433}, Database: &appmodel.DatabaseSpec{Engine: "postgres", Version: "17", Databases: []string{"photos"}}},
				Status: &appmodel.Status{OperationID: uuid.New(), Endpoints: []appmodel.Endpoint{{NodeID: node, Address: "192.0.2.10:5432"}}}}
			json.NewEncoder(w).Encode(item)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/"+id.String()+"/credentials/reveal":
			reveals++
			if r.Header.Get("Idempotency-Key") != "" {
				t.Error("a reveal carried a retry key")
			}
			json.NewEncoder(w).Encode(appmodel.Credential{AppID: id, CredentialID: credential, Version: 1, Username: "app", Password: "p@ss/word"})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	var human []string
	for _, flag := range flags {
		if flag != "--json" {
			human = append(human, flag)
		}
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(human, "db", "credentials", "photos-db"), &stdout, &stderr); code != 0 {
		t.Fatalf("code %d: %s %s", code, &stdout, &stderr)
	}
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.DatabaseCredential == nil || reveals != 1 {
		t.Fatalf("credentials are always JSON: %v %s", err, &stdout)
	}
	want := []string{
		"postgresql://app:p%40ss%2Fword@192.0.2.10:5432/photos?sslmode=verify-ca",
		"postgresql://app:p%40ss%2Fword@192.0.2.10:5433/photos?sslmode=verify-ca",
	}
	if !slices.Equal(result.DatabaseCredential.URLs, want) || result.DatabaseCredential.Password != "p@ss/word" {
		t.Fatalf("urls: %q", result.DatabaseCredential.URLs)
	}
	if !strings.Contains(stderr.String(), "Protect stdout") {
		t.Fatalf("stderr=%q, want the protected-output warning", &stderr)
	}
}

func TestDatabaseCredentialsRefusesANonDatabase(t *testing.T) {
	id := uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("a non-database reached %s %s", r.Method, r.URL.Path)
		}
		json.NewEncoder(w).Encode(appmodel.App{ID: id, Name: "web", Tier: appmodel.TierParallel, Status: &appmodel.Status{OperationID: uuid.New()}})
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "credentials", id.String()), &stdout, &stderr); code == 0 || !strings.Contains(stderr.String(), "invalid_request") {
		t.Fatalf("code %d: %s %s", code, &stdout, &stderr)
	}
}
