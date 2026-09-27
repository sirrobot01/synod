package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	appmodel "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/cli"
)

func TestAppImportRetriesSameAdmission(t *testing.T) {
	calls := 0
	var first []byte
	id, operationID := uuid.New(), uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/v1/apps" || r.Header.Get("Idempotency-Key") != "stable-key" {
			t.Errorf("request: %s %s", r.Method, r.URL.Path)
		}
		var spec appmodel.AppSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			t.Error(err)
		}
		body, err := json.Marshal(spec)
		if err != nil {
			t.Error(err)
		}
		if calls == 1 {
			first = body
		} else if !bytes.Equal(first, body) {
			t.Error("retry body changed")
		}
		if spec.Name != "frontend" || spec.Copies != 3 || spec.Router.Port != 9090 || len(spec.DevicePreference.Preferred) != 2 {
			t.Errorf("spec: %+v", spec)
		}
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(appmodel.Admission{AppID: id, OperationID: operationID, DesiredRevision: 1, AcceptedAt: time.Now().UTC(), Replayed: true}); err != nil {
			t.Error(err)
		}
	}))
	filename := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(filename, []byte("services: {web: {image: nginx, ports: ['8080:80']}}"), 0600); err != nil {
		t.Fatal(err)
	}
	args := append(flags, "app", "import", "-f", filename, "web", "--name", "frontend", "--copies", "3", "--port", "9090", "--prefer", "server,odin", "--idempotency-key", "stable-key")
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), args, &stdout, &stderr)
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if code != 0 || calls != 2 || result.AppAdmission == nil || result.AppAdmission.AppID != id || result.AppAdmission.OperationID != operationID || !strings.Contains(stderr.String(), "stable-key") {
		t.Fatalf("code %d, calls %d: %s %s", code, calls, &stdout, &stderr)
	}
}

func TestAppImportRejectsBeforeSending(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid import sent to API") }))
	filename := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(filename, []byte("services: {web: {image: nginx, build: ., ports: ['8080:80']}}"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "app", "import", "-f", filename), &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "build") {
		t.Fatalf("result: %d %s %s", code, &stdout, &stderr)
	}
}

func TestAppImportSendsTier2Settings(t *testing.T) {
	var spec appmodel.AppSpec
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(appmodel.Admission{AppID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1, AcceptedAt: time.Now().UTC()}); err != nil {
			t.Error(err)
		}
	}))
	filename := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(filename, []byte("services: {sonarr: {image: sonarr, ports: ['8989:8989'], volumes: ['config:/config']}}\nvolumes: {config: }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := append(flags, "app", "import", "-f", filename, "--tier", "2", "--prefer", "mac", "--allow", "server", "--never", "odin", "--standbys", "2", "--interval", "30s", "--failback", "manual")
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("code %d: %s %s", code, &stdout, &stderr)
	}
	want := appmodel.ReplicationSpec{Standbys: 2, IntervalSeconds: 30, FailoverAfterSeconds: 90, Failback: appmodel.FailbackManual, FailbackStableSeconds: 600}
	if spec.Tier != appmodel.TierSingleWriter || spec.Replication == nil || *spec.Replication != want {
		t.Fatalf("spec: %+v %+v", spec, spec.Replication)
	}
	if p := spec.DevicePreference; len(p.Preferred) != 1 || p.Allowed[0] != "server" || p.Never[0] != "odin" {
		t.Fatalf("preference: %+v", p)
	}
	if len(spec.Service.Mounts) != 1 || spec.Service.Mounts[0].Type != "replicated" {
		t.Fatalf("mounts: %+v", spec.Service.Mounts)
	}
}

func TestAppImportRejectsTier1BeforeSending(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid import sent to API") }))
	filename := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(filename, []byte("services: {web: {image: nginx, ports: ['8080:80']}}"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "app", "import", "-f", filename, "--tier", "1"), &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "tier 2 and tier 3") {
		t.Fatalf("result: %d %s %s", code, &stdout, &stderr)
	}
}

func TestAppImportRefusesZeroStandbysBeforeSending(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid import sent to API") }))
	filename := filepath.Join(t.TempDir(), "compose.yml")
	if err := os.WriteFile(filename, []byte("services: {sonarr: {image: sonarr, ports: ['8989:8989'], volumes: ['config:/config']}}\nvolumes: {config: }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "app", "import", "-f", filename, "--tier", "2", "--standbys", "0"), &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "1 to 3 standbys") {
		t.Fatalf("result: %d %s %s", code, &stdout, &stderr)
	}
}
