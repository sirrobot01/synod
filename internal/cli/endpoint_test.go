package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestCreatePatroniWithLoadBalancerEndpoint(t *testing.T) {
	var sent atomic.Pointer[cluster.CreateSpec]
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var spec cluster.CreateSpec
		if r.Method != http.MethodPost || r.URL.Path != "/v1/clusters" || json.NewDecoder(r.Body).Decode(&spec) != nil {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		sent.Store(&spec)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(client.Admission{ClusterID: uuid.New(), OperationID: uuid.New()})
	}))
	base := append(flags, "db", "create", "postgres", "--name", "ha", "--version", "17.11", "--topology", "patroni", "--failure-domain", "host")
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(base, "--endpoint-address", "192.0.2.10", "--endpoint-writer-port", "5432", "--endpoint-reader-port", "5433"), &stdout, &stderr); code != 0 {
		t.Fatalf("create with endpoint: code=%d out=%s err=%s", code, stdout.String(), stderr.String())
	}
	if spec := sent.Load(); spec == nil || spec.PostgreSQL.Endpoint == nil || *spec.PostgreSQL.Endpoint != (cluster.PostgresEndpoint{Address: "192.0.2.10", WriterPort: 5432, ReaderPort: 5433}) {
		t.Fatalf("sent spec: %+v", spec)
	}
	sent.Store(nil)
	if code := cli.Run(t.Context(), append(base, "--endpoint-address", "192.0.2.10"), &stdout, &stderr); code == 0 || sent.Load() != nil {
		t.Fatal("a partial endpoint was sent")
	}
}
