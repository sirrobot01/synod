package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestCreateDisclosesAndSendsPostgresReplicationPolicy(t *testing.T) {
	for _, test := range []struct {
		name, topology, policy, notice string
		sent                           bool
	}{
		{name: "standalone", topology: "standalone", sent: true},
		{name: "standalone synchronous", topology: "standalone", policy: "synchronous"},
		{name: "patroni", topology: "patroni", notice: "Recent writes can be lost", sent: true},
		{name: "patroni synchronous", topology: "patroni", policy: "synchronous", notice: "commits wait for one replica", sent: true},
		{name: "invalid", topology: "patroni", policy: "invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stderr actionDiagnostics
			requests := 0
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				var spec cluster.CreateSpec
				if err := json.NewDecoder(r.Body).Decode(&spec); err != nil || spec.PostgreSQL == nil || spec.PostgreSQL.ReplicationPolicy != test.policy {
					t.Errorf("policy not sent: %+v %v", spec, err)
				}
				stderr.Lock()
				message := stderr.buffer.String()
				stderr.Unlock()
				// Only Patroni replicates, so only a Patroni create discloses the policy.
				if (test.notice == "") != !strings.Contains(message, "replication") || !strings.Contains(message, test.notice) {
					t.Errorf("policy disclosure before admission: %q", message)
				}
				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode(client.Admission{ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1})
			}))
			args := append(flags, "db", "create", "postgres", "--name", "db", "--version", "17.11", "--idempotency-key", "policy-key", "--topology", test.topology)
			if test.topology == "patroni" {
				args = append(args, "--failure-domain", "host")
			}
			if test.policy != "" {
				args = append(args, "--replication-policy", test.policy)
			}
			var stdout bytes.Buffer
			code := cli.Run(t.Context(), args, &stdout, &stderr)
			if !test.sent {
				if code != 1 || requests != 0 {
					t.Fatal("an unsupported policy reached the API")
				}
				return
			}
			if code != 0 || requests != 1 {
				t.Fatalf("code=%d requests=%d", code, requests)
			}
		})
	}
}
