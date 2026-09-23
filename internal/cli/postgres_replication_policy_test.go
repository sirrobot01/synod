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
	for _, policy := range []string{"asynchronous", "synchronous", "invalid"} {
		t.Run(policy, func(t *testing.T) {
			var stderr actionDiagnostics
			requests := 0
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				var spec cluster.CreateSpec
				if err := json.NewDecoder(r.Body).Decode(&spec); err != nil || spec.PostgreSQL == nil || spec.PostgreSQL.ReplicationPolicy != policy {
					t.Errorf("policy not sent: %+v %v", spec, err)
				}
				stderr.Lock()
				message := stderr.buffer.String()
				stderr.Unlock()
				want := "Recent writes can be lost"
				if policy == "synchronous" {
					want = "add one before writing"
				}
				if !strings.Contains(message, want) {
					t.Error("policy was not disclosed before admission")
				}
				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode(client.Admission{ClusterID: uuid.New(), OperationID: uuid.New(), DesiredRevision: 1})
			}))
			args := append(flags, "db", "create", "postgres", "--name", "db", "--version", "17.11", "--idempotency-key", "policy-key")
			if policy != "asynchronous" {
				args = append(args, "--replication-policy", policy)
			}
			var stdout bytes.Buffer
			code := cli.Run(t.Context(), args, &stdout, &stderr)
			if policy == "invalid" {
				if code != 1 || requests != 0 {
					t.Fatal("invalid policy reached API")
				}
				return
			}
			if code != 0 || requests != 1 {
				t.Fatalf("create: %s", stdout.String())
			}
		})
	}
}
