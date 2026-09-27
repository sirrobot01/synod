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

// Members show their node and address, so an operator can tell them apart.
func TestMembersShowNodeAndAddress(t *testing.T) {
	id, member, node := uuid.New(), uuid.New(), uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/clusters/" + id.String():
			json.NewEncoder(w).Encode(cluster.Cluster{ID: id, DesiredRevision: 4})
		case "/v1/clusters/" + id.String() + "/members":
			json.NewEncoder(w).Encode(map[string]any{"members": []client.ClusterMember{
				{ID: member, Role: "primary", State: "active", NodeID: node, Address: "fd7a::1"},
				{ID: uuid.New(), Role: "replica", State: "pending"},
			}})
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags[1:], "db", "members", id.String()), &stdout, &stderr); code != 0 {
		t.Fatalf("members: code=%d err=%s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], member.String()) || !strings.Contains(lines[0], node.String()) || !strings.HasSuffix(lines[0], "fd7a::1") || !strings.HasSuffix(lines[1], "-") {
		t.Fatalf("members output:\n%s", stdout.String())
	}
}
