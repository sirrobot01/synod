package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestRenewCertificatesSendsAnEmptyAction(t *testing.T) {
	id := uuid.New()
	admission := client.Admission{ClusterID: id, OperationID: uuid.New(), DesiredRevision: 5}
	var renewals atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/clusters/"+id.String():
			json.NewEncoder(w).Encode(cluster.Cluster{ID: id, DesiredRevision: 4})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/clusters/"+id.String()+"/actions/renew-certificates":
			body, _ := io.ReadAll(r.Body)
			if string(body) != "{}" || r.Header.Get("If-Match") != `"4"` || r.Header.Get("Idempotency-Key") == "" {
				t.Errorf("renewal body=%s match=%s", body, r.Header.Get("If-Match"))
			}
			renewals.Add(1)
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(admission)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "renew-certificates", id.String()), &stdout, &stderr); code != 0 || renewals.Load() != 1 {
		t.Fatalf("renew-certificates: code=%d renewals=%d out=%s err=%s", code, renewals.Load(), stdout.String(), stderr.String())
	}
}

func TestSwitchoverSendsTheNamedMember(t *testing.T) {
	id, member := uuid.New(), uuid.New()
	admission := client.Admission{ClusterID: id, OperationID: uuid.New(), DesiredRevision: 5}
	var switches atomic.Int32
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/clusters/"+id.String():
			json.NewEncoder(w).Encode(cluster.Cluster{ID: id, DesiredRevision: 4})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/clusters/"+id.String()+"/actions/switchover":
			body, _ := io.ReadAll(r.Body)
			if string(body) != `{"member_id":"`+member.String()+`"}` || r.Header.Get("If-Match") != `"4"` || r.Header.Get("Idempotency-Key") == "" {
				t.Errorf("switchover body=%s match=%s", body, r.Header.Get("If-Match"))
			}
			switches.Add(1)
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(admission)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), append(flags, "db", "switchover", id.String()), &stdout, &stderr); code == 0 || switches.Load() != 0 {
		t.Fatalf("switchover without --member: code=%d switches=%d", code, switches.Load())
	}
	if code := cli.Run(t.Context(), append(flags, "db", "switchover", id.String(), "--member", member.String()), &stdout, &stderr); code != 0 || switches.Load() != 1 {
		t.Fatalf("switchover: code=%d switches=%d out=%s err=%s", code, switches.Load(), stdout.String(), stderr.String())
	}
}

// Members show their node and address, so an operator can name one for a switchover.
func TestMembersShowNodeAndAddress(t *testing.T) {
	id, member, node := uuid.New(), uuid.New(), uuid.New()
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/clusters/" + id.String():
			json.NewEncoder(w).Encode(cluster.Cluster{ID: id, DesiredRevision: 4})
		case "/v1/clusters/" + id.String() + "/members":
			json.NewEncoder(w).Encode(map[string]any{"members": []client.ClusterMember{
				{ID: member, Role: "patroni", State: "active", NodeID: node, Address: "fd7a::1"},
				{ID: uuid.New(), Role: "patroni", State: "pending"},
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
