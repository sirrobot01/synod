package cli_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestListClustersCLI(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(map[bool]string{false: "human", true: "json"}[jsonOutput], func(t *testing.T) {
			now := time.Now().UTC()
			item := cluster.Cluster{ID: uuid.New(), InstallationID: uuid.New(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "photos", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}, DesiredRevision: 3, DesiredLifecycle: cluster.DesiredStopped, ObservedRevision: 2, ObservedLifecycle: cluster.ObservedUnknown, CreatedAt: now, UpdatedAt: now}
			var calls atomic.Int32
			flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.Path != "/v1/clusters" || r.URL.RawQuery != "cursor=opaque+%2B%2F%3F%26%3D&include_deleted=true&limit=1" || r.Header.Get("Authorization") != "Bearer "+token {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				json.NewEncoder(w).Encode(cluster.Page{Items: []cluster.Cluster{item}, NextCursor: "next-page"})
			}))
			if !jsonOutput {
				flags = flags[1:]
			}
			var stdout, stderr bytes.Buffer
			code := cli.Run(t.Context(), append(flags, "cluster", "list", "--limit", "1", "--cursor", "opaque +/?&=", "--include-deleted"), &stdout, &stderr)
			if code != 0 || calls.Load() != 1 || stderr.Len() != 0 {
				t.Fatalf("code=%d calls=%d output=%s error=%s", code, calls.Load(), stdout.String(), stderr.String())
			}
			if jsonOutput {
				decoder := json.NewDecoder(&stdout)
				var result cli.Result
				if err := decoder.Decode(&result); err != nil || result.ClusterPage == nil || len(result.ClusterPage.Items) != 1 || result.ClusterPage.Items[0].ID != item.ID || result.ClusterPage.NextCursor != "next-page" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				if err := decoder.Decode(new(any)); err != io.EOF {
					t.Fatalf("extra JSON output: %v", err)
				}
			} else {
				for _, part := range []string{item.ID.String(), "photos", "postgres", "stopped (3)", "unknown (2)", "Next cursor: \"next-page\""} {
					if !strings.Contains(stdout.String(), part) {
						t.Errorf("missing %q: %s", part, stdout.String())
					}
				}
				if strings.Contains(strings.ToLower(stdout.String()), "health") {
					t.Error("list fabricated health")
				}
			}
		})
	}
}

func TestListClustersCLIEmptyDefault(t *testing.T) {
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "include_deleted=false&limit=50" {
			t.Errorf("query=%s", r.URL.RawQuery)
		}
		io.WriteString(w, `{"items":[],"next_cursor":""}`)
	}))
	var stdout, stderr bytes.Buffer
	code := cli.Run(t.Context(), append(flags[1:], "db", "list"), &stdout, &stderr)
	if code != 0 || stdout.String() != "No clusters on this page.\n" || stderr.Len() != 0 {
		t.Fatalf("code=%d output=%s error=%s", code, stdout.String(), stderr.String())
	}
}

func TestListClustersCLIInvalidOptions(t *testing.T) {
	for _, args := range [][]string{{"--limit", "0"}, {"--limit", "201"}, {"--cursor", ""}, {"--cursor", strings.Repeat("x", 1025)}} {
		flags := fixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid options reached server") }))
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(append(flags, "db", "list"), args...), &stdout, &stderr)
		var result cli.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != "invalid_request" || result.ClusterPage != nil {
			t.Fatalf("args=%v code=%d output=%s error=%s", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestListClustersCLIErrors(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized} {
		flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, `{"code":"unauthenticated","message":"private-secret"}`)
		}))
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), append(flags, "db", "list"), &stdout, &stderr)
		var result cli.Result
		want := "unauthenticated"
		if status == http.StatusOK {
			want = "invalid_response"
		}
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || code != 1 || result.Error == nil || result.Error.Code != want || result.ClusterPage != nil || strings.Contains(stdout.String()+stderr.String(), "private-secret") {
			t.Fatalf("code=%d output=%s error=%s", code, stdout.String(), stderr.String())
		}
	}
}
