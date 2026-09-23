package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestListClustersQueryAndSinglePage(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "deleted"}[deleted], func(t *testing.T) {
			var calls atomic.Int32
			now := time.Now().UTC()
			item := cluster.Cluster{ID: uuid.New(), InstallationID: uuid.New(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "photos", Engine: cluster.PostgreSQL, PostgreSQL: &cluster.PostgreSQLSpec{Version: "17.11", Topology: "standalone"}}, DesiredRevision: 2, DesiredLifecycle: cluster.DesiredStopped, ObservedRevision: 1, ObservedLifecycle: cluster.ObservedUnknown, CreatedAt: now, UpdatedAt: now}
			if deleted {
				item.DeletedAt = &now
				item.DesiredLifecycle = cluster.DesiredDeleted
				item.ObservedLifecycle = cluster.ObservedDeleted
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				want := "cursor=opaque+%2B%2F%3F%26%3D&include_deleted=false&limit=1"
				if deleted {
					want = strings.Replace(want, "false", "true", 1)
				}
				if r.Method != http.MethodGet || r.URL.Path != "/v1/clusters" || r.URL.RawQuery != want || r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("Idempotency-Key") != "" {
					t.Errorf("unexpected list request: %s %s", r.Method, r.URL)
				}
				json.NewEncoder(w).Encode(cluster.Page{Items: []cluster.Cluster{item}, NextCursor: "next-opaque"})
			}))
			defer server.Close()
			client := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 3}
			page, err := client.ListClusters(t.Context(), cluster.ListOptions{Limit: 1, Cursor: "opaque +/?&=", IncludeDeleted: deleted})
			if err != nil || len(page.Items) != 1 || page.Items[0].ID != item.ID || page.NextCursor != "next-opaque" || calls.Load() != 1 {
				t.Fatalf("page=%+v calls=%d err=%v", page, calls.Load(), err)
			}
		})
	}
}

func TestListClustersEmptyAndInvalidReplies(t *testing.T) {
	now := time.Now().UTC()
	item := cluster.Cluster{ID: uuid.MustParse("ffffffff-ffff-4fff-8fff-ffffffffffff"), InstallationID: uuid.New(), Spec: cluster.CreateSpec{SchemaVersion: 1, Name: "cache", Engine: cluster.Redis, Redis: &cluster.RedisSpec{Version: "8.2.7", Topology: "standalone", Persistence: "rdb+aof"}}, DesiredRevision: 1, DesiredLifecycle: cluster.DesiredRunning, ObservedLifecycle: cluster.ObservedProvisioning, CreatedAt: now, UpdatedAt: now}
	older := item
	older.ID = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	clockCorrection := item
	clockCorrection.UpdatedAt = now.Add(-time.Second)
	clockCorrection.ObservedRevision = 2
	tests := []struct {
		name  string
		body  any
		valid bool
	}{
		{"empty", cluster.Page{Items: []cluster.Cluster{}}, true},
		{"redis", cluster.Page{Items: []cluster.Cluster{item}}, true},
		{"equal timestamps descending ids", cluster.Page{Items: []cluster.Cluster{item, older}}, true},
		{"independent observed metadata", cluster.Page{Items: []cluster.Cluster{clockCorrection}}, true},
		{"repeated cursor", cluster.Page{Items: []cluster.Cluster{item, older}, NextCursor: "same-cursor"}, false},
		{"null items", cluster.Page{}, false},
		{"missing cursor", map[string]any{"items": []cluster.Cluster{}}, false},
		{"null cursor", map[string]any{"items": []cluster.Cluster{}, "next_cursor": nil}, false},
		{"empty continuation", cluster.Page{Items: []cluster.Cluster{}, NextCursor: "next"}, false},
		{"oversized cursor", cluster.Page{Items: []cluster.Cluster{item}, NextCursor: strings.Repeat("x", 1025)}, false},
		{"oversized page", cluster.Page{Items: []cluster.Cluster{item, item, item}}, false},
		{"duplicate identity", cluster.Page{Items: []cluster.Cluster{item, item}}, false},
	}
	for _, mutation := range []string{"zero id", "zero installation", "bad spec", "bad desired", "bad observed", "zero desired revision", "negative observed revision", "negative epoch", "zero created", "zero updated", "deleted", "mixed installation", "wrong time order", "wrong id order"} {
		changed := item
		switch mutation {
		case "zero id":
			changed.ID = uuid.Nil
		case "zero installation":
			changed.InstallationID = uuid.Nil
		case "bad spec":
			changed.Spec.Name = "SECRET\nname"
		case "bad desired":
			changed.DesiredLifecycle = "SECRET"
		case "bad observed":
			changed.ObservedLifecycle = "SECRET"
		case "zero desired revision":
			changed.DesiredRevision = 0
		case "negative observed revision":
			changed.ObservedRevision = -1
		case "negative epoch":
			changed.ExecutionEpoch = -1
		case "zero created":
			changed.CreatedAt = time.Time{}
		case "zero updated":
			changed.UpdatedAt = time.Time{}
		case "deleted":
			changed.DeletedAt = &now
		case "mixed installation":
			changed.ID = uuid.New()
			changed.InstallationID = uuid.New()
		case "wrong time order":
			changed.ID = uuid.New()
			changed.CreatedAt = now.Add(time.Second)
			changed.UpdatedAt = changed.CreatedAt
		case "wrong id order":
			changed.ID = uuid.MustParse("00000000-0000-4000-8000-000000000001")
		}
		items := []cluster.Cluster{changed}
		switch mutation {
		case "mixed installation", "wrong time order":
			items = []cluster.Cluster{item, changed}
		case "wrong id order":
			items = []cluster.Cluster{changed, item}
		}
		tests = append(tests, struct {
			name  string
			body  any
			valid bool
		}{mutation, cluster.Page{Items: items}, false})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(tt.body) }))
			defer server.Close()
			client := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
			page, err := client.ListClusters(t.Context(), cluster.ListOptions{Limit: 2, Cursor: "same-cursor"})
			if tt.valid {
				if err != nil || page.Items == nil {
					t.Fatalf("valid page=%+v err=%v", page, err)
				}
				return
			}
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != "invalid_response" || page.Items != nil || strings.Contains(failure.Message, "SECRET") {
				t.Fatalf("invalid page=%+v err=%v", page, err)
			}
		})
	}
}

func TestListClustersRejectsOptionsBeforeRequest(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid options reached server") }))
	defer server.Close()
	client := &Client{endpoint: server.URL, http: server.Client(), attempts: 1}
	for _, options := range []cluster.ListOptions{{}, {Limit: -1}, {Limit: 201}, {Limit: 1, Cursor: strings.Repeat("x", 1025)}} {
		_, err := client.ListClusters(t.Context(), options)
		failure, ok := errors.AsType[*Error](err)
		if !ok || failure.Code != "invalid_request" {
			t.Fatalf("options=%+v err=%v", options, err)
		}
	}
}

func TestListClustersPreservesSafeAPIErrors(t *testing.T) {
	requestID := uuid.NewString()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Error{Code: "unauthenticated", Message: "SECRET", RequestID: requestID})
	}))
	defer server.Close()
	client := &Client{endpoint: server.URL, http: server.Client(), attempts: 3}
	page, err := client.ListClusters(t.Context(), cluster.ListOptions{Limit: 50})
	failure, ok := errors.AsType[*Error](err)
	if !ok || failure.Code != "unauthenticated" || failure.RequestID != requestID || strings.Contains(failure.Message, "SECRET") || page.Items != nil {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}
