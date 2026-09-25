package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakePatroni serves a cluster view and answers switchover requests.
type fakePatroni struct {
	mu       sync.Mutex
	leader   string
	members  map[string]string
	status   int
	drop     bool
	moves    bool
	requests int
}

func (f *fakePatroni) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/cluster":
		var view patroniClusterView
		for name, state := range f.members {
			role := "replica"
			// Patroni reports a leader as running.
			if name == f.leader {
				role, state = "leader", "running"
			}
			view.Members = append(view.Members, struct {
				Name  string `json:"name"`
				Role  string `json:"role"`
				State string `json:"state"`
			}{name, role, state})
		}
		json.NewEncoder(w).Encode(view)
	case r.Method == http.MethodPost && r.URL.Path == "/switchover":
		f.requests++
		var body struct{ Leader, Candidate string }
		user, password, ok := r.BasicAuth()
		if !ok || user != "synod_patroni" || password != "rest-password" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Leader != f.leader {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if f.moves {
			f.leader = body.Candidate
		}
		if f.drop {
			// The reply is lost after Patroni acted.
			connection, _, _ := w.(http.Hijacker).Hijack()
			connection.Close()
			return
		}
		w.WriteHeader(f.status)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestPatroniSwitchover(t *testing.T) {
	leader, candidate, other := uuid.New(), uuid.New(), uuid.New()
	members := func() map[string]string {
		return map[string]string{leader.String(): "running", candidate.String(): "streaming", other.String(): "streaming"}
	}
	for _, test := range []struct {
		name     string
		fake     *fakePatroni
		timeout  time.Duration
		switched bool
		want     error
		requests int
	}{
		{name: "already leading", fake: &fakePatroni{leader: candidate.String(), members: members()}, requests: 0},
		{name: "switched", fake: &fakePatroni{leader: leader.String(), members: members(), status: http.StatusOK, moves: true}, switched: true, requests: 1},
		{name: "lost reply after the switch", fake: &fakePatroni{leader: leader.String(), members: members(), drop: true, moves: true}, switched: true, requests: 1},
		{name: "refused", fake: &fakePatroni{leader: leader.String(), members: members(), status: http.StatusPreconditionFailed}, want: ErrSwitchoverRefused, requests: 1},
		{name: "accepted but never moved", fake: &fakePatroni{leader: leader.String(), members: members(), status: http.StatusOK}, timeout: 1500 * time.Millisecond, want: ErrSwitchoverUnconfirmed, requests: 1},
		{name: "stopped candidate", fake: &fakePatroni{leader: leader.String(), members: map[string]string{leader.String(): "running", candidate.String(): "stopped"}}, want: ErrSwitchoverIneligible},
		{name: "absent candidate", fake: &fakePatroni{leader: leader.String(), members: map[string]string{leader.String(): "running"}}, want: ErrSwitchoverIneligible},
		{name: "no leader", fake: &fakePatroni{members: members()}, want: ErrSwitchoverIneligible},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(test.fake)
			defer server.Close()
			timeout := test.timeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			switched, err := PatroniSwitchover(ctx, server.Client(), server.URL, candidate, "rest-password")
			if !errors.Is(err, test.want) || (test.want == nil && err != nil) || switched != test.switched {
				t.Fatalf("switched %v, error %v; want %v, %v", switched, err, test.switched, test.want)
			}
			if test.fake.requests != test.requests {
				t.Fatalf("switchover requests: %d, want %d", test.fake.requests, test.requests)
			}
		})
	}
}

func TestDecodePatroniTLSMaterial(t *testing.T) {
	if _, err := DecodePatroniTLSMaterial([]byte(`{"ca":"Y2E=","certificate":"Yw==","private_key":"aw=="}`)); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"empty":         "",
		"unknown field": `{"ca":"Y2E=","extra":1}`,
		"trailing data": `{"ca":"Y2E="} {}`,
	} {
		if _, err := DecodePatroniTLSMaterial([]byte(value)); err == nil {
			t.Errorf("%s: material accepted", name)
		}
	}
}
