package docker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestGenericHTTPHealthTransitions(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path != "/ready" {
			t.Errorf("unexpected probe path %q", r.URL.Path)
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	host, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	ports := []*agentv1.PortBinding{{ContainerPort: 80, HostAddress: host, HostPort: uint32(number), Transport: "tcp"}}
	item := container.InspectResponse{ID: "copy", State: &container.State{Running: true, StartedAt: "first"}}
	r := &Runtime{health: map[string]genericHealth{}}
	probe := &agentv1.HealthCheckConfig{Type: "http", Path: "/ready", Port: 80, IntervalSeconds: 1, TimeoutSeconds: 1, UnhealthyThreshold: 2}
	for i, want := range []string{"starting", "unhealthy", "healthy"} {
		if i == 2 {
			status.Store(http.StatusOK)
		}
		prior := r.health[item.ID]
		prior.checked = time.Time{}
		r.health[item.ID] = prior
		if got := r.genericProbe(t.Context(), item, probe, ports); got != want {
			t.Fatalf("probe %d: got %q, want %q", i, got, want)
		}
	}
	if got := r.genericProbe(t.Context(), item, probe, ports); got != "healthy" || hits.Load() != 3 {
		t.Fatalf("interval cache: %q, %d calls", got, hits.Load())
	}
	status.Store(http.StatusServiceUnavailable)
	item.State.StartedAt = "restart"
	if got := r.genericProbe(t.Context(), item, probe, ports); got != "starting" {
		t.Fatalf("restart reused prior health: %q", got)
	}
	item.State.Running = false
	if got := r.genericProbe(t.Context(), item, probe, ports); got != "unhealthy" {
		t.Fatalf("stopped health: %q", got)
	}
}

func TestGenericTCPHealthAndCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	r := &Runtime{health: map[string]genericHealth{}}
	item := container.InspectResponse{ID: "copy", State: &container.State{Running: true, StartedAt: "first"}}
	ports := []*agentv1.PortBinding{{ContainerPort: 80, HostAddress: "0.0.0.0", HostPort: uint32(listener.Addr().(*net.TCPAddr).Port), Transport: "tcp"}}
	probe := &agentv1.HealthCheckConfig{Type: "tcp", Port: 80, TimeoutSeconds: 1, UnhealthyThreshold: 1}
	if got := r.genericProbe(t.Context(), item, probe, ports); got != "healthy" {
		t.Fatalf("TCP health: %q", got)
	}
	listener.Close()
	clear(r.health)
	if got := r.genericProbe(t.Context(), item, probe, ports); got != "unhealthy" {
		t.Fatalf("closed TCP port: %q", got)
	}
	clear(r.health)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := r.genericProbe(ctx, item, probe, ports); got != "unknown" {
		t.Fatalf("cancelled probe: %q", got)
	}
}
