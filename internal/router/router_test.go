package router

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestHTTPBalancesAndRemovesUnhealthyCopies(t *testing.T) {
	var failed atomic.Bool
	var servers []*httptest.Server
	for _, name := range []string{"one", "two"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/ready" && name == "one" && failed.Load() {
				w.WriteHeader(503)
				return
			}
			io.WriteString(w, name)
		}))
		defer server.Close()
		servers = append(servers, server)
	}
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint32(reserved.Addr().(*net.TCPAddr).Port)
	reserved.Close()
	m, err := New(t.Context(), Options{ListenAddress: "127.0.0.1", ProbeInterval: 10 * time.Millisecond, ProbeTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	route := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "http", IngressPort: port, HealthPath: "/ready", Backends: []string{strings.TrimPrefix(servers[0].URL, "http://"), strings.TrimPrefix(servers[1].URL, "http://")}}
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	endpoint := "http://127.0.0.1:" + strconv.Itoa(int(port))
	seen := map[string]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for len(seen) < 2 {
		response, err := client.Get(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode == 200 {
			seen[string(body)] = true
		}
		if time.Now().After(deadline) {
			t.Fatalf("backends were not both used: %v", seen)
		}
		time.Sleep(10 * time.Millisecond)
	}
	failed.Store(true)
	deadline = time.Now().Add(3 * time.Second)
	for {
		m.mu.Lock()
		r := m.routes[route.AppId+"/"+strconv.FormatUint(uint64(route.IngressPort), 10)]
		m.mu.Unlock()
		r.mu.RLock()
		healthy := r.backends[0].healthy.Load()
		r.mu.RUnlock()
		if !healthy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed HTTP health check remained eligible")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for range 8 {
		response, err := client.Get(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != "two" {
			t.Fatalf("unhealthy backend served traffic: %q %v", body, err)
		}
	}
	route.Backends = nil
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("empty pool returned %d", response.StatusCode)
	}
}

func TestTCPReloadPreservesAnOpenConnection(t *testing.T) {
	var addresses []string
	for _, name := range []string{"one\n", "two\n"} {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		addresses = append(addresses, listener.Addr().String())
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func() { defer conn.Close(); io.WriteString(conn, name); io.Copy(conn, conn) }()
			}
		}()
	}
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	port := uint32(reserved.Addr().(*net.TCPAddr).Port)
	reserved.Close()
	m, err := New(t.Context(), Options{ListenAddress: "127.0.0.1", ProbeInterval: 10 * time.Millisecond, ProbeTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	route := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "tcp", IngressPort: port, Backends: addresses[:1]}
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	var first net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		candidate, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		candidate.SetDeadline(time.Now().Add(time.Second))
		line, err := bufio.NewReader(candidate).ReadString('\n')
		if err == nil && line == "one\n" {
			first = candidate
			break
		}
		candidate.Close()
		if time.Now().After(deadline) {
			t.Fatalf("first backend did not become ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer first.Close()
	route.Backends = addresses[1:]
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		candidate, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		candidate.SetDeadline(time.Now().Add(time.Second))
		line, err := bufio.NewReader(candidate).ReadString('\n')
		candidate.Close()
		if err == nil && line == "two\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement backend did not become ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	first.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(first, "still open\n"); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(first).ReadString('\n')
	if err != nil || line != "still open\n" {
		t.Fatalf("reload dropped the established stream: %q %v", line, err)
	}
	if err := m.Apply(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(first, "after removal\n"); err != nil {
		t.Fatal(err)
	}
	line, err = bufio.NewReader(first).ReadString('\n')
	if err != nil || line != "after removal\n" {
		t.Fatalf("route removal dropped an active stream: %q %v", line, err)
	}
	if conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("removed ingress still accepts connections")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	first.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("router shutdown left a tunnel open")
	}

}

func TestRoutePortConflictRollsBackSnapshot(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := free.Addr().String()
	port := uint32(free.Addr().(*net.TCPAddr).Port)
	free.Close()
	m, err := New(t.Context(), Options{ListenAddress: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	first := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "tcp", IngressPort: port}
	second := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "http", IngressPort: uint32(occupied.Addr().(*net.TCPAddr).Port)}
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{first, second}); !errors.Is(err, ErrPortConflict) {
		t.Fatalf("conflict result: %v", err)
	}
	proof, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed snapshot leaked its first listener: %v", err)
	}
	proof.Close()
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{first}); err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(first).(*agentv1.AppRoute)
	changed.Protocol = "http"
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{changed}); !errors.Is(err, ErrPortConflict) {
		t.Fatalf("in-place protocol replacement: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.Apply(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled update: %v", err)
	}
	if m.routes[first.AppId+"/"+strconv.FormatUint(uint64(first.IngressPort), 10)].protocol != "tcp" {
		t.Fatal("failed snapshot changed prior routes")
	}
}

func TestRouteValidation(t *testing.T) {
	valid := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "http", IngressPort: 8080, Backends: []string{"127.0.0.1:3000"}}
	for name, change := range map[string]func(*agentv1.AppRoute){
		"no app":            func(r *agentv1.AppRoute) { r.AppId = "" },
		"port overflow":     func(r *agentv1.AppRoute) { r.IngressPort = 65536 },
		"unknown protocol":  func(r *agentv1.AppRoute) { r.Protocol = "udp" },
		"hostname":          func(r *agentv1.AppRoute) { r.Backends = []string{"localhost:3000"} },
		"wildcard":          func(r *agentv1.AppRoute) { r.Backends = []string{"0.0.0.0:3000"} },
		"health host":       func(r *agentv1.AppRoute) { r.HealthPath = "http://elsewhere/" },
		"duplicate backend": func(r *agentv1.AppRoute) { r.Backends = append(r.Backends, r.Backends[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(valid).(*agentv1.AppRoute)
			change(r)
			if !errors.Is(Validate([]*agentv1.AppRoute{r}), ErrConfiguration) {
				t.Fatal("invalid route accepted")
			}
		})
	}
	if err := Validate([]*agentv1.AppRoute{valid, valid}); !errors.Is(err, ErrConfiguration) {
		t.Fatal("duplicate ingress accepted")
	}
	// A database routes writers and readers on two ports of one app.
	reader := proto.Clone(valid).(*agentv1.AppRoute)
	reader.IngressPort = 8081
	if err := Validate([]*agentv1.AppRoute{valid, reader}); err != nil {
		t.Fatalf("second port of one app refused: %v", err)
	}
}

func TestHTTPReloadPreservesAnActiveRequest(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(entered)
			<-release
		}
		io.WriteString(w, "one")
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "two") }))
	defer second.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + reserved.Addr().String()
	port := uint32(reserved.Addr().(*net.TCPAddr).Port)
	reserved.Close()
	m, err := New(t.Context(), Options{ListenAddress: "127.0.0.1", ProbeInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	route := &agentv1.AppRoute{AppId: uuid.NewString(), Protocol: "http", IngressPort: port, Backends: []string{strings.TrimPrefix(first.URL, "http://")}}
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP backend did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	result := make(chan string, 1)
	go func() {
		response, err := client.Get(endpoint + "/hold")
		if err != nil {
			result <- err.Error()
			return
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		result <- string(body)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("request did not enter old backend")
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	route.Backends = []string{strings.TrimPrefix(second.URL, "http://")}
	if err := m.Apply(t.Context(), []*agentv1.AppRoute{route}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		response, err := client.Get(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if string(body) == "two" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new request did not reach the new backend: %q", body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(release)
	if body := <-result; body != "one" {
		t.Fatalf("update interrupted the old request: %q", body)
	}
}
