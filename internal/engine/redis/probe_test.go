package redis_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/engine/redis"
)

func TestProbeAuthenticatesRealRedis(t *testing.T) {
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Redis fixture")
	}
	api, err := client.New(client.WithHost(host))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { api.Close() })
	passwords := map[string]string{"synod_app": uuid.NewString(), "synod_admin": uuid.NewString()}
	port := network.MustParsePort("6379/tcp")
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{
		Name: "synod-probe-" + uuid.NewString(),
		Config: &container.Config{
			Image: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3",
			Cmd: []string{"redis-server", "--save", "", "--appendonly", "no", "--user", "default", "off",
				"--user", "synod_app", "on", fmt.Sprintf("#%x", sha256.Sum256([]byte(passwords["synod_app"]))), "~*", "&*", "+@connection",
				"--user", "synod_admin", "on", fmt.Sprintf("#%x", sha256.Sum256([]byte(passwords["synod_admin"]))), "~*", "&*", "+@all"},
			ExposedPorts: network.PortSet{port: {}}, Labels: map[string]string{"io.synod.fixture": "redis-probe"},
		},
		HostConfig: &container.HostConfig{PortBindings: network.PortMap{port: {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: "0"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := api.ContainerRemove(ctx, created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
			t.Error(err)
		}
	})
	if _, err := api.ContainerStart(t.Context(), created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	item, err := api.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bindings := item.Container.NetworkSettings.Ports[port]
	if len(bindings) != 1 {
		t.Fatalf("fixture has %d port bindings", len(bindings))
	}
	portNumber, err := strconv.ParseUint(bindings[0].HostPort, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := engine.Endpoint{Address: bindings[0].HostIP.String(), Port: uint16(portNumber), Database: "0", Role: "synod_app"}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		if redis.New().Probe(ctx, endpoint, []byte(passwords[endpoint.Role]), 7).Healthy() {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Redis fixture did not become ready")
		case <-time.After(25 * time.Millisecond):
		}
	}
	for role, password := range passwords {
		t.Run(role, func(t *testing.T) {
			endpoint.Role = role
			observed := redis.New().Probe(t.Context(), endpoint, []byte(password), 7)
			if !observed.Healthy() || observed.ObservedRevision != 7 || observed.ObservedAt.IsZero() {
				t.Fatalf("authenticated Redis is not healthy: %+v", observed)
			}
			observed = redis.New().Probe(t.Context(), endpoint, []byte("wrong-password"), 8)
			if observed.Healthy() || observed.Reachable.State != engine.ProbePassed || observed.ProcessLive.State != engine.ProbePassed || observed.Ready.State != engine.ProbeFailed || observed.Ready.Reason != "authentication_rejected" || observed.Endpoint.State != engine.ProbeUnavailable {
				t.Fatalf("wrong credential passed or lost its reason: %+v", observed)
			}
			for _, probe := range []engine.Probe{observed.Reachable, observed.ProcessLive, observed.Ready, observed.Endpoint} {
				for _, secret := range []string{password, "wrong-password", endpoint.Address, role} {
					if strings.Contains(probe.Reason, secret) {
						t.Fatalf("probe reason exposes %q", secret)
					}
				}
			}
			observed = redis.New().Probe(t.Context(), endpoint, nil, 9)
			if observed.Reachable.State != engine.ProbePassed || observed.Ready.State != engine.ProbeUnavailable || observed.Ready.Reason != "credential_unavailable" || observed.Healthy() {
				t.Fatalf("missing credential was not unavailable: %+v", observed)
			}
		})
	}
}

func TestProbeSeparatesReachabilityFromReadiness(t *testing.T) {
	for _, stall := range []bool{false, true} {
		t.Run(map[bool]string{false: "disconnect", true: "deadline"}[stall], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					if stall {
						_ = conn.SetReadDeadline(time.Now().Add(time.Second))
						_, _ = io.Copy(io.Discard, conn)
					}
					conn.Close()
				}
			}()
			t.Cleanup(func() { listener.Close(); <-done })
			address := listener.Addr().(*net.TCPAddr)
			endpoint := engine.Endpoint{Address: "127.0.0.1", Port: uint16(address.Port), Database: "0", Role: "synod_app"}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			observed := redis.New().Probe(ctx, endpoint, []byte("password"), 1)
			if observed.Healthy() || observed.Reachable.State != engine.ProbePassed || observed.ProcessLive.State != engine.ProbePassed || observed.Ready.State != engine.ProbeFailed || observed.Ready.Reason != "database_not_ready" || observed.Endpoint.State != engine.ProbeUnavailable {
				t.Fatalf("a listening port passed readiness: %+v", observed)
			}
			if time.Since(start) > time.Second {
				t.Fatal("probe did not respect its shorter caller deadline")
			}
		})
	}
}

func TestProbeRefusesMissingEndpointAndConnection(t *testing.T) {
	for _, endpoint := range []engine.Endpoint{
		{},
		{Address: "127.0.0.1", Port: 6379, Database: "1", Role: "synod_app"},
		{Address: "127.0.0.1", Port: 6379, Database: "0", Role: "default"},
	} {
		observed := redis.New().Probe(t.Context(), endpoint, []byte("password"), 1)
		if observed.Reachable.Reason != "endpoint_not_declared" || observed.ProcessLive.State != engine.ProbeUnavailable || observed.Ready.State != engine.ProbeUnavailable || observed.Endpoint.State != engine.ProbeUnavailable {
			t.Fatalf("invalid endpoint was probed: %+v", observed)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	endpoint := engine.Endpoint{Address: "127.0.0.1", Port: uint16(port), Database: "0", Role: "synod_app"}
	observed := redis.New().Probe(t.Context(), endpoint, []byte("password"), 1)
	if observed.Reachable.State != engine.ProbeFailed || observed.ProcessLive.State != engine.ProbeUnavailable || observed.Ready.State != engine.ProbeUnavailable || observed.Endpoint.State != engine.ProbeUnavailable {
		t.Fatalf("refused connection reported a later result: %+v", observed)
	}
}

func TestProbeNativeReplicationReplies(t *testing.T) {
	const primary = "*3\r\n$6\r\nmaster\r\n:12\r\n*0\r\n"
	const replica = "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:6379\r\n$9\r\nconnected\r\n:12\r\n"
	const info = "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_link_status:up\r\nmaster_sync_in_progress:0\r\n"
	for _, test := range []struct {
		name, expected, role, info, password, reason, upstream string
	}{
		{name: "primary", expected: "primary", role: primary},
		{name: "primary with replica", expected: "primary", role: strings.Replace(primary, "*0\r\n", "*1\r\n*3\r\n$9\r\n127.0.0.2\r\n$4\r\n6380\r\n$2\r\n12\r\n", 1)},
		{name: "replica", expected: "replica", role: replica, info: info},
		{name: "IPv6 upstream", expected: "replica", upstream: "2001:db8::8", role: strings.Replace(replica, "$9\r\n127.0.0.1", "$11\r\n2001:db8::8", 1), info: strings.Replace(info, "127.0.0.1", "2001:db8::8", 1)},
		{name: "ordinary application ping", role: "-NOPERM forbidden\r\n"},
		{name: "primary instead of replica", expected: "replica", role: primary, reason: "redis_role_mismatch"},
		{name: "replica instead of primary", expected: "primary", role: replica, reason: "redis_role_mismatch"},
		{name: "sentinel", expected: "primary", role: "*2\r\n$8\r\nsentinel\r\n*0\r\n", reason: "redis_role_mismatch"},
		{name: "wrong upstream address", expected: "replica", role: strings.Replace(replica, "127.0.0.1", "127.0.0.2", 1), reason: "redis_upstream_mismatch"},
		{name: "wrong upstream port", expected: "replica", role: strings.Replace(replica, ":6379", ":6380", 1), reason: "redis_upstream_mismatch"},
		{name: "connecting", expected: "replica", role: strings.Replace(replica, "$9\r\nconnected", "$10\r\nconnecting", 1), reason: "redis_replication_link_down"},
		{name: "handshake", expected: "replica", role: strings.Replace(replica, "connected", "handshake", 1), reason: "redis_replication_link_down"},
		{name: "no link", expected: "replica", role: strings.Replace(replica, "$9\r\nconnected", "$4\r\nnone", 1), reason: "redis_replication_link_down"},
		{name: "sync in ROLE", expected: "replica", role: strings.Replace(replica, "$9\r\nconnected", "$4\r\nsync", 1), reason: "redis_replication_syncing"},
		{name: "link down in INFO", expected: "replica", role: replica, info: strings.Replace(info, "status:up", "status:down", 1), reason: "redis_replication_link_down"},
		{name: "sync in INFO", expected: "replica", role: replica, info: strings.Replace(info, "progress:0", "progress:1", 1), reason: "redis_replication_syncing"},
		{name: "role changed before INFO", expected: "replica", role: replica, info: strings.Replace(info, "role:slave", "role:master", 1), reason: "redis_role_mismatch"},
		{name: "upstream changed before INFO", expected: "replica", role: replica, info: strings.Replace(info, "127.0.0.1", "127.0.0.2", 1), reason: "redis_upstream_mismatch"},
		{name: "auth rejected", expected: "primary", role: primary, password: "wrong-password", reason: "authentication_rejected"},
		{name: "role denied with secret error", expected: "primary", role: "-NOPERM private-native-error\r\n", reason: "redis_role_unavailable"},
		{name: "empty role array", expected: "primary", role: "*0\r\n", reason: "redis_replication_invalid_response"},
		{name: "short primary array", expected: "primary", role: "*1\r\n$6\r\nmaster\r\n", reason: "redis_replication_invalid_response"},
		{name: "wrong primary offset type", expected: "primary", role: strings.Replace(primary, ":12", "+12", 1), reason: "redis_replication_invalid_response"},
		{name: "malformed primary replica tuple", expected: "primary", role: strings.Replace(primary, "*0\r\n", "*1\r\n*0\r\n", 1), reason: "redis_replication_invalid_response"},
		{name: "wrong replica port type", expected: "replica", role: strings.Replace(replica, ":6379", "+6379", 1), reason: "redis_replication_invalid_response"},
		{name: "short replica array", expected: "replica", role: "*1\r\n$5\r\nslave\r\n", reason: "redis_replication_invalid_response"},
		{name: "invalid role link", expected: "replica", role: strings.Replace(replica, "$9\r\nconnected", "$7\r\ninvalid", 1), reason: "redis_replication_invalid_response"},
		{name: "connected negative offset", expected: "replica", role: strings.Replace(replica, ":12", ":-1", 1), reason: "redis_replication_invalid_response"},
		{name: "invalid role type", expected: "primary", role: "*1\r\n:1\r\n", reason: "redis_replication_invalid_response"},
		{name: "INFO denied with private error", expected: "replica", role: replica, info: "-NOPERM private-info-error\r\n", reason: "redis_replication_unavailable"},
		{name: "INFO missing sync", expected: "replica", role: replica, info: strings.Replace(info, "master_sync_in_progress:0\r\n", "", 1), reason: "redis_replication_invalid_response"},
		{name: "INFO duplicate required field", expected: "replica", role: replica, info: info + "master_sync_in_progress:0\r\n", reason: "redis_replication_invalid_response"},
		{name: "INFO malformed line", expected: "replica", role: replica, info: info + "broken-line\r\n", reason: "redis_replication_invalid_response"},
		{name: "INFO invalid sync flag", expected: "replica", role: replica, info: strings.Replace(info, "progress:0", "progress:2", 1), reason: "redis_replication_invalid_response"},
		{name: "INFO invalid link", expected: "replica", role: replica, info: strings.Replace(info, "status:up", "status:unknown", 1), reason: "redis_replication_invalid_response"},
		{name: "INFO over bound", expected: "replica", role: replica, info: info + strings.Repeat("x", 64*1024), reason: "redis_replication_invalid_response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			address, commands := replicationReplyServer(t, test.role, test.info)
			endpoint := engine.Endpoint{Address: "127.0.0.1", Port: uint16(address.Port), Database: "0", Role: "synod_admin"}
			if test.expected != "" {
				endpoint.RedisReplication = &engine.RedisReplicationCheck{ExpectedRole: test.expected}
				if test.expected == "replica" {
					upstream := test.upstream
					if upstream == "" {
						upstream = "127.0.0.1"
					}
					endpoint.RedisReplication.Upstream = &engine.RedisUpstream{Address: upstream, Port: 6379}
				}
			} else {
				endpoint.Role = "synod_app"
			}
			password := test.password
			if password == "" {
				password = "fixture-password"
			}
			observed := redis.New().Probe(t.Context(), endpoint, []byte(password), 9)
			if test.reason == "" {
				if !observed.Healthy() || observed.ObservedRevision != 9 {
					t.Fatalf("native expectation did not pass: %+v", observed)
				}
			} else if observed.Healthy() || observed.Ready.Reason != test.reason || observed.Endpoint.State != engine.ProbeUnavailable {
				t.Fatalf("native expectation result: %+v; want reason %s", observed, test.reason)
			}
			if test.expected == "" {
				for len(commands) > 0 {
					if command := <-commands; command == "role" || command == "info" {
						t.Fatal("application PING requested an administrative observation")
					}
				}
			}
		})
	}
}

func TestProbeRefusesInvalidReplicationExpectationBeforeConnecting(t *testing.T) {
	for _, check := range []*engine.RedisReplicationCheck{
		{}, {ExpectedRole: "master"}, {ExpectedRole: "primary", Upstream: &engine.RedisUpstream{Address: "127.0.0.1", Port: 6379}},
		{ExpectedRole: "replica"}, {ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "redis.internal", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "::ffff:127.0.0.1", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "::", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "ff02::1", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "fe80::1%lo0", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "2001:0db8::1", Port: 6379}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "127.0.0.1", Port: 0}},
		{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "127.0.0.1", Port: 65536}},
	} {
		endpoint := engine.Endpoint{Address: "127.0.0.1", Port: 1, Database: "0", Role: "synod_admin", RedisReplication: check}
		observed := redis.New().Probe(t.Context(), endpoint, []byte("password"), 1)
		if observed.Reachable.Reason != "redis_replication_invalid_request" || observed.ProcessLive.State != engine.ProbeUnavailable {
			t.Fatalf("invalid expectation reached the endpoint: %+v", observed)
		}
	}
	endpoint := engine.Endpoint{Address: "127.0.0.1", Port: 1, Database: "0", Role: "synod_app", RedisReplication: &engine.RedisReplicationCheck{ExpectedRole: "primary"}}
	if observed := redis.New().Probe(t.Context(), endpoint, []byte("password"), 1); observed.Reachable.Reason != "redis_replication_invalid_request" {
		t.Fatalf("application credential accepted for native role check: %+v", observed)
	}
}

// replicationReplyServer injects malformed native replies that a real Redis will not emit.
func replicationReplyServer(t *testing.T, role, info string) (*net.TCPAddr, chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	commands := make(chan string, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if len(line) < 3 || line[0] != '*' {
						return
					}
					count, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSuffix(line[1:], "\n"), "\r"))
					if err != nil || count < 1 || count > 8 {
						return
					}
					var args []string
					for range count {
						line, err = reader.ReadString('\n')
						if err != nil || len(line) < 3 || line[0] != '$' {
							return
						}
						size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
						if err != nil || size < 0 || size > 1024 {
							return
						}
						data := make([]byte, size+2)
						if _, err := io.ReadFull(reader, data); err != nil {
							return
						}
						args = append(args, string(data[:size]))
					}
					command := strings.ToLower(args[0])
					commands <- command
					switch command {
					case "hello":
						_, _ = io.WriteString(conn, "-ERR unknown command\r\n")
					case "auth":
						if len(args) != 3 || args[2] != "fixture-password" {
							_, _ = io.WriteString(conn, "-WRONGPASS private-auth-error\r\n")
						} else {
							_, _ = io.WriteString(conn, "+OK\r\n")
						}
					case "ping":
						_, _ = io.WriteString(conn, "+PONG\r\n")
					case "role":
						_, _ = io.WriteString(conn, role)
					case "info":
						if len(args) != 2 || args[1] != "replication" {
							t.Error("probe requested an undeclared INFO section")
						}
						if strings.HasPrefix(info, "-NOPERM ") {
							_, _ = io.WriteString(conn, info)
						} else {
							_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(info), info)
						}
					default:
						_, _ = io.WriteString(conn, "-ERR unexpected command\r\n")
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { listener.Close(); <-done })
	return listener.Addr().(*net.TCPAddr), commands
}
