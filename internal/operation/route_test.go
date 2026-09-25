package operation

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// routeCommand builds a valid pooler or router container command.
func routeCommand(router bool) *agentv1.Command {
	command := commandFixture()
	member := &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 2, Purpose: "patroni_member_tls"}
	route := &agentv1.PostgresRouteConfig{Pooler: &agentv1.PostgresPoolerConfig{Address: "10.0.0.2", Port: 6432, DatabasePort: 5432, RestPort: 8008},
		Credentials: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "application"}, member}}
	if router {
		route = &agentv1.PostgresRouteConfig{Router: &agentv1.PostgresRouterConfig{Address: "10.0.0.2", WriterPort: 5000, ReaderPort: 5001,
			Backends: []*agentv1.PostgresRouteBackend{{Address: "10.0.0.2", PoolerPort: 6432, RestPort: 8008}, {Address: "10.0.0.3", PoolerPort: 6432, RestPort: 8008}}},
			Credentials: []*agentv1.SecretReference{member}}
	}
	command.Input.Secrets = route.Credentials
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		Engine: "postgres", ImageDigest: "synod-patroni@sha256:" + strings.Repeat("a", 64),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, Route: route,
	}}
	return command
}

func TestRouteContainerValidation(t *testing.T) {
	for _, router := range []bool{false, true} {
		if _, err := CommandHash(routeCommand(router)); err != nil {
			t.Fatalf("valid route container (router %t) refused: %v", router, err)
		}
	}
	for name, change := range map[string]func(*agentv1.Command, *agentv1.EnsureContainer){
		"volume": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.VolumeId = uuid.NewString() },
		"bridge": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.NetworkMode = agentv1.NetworkMode_NETWORK_MODE_BRIDGE
		},
		"published port": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Ports = []*agentv1.PortBinding{{HostPort: 1, ContainerPort: 1, Transport: "tcp"}}
		},
		"environment": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.PlainEnv = []*agentv1.PlainEnv{{Name: "A", Value: "b"}}
		},
		"postgres section": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres = &agentv1.PostgresContainerConfig{Port: 5432}
		},
		"redis engine": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Engine = "redis" },
		"both kinds": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Route.Router = &agentv1.PostgresRouterConfig{Address: "10.0.0.2", WriterPort: 5000, ReaderPort: 5001}
		},
		"missing credential": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Route.Credentials = p.Route.Credentials[1:]
			c.Input.Secrets = p.Route.Credentials
		},
		"secrets differ":   func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets = c.Input.Secrets[:1] },
		"loopback address": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Route.Pooler.Address = "127.0.0.1" },
		"same ports":       func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Route.Pooler.Port = 5432 },
		"wide port":        func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Route.Pooler.Port = 70000 },
		"wrong purpose": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Route.Credentials[0].Purpose = "superuser"
			c.Input.Secrets = p.Route.Credentials
		},
	} {
		command := routeCommand(false)
		change(command, command.Input.GetEnsureContainer())
		if _, err := CommandHash(command); err == nil {
			t.Errorf("%s: route container accepted", name)
		}
	}
	router := routeCommand(true)
	router.Input.GetEnsureContainer().Route.Router.Backends = append(router.Input.GetEnsureContainer().Route.Router.Backends, proto.Clone(router.Input.GetEnsureContainer().Route.Router.Backends[0]).(*agentv1.PostgresRouteBackend))
	if _, err := CommandHash(router); err == nil {
		t.Error("router with a repeated backend accepted")
	}
}

// A missing container runs nothing, so absence proves a stop.
func TestAbsentContainerProvesStop(t *testing.T) {
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{}}
	step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED}
	if err := CheckStepEffects(step, []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "absent"}}); err != nil {
		t.Fatal("absent container did not prove a stop", err)
	}
}
