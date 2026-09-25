package operation

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresReadinessBindsTLSIdentityAndTrust(t *testing.T) {
	root, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "primary", Address: "127.0.0.1", Port: 5432, Database: "app", Role: "synod_app", PostgresTls: &agentv1.PostgresTLS{RootCaPem: root, ServerName: "192.0.2.10"}}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.CheckReadiness){
		"missing root":     func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = nil },
		"malformed root":   func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = []byte("not a CA") },
		"missing identity": func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "" },
		"DNS identity":     func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "db.example" },
		"nonlocal target":  func(p *agentv1.CheckReadiness) { p.Address = "192.0.2.10" },
		"redis":            func(p *agentv1.CheckReadiness) { p.Engine = "redis"; p.PostgresRole = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(command).(*agentv1.Command)
			change(bad.Input.GetCheckReadiness())
			if _, err := CommandHash(bad); err == nil {
				t.Fatal("invalid TLS readiness admitted")
			}
		})
	}
	for name, change := range map[string]func(*agentv1.CheckReadiness){
		"identity": func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "192.0.2.11" },
		"trust":    func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = append(p.PostgresTls.RootCaPem, '\n') },
		"mode":     func(p *agentv1.CheckReadiness) { p.PostgresTls = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed.Input.GetCheckReadiness())
			other, err := CommandHash(changed)
			if err != nil || other == hash {
				t.Fatal("TLS contract not bound", err)
			}
		})
	}
}

// Role any serves Patroni members over TLS. Its receipt must name the observed role.
func TestPostgresReadinessForAnyRole(t *testing.T) {
	root, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "any", Address: "127.0.0.2", Port: 5432, Database: "app", Role: "synod_app", PostgresTls: &agentv1.PostgresTLS{RootCaPem: root, ServerName: "192.0.2.10"}}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal("valid role-any readiness refused", err)
	}
	for name, change := range map[string]func(*agentv1.CheckReadiness){
		"without TLS":      func(p *agentv1.CheckReadiness) { p.PostgresTls = nil },
		"with replication": func(p *agentv1.CheckReadiness) { p.PostgresReplication = &agentv1.PostgresReplicationCheck{} },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(command).(*agentv1.Command)
			change(bad.Input.GetCheckReadiness())
			if _, err := CommandHash(bad); err == nil {
				t.Fatal("invalid role-any readiness admitted")
			}
		})
	}
	command.InputHash = hash[:]
	step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_ENGINE_READY}
	effect := func(state string) []*agentv1.Effect {
		return []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "readiness:" + command.CommandId, State: state}}
	}
	for state, proved := range map[string]bool{"ready_primary": true, "ready_replica": true, "ready": false, "ready_unknown": false} {
		if err := CheckStepEffects(step, effect(state)); (err == nil) != proved {
			t.Fatalf("role any with state %q: %v", state, err)
		}
	}
	command.Input.GetCheckReadiness().PostgresRole = "primary"
	if err := CheckStepEffects(step, effect("ready_primary")); err == nil {
		t.Fatal("a fixed-role check accepted a role-any receipt")
	}
}
