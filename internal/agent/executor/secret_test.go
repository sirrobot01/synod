package executor

import (
	"errors"
	"testing"
	"time"

	"github.com/sirrobot01/synod/internal/engine"

	"github.com/google/uuid"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// A credential is held in session memory and resolved for the command that names it.
func TestSecretIsHeldForTheSessionAndResolved(t *testing.T) {
	native := &measuredRuntime{}
	e, ctx, _ := challenged(t, native)
	credential := uuid.NewString()
	material := &agentv1.SecretMaterial{CredentialId: credential, Version: 1, Purpose: "superuser", Material: []byte("value")}
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: material}}); err != nil {
		t.Fatal(err)
	}
	command := &agentv1.Command{Input: &agentv1.CommandInput{
		Secrets: []*agentv1.SecretReference{{CredentialId: credential, Version: 1, Purpose: "superuser"}},
	}}
	resolved, err := e.resolve(command)
	if err != nil {
		t.Fatal(err)
	}
	if string(resolved["superuser"]) != "value" {
		t.Fatalf("resolved=%q, want the material the session was sent", resolved["superuser"])
	}
	// A command that names no credential resolves to nothing rather than failing.
	if got, err := e.resolve(&agentv1.Command{Input: &agentv1.CommandInput{}}); err != nil || got != nil {
		t.Fatalf("resolved=%v err=%v, want nothing for a command with no reference", got, err)
	}
}

// A command whose credential this session was never sent is refused. Starting an
// engine with no credential is worse than a command that did not run.
func TestAnUnsentSecretRefusesTheCommand(t *testing.T) {
	native := &measuredRuntime{}
	e, ctx, _ := challenged(t, native)
	credential := uuid.NewString()
	material := &agentv1.SecretMaterial{CredentialId: credential, Version: 1, Purpose: "superuser", Material: []byte("value")}
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: material}}); err != nil {
		t.Fatal(err)
	}
	for name, reference := range map[string]*agentv1.SecretReference{
		"a purpose nothing was sent for": {CredentialId: credential, Version: 1, Purpose: "application"},
		// A rotation in flight means two versions exist. Running against the wrong
		// one configures an engine with a credential nobody expects it to have.
		"an older version":   {CredentialId: credential, Version: 2, Purpose: "superuser"},
		"another credential": {CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"},
	} {
		t.Run(name, func(t *testing.T) {
			command := &agentv1.Command{Input: &agentv1.CommandInput{Secrets: []*agentv1.SecretReference{reference}}}
			if _, err := e.resolve(command); !errors.Is(err, ErrSecretUnavailable) {
				t.Fatalf("err=%v, want ErrSecretUnavailable", err)
			}
		})
	}
}

// A malformed push is a protocol error, not a silently ignored message.
func TestAMalformedSecretPushIsRefused(t *testing.T) {
	native := &measuredRuntime{}
	e, ctx, _ := challenged(t, native)
	for name, material := range map[string]*agentv1.SecretMaterial{
		"no credential": {Version: 1, Purpose: "superuser", Material: []byte("value")},
		"no version":    {CredentialId: uuid.NewString(), Purpose: "superuser", Material: []byte("value")},
		"no purpose":    {CredentialId: uuid.NewString(), Version: 1, Material: []byte("value")},
		"no material":   {CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"},
		"absent":        nil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: material}}); err == nil {
				t.Fatalf("a malformed push was accepted: %s", name)
			}
		})
	}
}

// A session that ends takes its credentials with it. Nothing is written down, so a
// new session starts with nothing until the controller sends it again.
func TestSecretsDieWithTheSession(t *testing.T) {
	native := &measuredRuntime{}
	e, ctx, _ := challenged(t, native)
	credential := uuid.NewString()
	material := &agentv1.SecretMaterial{CredentialId: credential, Version: 1, Purpose: "superuser", Material: []byte("value")}
	if _, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: material}}); err != nil {
		t.Fatal(err)
	}
	// A replacement session holds nothing.
	next, _ := localSession(t, e)
	_ = next
	command := &agentv1.Command{Input: &agentv1.CommandInput{
		Secrets: []*agentv1.SecretReference{{CredentialId: credential, Version: 1, Purpose: "superuser"}},
	}}
	if _, err := e.resolve(command); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("err=%v, want a replacement session to hold nothing", err)
	}
}

// The resolved material is a function argument and never part of a command, so a
// command the journal stores can never carry one.
func TestResolvedMaterialIsNeverPartOfACommand(t *testing.T) {
	var _ noderuntime.Secrets = noderuntime.Secrets{}
	command := &agentv1.Command{Input: &agentv1.CommandInput{
		Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}},
	}}
	// The reference type carries an identity, a version, and a purpose. There is no
	// field for material, which is what makes the guarantee structural.
	reference := command.Input.Secrets[0]
	if reference.ProtoReflect().Descriptor().Fields().Len() != 3 {
		t.Fatal("SecretReference gained a field; a credential must never be one of them")
	}
}

// An effect state is a short stable token, not a payload: the store bounds it to 64
// characters of [a-z0-9_] and rejects a receipt carrying anything else. A readiness
// effect that carried its observation was refused by the controller and replayed by
// this agent thousands of times, because a refused result is never acknowledged.
func TestReadinessEffectStateFitsWhatAReceiptAccepts(t *testing.T) {
	command := &agentv1.Command{
		CommandId: uuid.NewString(),
		Input:     &agentv1.CommandInput{ResourceId: uuid.NewString()},
	}
	healthy := engine.Probe{State: engine.ProbePassed}
	result, err := readinessResult(command, engine.Observation{
		Reachable: healthy, ProcessLive: healthy, Ready: healthy, Endpoint: healthy,
		ObservedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Effects) != 1 {
		t.Fatalf("effects=%d, want one", len(result.Effects))
	}
	state := result.Effects[0].State
	if len(state) < 1 || len(state) > 64 {
		t.Fatalf("state=%q is %d bytes, want the 1 to 64 a receipt accepts", state, len(state))
	}
	for _, r := range state {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			t.Fatalf("state %q contains %q, which a receipt refuses", state, r)
		}
	}
}

// The failure code names the probe that did not pass, so an operator learns which one
// rather than only that something did. Every reason must also fit a receipt.
func TestReadinessFailureNamesTheProbeThatFailed(t *testing.T) {
	for name, observed := range map[string]engine.Observation{
		"unreachable": {Reachable: engine.Probe{State: engine.ProbeFailed, Reason: "endpoint_refused_connection"}},
		"rejected": {
			Reachable: engine.Probe{State: engine.ProbePassed}, ProcessLive: engine.Probe{State: engine.ProbePassed},
			Ready: engine.Probe{State: engine.ProbeFailed, Reason: "authentication_rejected"},
		},
		"nothing said why": {Reachable: engine.Probe{State: engine.ProbeFailed}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readinessResult(&agentv1.Command{Input: &agentv1.CommandInput{}}, observed)
			if !errors.Is(err, ErrEngineNotReady) {
				t.Fatalf("err=%v, want ErrEngineNotReady", err)
			}
			code := failureCode(err)
			if code == "" || len(code) > 128 {
				t.Fatalf("code=%q, want a stable code a receipt accepts", code)
			}
			for _, r := range code {
				if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
					t.Fatalf("code %q contains %q, which a receipt refuses", code, r)
				}
			}
		})
	}
}
