package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestReadinessPropagatesRedisReplicationExpectation(t *testing.T) {
	for _, test := range []struct {
		name, role, purpose string
		check               *agentv1.RedisReplicationCheck
		want                *engine.RedisReplicationCheck
	}{
		{name: "application ping", role: "synod_app", purpose: "application"},
		{name: "primary", role: "synod_admin", purpose: "superuser",
			check: &agentv1.RedisReplicationCheck{ExpectedRole: "primary"},
			want:  &engine.RedisReplicationCheck{ExpectedRole: "primary"}},
		{name: "replica", role: "synod_admin", purpose: "superuser",
			check: &agentv1.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &agentv1.RedisUpstream{Address: "2001:db8::8", Port: 16379}},
			want:  &engine.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &engine.RedisUpstream{Address: "2001:db8::8", Port: 16379}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := &readinessCapture{}
			executor := &Executor{options: Options{Probers: map[string]engine.Prober{"redis": capture}}}
			command := &agentv1.Command{CommandId: uuid.NewString(), DesiredRevision: 12, Input: &agentv1.CommandInput{
				ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second),
				Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Purpose: test.purpose, Version: 1}},
				Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
					Address: "127.0.0.1", Port: 16380, Database: "0", Engine: "redis", Role: test.role, RedisReplication: test.check,
				}},
			}}
			secrets := runtime.Secrets{"application": []byte("application-only"), "superuser": []byte("admin-only"), "replication": []byte("replication-only")}
			result, err := executor.readiness(t.Context(), command, secrets)
			if err != nil || len(result.Effects) != 1 || result.Effects[0].State != readyState || result.Effects[0].Active {
				t.Fatalf("readiness did not return a quiescent ready effect: result=%+v error=%v", result, err)
			}
			want := engine.Endpoint{Address: "127.0.0.1", Port: 16380, Database: "0", Role: test.role, RedisReplication: test.want}
			if !reflect.DeepEqual(capture.endpoint, want) || capture.revision != 12 || capture.calls != 1 {
				t.Fatalf("native probe lost its typed endpoint or revision: %+v", capture.endpoint)
			}
			if string(capture.password) != string(secrets[test.purpose]) {
				t.Fatal("probe received a credential for the wrong purpose")
			}
			if test.check != nil && test.check.Upstream != nil {
				capture.endpoint.RedisReplication.Upstream.Address = "192.0.2.10"
				if test.check.Upstream.Address != "2001:db8::8" {
					t.Fatal("probe endpoint mutation changed the immutable command")
				}
			}
		})
	}
}

func TestReadinessPreservesNativeReplicationFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	capture := &readinessCapture{cancel: cancel}
	executor := &Executor{options: Options{Probers: map[string]engine.Prober{"redis": capture}}}
	command := &agentv1.Command{CommandId: uuid.NewString(), DesiredRevision: 12, Input: &agentv1.CommandInput{
		ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second),
		Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Purpose: "superuser", Version: 1}},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: "127.0.0.1", Port: 16380, Database: "0", Engine: "redis", Role: "synod_admin",
			RedisReplication: &agentv1.RedisReplicationCheck{ExpectedRole: "replica", Upstream: &agentv1.RedisUpstream{Address: "192.0.2.8", Port: 16379}},
		}},
	}}
	result, err := executor.readiness(ctx, command, runtime.Secrets{"superuser": []byte("admin-only")})
	if !errors.Is(err, ErrEngineNotReady) || failureCode(err) != "redis_upstream_mismatch" || len(result.Effects) != 0 {
		t.Fatalf("native mismatch did not remain a fixed failed readiness result: result=%+v error=%v", result, err)
	}
}

type readinessCapture struct {
	endpoint engine.Endpoint
	password []byte
	revision int64
	calls    int
	cancel   context.CancelFunc
}

func (p *readinessCapture) Probe(_ context.Context, endpoint engine.Endpoint, password []byte, revision int64) engine.Observation {
	p.endpoint, p.password, p.revision = endpoint, password, revision
	p.calls++
	passed := engine.Probe{State: engine.ProbePassed}
	observed := engine.Observation{Reachable: passed, ProcessLive: passed, Ready: passed, Endpoint: passed, ObservedAt: time.Now(), ObservedRevision: revision}
	if p.cancel != nil {
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: "redis_upstream_mismatch"}
		observed.Endpoint = engine.Probe{}
		p.cancel()
	}
	return observed
}

// An engine this agent cannot probe is a different problem from a probe that ran and
// failed. Both fail the step, and the failure code must tell them apart.
func TestReadinessNamesAnUnavailableProber(t *testing.T) {
	executor := &Executor{options: Options{Probers: map[string]engine.Prober{"postgres": nil}}}
	for name, engineName := range map[string]string{"unregistered": "redis", "registered as nil": "postgres"} {
		t.Run(name, func(t *testing.T) {
			command := &agentv1.Command{
				DesiredRevision: 1,
				Input: &agentv1.CommandInput{
					ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second),
					Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
						Address: "127.0.0.1", Port: 6379, Database: "0", Role: "synod_app", Engine: engineName,
					}},
				},
			}
			result, err := executor.readiness(t.Context(), command, nil)
			if !errors.Is(err, ErrEngineNotReady) {
				t.Fatalf("err=%v, want the step to fail", err)
			}
			if len(result.Effects) != 0 {
				t.Fatalf("effects=%+v, want none for a check that never ran", result.Effects)
			}
			if code := failureCode(err); code != "engine_prober_unavailable" {
				t.Fatalf("code=%q, want the missing prober named", code)
			}
		})
	}
}

func TestReadinessPropagatesPostgresNativeRole(t *testing.T) {
	for _, role := range []string{"primary", "replica"} {
		capture := &readinessCapture{}
		executor := &Executor{options: Options{Probers: map[string]engine.Prober{"postgres": capture}}}
		command := &agentv1.Command{CommandId: uuid.NewString(), DesiredRevision: 12, Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second), Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Purpose: "application", Version: 1}}, Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Address: "127.0.0.1", Port: 5432, Database: "app", Role: "synod_app", Engine: "postgres", PostgresRole: role}}}}
		command.Input.GetCheckReadiness().PostgresTls = &agentv1.PostgresTLS{RootCaPem: []byte("public-ca"), ServerName: "192.0.2.10"}
		result, err := executor.readiness(t.Context(), command, runtime.Secrets{"application": []byte("application-only")})
		if capture.endpoint.PostgresTLS == nil || capture.endpoint.PostgresTLS.ServerName != "192.0.2.10" || string(capture.endpoint.PostgresTLS.RootCAPEM) != "public-ca" {
			t.Fatal("TLS settings lost")
		}
		capture.endpoint.PostgresTLS.RootCAPEM[0] = 'x'
		if string(command.Input.GetCheckReadiness().PostgresTls.RootCaPem) != "public-ca" {
			t.Fatal("probe shares command trust storage")
		}
		if err != nil || len(result.Effects) != 1 || capture.endpoint.PostgresRole != role || capture.endpoint.Role != "synod_app" || capture.revision != 12 || string(capture.password) != "application-only" {
			t.Fatalf("lost PostgreSQL role expectation: %+v %+v %v", capture.endpoint, result, err)
		}
	}
}

func TestReadinessPropagatesPostgresReplicationIdentity(t *testing.T) {
	capture := &readinessCapture{}
	executor := &Executor{options: Options{Probers: map[string]engine.Prober{"postgres": capture}}}
	command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: uuid.NewString(), ClusterId: uuid.NewString()}, CommandId: uuid.NewString(), DesiredRevision: 125, Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second), Secrets: []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}}, Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "replica", Address: "127.0.0.1", Port: 5432, Database: "postgres", Role: "synod_admin", PostgresReplication: &agentv1.PostgresReplicationCheck{Source: &agentv1.PostgresReplicaSource{Address: "192.0.2.1", Port: 15432, ResourceId: uuid.NewString()}, MaxReplayLagBytes: 16 << 20, MaxMessageAge: durationpb.New(60 * time.Second)}}}}}
	result, err := executor.readiness(t.Context(), command, runtime.Secrets{"superuser": []byte("native-admin"), "application": []byte("unused-application")})
	got := capture.endpoint.PostgresReplication
	expected := command.Input.GetCheckReadiness().PostgresReplication
	if err != nil || len(result.Effects) != 1 || got == nil || got.InstallationID != command.Epoch.InstallationId || got.ClusterID != command.Epoch.ClusterId || got.ResourceID != command.Input.ResourceId || got.Source == nil || got.Source.Address != expected.Source.Address || got.Source.Port != expected.Source.Port || got.Source.ResourceID != expected.Source.ResourceId || got.MaxReplayLagBytes != expected.MaxReplayLagBytes || got.MaxMessageAge != 60*time.Second || string(capture.password) != "native-admin" {
		t.Fatalf("lost native replication contract: %+v %v", got, err)
	}
}
