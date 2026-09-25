package executor

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// superuserPurpose is the one credential that exists before any other role is created.
const superuserPurpose = "superuser"

// readinessInterval is how often a starting engine is asked again.
const readinessInterval = time.Second

// readiness checks that the engine answers through the endpoint it publishes. It is
// handled here rather than in the container runtime because it is a network question.
//
// It reports and never corrects.
func (e *Executor) readiness(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	// This path does not go through runtime.Execute, which is where every other
	// command gets its timeout, so it applies the same one itself.
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	p := command.Input.GetCheckReadiness()
	prober, ok := e.options.Probers[p.Engine]
	if !ok || prober == nil {
		// Nothing here can tell. Reporting ready would claim a check that never ran, so
		// the step fails. The reason names the condition, because an engine this agent
		// cannot probe and a probe that failed are different problems with different
		// answers, and an empty observation makes them read the same.
		return readinessResult(command, engine.Observation{
			ObservedRevision: command.DesiredRevision,
			Reachable:        engine.Probe{State: engine.ProbeFailed, Reason: noProberCode},
		})
	}
	endpoint := engine.Endpoint{Address: p.Address, Port: uint16(p.Port), Database: p.Database, Role: p.Role, PostgresRole: p.PostgresRole}
	if settings := p.PostgresTls; settings != nil {
		endpoint.PostgresTLS = &engine.PostgresTLS{RootCAPEM: bytes.Clone(settings.RootCaPem), ServerName: settings.ServerName}
	}
	if check := p.PostgresReplication; check != nil {
		endpoint.PostgresReplication = &engine.PostgresReplicationCheck{InstallationID: command.Epoch.InstallationId, ClusterID: command.Epoch.ClusterId, ResourceID: command.Input.ResourceId, MaxReplayLagBytes: check.MaxReplayLagBytes}
		if check.MaxMessageAge != nil {
			endpoint.PostgresReplication.MaxMessageAge = check.MaxMessageAge.AsDuration()
		}
		if check.Source != nil {
			endpoint.PostgresReplication.Source = &engine.PostgresSource{Address: check.Source.Address, Port: check.Source.Port, ResourceID: check.Source.ResourceId}
		}
	}
	if check := p.RedisReplication; check != nil {
		endpoint.RedisReplication = &engine.RedisReplicationCheck{ExpectedRole: check.ExpectedRole}
		if check.Upstream != nil {
			endpoint.RedisReplication.Upstream = &engine.RedisUpstream{Address: check.Upstream.Address, Port: check.Upstream.Port}
		}
	}
	password := secrets[superuserPurpose]
	if len(command.Input.Secrets) == 1 {
		password = secrets[command.Input.Secrets[0].Purpose]
	}
	// A database does not become ready the instant its container starts, so this
	// waits rather than asking once.
	for {
		observed := prober.Probe(ctx, endpoint, password, command.DesiredRevision)
		if observed.Healthy() || ctx.Err() != nil {
			return readinessResult(command, observed)
		}
		select {
		case <-ctx.Done():
			return readinessResult(command, observed)
		case <-time.After(readinessInterval):
		}
	}
}

// readinessResult reports one observation as an effect.
//
// The effect state is a short stable token, because that is what an effect state is:
// the store bounds it to 64 characters of [a-z0-9_]. The detail an operator needs is
// carried by the failure code instead, which names the probe that did not pass.
func readinessResult(command *agentv1.Command, observed engine.Observation) (runtime.Result, error) {
	// A readiness check creates no native object, so its effect is never active.
	// Active means work is still running on the host, and claiming it for an unmet
	// postcondition leaves the command unresolved instead of answered.
	result := runtime.Result{Effects: []*agentv1.Effect{{
		CommandId:         command.CommandId,
		ResourceId:        command.Input.ResourceId,
		NativeOperationId: "readiness:" + command.CommandId,
		State:             readyState,
	}}}
	// Role any reports the observed role, so completion can prove exactly one writer.
	if command.Input.GetCheckReadiness().GetPostgresRole() == "any" && observed.Postgres != nil {
		result.Effects[0].State = readyState + "_" + observed.Postgres.Role
	}
	if observed.Healthy() {
		return result, nil
	}
	return runtime.Result{}, &notReadyError{reason: firstUnmet(observed)}
}

// noProberCode reports an engine this agent has no prober for.
const noProberCode = "engine_prober_unavailable"

// readyState is the effect state a proved readiness check reports.
const readyState = "ready"

// firstUnmet names the probe that stopped this engine being ready, in the order the
// probes depend on each other, so the answer is the earliest thing to look at.
func firstUnmet(observed engine.Observation) string {
	for _, probe := range []engine.Probe{observed.Reachable, observed.ProcessLive, observed.Ready, observed.Endpoint} {
		if probe.State == engine.ProbePassed {
			continue
		}
		if probe.Reason != "" {
			return probe.Reason
		}
		return notReadyCode
	}
	return notReadyCode
}

// notReadyError carries which probe failed, so the failure code names it rather than
// saying only that something did.
type notReadyError struct{ reason string }

func (e *notReadyError) Error() string { return notReadyCode + ": " + e.reason }

// Is lets a caller test for the condition without knowing the reason.
func (e *notReadyError) Is(target error) bool { return target == ErrEngineNotReady }

// ErrEngineNotReady reports an engine that did not answer through its endpoint before
// the step's timeout. The container is running; the operator is told which probe failed.
var ErrEngineNotReady = errors.New(notReadyCode)

const notReadyCode = "engine_not_ready"
