package provision

import (
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// postgresReplicaPlan binds a physical copy to its source, owned target, and final readiness.
func (p *Planner) postgresReplicaPlan(claim operation.Claim, found cluster.Cluster, added resources, nodeID uuid.UUID, port uint16, digest string, intent postgres.ClusterIntent, primary member, upstream *agentv1.RedisUpstream, diskBytes int64) (*agentv1.ExecutionPlan, error) {
	base, err := p.plan(claim, found, added, nodeID, port, digest, intent, nil)
	if err != nil {
		return nil, err
	}
	volume, container, start, app := base.Steps[0], base.Steps[1], base.Steps[2], base.Steps[3]
	config := container.Command.Input.GetEnsureContainer()
	source := &agentv1.PostgresReplicaSource{Address: upstream.Address, Port: upstream.Port, ResourceId: primary.ResourceID.String()}
	config.Postgres.ReplicaSource = source
	config.SecretEnv = []*agentv1.SecretEnv{{Name: "SYNOD_APP_VERIFIER", Purpose: postgres.ApplicationCredential}, {Name: "SYNOD_REPL_VERIFIER", Purpose: postgres.ReplicationCredential}, {Name: "SYNOD_REPL_PASSWORD", Purpose: postgres.ReplicationCredential}}
	admin := container.Command.Input.Secrets[0]
	container.Command.Input.Secrets = container.Command.Input.Secrets[1:]
	start.Command.Input.GetStartContainer().AllowInitialization = false
	app.Command.Input.GetCheckReadiness().PostgresRole = "replica"
	epoch := epochFor(claim, nodeID)
	primaryReady, err := p.step(claim, epochFor(claim, primary.NodeID), found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: primary.ResourceID.String(), Secrets: []*agentv1.SecretReference{admin},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: p.config.EndpointAddress, Port: uint32(primary.Port), Database: "postgres", Role: "synod_admin", Engine: "postgres", PostgresRole: "primary", PostgresReplication: &agentv1.PostgresReplicationCheck{},
		}},
	}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, nil, beforeExecution)
	if err != nil {
		return nil, err
	}
	volume.DependsOn = []string{primaryReady.Command.StepId}
	prepare, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: added.instance.String(), Payload: &agentv1.CommandInput_PreparePostgresReplica{PreparePostgresReplica: &agentv1.PreparePostgresReplica{}},
	}, agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_PREPARED, []string{container.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	clone, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: added.instance.String(), Secrets: container.Command.Input.Secrets,
		Payload: &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{Postgres: config.Postgres, RequiredFreeBytes: diskBytes}},
	}, agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED, []string{prepare.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	stop, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: added.instance.String(), Payload: &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
	}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, []string{clone.Command.StepId}, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
	if err != nil {
		return nil, err
	}
	start.DependsOn = []string{stop.Command.StepId}
	native, err := p.step(claim, epoch, found.DesiredRevision, &agentv1.CommandInput{
		ResourceId: added.instance.String(), Secrets: []*agentv1.SecretReference{admin},
		Payload: &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{
			Address: p.config.EndpointAddress, Port: uint32(port), Database: "postgres", Role: "synod_admin", Engine: "postgres", PostgresRole: "replica",
			PostgresReplication: &agentv1.PostgresReplicationCheck{Source: source, MaxReplayLagBytes: 16 << 20, MaxMessageAge: durationpb.New(30 * time.Second)},
		}},
	}, agentv1.Postcondition_POSTCONDITION_ENGINE_READY, []string{start.Command.StepId}, beforeExecution)
	if err != nil {
		return nil, err
	}
	app.DependsOn = []string{native.Command.StepId}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{primaryReady, volume, container, prepare, clone, stop, start, native, app}}
	// The replica changes the base container, startup, and application inputs before sealing.
	for _, step := range plan.Steps {
		hash, err := operation.CommandHash(step.Command)
		if err != nil {
			return nil, err
		}
		step.Command.InputHash = hash[:]
	}
	return plan, nil
}
