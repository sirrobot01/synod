package operation

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func postgresContainerCommand() *agentv1.Command {
	command := commandFixture()
	application, superuser, replication := uuid.NewString(), uuid.NewString(), uuid.NewString()
	command.Input.Secrets = []*agentv1.SecretReference{
		{CredentialId: superuser, Version: 2, Purpose: "superuser"},
		{CredentialId: application, Version: 3, Purpose: "application"},
		{CredentialId: replication, Version: 4, Purpose: "replication"},
	}
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		Engine: "postgres", ImageDigest: "postgres@sha256:" + strings.Repeat("a", 64), VolumeId: uuid.NewString(),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
		Postgres: &agentv1.PostgresContainerConfig{Port: 15432, ApplicationCredentialId: application, ApplicationCredentialVersion: 3,
			SuperuserCredentialId: superuser, SuperuserCredentialVersion: 2, ReplicationCredentialId: replication, ReplicationCredentialVersion: 4},
		SecretEnv: []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}, {Name: "SYNOD_APP_VERIFIER", Purpose: "application"}, {Name: "SYNOD_REPL_VERIFIER", Purpose: "replication"}},
	}}
	return command
}

func TestPostgresContainerRequiresBoundCredentialConfiguration(t *testing.T) {
	for name, change := range map[string]func(*agentv1.Command, *agentv1.EnsureContainer){
		"missing configuration": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres = nil },
		"wrong engine":          func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Engine = "redis" },
		"both configurations": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Redis = &agentv1.RedisContainerConfig{Port: 6379, Persistence: "none", MaxmemoryBytes: 48 << 20, EvictionPolicy: "noeviction"}
		},
		"zero port":    func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.Port = 0 },
		"invalid port": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.Port = 65536 },
		"missing application identity": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres.ApplicationCredentialId = ""
		},
		"nil admin identity": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres.SuperuserCredentialId = uuid.Nil.String()
		},
		"noncanonical identity": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres.ApplicationCredentialId = strings.ReplaceAll(p.Postgres.ApplicationCredentialId, "-", "")
			c.Input.Secrets[1].CredentialId = p.Postgres.ApplicationCredentialId
		},
		"shared identity": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres.ApplicationCredentialId = p.Postgres.SuperuserCredentialId
			c.Input.Secrets[1].CredentialId = p.Postgres.SuperuserCredentialId
		},
		"missing replication identity": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.ReplicationCredentialId = "" },
		"shared replication identity": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			p.Postgres.ReplicationCredentialId = p.Postgres.ApplicationCredentialId
			c.Input.Secrets[2].CredentialId = p.Postgres.ApplicationCredentialId
		},
		"zero replication version":         func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.ReplicationCredentialVersion = 0 },
		"missing replication reference":    func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets = c.Input.Secrets[:2] },
		"mismatched replication reference": func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets[2].Version++ },
		"plaintext replication binding":    func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.SecretEnv[2].Name = "SYNOD_REPL_PASSWORD" },
		"zero application version":         func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.ApplicationCredentialVersion = 0 },
		"negative admin version":           func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.SuperuserCredentialVersion = -1 },
		"missing references":               func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets = nil },
		"missing application reference": func(c *agentv1.Command, _ *agentv1.EnsureContainer) {
			c.Input.Secrets = c.Input.Secrets[:1]
		},
		"nil reference": func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets[1] = nil },
		"duplicate purpose": func(c *agentv1.Command, _ *agentv1.EnsureContainer) {
			c.Input.Secrets[1].Purpose = "superuser"
		},
		"wrong purpose": func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets[1].Purpose = "replication" },
		"mismatched identity": func(c *agentv1.Command, _ *agentv1.EnsureContainer) {
			c.Input.Secrets[1].CredentialId = uuid.NewString()
		},
		"mismatched version": func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets[0].Version++ },
		"extra reference": func(c *agentv1.Command, _ *agentv1.EnsureContainer) {
			c.Input.Secrets = append(c.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"})
		},
		"missing binding": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.SecretEnv = p.SecretEnv[:1] },
		"nil binding":     func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.SecretEnv[1] = nil },
		"plaintext application binding": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.SecretEnv[1].Name = "SYNOD_APP_PASSWORD"
		},
		"swapped purposes": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.SecretEnv[0].Purpose, p.SecretEnv[1].Purpose = p.SecretEnv[1].Purpose, p.SecretEnv[0].Purpose
		},
		"extra binding": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.SecretEnv = append(p.SecretEnv, &agentv1.SecretEnv{Name: "PGPASSWORD", Purpose: "superuser"})
		},
		"auth override": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.PlainEnv = []*agentv1.PlainEnv{{Name: "POSTGRES_HOST_AUTH_METHOD", Value: "trust"}}
		},
		"role override": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.PlainEnv = []*agentv1.PlainEnv{{Name: "POSTGRES_USER", Value: "other_admin"}}
		},
		"SQL input": func(_ *agentv1.Command, p *agentv1.EnsureContainer) {
			p.PlainEnv = []*agentv1.PlainEnv{{Name: "PGOPTIONS", Value: "SELECT pg_sleep(10)"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			command := postgresContainerCommand()
			if _, err := CommandHash(command); err != nil {
				t.Fatalf("valid PostgreSQL configuration: %v", err)
			}
			change(command, command.Input.GetEnsureContainer())
			if _, err := CommandHash(command); err == nil {
				t.Fatal("accepted invalid PostgreSQL configuration")
			}
		})
	}
}

func TestPostgresConfigurationHashBindsPortAndAllCredentials(t *testing.T) {
	command := postgresContainerCommand()
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	encoded, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	var decoded agentv1.Command
	if err := proto.Unmarshal(encoded, &decoded); err != nil || ValidateCommand(&decoded) != nil || !proto.Equal(command, &decoded) {
		t.Fatalf("typed PostgreSQL command did not survive its wire round trip: %v", err)
	}
	for name, change := range map[string]func(*agentv1.Command, *agentv1.PostgresContainerConfig){
		"port":               func(_ *agentv1.Command, p *agentv1.PostgresContainerConfig) { p.Port++ },
		"replication policy": func(_ *agentv1.Command, p *agentv1.PostgresContainerConfig) { p.SynchronousReplication = true },
		"application ID": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ApplicationCredentialId = uuid.NewString()
			c.Input.Secrets[1].CredentialId = p.ApplicationCredentialId
		},
		"application version": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ApplicationCredentialVersion++
			c.Input.Secrets[1].Version = p.ApplicationCredentialVersion
		},
		"replication ID": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ReplicationCredentialId = uuid.NewString()
			c.Input.Secrets[2].CredentialId = p.ReplicationCredentialId
		},
		"replication version": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ReplicationCredentialVersion++
			c.Input.Secrets[2].Version = p.ReplicationCredentialVersion
		},
		"admin ID": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.SuperuserCredentialId = uuid.NewString()
			c.Input.Secrets[0].CredentialId = p.SuperuserCredentialId
		},
		"admin version": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.SuperuserCredentialVersion++
			c.Input.Secrets[0].Version = p.SuperuserCredentialVersion
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed, changed.Input.GetEnsureContainer().Postgres)
			if err := ValidateCommand(changed); !errors.Is(err, ErrCommandConflict) {
				t.Fatalf("credential or port change preserved the old command identity: %v", err)
			}
		})
	}
	command.Epoch.ExecutionEpoch++
	command.Epoch.RecoveryGeneration = uuid.NewString()
	command.TraceId = "recovery"
	if err := ValidateCommand(command); err != nil {
		t.Fatalf("new authority changed the fixed PostgreSQL input: %v", err)
	}
}

func TestPostgresRecoveryBindsConfigurationAndCredentials(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*agentv1.Command)
	}{
		{"missing configuration", func(c *agentv1.Command) { c.Input.GetRecoverPostgresSetup().Postgres = nil }},
		{"missing reference", func(c *agentv1.Command) { c.Input.Secrets = c.Input.Secrets[:1] }},
		{"wrong version", func(c *agentv1.Command) { c.Input.GetRecoverPostgresSetup().Postgres.ApplicationCredentialVersion++ }},
		{"wrong identity", func(c *agentv1.Command) { c.Input.Secrets[0].CredentialId = uuid.NewString() }},
		{"wrong purpose", func(c *agentv1.Command) { c.Input.Secrets[1].Purpose = "replication" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := postgresContainerCommand()
			config := command.Input.GetEnsureContainer().Postgres
			command.Input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: config}}
			if _, err := CommandHash(command); err != nil {
				t.Fatal(err)
			}
			test.change(command)
			if _, err := CommandHash(command); err == nil {
				t.Fatal("accepted mismatched recovery configuration")
			}
		})
	}
}

func TestPostgresRecoveryPostconditionDoesNotClaimReadiness(t *testing.T) {
	command := postgresContainerCommand()
	config := command.Input.GetEnsureContainer().Postgres
	command.Input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: config}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	step := &agentv1.PlanStep{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_SETUP_RECOVERED}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}
	op := Operation{ID: uuid.MustParse(command.OperationId), InstallationID: uuid.MustParse(command.Epoch.InstallationId), ClusterID: uuid.MustParse(command.Epoch.ClusterId), DesiredRevision: command.DesiredRevision}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: "setup_recovered"}
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"ready", "running", "bootstrap_recovered", "setup_refused"} {
		effect.State = state
		if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err == nil {
			t.Fatalf("recovery accepted unrelated state %q", state)
		}
	}
	effect.State = "setup_recovered"
	effect.Active = true
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err == nil {
		t.Fatal("recovery accepted an active effect")
	}
	step.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("recovery accepted unsupported safe-stop cancellation")
	}
}

func TestReplicaCommandsBindSourceAndLeastPrivilegeSecrets(t *testing.T) {
	base := postgresContainerCommand()
	p := base.Input.GetEnsureContainer()
	p.Postgres.ReplicaSource = &agentv1.PostgresReplicaSource{Address: "127.0.0.1", Port: 5432, ResourceId: uuid.NewString()}
	p.SecretEnv[0] = &agentv1.SecretEnv{Name: "SYNOD_REPL_PASSWORD", Purpose: "replication"}
	base.Input.Secrets = base.Input.Secrets[1:]
	if _, err := CommandHash(base); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"admin delivery": func(c *agentv1.Command) {
			p := c.Input.GetEnsureContainer().Postgres
			c.Input.Secrets = append(c.Input.Secrets, &agentv1.SecretReference{CredentialId: p.SuperuserCredentialId, Version: p.SuperuserCredentialVersion, Purpose: "superuser"})
		},
		"missing replication": func(c *agentv1.Command) { c.Input.Secrets = c.Input.Secrets[:1] },
		"DNS source":          func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.ReplicaSource.Address = "localhost" },
		"multicast":           func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.ReplicaSource.Address = "224.0.0.1" },
		"zone":                func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.ReplicaSource.Address = "fe80::1%en0" },
		"invalid port":        func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.ReplicaSource.Port = 65536 },
		"self source": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.ReplicaSource.ResourceId = c.Input.ResourceId
		},
		"unbound source": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.ReplicaSource.ResourceId = "" },
		"primary password": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().SecretEnv[0] = &agentv1.SecretEnv{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(base).(*agentv1.Command)
			change(c)
			if _, err := CommandHash(c); err == nil {
				t.Fatal("accepted invalid replica input")
			}
		})
	}
	clone := proto.Clone(base).(*agentv1.Command)
	clone.Input.Payload = &agentv1.CommandInput_ClonePostgresReplica{ClonePostgresReplica: &agentv1.ClonePostgresReplica{RequiredFreeBytes: 1 << 30, Postgres: clone.Input.GetEnsureContainer().Postgres}}
	if _, err := CommandHash(clone); err != nil {
		t.Fatal(err)
	}
	for _, bytes := range []int64{0, -1, 1<<50 + 1} {
		changed := proto.Clone(clone).(*agentv1.Command)
		changed.Input.GetClonePostgresReplica().RequiredFreeBytes = bytes
		if _, err := CommandHash(changed); err == nil {
			t.Fatalf("accepted invalid clone space %d", bytes)
		}
	}
	for _, bytes := range []int64{1, 1 << 50} {
		changed := proto.Clone(clone).(*agentv1.Command)
		changed.Input.GetClonePostgresReplica().RequiredFreeBytes = bytes
		if _, err := CommandHash(changed); err != nil {
			t.Fatalf("refused valid clone space %d: %v", bytes, err)
		}
	}
	hash, err := CommandHash(clone)
	if err != nil {
		t.Fatal(err)
	}
	clone.InputHash = hash[:]
	clone.Input.GetClonePostgresReplica().RequiredFreeBytes++
	if err := ValidateCommand(clone); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("clone space changed without invalidating the command hash: %v", err)
	}
	clone.Input.GetClonePostgresReplica().Postgres.ReplicaSource = nil
	if _, err := CommandHash(clone); err == nil {
		t.Fatal("cloned primary configuration")
	}
	recover := proto.Clone(base).(*agentv1.Command)
	recover.Input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: recover.Input.GetEnsureContainer().Postgres}}
	if _, err := CommandHash(recover); err == nil {
		t.Fatal("recovered setup on a replica")
	}
	prepare := proto.Clone(base).(*agentv1.Command)
	prepare.Input.Payload = &agentv1.CommandInput_PreparePostgresReplica{PreparePostgresReplica: &agentv1.PreparePostgresReplica{}}
	if _, err := CommandHash(prepare); err == nil {
		t.Fatal("prepare accepted secrets")
	}
	prepare.Input.Secrets = nil
	if _, err := CommandHash(prepare); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresReplicationReadinessContract(t *testing.T) {
	base := commandFixture()
	base.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "superuser"}}
	base.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "replica", Address: "127.0.0.1", Port: 5432, Database: "postgres", Role: "synod_admin", PostgresReplication: &agentv1.PostgresReplicationCheck{Source: &agentv1.PostgresReplicaSource{Address: "192.0.2.1", Port: 5432, ResourceId: uuid.NewString()}, MaxReplayLagBytes: 16 << 20, MaxMessageAge: durationpb.New(60 * time.Second)}}}
	baseline, err := CommandHash(base)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.Command, *agentv1.CheckReadiness){
		"wrong engine":           func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.Engine = "redis"; p.PostgresRole = "" },
		"application login":      func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.Role = "synod_app" },
		"application database":   func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.Database = "app" },
		"application credential": func(c *agentv1.Command, _ *agentv1.CheckReadiness) { c.Input.Secrets[0].Purpose = "application" },
		"missing credential":     func(c *agentv1.Command, _ *agentv1.CheckReadiness) { c.Input.Secrets = nil },
		"missing source":         func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresReplication.Source = nil },
		"self source": func(c *agentv1.Command, p *agentv1.CheckReadiness) {
			p.PostgresReplication.Source.ResourceId = c.Input.ResourceId
		},
		"DNS source": func(_ *agentv1.Command, p *agentv1.CheckReadiness) {
			p.PostgresReplication.Source.Address = "localhost"
		},
		"zero source port":    func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresReplication.Source.Port = 0 },
		"bad source identity": func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresReplication.Source.ResourceId = "" },
		"zero limit":          func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresReplication.MaxReplayLagBytes = 0 },
		"unbounded backlog": func(_ *agentv1.Command, p *agentv1.CheckReadiness) {
			p.PostgresReplication.MaxReplayLagBytes = 1<<30 + 1
		},
		"absent age": func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresReplication.MaxMessageAge = nil },
		"negative age": func(_ *agentv1.Command, p *agentv1.CheckReadiness) {
			p.PostgresReplication.MaxMessageAge = durationpb.New(-time.Second)
		},
		"unbounded age": func(_ *agentv1.Command, p *agentv1.CheckReadiness) {
			p.PostgresReplication.MaxMessageAge = durationpb.New(121 * time.Second)
		},
		"primary with replica fields": func(_ *agentv1.Command, p *agentv1.CheckReadiness) { p.PostgresRole = "primary" },
	} {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(base).(*agentv1.Command)
			change(c, c.Input.GetCheckReadiness())
			if _, err := CommandHash(c); err == nil {
				t.Fatal("accepted invalid native check")
			}
		})
	}
	for _, change := range []func(*agentv1.PostgresReplicationCheck){func(p *agentv1.PostgresReplicationCheck) { p.Source.ResourceId = uuid.NewString() }, func(p *agentv1.PostgresReplicationCheck) { p.MaxReplayLagBytes++ }, func(p *agentv1.PostgresReplicationCheck) { p.MaxMessageAge = durationpb.New(30 * time.Second) }} {
		c := proto.Clone(base).(*agentv1.Command)
		change(c.Input.GetCheckReadiness().PostgresReplication)
		hash, err := CommandHash(c)
		if err != nil || hash == baseline {
			t.Fatalf("native input escaped hash: %v", err)
		}
	}
	primary := proto.Clone(base).(*agentv1.Command)
	primary.Input.GetCheckReadiness().PostgresRole = "primary"
	primary.Input.GetCheckReadiness().PostgresReplication = &agentv1.PostgresReplicationCheck{}
	if _, err := CommandHash(primary); err != nil {
		t.Fatal(err)
	}
}

func TestPriorCloneVerificationBindsIdentityWithoutSecrets(t *testing.T) {
	base := postgresContainerCommand()
	config := base.Input.GetEnsureContainer().Postgres
	config.ReplicaSource = &agentv1.PostgresReplicaSource{Address: "127.0.0.1", Port: 5432, ResourceId: uuid.NewString()}
	base.Input.Secrets = nil
	base.Input.Payload = &agentv1.CommandInput_VerifyPostgresReplicaClone{VerifyPostgresReplicaClone: &agentv1.VerifyPostgresReplicaClone{Postgres: config, CloneCommandId: uuid.NewString()}}
	hash, err := CommandHash(base)
	if err != nil {
		t.Fatal(err)
	}
	base.InputHash = hash[:]
	for name, alter := range map[string]func(*agentv1.Command){
		"missing configuration": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaClone().Postgres = nil },
		"primary configuration": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaClone().Postgres.ReplicaSource = nil },
		"self source": func(c *agentv1.Command) {
			c.Input.GetVerifyPostgresReplicaClone().Postgres.ReplicaSource.ResourceId = c.Input.ResourceId
		},
		"missing original": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaClone().CloneCommandId = "" },
		"nil original":     func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaClone().CloneCommandId = uuid.Nil.String() },
		"noncanonical original": func(c *agentv1.Command) {
			c.Input.GetVerifyPostgresReplicaClone().CloneCommandId = strings.ReplaceAll(uuid.NewString(), "-", "")
		},
		"self original": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaClone().CloneCommandId = c.CommandId },
		"bad version": func(c *agentv1.Command) {
			c.Input.GetVerifyPostgresReplicaClone().Postgres.ApplicationCredentialVersion = 0
		},
		"secret delivery": func(c *agentv1.Command) {
			c.Input.Secrets = []*agentv1.SecretReference{{CredentialId: config.ApplicationCredentialId, Version: config.ApplicationCredentialVersion, Purpose: "application"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(base).(*agentv1.Command)
			alter(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("invalid prior-clone proof accepted")
			}
		})
	}
	for _, alter := range []func(*agentv1.VerifyPostgresReplicaClone){
		func(v *agentv1.VerifyPostgresReplicaClone) { v.CloneCommandId = uuid.NewString() },
		func(v *agentv1.VerifyPostgresReplicaClone) { v.Postgres.ReplicationCredentialVersion++ },
	} {
		changed := proto.Clone(base).(*agentv1.Command)
		alter(changed.Input.GetVerifyPostgresReplicaClone())
		if err := ValidateCommand(changed); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed verification kept input hash: %v", err)
		}
	}
	step := &agentv1.PlanStep{Command: base, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED}
	op := Operation{ID: uuid.MustParse(base.OperationId), InstallationID: uuid.MustParse(base.Epoch.InstallationId), ClusterID: uuid.MustParse(base.Epoch.ClusterId), DesiredRevision: base.DesiredRevision}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatalf("read-only verification requires a new copy: %v", err)
	}
	step.Postcondition = agentv1.Postcondition_POSTCONDITION_ENGINE_READY
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("clone verification claimed application readiness")
	}
	step.Postcondition = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED
	step.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("verification permits unsupported cancellation")
	}
}

func TestFailedCloneVerificationRequiresItsOwnProof(t *testing.T) {
	base := postgresContainerCommand()
	config := base.Input.GetEnsureContainer().Postgres
	config.ReplicaSource = &agentv1.PostgresReplicaSource{Address: "127.0.0.1", Port: 5432, ResourceId: uuid.NewString()}
	base.Input.Secrets = nil
	base.Input.Payload = &agentv1.CommandInput_VerifyPostgresReplicaFailure{VerifyPostgresReplicaFailure: &agentv1.VerifyPostgresReplicaFailure{Postgres: config, CloneCommandId: uuid.NewString()}}
	hash, err := CommandHash(base)
	if err != nil {
		t.Fatal(err)
	}
	base.InputHash = hash[:]
	for name, alter := range map[string]func(*agentv1.Command){
		"nil payload":           func(c *agentv1.Command) { c.Input.Payload = &agentv1.CommandInput_VerifyPostgresReplicaFailure{} },
		"missing configuration": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaFailure().Postgres = nil },
		"primary":               func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaFailure().Postgres.ReplicaSource = nil },
		"self source": func(c *agentv1.Command) {
			c.Input.GetVerifyPostgresReplicaFailure().Postgres.ReplicaSource.ResourceId = c.Input.ResourceId
		},
		"self original":    func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaFailure().CloneCommandId = c.CommandId },
		"missing original": func(c *agentv1.Command) { c.Input.GetVerifyPostgresReplicaFailure().CloneCommandId = "" },
		"secret delivery": func(c *agentv1.Command) {
			c.Input.Secrets = []*agentv1.SecretReference{{CredentialId: config.ApplicationCredentialId, Version: 1, Purpose: "application"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(base).(*agentv1.Command)
			alter(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("invalid failure proof accepted")
			}
		})
	}
	for _, changeID := range []bool{false, true} {
		changed := proto.Clone(base).(*agentv1.Command)
		if changeID {
			changed.Input.GetVerifyPostgresReplicaFailure().CloneCommandId = uuid.NewString()
		} else {
			changed.Input.GetVerifyPostgresReplicaFailure().Postgres.ReplicationCredentialVersion++
		}
		if err := ValidateCommand(changed); !errors.Is(err, ErrCommandConflict) {
			t.Fatalf("changed failure proof retained hash: %v", err)
		}
	}
	step := &agentv1.PlanStep{Command: base, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED}
	op := Operation{ID: uuid.MustParse(base.OperationId), InstallationID: uuid.MustParse(base.Epoch.InstallationId), ClusterID: uuid.MustParse(base.Epoch.ClusterId), DesiredRevision: base.DesiredRevision}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal(err)
	}
	effect := &agentv1.Effect{CommandId: base.CommandId, ResourceId: base.Input.ResourceId, NativeOperationId: "docker-command:" + base.CommandId, State: "replica_refused"}
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"replica_cloned", "replica_prepared", "ready", "bootstrap_failed"} {
		effect.State = state
		if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err == nil {
			t.Fatalf("state %s passed failure proof", state)
		}
	}
	effect.State = "replica_refused"
	effect.Active = true
	if err := CheckStepEffects(step, []*agentv1.Effect{effect}); err == nil {
		t.Fatal("active native work passed failure proof")
	}
	for _, postcondition := range []agentv1.Postcondition{agentv1.Postcondition_POSTCONDITION_ENGINE_READY, agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_CLONED} {
		step.Postcondition = postcondition
		if err := ValidatePlan(plan, op); err == nil {
			t.Fatal("failure verification claimed another postcondition")
		}
	}
	step.Postcondition = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED
	step.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("failure verification permits unsupported cancellation")
	}
}
