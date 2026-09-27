package operation

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func postgresContainerCommand() *agentv1.Command {
	command := commandFixture()
	application, superuser := uuid.NewString(), uuid.NewString()
	command.Input.Secrets = []*agentv1.SecretReference{
		{CredentialId: superuser, Version: 2, Purpose: "superuser"},
		{CredentialId: application, Version: 3, Purpose: "application"},
	}
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		Engine: "postgres", ImageDigest: "postgres@sha256:" + strings.Repeat("a", 64), VolumeId: uuid.NewString(),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20,
		Postgres: &agentv1.PostgresContainerConfig{Port: 15432, ApplicationCredentialId: application, ApplicationCredentialVersion: 3,
			SuperuserCredentialId: superuser, SuperuserCredentialVersion: 2},
		SecretEnv: []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}, {Name: "SYNOD_APP_VERIFIER", Purpose: "application"}},
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
		// PostgreSQL binds exactly its superuser and application credentials.
		"extra credential": func(c *agentv1.Command, p *agentv1.EnsureContainer) {
			c.Input.Secrets = append(c.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: "replication"})
		},
		"zero application version": func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.ApplicationCredentialVersion = 0 },
		"negative admin version":   func(_ *agentv1.Command, p *agentv1.EnsureContainer) { p.Postgres.SuperuserCredentialVersion = -1 },
		"missing references":       func(c *agentv1.Command, _ *agentv1.EnsureContainer) { c.Input.Secrets = nil },
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
		"port": func(_ *agentv1.Command, p *agentv1.PostgresContainerConfig) { p.Port++ },
		"application ID": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ApplicationCredentialId = uuid.NewString()
			c.Input.Secrets[1].CredentialId = p.ApplicationCredentialId
		},
		"application version": func(c *agentv1.Command, p *agentv1.PostgresContainerConfig) {
			p.ApplicationCredentialVersion++
			c.Input.Secrets[1].Version = p.ApplicationCredentialVersion
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
