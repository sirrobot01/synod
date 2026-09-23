package operation

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPatroniContainerCommandBindings(t *testing.T) {
	command := postgresContainerCommand()
	p := command.Input.GetEnsureContainer()
	p.SecretEnv = nil
	for _, purpose := range []string{"patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		command.Input.Secrets = append(command.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
	}
	p.Postgres.Patroni = &agentv1.PatroniContainerConfig{RequiredFreeBytes: 1 << 30, Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"}, ProvisionId: uuid.NewString(), AllowInitialization: true}
	for _, reference := range command.Input.Secrets {
		p.Postgres.Patroni.Credentials = append(p.Postgres.Patroni.Credentials, proto.Clone(reference).(*agentv1.SecretReference))
	}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	opPrepare := Operation{ID: uuid.MustParse(command.OperationId), InstallationID: uuid.MustParse(command.Epoch.InstallationId), ClusterID: uuid.MustParse(command.Epoch.ClusterId), DesiredRevision: command.DesiredRevision, Kind: "cluster.create"}
	preparation := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS}}}
	if err := ValidatePlan(preparation, opPrepare); err != nil {
		t.Fatal("native credentials refused by preparation plan", err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"missing_copy_space":  func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.RequiredFreeBytes = 0 },
		"negative_copy_space": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.RequiredFreeBytes = -1 },
		"missing_ref":         func(c *agentv1.Command) { c.Input.Secrets = c.Input.Secrets[:7] },
		"changed_version":     func(c *agentv1.Command) { c.Input.Secrets[7].Version++ },
		"duplicate_identity": func(c *agentv1.Command) {
			n := c.Input.GetEnsureContainer().Postgres.Patroni
			n.Credentials[7].CredentialId = n.Credentials[6].CredentialId
			c.Input.Secrets[7].CredentialId = n.Credentials[6].CredentialId
		},
		"duplicate_purpose": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.Patroni.Credentials[7].Purpose = "patroni_member_tls"
		},
		"unknown_purpose": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.Patroni.Credentials[7].Purpose = "arbitrary"
			c.Input.Secrets[7].Purpose = "arbitrary"
		},
		"nil_binding": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.Credentials[7] = nil },
		"wrong_application": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.ApplicationCredentialId = uuid.NewString()
		},
		"mixed_topology": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.ReplicaSource = &agentv1.PostgresReplicaSource{Address: "192.0.2.30", Port: 5432, ResourceId: uuid.NewString()}
		},
		"secret_environment": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().SecretEnv = []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: "superuser"}}
		},
		"plain_environment": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().PlainEnv = []*agentv1.PlainEnv{{Name: "PGHOST", Value: "example"}}
		},
		"bad_address":       func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.Address = "127.0.0.1" },
		"oversized_port":    func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.RestPort = 65536 },
		"port_collision":    func(c *agentv1.Command) { p := c.Input.GetEnsureContainer().Postgres; p.Patroni.RestPort = p.Port },
		"missing_provision": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.ProvisionId = "" },
		"bad_network": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.Patroni.PeerNetworks = []string{"192.0.2.1/24"}
		},
		"bad_DCS": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.Patroni.DcsEndpoints = []string{"https://private@example:2379"}
		},
		"standalone_recovery": func(c *agentv1.Command) {
			c.Input.Payload = &agentv1.CommandInput_RecoverPostgresSetup{RecoverPostgresSetup: &agentv1.RecoverPostgresSetup{Postgres: c.Input.GetEnsureContainer().Postgres}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("invalid Patroni command accepted")
			}
		})
	}
	for name, change := range map[string]func(*agentv1.Command){
		"copy_space":     func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.RequiredFreeBytes++ },
		"initialization": func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.AllowInitialization = false },
		"policy":         func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.SynchronousReplication = true },
		"provision":      func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.ProvisionId = uuid.NewString() },
		"member":         func(c *agentv1.Command) { c.Input.GetEnsureContainer().Postgres.Patroni.Address = "192.0.2.11" },
		"credential": func(c *agentv1.Command) {
			c.Input.GetEnsureContainer().Postgres.Patroni.Credentials[7].Version++
			c.Input.Secrets[7].Version++
		},
	} {
		t.Run("hash_"+name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed)
			other, err := CommandHash(changed)
			if err != nil || hash == other {
				t.Fatal("Patroni input is not bound to its hash", err)
			}
		})
	}
	start := proto.Clone(command).(*agentv1.Command)
	start.Input.Payload = &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: start.Input.GetEnsureContainer(), AllowInitialization: true}}
	start.Input.Secrets = nil
	startHash, err := CommandHash(start)
	if err != nil {
		t.Fatal("valid Patroni start refused", err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"secret_delivery":  func(c *agentv1.Command) { c.Input.Secrets = command.Input.Secrets },
		"absent_container": func(c *agentv1.Command) { c.Input.GetStartPatroniMember().Container = nil },
		"standalone":       func(c *agentv1.Command) { c.Input.GetStartPatroniMember().Container.Postgres.Patroni = nil },
		"initialization_not_authorized": func(c *agentv1.Command) {
			c.Input.GetStartPatroniMember().Container.Postgres.Patroni.AllowInitialization = false
		},
		"missing_binding": func(c *agentv1.Command) { c.Input.GetStartPatroniMember().Container.Postgres.Patroni.Credentials = nil },
	} {
		t.Run("start_"+name, func(t *testing.T) {
			changed := proto.Clone(start).(*agentv1.Command)
			change(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("invalid native start accepted")
			}
		})
	}
	start.Input.GetStartPatroniMember().AllowInitialization = false
	restartHash, err := CommandHash(start)
	if err != nil || restartHash == startHash {
		t.Fatal("restart permission is not bound", err)
	}
	start.InputHash = restartHash[:]
	op := Operation{ID: uuid.MustParse(start.OperationId), InstallationID: uuid.MustParse(start.Epoch.InstallationId), ClusterID: uuid.MustParse(start.Epoch.ClusterId), DesiredRevision: start.DesiredRevision, Kind: "cluster.start"}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{{Command: start, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING}}}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal("valid native start plan refused", err)
	}
	plan.Steps[0].CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("native bootstrap claimed a safe cancellation boundary")
	}
	plan.Steps[0].CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION
	plan.Steps[0].Postcondition = agentv1.Postcondition_POSTCONDITION_ENGINE_READY
	if err := ValidatePlan(plan, op); err == nil {
		t.Fatal("native startup claimed application readiness")
	}
}
