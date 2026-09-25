package operation

import (
	"errors"
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

	verify := proto.Clone(command).(*agentv1.Command)
	verify.Input.Payload = &agentv1.CommandInput_VerifyPatroniStartFailure{VerifyPatroniStartFailure: &agentv1.VerifyPatroniStartFailure{Container: verify.Input.GetEnsureContainer()}}
	verify.Input.Secrets = nil
	verifyHash, err := CommandHash(verify)
	if err != nil {
		t.Fatal("valid Patroni failure proof refused", err)
	}
	for name, change := range map[string]func(*agentv1.Command){
		"secret_delivery":  func(c *agentv1.Command) { c.Input.Secrets = command.Input.Secrets },
		"absent_message":   func(c *agentv1.Command) { c.Input.Payload = &agentv1.CommandInput_VerifyPatroniStartFailure{} },
		"absent_container": func(c *agentv1.Command) { c.Input.GetVerifyPatroniStartFailure().Container = nil },
		"standalone":       func(c *agentv1.Command) { c.Input.GetVerifyPatroniStartFailure().Container.Postgres.Patroni = nil },
		"missing_binding": func(c *agentv1.Command) {
			c.Input.GetVerifyPatroniStartFailure().Container.Postgres.Patroni.Credentials = nil
		},
	} {
		t.Run("verify_"+name, func(t *testing.T) {
			changed := proto.Clone(verify).(*agentv1.Command)
			change(changed)
			if _, err := CommandHash(changed); err == nil {
				t.Fatal("invalid native failure proof accepted")
			}
		})
	}
	changed := proto.Clone(verify).(*agentv1.Command)
	changed.Input.GetVerifyPatroniStartFailure().Container.Postgres.Patroni.ProvisionId = uuid.NewString()
	if other, err := CommandHash(changed); err != nil || other == verifyHash {
		t.Fatal("native failure proof does not bind its provision identity", err)
	}
	verify.InputHash = verifyHash[:]
	step := &agentv1.PlanStep{Command: verify, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_PATRONI_START_FAILED}
	plan = &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}
	if err := ValidatePlan(plan, op); err != nil {
		t.Fatal("valid native failure proof plan refused", err)
	}
	for name, change := range map[string]func(*agentv1.PlanStep){
		"safe_stop": func(s *agentv1.PlanStep) {
			s.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
		},
		"stopped_claim": func(s *agentv1.PlanStep) { s.Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED },
		"replica_claim": func(s *agentv1.PlanStep) {
			s.Postcondition = agentv1.Postcondition_POSTCONDITION_POSTGRES_REPLICA_FAILED
		},
	} {
		t.Run("verify_plan_"+name, func(t *testing.T) {
			changed := proto.Clone(plan).(*agentv1.ExecutionPlan)
			change(changed.Steps[0])
			if err := ValidatePlan(changed, op); err == nil {
				t.Fatal("native failure proof accepted an unsafe plan step")
			}
		})
	}
	effect := func(state string) []*agentv1.Effect {
		return []*agentv1.Effect{{CommandId: verify.CommandId, ResourceId: verify.Input.ResourceId, NativeOperationId: "docker-command:" + verify.CommandId, State: state}}
	}
	for _, state := range []string{"patroni_storage_not_started", "patroni_storage_attempt_failed", "patroni_bootstrap_cancelled"} {
		if err := CheckStepEffects(step, effect(state)); err != nil {
			t.Fatalf("proved native failure %q refused: %v", state, err)
		}
	}
	for _, state := range []string{"exited", "created", "running", "replica_refused"} {
		if err := CheckStepEffects(step, effect(state)); !errors.Is(err, ErrPostconditionUnproved) {
			t.Fatalf("state %q proved a native failure: %v", state, err)
		}
	}
	unknown := effect("patroni_storage_attempt_failed")
	unknown[0].OutcomeUnknown = true
	if err := CheckStepEffects(step, unknown); !errors.Is(err, ErrPostconditionUnproved) {
		t.Fatal("unknown native failure evidence proved the postcondition", err)
	}
}

// Member proofs repeat the stored configuration, carry no secret, and prove only their own states.
func TestPatroniMemberFailureProofs(t *testing.T) {
	base := postgresContainerCommand()
	p := base.Input.GetEnsureContainer()
	p.SecretEnv = nil
	for _, purpose := range []string{"patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		base.Input.Secrets = append(base.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
	}
	p.Postgres.Patroni = &agentv1.PatroniContainerConfig{RequiredFreeBytes: 1 << 30, Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"}, ProvisionId: uuid.NewString(), AllowInitialization: true}
	for _, reference := range base.Input.Secrets {
		p.Postgres.Patroni.Credentials = append(p.Postgres.Patroni.Credentials, proto.Clone(reference).(*agentv1.SecretReference))
	}
	// Per-step rules hold for every kind. The fixed cluster.rebuild_member shape has its own planner test.
	op := Operation{ID: uuid.MustParse(base.OperationId), InstallationID: uuid.MustParse(base.Epoch.InstallationId), ClusterID: uuid.MustParse(base.Epoch.ClusterId), DesiredRevision: base.DesiredRevision, Kind: "cluster.stop"}
	for name, proof := range map[string]struct {
		set           func(*agentv1.Command)
		postcondition agentv1.Postcondition
		state         string
	}{
		"not serving": {func(c *agentv1.Command) {
			c.Input.Payload = &agentv1.CommandInput_VerifyPatroniMemberNotServing{VerifyPatroniMemberNotServing: &agentv1.VerifyPatroniMemberNotServing{Container: c.Input.GetEnsureContainer()}}
		}, agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_NOT_SERVING, "patroni_member_not_serving"},
		"storage failed": {func(c *agentv1.Command) {
			c.Input.Payload = &agentv1.CommandInput_VerifyPatroniMemberFailed{VerifyPatroniMemberFailed: &agentv1.VerifyPatroniMemberFailed{Container: c.Input.GetEnsureContainer()}}
		}, agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_FAILED, "patroni_member_storage_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			command := proto.Clone(base).(*agentv1.Command)
			proof.set(command)
			command.Input.Secrets = nil
			hash, err := CommandHash(command)
			if err != nil {
				t.Fatal("valid member proof refused", err)
			}
			withSecrets := proto.Clone(command).(*agentv1.Command)
			withSecrets.Input.Secrets = base.Input.Secrets
			standalone := proto.Clone(command).(*agentv1.Command)
			PatroniMemberContainer(standalone.Input).Postgres.Patroni = nil
			for _, bad := range []*agentv1.Command{withSecrets, standalone} {
				if _, err := CommandHash(bad); err == nil {
					t.Fatal("invalid member proof admitted")
				}
			}
			command.InputHash = hash[:]
			step := &agentv1.PlanStep{Command: command, MaxAttempts: 1, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: proof.postcondition}
			if err := ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{step}}, op); err != nil {
				t.Fatal("valid member proof plan refused", err)
			}
			unsafe := proto.Clone(step).(*agentv1.PlanStep)
			unsafe.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
			if err := ValidatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{unsafe}}, op); err == nil {
				t.Fatal("member proof claimed a safe-stop boundary")
			}
			effect := func(state string) []*agentv1.Effect {
				return []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: state}}
			}
			if err := CheckStepEffects(step, effect(proof.state)); err != nil {
				t.Fatal("proved member state refused", err)
			}
			for _, other := range []string{"patroni_member_not_serving", "patroni_member_storage_failed", "patroni_storage_attempt_failed", "exited"} {
				if other != proof.state && CheckStepEffects(step, effect(other)) == nil {
					t.Fatalf("state %q proved %s", other, name)
				}
			}
		})
	}
}

// A recreate may only move the two certificate credentials to higher versions.
func TestPatroniRecreateValidation(t *testing.T) {
	base := postgresContainerCommand()
	p := base.Input.GetEnsureContainer()
	p.SecretEnv = nil
	for _, purpose := range []string{"patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"} {
		base.Input.Secrets = append(base.Input.Secrets, &agentv1.SecretReference{CredentialId: uuid.NewString(), Version: 1, Purpose: purpose})
	}
	p.Postgres.Patroni = &agentv1.PatroniContainerConfig{RequiredFreeBytes: 1 << 30, Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"}, ProvisionId: uuid.NewString(), AllowInitialization: true}
	for _, reference := range base.Input.Secrets {
		p.Postgres.Patroni.Credentials = append(p.Postgres.Patroni.Credentials, proto.Clone(reference).(*agentv1.SecretReference))
	}
	renew := func(previous *agentv1.EnsureContainer, purpose string, version int64) *agentv1.EnsureContainer {
		next := proto.Clone(previous).(*agentv1.EnsureContainer)
		for _, reference := range next.Postgres.Patroni.Credentials {
			if reference.Purpose == purpose {
				reference.Version = version
			}
		}
		return next
	}
	command := func(previous, next *agentv1.EnsureContainer) *agentv1.Command {
		recreate := proto.Clone(base).(*agentv1.Command)
		recreate.Input.Secrets = next.Postgres.Patroni.Credentials
		recreate.Input.Payload = &agentv1.CommandInput_RecreatePatroniMember{RecreatePatroniMember: &agentv1.RecreatePatroniMember{Previous: previous, Next: next}}
		return recreate
	}
	both := renew(renew(p, "patroni_member_tls", 2), "patroni_dcs_tls", 3)
	for name, next := range map[string]*agentv1.EnsureContainer{"member certificate": renew(p, "patroni_member_tls", 2), "both certificates": both} {
		if _, err := CommandHash(command(p, next)); err != nil {
			t.Errorf("%s renewal refused: %v", name, err)
		}
	}
	moved := renew(p, "patroni_member_tls", 2)
	moved.Postgres.Patroni.ProvisionId = uuid.NewString()
	identity := renew(p, "patroni_member_tls", 2)
	identity.Postgres.Patroni.Credentials[3].CredentialId = uuid.NewString()
	lower := renew(p, "patroni_member_tls", 2)
	for name, invalid := range map[string]*agentv1.Command{
		"no change":             command(p, p),
		"another purpose":       command(p, renew(p, "patroni_dcs", 2)),
		"lower version":         command(lower, p),
		"changed provision":     command(p, moved),
		"changed credential ID": command(p, identity),
		"previous as secrets": func() *agentv1.Command {
			c := command(p, both)
			c.Input.Secrets = p.Postgres.Patroni.Credentials
			return c
		}(),
		"not a Patroni previous": command(&agentv1.EnsureContainer{}, both),
	} {
		if _, err := CommandHash(invalid); err == nil {
			t.Errorf("%s: recreate accepted", name)
		}
	}
	if PatroniMemberContainer(command(p, both).Input) != both {
		t.Fatal("the recreate member container is not next")
	}
}
