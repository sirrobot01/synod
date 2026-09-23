package postgres

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// nativePlacementIntent binds a typed preparation plan to eight sealed Store test values.
func nativePlacementIntent(t *testing.T, host *placementHost, claim operation.Claim, request *placement.Request, plan *agentv1.ExecutionPlan) ClusterIntent {
	t.Helper()
	ring := keyring(t)
	intent := ClusterIntent{InstallationID: host.id, ClusterID: claim.ClusterID, Databases: []Database{{Name: "app", OwnerRole: "synod_app"}}}
	for _, pair := range []struct{ purpose, role string }{
		{SuperuserCredential, "synod_admin"}, {ApplicationCredential, "synod_app"}, {ReplicationCredential, "synod_repl"},
		{PatroniRewindCredential, "synod_rewind"}, {PatroniDCSCredential, "synod_patroni_" + claim.ClusterID.String()},
		{PatroniRESTCredential, "synod_patroni"}, {PatroniMemberTLSCredential, ""}, {PatroniDCSTLSCredential, ""},
	} {
		credential := sealed(t, ring, host, claim.ClusterID, pair.purpose, pair.role, "sealed-store-test-"+pair.purpose)
		if pair.purpose == PatroniMemberTLSCredential {
			credential.MemberID = request.Resources[0].ID
		}
		intent.Credentials = append(intent.Credentials, credential)
	}
	step := plan.Steps[0]
	step.Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS
	command := step.Command
	for _, credential := range intent.Credentials {
		command.Input.Secrets = append(command.Input.Secrets, &agentv1.SecretReference{CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: credential.Purpose})
	}
	volume := uuid.New()
	request.Resources[0].OwnedResourceIDs = append(request.Resources[0].OwnedResourceIDs, volume)
	config := &agentv1.PostgresContainerConfig{
		Port: 15432, SuperuserCredentialId: intent.Credentials[0].CredentialID.String(), SuperuserCredentialVersion: 1,
		ApplicationCredentialId: intent.Credentials[1].CredentialID.String(), ApplicationCredentialVersion: 1,
		ReplicationCredentialId: intent.Credentials[2].CredentialID.String(), ReplicationCredentialVersion: 1,
		Patroni: &agentv1.PatroniContainerConfig{
			Address: "192.0.2.10", RestPort: 8008, DcsEndpoints: []string{"192.0.2.20:2379"},
			PeerNetworks: []string{"192.0.2.0/24"}, ClientNetworks: []string{"198.51.100.0/24"},
			ProvisionId: uuid.NewString(), AllowInitialization: true, RequiredFreeBytes: 1 << 30, Credentials: command.Input.Secrets,
		},
	}
	command.Input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
		Engine: "postgres", ImageDigest: "postgres@sha256:" + strings.Repeat("a", 64), VolumeId: volume.String(),
		NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, Postgres: config,
	}}
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
	return intent
}

func TestNativePlacementIntentCommitsAtomicallyAndRecoversLostReply(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "native")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	intent := nativePlacementIntent(t, host, claim, &request, plan)
	beforeCommit, proceed := make(chan struct{}), make(chan struct{})
	host.store.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		close(beforeCommit)
		select {
		case <-proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	done := make(chan error, 1)
	go func() {
		_, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, &intent)
		done <- err
	}()
	select {
	case <-beforeCommit:
	case err := <-done:
		t.Fatalf("transaction did not reach commit: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	for _, table := range []string{"resource_allocations", "cluster_members", "cluster_credentials", "cluster_databases", "operation_placements"} {
		if total := host.count(t, "SELECT count(*) FROM "+table+" WHERE installation_id=$1", host.id); total != 0 {
			t.Errorf("%s visible before commit", table)
		}
	}
	if total := host.count(t, `SELECT count(*) FROM operations WHERE id=$1 AND plan_bytes IS NOT NULL`, claim.OperationID); total != 0 {
		t.Error("plan visible before credentials commit")
	}
	close(proceed)
	requireCode(t, <-done, OutcomeUnknown)
	host.store.commitTx = nil
	record, err := host.store.ReadPlacement(t.Context(), host.id, claim.OperationID)
	if err != nil || len(record.CredentialIntentHash) != 32 || len(record.Allocations) != 1 {
		t.Fatal("lost commit not readable", err)
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 8 {
		t.Fatalf("credentials=%d", total)
	}
	replay, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, &intent)
	if err != nil || !bytes.Equal(replay.CredentialIntentHash, record.CredentialIntentHash) {
		t.Fatal("exact replay failed", err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); !errors.Is(err, operation.ErrCommandConflict) {
		t.Fatal("omitted intent accepted", err)
	}
	for name, change := range map[string]func(*ClusterIntent){
		"ciphertext":         func(i *ClusterIntent) { i.Credentials[0].Envelope.Sealed = []byte("different sealed bytes") },
		"database":           func(i *ClusterIntent) { i.Databases = []Database{{Name: "other_app", OwnerRole: "synod_app"}} },
		"omitted credential": func(i *ClusterIntent) { i.Credentials = i.Credentials[:7] },
	} {
		t.Run(name, func(t *testing.T) {
			changed := intent
			changed.Credentials = slices.Clone(intent.Credentials)
			change(&changed)
			if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, &changed); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatal("changed replay accepted", err)
			}
		})
	}
	// Recovery checks existing evidence. It must not restore a deleted secret row.
	if _, err := host.store.pool.Exec(t.Context(), `DELETE FROM cluster_credentials WHERE credential_id=$1`, intent.Credentials[7].CredentialID); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, &intent); !errors.Is(err, ErrCredentialConflict) {
		t.Fatal("replay repaired missing secret evidence", err)
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 7 {
		t.Fatal("replay wrote a missing credential")
	}
}

func TestNativePlacementIntentRefusalsRollback(t *testing.T) {
	for _, fault := range []string{"missing reference", "wrong member", "wrong node", "wrong engine", "outside placement", "stale claim", "active claim", "existing plan", "conflicting envelope", "commit rollback"} {
		t.Run(fault, func(t *testing.T) {
			host := placementFixture(t)
			claim := host.claimCluster(t, "native")
			request, decision, plan := host.place(t, claim, smallBudget, nil)
			intent := nativePlacementIntent(t, host, claim, &request, plan)
			priorCredentials, priorPlans := 0, 0
			switch fault {
			case "missing reference":
				intent.Credentials = intent.Credentials[:7]
			case "wrong member":
				plan.Steps[0].Command.Input.ResourceId = uuid.NewString()
				hash, err := operation.CommandHash(plan.Steps[0].Command)
				if err != nil {
					t.Fatal(err)
				}
				plan.Steps[0].Command.InputHash = hash[:]
			case "wrong node":
				plan.Steps[0].Command.Epoch.NodeId = uuid.NewString()
				hash, err := operation.CommandHash(plan.Steps[0].Command)
				if err != nil {
					t.Fatal(err)
				}
				plan.Steps[0].Command.InputHash = hash[:]
			case "wrong engine":
				request.Resources[0].Engine = "redis"
				request.Resources[0].Version = "8.2.7"
			case "outside placement":
				intent.Credentials[6].MemberID = uuid.New()
			case "stale claim":
				claim.OwnerID = "stale"
			case "active claim":
				if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET claim_active=true WHERE id=$1`, claim.OperationID); err != nil {
					t.Fatal(err)
				}
			case "existing plan":
				if err := host.store.InstallPlan(t.Context(), claim, plan); err != nil {
					t.Fatal(err)
				}
				priorPlans = 1
			case "conflicting envelope":
				prior := intent.Credentials[0]
				prior.Envelope.Sealed = []byte("other sealed bytes")
				if err := host.store.RecordClusterIntent(t.Context(), ClusterIntent{InstallationID: host.id, ClusterID: claim.ClusterID, Credentials: []Credential{prior}}); err != nil {
					t.Fatal(err)
				}
				priorCredentials = 1
			case "commit rollback":
				host.store.commitTx = func(ctx context.Context, tx pgx.Tx) error {
					if err := tx.Rollback(ctx); err != nil {
						return err
					}
					return pgx.ErrTxCommitRollback
				}
			}
			if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, &intent); err == nil {
				t.Fatal("unsafe transaction accepted")
			}
			for _, table := range []string{"resource_allocations", "cluster_members", "cluster_databases", "operation_placements"} {
				if total := host.count(t, "SELECT count(*) FROM "+table+" WHERE installation_id=$1", host.id); total != 0 {
					t.Fatalf("%s retained %d rows after refusal", table, total)
				}
			}
			if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != priorCredentials {
				t.Fatalf("credentials=%d, want %d", total, priorCredentials)
			}
			if total := host.count(t, `SELECT count(*) FROM operations WHERE installation_id=$1 AND plan_bytes IS NOT NULL`, host.id); total != priorPlans {
				t.Fatal("failed transaction installed plan")
			}
		})
	}
}

func TestNativePlacementIntentAddsMemberAfterPriorDispatch(t *testing.T) {
	host := placementFixture(t)
	first := host.claimCluster(t, "native")
	request, decision, plan := host.place(t, first, smallBudget, nil)
	intent := nativePlacementIntent(t, host, first, &request, plan)
	if _, err := host.store.InstallPlacementPlan(t.Context(), first, request, decision, plan, &intent); err != nil {
		t.Fatal(err)
	}
	command := plan.Steps[0].Command
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO command_identities(command_id,input_hash) VALUES($1,$2)`, command.CommandId, command.InputHash); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO command_dispatches
 (command_id,execution_epoch,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,command_bytes)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, command.CommandId, first.ExecutionEpoch, host.id, first.ClusterID, first.OperationID, host.generation, host.node, command.StepId, command.InputHash, []byte("retained dispatch identity")); err != nil {
		t.Fatal(err)
	}
	second := host.reclaim(t, first, "failed")
	nextRequest, nextDecision, nextPlan := host.place(t, second, smallBudget, nil)
	nextRequest.Resources[0].Role = "replica"
	nextIntent := nativePlacementIntent(t, host, second, &nextRequest, nextPlan)
	nextCommand := nextPlan.Steps[0].Command
	for i, reference := range nextCommand.Input.Secrets {
		if i != 6 {
			reference.CredentialId = intent.Credentials[i].CredentialID.String()
		}
	}
	config := nextCommand.Input.GetEnsureContainer().Postgres
	config.SuperuserCredentialId = intent.Credentials[0].CredentialID.String()
	config.ApplicationCredentialId = intent.Credentials[1].CredentialID.String()
	config.ReplicationCredentialId = intent.Credentials[2].CredentialID.String()
	nextIntent.Credentials = []Credential{nextIntent.Credentials[6]}
	nextIntent.Databases = nil
	hash, err := operation.CommandHash(nextCommand)
	if err != nil {
		t.Fatal(err)
	}
	nextCommand.InputHash = hash[:]
	if err := host.store.RecordClusterIntent(t.Context(), nextIntent); !errors.Is(err, ErrDispatchAlreadyHappened) {
		t.Fatal("legacy intent path ignored prior dispatch", err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), second, nextRequest, nextDecision, nextPlan, &nextIntent); err != nil {
		t.Fatal("later atomic member intent refused", err)
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 9 {
		t.Fatalf("credentials=%d, want 9", total)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations WHERE installation_id=$1`, host.id); total != 2 {
		t.Fatalf("allocations=%d, want 2", total)
	}
	for _, credential := range intent.Credentials {
		found, err := host.store.ReadCredentialVersion(t.Context(), host.id, first.ClusterID, credential.CredentialID, credential.Version, credential.Purpose)
		if err != nil || !bytes.Equal(found.Envelope.Sealed, credential.Envelope.Sealed) {
			t.Fatal("existing credential changed", err)
		}
	}
}
