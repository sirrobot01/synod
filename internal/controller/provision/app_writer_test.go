package provision

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	imageinspect "github.com/sirrobot01/synod/internal/runtime/image"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTier2PlacementAndFailover(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			f := newFixture(t, postgresSpec())
			extra := 2
			if replacement {
				extra++
			}
			for i := range extra {
				node := uuid.New()
				if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes(id,installation_id,public_key_hash,enrolled_at) VALUES($1,$2,$3,clock_timestamp())`, node, f.claim.InstallationID, make([]byte, 32)); err != nil {
					t.Fatal(err)
				}
				readyNodeWith(t, f.store, f.claim.InstallationID, node, func(p *placement.NodePolicy) {
					p.Architecture = "arm64"
					p.Engines = nil
					p.ClientAddress = []string{"192.0.2.11", "192.0.2.12", "192.0.2.13"}[i]
				})
			}
			r := app.DefaultReplication()
			r.Standbys = 2
			r.FailoverAfterSeconds = 60
			admitted, err := f.store.AdmitApp(t.Context(), postgres.CreateAppRequest{Identity: postgres.RequestIdentity{InstallationID: f.claim.InstallationID, ActorID: "writer-test", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: "writer-test", Spec: app.AppSpec{SchemaVersion: 1, Name: "writer", Tier: app.TierSingleWriter, Copies: 1, Replication: &r, DevicePreference: app.DevicePreference{Preferred: []string{f.node.String()}}, Service: app.ServiceSpec{Image: "example", Ports: []app.PortSpec{{ContainerPort: 80}}, Mounts: []app.MountSpec{{Type: "replicated", Source: "config", Target: "/config"}}}, Router: app.RouterSpec{Protocol: "tcp", Port: 8080}}})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: f.claim.InstallationID, OperationID: admitted.OperationID, RecoveryGeneration: f.claim.RecoveryGeneration, OwnerID: "writer-test", TTL: time.Minute, LeaseID: 2, LeaseRevision: 2})
			if err != nil {
				t.Fatal(err)
			}
			f.planner.config.ResolveImage = func(_ context.Context, s app.ServiceSpec) (imageinspect.Resolution, error) {
				s.Image = "example@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
				s.SupportedArchitectures = []string{"amd64", "arm64"}
				return imageinspect.Resolution{Service: s}, nil
			}
			if err := f.planner.Plan(t.Context(), claim); err != nil {
				t.Fatalf("deploy: %v (%v)", err, errors.Unwrap(err))
			}
			copies, err := f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || len(copies) != 3 || copies[0].Role != "active" || copies[0].NodeID != f.node {
				t.Fatalf("copies: %+v %v", copies, err)
			}
			plan, err := f.store.ReadPlan(t.Context(), claim)
			if err != nil || len(plan.Steps) != 2 {
				t.Fatalf("standby got commands: %v %v", plan, err)
			}
			record, err := f.store.ReadPlacement(t.Context(), claim.InstallationID, claim.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			for _, copy := range copies[1:] {
				for _, allocation := range record.Allocations {
					if allocation.ResourceID == copy.ID && (allocation.Budget.CPUMillis != 0 || allocation.Budget.MemoryBytes != 0 || allocation.Budget.DiskBytes != 1<<30) {
						t.Fatalf("standby budget: %+v", allocation)
					}
				}
			}
			tables, err := f.store.AppReplicationTargets(t.Context(), claim.InstallationID)
			if err != nil || len(tables) != extra+1 {
				t.Fatalf("tables: %v %v", tables, err)
			}
			for node, desired := range tables {
				table, _, err := f.store.PrepareAppReplication(t.Context(), claim.InstallationID, node, desired)
				if err != nil {
					t.Fatal(err)
				}
				session := uuid.MustParse(table.SessionId)
				if err := f.store.RecordAppReplicationAck(t.Context(), claim.InstallationID, node, session, &agentv1.ReplicationAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, node, session, &agentv1.Heartbeat{Sequence: 1, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour)}); err != nil {
					t.Fatal(err)
				}
			}
			session := uuid.MustParse(tables[f.node].SessionId)
			grant, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.ClusterID, f.node, session, 1, false)
			if err != nil || grant == nil {
				t.Fatalf("initial grant: %v %v", grant, err)
			}

			// A standby cannot become a serving backend even if it reports a healthy container.
			for _, copy := range copies {
				session := uuid.MustParse(tables[copy.NodeID].SessionId)
				h := &agentv1.Heartbeat{Sequence: 2, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Observations: []*agentv1.Observation{{ResourceId: copy.ID.String(), Lifecycle: "running", Health: "healthy", ObservedAt: timestamppb.Now(), Ports: []*agentv1.PortBinding{{HostAddress: "0.0.0.0", HostPort: uint32(copy.HostPort), ContainerPort: 80, Transport: "tcp"}}}}}
				if copy.Role == "active" {
					h.Replicas = []*agentv1.ReplicaStatus{{AppId: claim.ClusterID.String(), Role: "active", Epoch: 1, Sequence: 1, CompletedAt: timestamppb.Now(), LastPauseNanos: uint64(100 * time.Millisecond), MaxPauseNanos: uint64(200 * time.Millisecond), SkippedSnapshots: 2}}
				}
				if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, copy.NodeID, session, h); err != nil {
					t.Fatal(err)
				}
			}
			routes, err := f.store.AppRouteTargets(t.Context(), claim.InstallationID)
			if err != nil {
				t.Fatal(err)
			}
			for node, target := range routes {
				if len(target.Table.Routes) != 1 || len(target.Table.Routes[0].Backends) != 1 {
					t.Fatalf("standby routed: %v", target.Table)
				}
				table, _, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, target.Table)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.RecordAppRouteAck(t.Context(), claim.InstallationID, node, uuid.MustParse(table.SessionId), &agentv1.RouteAck{SessionId: table.SessionId, Revision: table.Revision}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='succeeded',claim_active=false,finished_at=clock_timestamp() WHERE id=$1`, claim.OperationID); err != nil {
				t.Fatal(err)
			}
			status, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || status.Status.Placement != "preferred" || status.Status.State != "running" || status.Status.ActiveNodeID != f.node || status.Status.WriterEpoch != 1 {
				t.Fatalf("preferred status: %+v %v", status.Status, err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '61 seconds' WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			if id, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.ClusterID); err != nil || id != uuid.Nil {
				t.Fatalf("empty replica promoted: %v %v", id, err)
			}
			status, err = f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || status.Status.State != "down" || status.Status.FailureCode != "no_replica" || status.Status.LossWindowSeconds != nil {
				t.Fatalf("no replica status: %+v %v", status.Status, err)
			}
			var selected uuid.UUID
			for _, copy := range copies[1:] {
				seq := uint64(copy.CopyIndex * 10)
				if copy.CopyIndex == 2 {
					selected = copy.NodeID
				}
				session := uuid.MustParse(tables[copy.NodeID].SessionId)
				h := &agentv1.Heartbeat{Sequence: 3, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Replicas: []*agentv1.ReplicaStatus{{AppId: claim.ClusterID.String(), Role: "standby", Epoch: 1, Sequence: seq, CompletedAt: timestamppb.New(time.Now().Add(-time.Minute))}}}
				if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, copy.NodeID, session, h); err != nil {
					t.Fatal(err)
				}
			}
			status, err = f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || status.Status.LossWindowSeconds == nil || *status.Status.LossWindowSeconds < 120 || *status.Status.LossWindowSeconds > 130 || len(status.Status.Standbys) != 2 {
				t.Fatalf("loss window: %+v %v", status.Status, err)
			}

			// A retained sequence cannot prove a replica after corruption or a later failure.
			for index, code := range []string{"replica_corrupt", "stale_snapshot", ""} {
				for _, copy := range copies[1:] {
					session := uuid.MustParse(tables[copy.NodeID].SessionId)
					h := &agentv1.Heartbeat{Sequence: uint64(4 + index), ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Replicas: []*agentv1.ReplicaStatus{{AppId: claim.ClusterID.String(), Role: "standby", Epoch: 1, Sequence: uint64(copy.CopyIndex * 10), CompletedAt: timestamppb.New(time.Now().Add(-time.Minute)), FailureCode: code}}}
					if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, copy.NodeID, session, h); err != nil {
						t.Fatal(err)
					}
				}
				eligible, err := f.store.AppFailoverCandidates(t.Context(), claim.InstallationID, claim.ClusterID)
				if err != nil {
					t.Fatal(err)
				}
				if code == "" {
					if len(eligible) != 2 {
						t.Fatalf("verified replicas did not recover: %+v", eligible)
					}
					continue
				}
				if len(eligible) != 0 {
					t.Fatalf("failed replicas eligible for %s: %+v", code, eligible)
				}
				if id, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.ClusterID); err != nil || id != uuid.Nil {
					t.Fatalf("failed replica admitted for %s: %v %v", code, id, err)
				}
				status, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.ClusterID)
				if err != nil || status.Status.FailureCode != "no_replica" || status.Status.LossWindowSeconds != nil {
					t.Fatalf("failed replica status for %s: %+v %v", code, status.Status, err)
				}
			}

			// A complete snapshot is unusable when its host has no compute capacity.
			policy, err := f.store.ReadNodePlacementPolicy(t.Context(), claim.InstallationID, selected)
			if err != nil {
				t.Fatal(err)
			}
			policy.Cordoned = true
			policy.Revision, err = f.store.PutNodePlacementPolicy(t.Context(), policy, policy.Revision)
			if err != nil {
				t.Fatal(err)
			}
			eligible, err := f.store.AppFailoverCandidates(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || len(eligible) != 1 || eligible[0].NodeID == selected {
				t.Fatalf("cordoned replica usable: %+v %v", eligible, err)
			}
			policy.Cordoned = false
			policy.Revision, err = f.store.PutNodePlacementPolicy(t.Context(), policy, policy.Revision)
			if err != nil {
				t.Fatal(err)
			}
			// Restore fresh capacity facts at the new policy revision.
			oldReport, err := f.store.ReadNodeCapacityReport(t.Context(), claim.InstallationID, selected)
			if err != nil {
				t.Fatal(err)
			}
			probe, err := f.store.BeginCapacityProbe(t.Context(), claim.InstallationID, selected, oldReport.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			oldReport.ProbeID, oldReport.ProbeRevision = probe.ID, probe.Revision
			if err := f.store.RecordNodeCapacity(t.Context(), claim.InstallationID, selected, *oldReport); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '59 seconds' WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			if id, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.ClusterID); err != nil || id != uuid.Nil {
				t.Fatalf("early promotion: %v %v", id, err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '61 seconds' WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			id, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || id == uuid.Nil {
				t.Fatalf("failover: %v %v (%v)", id, err, errors.Unwrap(err))
			}
			replay, err := f.store.AdmitAppFailover(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || replay != id {
				t.Fatalf("duplicate failover: %v %v", replay, err)
			}
			if grant, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.ClusterID, f.node, session, 1, true); err != nil || grant != nil {
				t.Fatalf("old writer renewed during promotion: %v %v", grant, err)
			}
			// The old writer received commands before it was lost.
			lost := uuid.New()
			if _, err := f.pool.Exec(t.Context(), `INSERT INTO command_identities (command_id,input_hash) VALUES ($1,$2)`, lost, make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(t.Context(), `INSERT INTO command_dispatches (command_id,execution_epoch,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,command_bytes) VALUES ($1,1,$2,$3,$4,$5,$6,$7,$8,'')`,
				lost, claim.InstallationID, claim.ClusterID, claim.OperationID, claim.RecoveryGeneration, f.node, uuid.New(), make([]byte, 32)); err != nil {
				t.Fatal(err)
			}
			next, err := f.store.AcquireClaim(t.Context(), postgres.ClaimRequest{ClaimID: uuid.New(), InstallationID: claim.InstallationID, OperationID: id, RecoveryGeneration: claim.RecoveryGeneration, OwnerID: "failover-test", TTL: time.Minute, LeaseID: 3, LeaseRevision: 3})
			if err != nil {
				t.Fatal(err)
			}
			metrics, err := f.store.GetAppStatus(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || metrics.Status.LastPauseSeconds != 0.1 || metrics.Status.MaxPauseSeconds != 0.2 || metrics.Status.SkippedSnapshots != 2 {
				t.Fatalf("snapshot metrics: %+v %v", metrics.Status, err)
			}
			// Recheck the lease boundary at commit, even after operation admission.
			if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp() WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			if err := f.planner.Plan(t.Context(), next); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("changed boundary accepted: %v (%v)", err, errors.Unwrap(err))
			}
			writer, err := f.store.GetAppWriter(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || writer.Epoch != 1 || writer.NodeID != f.node {
				t.Fatalf("partial promotion: %+v %v", writer, err)
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE apps SET writer_last_grant_at=clock_timestamp()-interval '61 seconds' WHERE id=$1`, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			// A failure after selection must abort the transaction that promotes the writer.
			f.decider.afterDecision = func() {
				if _, err := f.pool.Exec(t.Context(), `UPDATE app_replicas SET failure_code='replica_corrupt' WHERE installation_id=$1 AND app_id=$2 AND node_id=$3`, claim.InstallationID, claim.ClusterID, selected); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.planner.Plan(t.Context(), next); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("corruption after selection accepted: %v", err)
			}
			f.decider.afterDecision = nil
			writer, err = f.store.GetAppWriter(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || writer.Epoch != 1 || writer.NodeID != f.node {
				t.Fatalf("failed replica changed writer: %+v %v", writer, err)
			}
			if _, err := f.store.ReadPlan(t.Context(), next); !errors.Is(err, operation.ErrBarrierPending) {
				t.Fatalf("failed promotion left commands: %v", err)
			}
			// A new verified snapshot restores the selected standby.
			if err := f.store.RecordAgentHeartbeat(t.Context(), claim.InstallationID, selected, uuid.MustParse(tables[selected].SessionId), &agentv1.Heartbeat{Sequence: 7, ObservedAt: timestamppb.Now(), WriterClockNanos: uint64(time.Hour), Replicas: []*agentv1.ReplicaStatus{{AppId: claim.ClusterID.String(), Role: "standby", Epoch: 1, Sequence: 21, CompletedAt: timestamppb.Now()}}}); err != nil {
				t.Fatal(err)
			}
			if replacement {
				if _, err := f.pool.Exec(t.Context(), `UPDATE agent_presence SET last_seen_at=clock_timestamp()-interval '1 minute' WHERE node_id=$1`, f.node); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.planner.Plan(t.Context(), next); err != nil {
				t.Fatalf("promote: %v (%v)", err, errors.Unwrap(err))
			}
			// The lost writer cannot drain. Its expired lease fences it, so promotion
			// waits only for the promoted node.
			var barriers []uuid.UUID
			rows, err := f.pool.Query(t.Context(), `SELECT node_id FROM claim_barriers WHERE claim_id=$1`, next.ID)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var node uuid.UUID
				if err := rows.Scan(&node); err != nil {
					t.Fatal(err)
				}
				barriers = append(barriers, node)
			}
			rows.Close()
			if len(barriers) != 1 || barriers[0] != selected {
				t.Fatalf("failover barriers %v, want only %s", barriers, selected)
			}
			writer, err = f.store.GetAppWriter(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || writer.Epoch != 2 || writer.NodeID != selected || writer.LastGrantAt != nil {
				t.Fatalf("promoted writer: %+v %v", writer, err)
			}
			if _, err := f.store.PrepareAppWriterLease(t.Context(), claim.InstallationID, claim.ClusterID, f.node, session, 1, true); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("old epoch grant: %v", err)
			}
			plan, err = f.store.ReadPlan(t.Context(), next)
			if err != nil || len(plan.Steps) != 2 || plan.Steps[0].Command.Epoch.NodeId != selected.String() {
				t.Fatalf("promotion plan: %v %v", plan, err)
			}
			current, err := f.store.GetAppCopies(t.Context(), claim.InstallationID, claim.ClusterID)
			if err != nil || len(current) != 3 {
				t.Fatalf("replacement copies: %+v %v", current, err)
			}
			for _, copy := range current {
				if replacement && copy.NodeID == f.node {
					t.Fatal("offline copy kept the replacement slot")
				}
			}
			placementRecord, err := f.store.ReadPlacement(t.Context(), claim.InstallationID, next.OperationID)
			if err != nil || (replacement && len(placementRecord.Request.Resources) != 2) {
				t.Fatalf("replacement reservation: %+v %v", placementRecord.Request, err)
			}
			for node, target := range routes {
				if _, _, err := f.store.PrepareAppRoutes(t.Context(), claim.InstallationID, node, target.Table); !errors.Is(err, operation.ErrCommandConflict) {
					t.Fatalf("stale writer route: %v", err)
				}
			}

		})
	}

}
