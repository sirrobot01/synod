package postgres

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// installation holds one node that is ready to receive placement.
type placementHost struct {
	store      *Store
	id         uuid.UUID
	node       uuid.UUID
	generation uuid.UUID
	policy     placement.NodePolicy
	// ports counts default backend ports, so two placements never share one.
	ports uint16
}

func placementFixture(t *testing.T) *placementHost {
	t.Helper()
	s, installation, node, session := capacityFixture(t)
	host := &placementHost{store: s, id: installation, node: node, generation: uuid.New()}
	host.policy = readyNode(t, s, installation, node, session)
	if err := s.InitializeGeneration(t.Context(), installation, host.generation); err != nil {
		t.Fatal(err)
	}
	return host
}

// claimApp admits one tier 3 app and takes its operation claim.
func (h *placementHost) claimApp(t *testing.T, name string) operation.Claim {
	t.Helper()
	return h.claim(t, name, testRequest())
}

func (h *placementHost) claim(t *testing.T, name string, request CreateAppRequest) operation.Claim {
	t.Helper()
	request.Identity.InstallationID = h.id
	request.Spec.Name = name
	admission, err := h.store.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.AcquireClaim(t.Context(), ClaimRequest{
		ClaimID: uuid.New(), InstallationID: h.id, OperationID: admission.OperationID, RecoveryGeneration: h.generation,
		OwnerID: "owner-" + name, TTL: 2 * time.Minute, LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// reclaim finishes the current operation and claims a start for the same app.
// A later lifecycle operation reuses the app's allocations, so it must share its app.
func (h *placementHost) reclaim(t *testing.T, previous operation.Claim, status string) operation.Claim {
	t.Helper()
	if _, err := h.store.pool.Exec(t.Context(), `UPDATE operations SET status=$1,finished_at=clock_timestamp(),claim_active=false WHERE id=$2`, status, previous.OperationID); err != nil {
		t.Fatal(err)
	}
	next := uuid.New()
	if _, err := h.store.pool.Exec(t.Context(), `INSERT INTO operations
 (id,installation_id,app_id,actor_id,api_operation,idempotency_key,request_hash,hash_version,kind,desired_revision,status,plan_version,execution_epoch,attempts,created_at,updated_at)
 VALUES ($1,$2,$3,'actor-1','app.start',$4,$5,1,'app.start',1,'pending',0,0,0,clock_timestamp(),clock_timestamp())`,
		next, h.id, previous.AppID, uuid.NewString(), make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.AcquireClaim(t.Context(), ClaimRequest{
		ClaimID: uuid.New(), InstallationID: h.id, OperationID: next, RecoveryGeneration: h.generation,
		OwnerID: "owner-next", TTL: 2 * time.Minute, LeaseID: 1, LeaseRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// planFor builds one immutable plan that creates and starts the placed app copy.
func (h *placementHost) planFor(t *testing.T, claim operation.Claim, resource uuid.UUID, port placement.Port, revision int64) *agentv1.ExecutionPlan {
	t.Helper()
	var name string
	if err := h.store.pool.QueryRow(t.Context(), `SELECT name FROM apps WHERE id=$1`, claim.AppID).Scan(&name); err != nil {
		t.Fatal(err)
	}
	image := "nginx@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	plan := &agentv1.ExecutionPlan{Version: 1}
	for _, input := range []*agentv1.CommandInput{
		{ResourceId: resource.String(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			ImageDigest: image, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE,
			Ports:   []*agentv1.PortBinding{{HostAddress: port.Address, HostPort: uint32(port.Number), ContainerPort: 80, Transport: "tcp"}},
			Generic: &agentv1.GenericContainerConfig{Image: image, AppName: name},
		}}},
		{ResourceId: resource.String(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}},
	} {
		command := &agentv1.Command{
			Epoch:       &agentv1.Epoch{InstallationId: claim.InstallationID.String(), NodeId: h.node.String(), AppId: claim.AppID.String(), RecoveryGeneration: claim.RecoveryGeneration.String(), ExecutionEpoch: claim.ExecutionEpoch},
			OperationId: claim.OperationID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(), DesiredRevision: revision, PlanVersion: 1, Input: input,
		}
		hash, err := operation.CommandHash(command)
		if err != nil {
			t.Fatal(err)
		}
		command.InputHash = hash[:]
		step := &agentv1.PlanStep{Command: command, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS}
		if input.GetStartContainer() != nil {
			step.Postcondition, step.DependsOn = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{plan.Steps[0].Command.StepId}
		}
		plan.Steps = append(plan.Steps, step)
	}
	return plan
}

// place plans one app copy for a claim and returns everything the install needs.
// The first port is the copy's backend. A placement without ports gets a fresh one.
func (h *placementHost) place(t *testing.T, claim operation.Claim, budget placement.Vector, ports []placement.Port) (placement.Request, placement.Decision, *agentv1.ExecutionPlan) {
	t.Helper()
	ports = slices.Clone(ports)
	if len(ports) == 0 {
		h.ports++
		ports = []placement.Port{{Address: "0.0.0.0", Transport: "tcp", Number: 20000 + h.ports}}
	}
	if ports[0].Name == "" {
		ports[0].Name = "backend"
	}
	request := placementRequest(h.id, claim.AppID, claim.OperationID, h.policy, ports)
	request.Resources[0].Budgets.Workload = &budget
	snapshot, err := h.store.ReadPlacementSnapshot(t.Context(), h.id)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := placement.Plan(request, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return request, decision, h.planFor(t, claim, request.Resources[0].ID, ports[0], request.DesiredRevision)
}

func (h *placementHost) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var total int
	if err := h.store.pool.QueryRow(t.Context(), query, args...).Scan(&total); err != nil {
		t.Fatal(err)
	}
	return total
}

var smallBudget = placement.Vector{CPUMillis: 1000, MemoryBytes: 1 << 30, DiskBytes: 1 << 32}
var halfBudget = placement.Vector{CPUMillis: 4000, MemoryBytes: 1 << 32, DiskBytes: 1 << 36}

func TestInstallPlacementPlanCommitsReservationsAndPlanTogether(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	ports := []placement.Port{{Address: "127.0.0.1", Transport: "tcp", Number: 5432}}
	request, decision, plan := host.place(t, claim, smallBudget, ports)
	record, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Allocations) != 1 || record.Allocations[0].ID != request.Resources[0].ReservationID || record.Allocations[0].NodeID != host.node {
		t.Fatalf("record %+v", record)
	}
	for name, expected := range map[string]struct {
		query string
		count int
	}{
		"allocation": {`SELECT count(*) FROM resource_allocations WHERE installation_id=$1`, 1},
		"resource":   {`SELECT count(*) FROM allocation_resources WHERE installation_id=$1`, 1},
		"port":       {`SELECT count(*) FROM allocation_ports WHERE installation_id=$1 AND released_at IS NULL`, 1},
		"binding":    {`SELECT count(*) FROM operation_allocation_bindings WHERE installation_id=$1`, 1},
		"placement":  {`SELECT count(*) FROM operation_placements WHERE installation_id=$1`, 1},
		"plan":       {`SELECT count(*) FROM operations WHERE installation_id=$1 AND plan_bytes IS NOT NULL`, 1},
	} {
		if total := host.count(t, expected.query, host.id); total != expected.count {
			t.Fatalf("%s rows=%d, want %d", name, total, expected.count)
		}
	}
	// An immutable retry returns the committed placement without new reservations.
	replay, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan)
	if err != nil {
		t.Fatal(err)
	}
	if replay.RequestHash != record.RequestHash || replay.PlanHash != record.PlanHash || len(replay.Allocations) != 1 {
		t.Fatalf("replay %+v", replay)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations WHERE installation_id=$1`, host.id); total != 1 {
		t.Fatalf("allocations after replay=%d", total)
	}
	// A changed request for the same operation is a conflict.
	changed := request
	changed.Profile = "ha"
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, changed, decision, plan); err == nil {
		t.Fatal("a changed request was accepted")
	}
	// An uncertain commit resolves by the original operation identity.
	stored, err := host.store.ReadPlacement(t.Context(), host.id, claim.OperationID)
	if err != nil || stored.RequestHash != record.RequestHash || len(stored.Allocations) != 1 {
		t.Fatalf("read placement %+v: %v", stored, err)
	}
}

// A plan that the claim rejects must leave no reservation behind.
func TestInstallPlacementPlanRollsBackWithThePlan(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	request, decision, _ := host.place(t, claim, smallBudget, nil)
	// The operation wants revision one, so a plan for revision two cannot install.
	wrong := host.planFor(t, claim, request.Resources[0].ID, request.Resources[0].Ports[0], 2)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, wrong); err == nil {
		t.Fatal("a plan for another revision was installed")
	}
	for _, table := range []string{"resource_allocations", "allocation_resources", "allocation_ports", "operation_allocation_bindings", "operation_placements", "app_copies"} {
		if total := host.count(t, `SELECT count(*) FROM `+table); total != 0 {
			t.Fatalf("%s kept %d rows after rollback", table, total)
		}
	}
	if total := host.count(t, `SELECT count(*) FROM operations WHERE plan_bytes IS NOT NULL`); total != 0 {
		t.Fatal("a rejected plan was installed")
	}
	// The same host still accepts a correct plan afterwards.
	correct := host.planFor(t, claim, request.Resources[0].ID, request.Resources[0].Ports[0], 1)
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, correct); err != nil {
		t.Fatal(err)
	}
}

// Two independent transactions must not both reserve the same capacity.
func TestConcurrentPlacementReservesCapacityOnce(t *testing.T) {
	host := placementFixture(t)
	first, second := host.claimApp(t, "one"), host.claimApp(t, "two")
	firstRequest, firstDecision, firstPlan := host.place(t, first, halfBudget, nil)
	secondRequest, secondDecision, secondPlan := host.place(t, second, halfBudget, nil)
	results := make(chan error, 2)
	start := make(chan struct{})
	var racing sync.WaitGroup
	racing.Go(func() {
		<-start
		_, err := host.store.InstallPlacementPlan(t.Context(), first, firstRequest, firstDecision, firstPlan)
		results <- err
	})
	racing.Go(func() {
		<-start
		_, err := host.store.InstallPlacementPlan(t.Context(), second, secondRequest, secondDecision, secondPlan)
		results <- err
	})
	close(start)
	racing.Wait()
	close(results)
	committed := 0
	for err := range results {
		if err == nil {
			committed++
			continue
		}
		if !errors.Is(err, ErrPlacementConflict) {
			t.Fatalf("unexpected placement failure: %v", err)
		}
	}
	if committed != 1 {
		t.Fatalf("committed=%d, want one of two competing reservations", committed)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations`); total != 1 {
		t.Fatalf("allocations=%d, want one", total)
	}
}

// Two independent transactions must not both reserve the same host port.
func TestConcurrentPlacementReservesAPortOnce(t *testing.T) {
	host := placementFixture(t)
	first, second := host.claimApp(t, "one"), host.claimApp(t, "two")
	ports := []placement.Port{{Address: "0.0.0.0", Transport: "tcp", Number: 6432}}
	firstRequest, firstDecision, firstPlan := host.place(t, first, smallBudget, ports)
	secondRequest, secondDecision, secondPlan := host.place(t, second, smallBudget, ports)
	results := make(chan error, 2)
	start := make(chan struct{})
	var racing sync.WaitGroup
	racing.Go(func() {
		<-start
		_, err := host.store.InstallPlacementPlan(t.Context(), first, firstRequest, firstDecision, firstPlan)
		results <- err
	})
	racing.Go(func() {
		<-start
		_, err := host.store.InstallPlacementPlan(t.Context(), second, secondRequest, secondDecision, secondPlan)
		results <- err
	})
	close(start)
	racing.Wait()
	close(results)
	committed := 0
	for err := range results {
		if err == nil {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("committed=%d, want one of two competing port claims", committed)
	}
	if total := host.count(t, `SELECT count(*) FROM allocation_ports WHERE released_at IS NULL`); total != 1 {
		t.Fatalf("live ports=%d, want one", total)
	}
}

// An uncertain commit must be resolvable without a second decision.
func TestPlacementUnknownCommitResolvesByIdentity(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	host.store.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	_, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan)
	requireCode(t, err, OutcomeUnknown)
	host.store.commitTx = nil
	record, err := host.store.ReadPlacement(t.Context(), host.id, claim.OperationID)
	if err != nil || len(record.Allocations) != 1 {
		t.Fatalf("the uncertain commit is unreadable: %v", err)
	}
	// The exact retry returns the same committed placement.
	replay, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan)
	if err != nil || replay.RequestHash != record.RequestHash {
		t.Fatalf("retry after an uncertain commit: %v", err)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations`); total != 1 {
		t.Fatalf("allocations=%d, want one", total)
	}
}

// A reused reservation that grows must store the larger budget.
// The planner charges the growth, so an unrecorded one lets the node be oversold.
func TestInstallPlacementPlanStoresAGrownBudget(t *testing.T) {
	host := placementFixture(t)
	first := host.claimApp(t, "db")
	request, decision, plan := host.place(t, first, smallBudget, nil)
	if _, err := host.store.InstallPlacementPlan(t.Context(), first, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	// A later operation on the same app asks for more of the same node.
	second := host.reclaim(t, first, "succeeded")
	// Reuse the same resource and reservation identity on the same node.
	grown := placementRequest(host.id, second.AppID, second.OperationID, host.policy, request.Resources[0].Ports)
	grown.Resources[0].ID = request.Resources[0].ID
	grown.Resources[0].ReservationID = request.Resources[0].ReservationID
	grown.Resources[0].OwnedResourceIDs = request.Resources[0].OwnedResourceIDs
	grown.Resources[0].Budgets.Workload = &halfBudget
	snapshot, err := host.store.ReadPlacementSnapshot(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	grownDecision, err := placement.Plan(grown, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	grownPlan := host.planFor(t, second, grown.Resources[0].ID, grown.Resources[0].Ports[0], 1)
	record, err := host.store.InstallPlacementPlan(t.Context(), second, grown, grownDecision, grownPlan)
	if err != nil {
		t.Fatal(err)
	}
	if record.Allocations[0].Budget.CPUMillis != halfBudget.CPUMillis {
		t.Fatalf("returned budget %+v, want the grown one", record.Allocations[0].Budget)
	}
	// The stored reservation must carry the growth, not the first small budget.
	stored, err := host.store.ReadPlacementSnapshot(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Allocations) != 1 || stored.Allocations[0].Budget.CPUMillis != halfBudget.CPUMillis {
		t.Fatalf("stored budget %+v, want the grown one", stored.Allocations)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations`); total != 1 {
		t.Fatalf("allocations=%d, want the reservation reused", total)
	}
}

// A reordered port list is the same set of bindings on a host.
func TestInstallPlacementPlanAcceptsReorderedPorts(t *testing.T) {
	host := placementFixture(t)
	first := host.claimApp(t, "db")
	ports := []placement.Port{
		{Name: "backend", Address: "127.0.0.1", Transport: "tcp", Number: 5432},
		{Name: "admin", Address: "127.0.0.1", Transport: "tcp", Number: 5433},
	}
	request, decision, plan := host.place(t, first, smallBudget, ports)
	if _, err := host.store.InstallPlacementPlan(t.Context(), first, request, decision, plan); err != nil {
		t.Fatal(err)
	}
	second := host.reclaim(t, first, "succeeded")
	reordered := placementRequest(host.id, second.AppID, second.OperationID, host.policy, []placement.Port{ports[1], ports[0]})
	reordered.Resources[0].ID = request.Resources[0].ID
	reordered.Resources[0].ReservationID = request.Resources[0].ReservationID
	reordered.Resources[0].OwnedResourceIDs = request.Resources[0].OwnedResourceIDs
	snapshot, err := host.store.ReadPlacementSnapshot(t.Context(), host.id)
	if err != nil {
		t.Fatal(err)
	}
	reorderedDecision, err := placement.Plan(reordered, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), second, reordered, reorderedDecision, host.planFor(t, second, reordered.Resources[0].ID, ports[0], 1)); err != nil {
		t.Fatalf("a reordered port list was refused: %v", err)
	}
}

// A terminal operation keeps its claim row for the rest of its lease. It must reserve nothing.
func TestInstallPlacementPlanRefusesATerminalOperation(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimApp(t, "db")
	request, decision, plan := host.place(t, claim, smallBudget, nil)
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, claim.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("a failed operation reserved capacity: %v", err)
	}
	if total := host.count(t, `SELECT count(*) FROM resource_allocations`); total != 0 {
		t.Fatalf("allocations=%d, want none for a terminal operation", total)
	}
}

// A first policy write and a first probe must serialize on the node they describe.
func TestConcurrentFirstWritesSerializeOnTheNode(t *testing.T) {
	s, installation, node, session := capacityFixture(t)
	policy := testPolicy(installation, node)
	start := make(chan struct{})
	results := make(chan error, 4)
	var racing sync.WaitGroup
	for range 4 {
		racing.Go(func() {
			<-start
			_, err := s.PutNodePlacementPolicy(t.Context(), policy, 0)
			results <- err
		})
	}
	close(start)
	racing.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
			continue
		}
		if !errors.Is(err, ErrPolicyRevision) {
			t.Fatalf("unexpected policy failure: %v", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d first policy writes, want one", accepted)
	}
	// Concurrent first probes must not share a revision.
	probes := make(chan placement.CapacityProbe, 4)
	begin := make(chan struct{})
	var probing sync.WaitGroup
	for range 4 {
		probing.Go(func() {
			<-begin
			probe, err := s.BeginCapacityProbe(t.Context(), installation, node, session)
			if err == nil {
				probes <- probe
			}
		})
	}
	close(begin)
	probing.Wait()
	close(probes)
	seen := make(map[int64]bool)
	for probe := range probes {
		if seen[probe.Revision] {
			t.Fatalf("probe revision %d was issued twice", probe.Revision)
		}
		seen[probe.Revision] = true
	}
	if len(seen) != 4 {
		t.Fatalf("issued %d distinct probe revisions, want four", len(seen))
	}
}

// enroll adds a second node to the same installation.
func (h *placementHost) enroll(t *testing.T) uuid.UUID {
	t.Helper()
	node := uuid.New()
	if _, err := h.store.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, h.id, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	return node
}
