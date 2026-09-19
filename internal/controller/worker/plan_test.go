package worker

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const testImageDigest = "postgres@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testOperation() operation.Operation {
	return operation.Operation{ID: uuid.New(), InstallationID: uuid.New(), ClusterID: uuid.New(), DesiredRevision: 3, PlanVersion: 1}
}

// planStep builds one hashed step whose payload matches its postcondition.
func planStep(t *testing.T, op operation.Operation, nodeID, resourceID string, postcondition agentv1.Postcondition, dependsOn ...string) *agentv1.PlanStep {
	t.Helper()
	input := &agentv1.CommandInput{ResourceId: resourceID, PayloadVersion: operation.PayloadVersion, Timeout: durationpb.New(time.Minute)}
	switch postcondition {
	case agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS:
		input.Payload = &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: resourceID}}
	case agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS:
		input.Payload = &agentv1.CommandInput_EnsureNetwork{EnsureNetwork: &agentv1.EnsureNetwork{NetworkId: resourceID}}
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS:
		input.Payload = &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{ImageDigest: testImageDigest, Engine: "postgres", VolumeId: uuid.NewString(), NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST}}
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING:
		input.Payload = &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
		input.Payload = &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(10 * time.Second)}}
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
		input.Payload = &agentv1.CommandInput_RemoveContainer{RemoveContainer: &agentv1.RemoveContainer{}}
	case agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED:
		input.Payload = &agentv1.CommandInput_InspectContainer{InspectContainer: &agentv1.InspectContainer{}}
	default:
		t.Fatalf("postcondition %v has no payload", postcondition)
	}
	command := &agentv1.Command{
		Epoch:       &agentv1.Epoch{InstallationId: op.InstallationID.String(), NodeId: nodeID, ClusterId: op.ClusterID.String(), RecoveryGeneration: uuid.NewString(), ExecutionEpoch: 1},
		OperationId: op.ID.String(), StepId: uuid.NewString(), CommandId: uuid.NewString(),
		DesiredRevision: op.DesiredRevision, PlanVersion: 1, Input: input,
	}
	rehash(t, command)
	return &agentv1.PlanStep{Command: command, DependsOn: dependsOn, MaxAttempts: 3, CancellationBoundary: agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_BEFORE_EXECUTION, Postcondition: postcondition}
}

// rehash restores the immutable input hash so that a mutation reaches the check it targets.
func rehash(t *testing.T, command *agentv1.Command) {
	t.Helper()
	hash, err := operation.CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	command.InputHash = hash[:]
}

// provisionPlan ensures a volume and a network, then creates and starts one container.
func provisionPlan(t *testing.T, op operation.Operation) *agentv1.ExecutionPlan {
	t.Helper()
	node, container := uuid.NewString(), uuid.NewString()
	volume := planStep(t, op, node, uuid.NewString(), agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS)
	network := planStep(t, op, node, uuid.NewString(), agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS)
	create := planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, volume.Command.StepId, network.Command.StepId)
	start := planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, create.Command.StepId)
	return &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{volume, network, create, start}}
}

func TestValidatePlanAcceptsDeclaredOrder(t *testing.T) {
	op := testOperation()
	if err := validatePlan(provisionPlan(t, op), op); err != nil {
		t.Fatal(err)
	}
}

func TestValidatePlanRejectsUnsupportedContent(t *testing.T) {
	op := testOperation()
	if err := validatePlan(nil, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatal("accepted an absent plan")
	}
	for name, change := range map[string]func(*agentv1.ExecutionPlan){
		"unknown plan version":  func(p *agentv1.ExecutionPlan) { p.Version = 2 },
		"no steps":              func(p *agentv1.ExecutionPlan) { p.Steps = nil },
		"absent step":           func(p *agentv1.ExecutionPlan) { p.Steps[1] = nil },
		"absent command":        func(p *agentv1.ExecutionPlan) { p.Steps[1].Command = nil },
		"no attempt budget":     func(p *agentv1.ExecutionPlan) { p.Steps[1].MaxAttempts = 0 },
		"excess attempt budget": func(p *agentv1.ExecutionPlan) { p.Steps[1].MaxAttempts = 11 },
		"no cancellation boundary": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_UNSPECIFIED
		},
		"unknown cancellation boundary": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP + 1
		},
		"broken input hash": func(p *agentv1.ExecutionPlan) { p.Steps[1].Command.InputHash = []byte("not the input hash") },
		"other installation": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.Epoch.InstallationId = uuid.NewString()
			rehash(t, p.Steps[1].Command)
		},
		"other cluster": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.Epoch.ClusterId = uuid.NewString()
			rehash(t, p.Steps[1].Command)
		},
		"other operation": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.OperationId = uuid.NewString()
			rehash(t, p.Steps[1].Command)
		},
		"other desired revision": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.DesiredRevision = op.DesiredRevision + 1
			rehash(t, p.Steps[1].Command)
		},
		"other command plan version": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.PlanVersion = 2
			rehash(t, p.Steps[1].Command)
		},
		"plaintext secret reference": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.Input.Secrets = []*agentv1.SecretReference{{CredentialId: uuid.NewString(), Version: 1, Purpose: "database-password"}}
			rehash(t, p.Steps[1].Command)
		},
		"repeated command identity": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.CommandId = p.Steps[0].Command.CommandId
			rehash(t, p.Steps[1].Command)
		},
		"repeated step identity": func(p *agentv1.ExecutionPlan) {
			p.Steps[1].Command.StepId = p.Steps[0].Command.StepId
			rehash(t, p.Steps[1].Command)
		},
		"postcondition without payload": func(p *agentv1.ExecutionPlan) {
			p.Steps[0].Postcondition = agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING
		},
		"unknown dependency": func(p *agentv1.ExecutionPlan) { p.Steps[2].DependsOn = []string{uuid.NewString()} },
		"forward dependency": func(p *agentv1.ExecutionPlan) {
			p.Steps[0].DependsOn = []string{p.Steps[3].Command.StepId}
		},
		"self dependency": func(p *agentv1.ExecutionPlan) {
			p.Steps[0].DependsOn = []string{p.Steps[0].Command.StepId}
		},
	} {
		plan := provisionPlan(t, op)
		change(plan)
		if err := validatePlan(plan, op); !errors.Is(err, ErrUnsupportedPlan) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestValidatePlanBoundsStepCount(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	var steps []*agentv1.PlanStep
	for range 129 {
		steps = append(steps, planStep(t, op, node, uuid.NewString(), agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS))
	}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: steps[:128]}, op); err != nil {
		t.Fatal(err)
	}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: steps}, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("accepted %d steps: %v", len(steps), err)
	}
}

// succeededResult reports the step as done with one resolved native effect in the given state.
func succeededResult(step *agentv1.PlanStep, state string) *agentv1.TerminalResult {
	command := step.Command
	return &agentv1.TerminalResult{
		Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), CommandId: command.CommandId, InputHash: command.InputHash, Sequence: 1, State: "succeeded",
		Effects: []*agentv1.Effect{{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "docker-command:" + command.CommandId, State: state}},
	}
}

func TestCheckResultMatchesPostconditionState(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	for _, tt := range []struct {
		postcondition    agentv1.Postcondition
		proved, unproved string
	}{
		{agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS, "present", "absent"},
		{agentv1.Postcondition_POSTCONDITION_NETWORK_EXISTS, "present", ""},
		{agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, "created", "absent"},
		{agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, "running", "exited"},
		{agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, "exited", "running"},
		{agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, "absent", "exited"},
		{agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED, "paused", "absent"},
	} {
		step := planStep(t, op, node, uuid.NewString(), tt.postcondition)
		if err := checkResult(step, succeededResult(step, tt.proved)); err != nil {
			t.Fatalf("%v rejected %q: %v", tt.postcondition, tt.proved, err)
		}
		if err := checkResult(step, succeededResult(step, tt.unproved)); !errors.Is(err, ErrPostconditionUnproved) {
			t.Fatalf("%v accepted %q: %v", tt.postcondition, tt.unproved, err)
		}
	}
}

// A historical receipt keeps the epoch of the ownership run that dispatched it.
func TestCheckResultAcceptsEarlierExecutionEpoch(t *testing.T) {
	op := testOperation()
	step := planStep(t, op, uuid.NewString(), uuid.NewString(), agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING)
	step.Command.Epoch.ExecutionEpoch = 7
	result := succeededResult(step, "running")
	result.Epoch.ExecutionEpoch = 4
	result.Epoch.RecoveryGeneration = uuid.NewString()
	if err := checkResult(step, result); err != nil {
		t.Fatal(err)
	}
}

func TestCheckResultRejectsWeakEvidence(t *testing.T) {
	op := testOperation()
	step := planStep(t, op, uuid.NewString(), uuid.NewString(), agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING)
	if err := checkResult(step, nil); !errors.Is(err, ErrPostconditionUnproved) {
		t.Fatal("accepted an absent result")
	}
	for name, change := range map[string]func(*agentv1.TerminalResult){
		"absent epoch":       func(r *agentv1.TerminalResult) { r.Epoch = nil },
		"other command":      func(r *agentv1.TerminalResult) { r.CommandId = uuid.NewString() },
		"other input hash":   func(r *agentv1.TerminalResult) { r.InputHash = []byte("another input hash") },
		"other installation": func(r *agentv1.TerminalResult) { r.Epoch.InstallationId = uuid.NewString() },
		"other node":         func(r *agentv1.TerminalResult) { r.Epoch.NodeId = uuid.NewString() },
		"other cluster":      func(r *agentv1.TerminalResult) { r.Epoch.ClusterId = uuid.NewString() },
		"unfinished state":   func(r *agentv1.TerminalResult) { r.State = "failed" },
		"failure code":       func(r *agentv1.TerminalResult) { r.FailureCode = "container_start_failed" },
		"absent effect":      func(r *agentv1.TerminalResult) { r.Effects = []*agentv1.Effect{nil} },
		"active effect":      func(r *agentv1.TerminalResult) { r.Effects[0].Active = true },
		"unknown outcome":    func(r *agentv1.TerminalResult) { r.Effects[0].OutcomeUnknown = true },
		"other command effect": func(r *agentv1.TerminalResult) {
			r.Effects[0].CommandId = uuid.NewString()
		},
		"other resource effect": func(r *agentv1.TerminalResult) {
			r.Effects[0].ResourceId = uuid.NewString()
		},
		"no native operation": func(r *agentv1.TerminalResult) {
			r.Effects[0].NativeOperationId = "docker-command:" + uuid.NewString()
		},
		"no effects": func(r *agentv1.TerminalResult) { r.Effects = nil },
		"repeated native operation": func(r *agentv1.TerminalResult) {
			r.Effects = append(r.Effects, proto.Clone(r.Effects[0]).(*agentv1.Effect))
		},
	} {
		result := succeededResult(step, "running")
		change(result)
		if err := checkResult(step, result); !errors.Is(err, ErrPostconditionUnproved) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// stepIDs names frontier steps by their position in the plan.
func stepIDs(plan *agentv1.ExecutionPlan, frontier []*agentv1.PlanStep) []int {
	var positions []int
	for index, step := range plan.Steps {
		for _, kept := range frontier {
			if kept == step {
				positions = append(positions, index)
			}
		}
	}
	return positions
}

func completedThrough(plan *agentv1.ExecutionPlan, count int) map[string]bool {
	completed := make(map[string]bool)
	for _, step := range plan.Steps[:count] {
		completed[step.Command.StepId] = true
	}
	return completed
}

func TestRecoveryFrontierKeepsLatestObligation(t *testing.T) {
	op, node, container := testOperation(), uuid.NewString(), uuid.NewString()
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{
		planStep(t, op, node, uuid.NewString(), agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT),
	}}
	for _, tt := range []struct {
		completed int
		want      []int
	}{
		{1, []int{0}},
		{3, []int{0, 1, 2}},
		{4, []int{0, 1, 3}},
		{5, []int{0, 4}},
	} {
		frontier, _, err := recoveryFrontier(plan, completedThrough(plan, tt.completed))
		if err != nil {
			t.Fatal(err)
		}
		if got := stepIDs(plan, frontier); !slices.Equal(got, tt.want) {
			t.Fatalf("%d completed: frontier %v, want %v", tt.completed, got, tt.want)
		}
	}
}

func TestRecoveryFrontierRestoresRemovedResource(t *testing.T) {
	op, node, container := testOperation(), uuid.NewString(), uuid.NewString()
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT),
		planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS),
	}}
	frontier, _, err := recoveryFrontier(plan, completedThrough(plan, 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := stepIDs(plan, frontier); len(got) != 1 || got[0] != 2 {
		t.Fatalf("frontier %v, want the later creation alone", got)
	}
}

func TestRecoveryFrontierKeepsInspectionWithoutOwnership(t *testing.T) {
	op, node, container := testOperation(), uuid.NewString(), uuid.NewString()
	inspect := planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED)
	create := planStep(t, op, node, container, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS)
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{inspect, create}}
	frontier, _, err := recoveryFrontier(plan, map[string]bool{inspect.Command.StepId: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := stepIDs(plan, frontier); len(got) != 1 || got[0] != 0 {
		t.Fatalf("frontier %v, want the inspection alone", got)
	}
	frontier, _, err = recoveryFrontier(plan, completedThrough(plan, 2))
	if err != nil {
		t.Fatal(err)
	}
	if got := stepIDs(plan, frontier); len(got) != 1 || got[0] != 1 {
		t.Fatalf("frontier %v, want the configuration alone", got)
	}
}

// One resource ID on two nodes carries two separate obligations.
func TestRecoveryFrontierSeparatesNodes(t *testing.T) {
	op, resource := testOperation(), uuid.NewString()
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: []*agentv1.PlanStep{
		planStep(t, op, uuid.NewString(), resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS),
		planStep(t, op, uuid.NewString(), resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT),
	}}
	frontier, _, err := recoveryFrontier(plan, completedThrough(plan, 2))
	if err != nil {
		t.Fatal(err)
	}
	if got := stepIDs(plan, frontier); len(got) != 2 {
		t.Fatalf("frontier %v, want both nodes", got)
	}
}

func TestRecoveryFrontierHashTracksContent(t *testing.T) {
	op := testOperation()
	plan := provisionPlan(t, op)
	first, hash, err := recoveryFrontier(plan, completedThrough(plan, 3))
	if err != nil {
		t.Fatal(err)
	}
	_, repeated, err := recoveryFrontier(plan, completedThrough(plan, 3))
	if err != nil {
		t.Fatal(err)
	}
	if hash != repeated {
		t.Fatal("the same frontier produced two hashes")
	}
	second, changed, err := recoveryFrontier(plan, completedThrough(plan, 4))
	if err != nil {
		t.Fatal(err)
	}
	if hash == changed {
		t.Fatal("a changed frontier kept its hash")
	}
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("recovery produced an empty frontier")
	}
}

func TestValidatePlanAcceptsEveryPostcondition(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	var steps []*agentv1.PlanStep
	// A removal must depend on the stop of its own resource, so those two share a
	// resource and the removal names the stop.
	torn := uuid.NewString()
	var stopStep string
	for postcondition := agentv1.Postcondition_POSTCONDITION_VOLUME_EXISTS; postcondition <= agentv1.Postcondition_POSTCONDITION_CONTAINER_INSPECTED; postcondition++ {
		switch postcondition {
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED:
			step := planStep(t, op, node, torn, postcondition)
			stopStep = step.Command.StepId
			steps = append(steps, step)
		case agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT:
			steps = append(steps, planStep(t, op, node, torn, postcondition, stopStep))
		default:
			steps = append(steps, planStep(t, op, node, uuid.NewString(), postcondition))
		}
	}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: steps}, op); err != nil {
		t.Fatal(err)
	}
}

// Specification 33 requires stopping active work before runtime objects are removed.
func TestValidatePlanRequiresAStopBeforeARemoval(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	resource := uuid.NewString()
	stop := planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED)

	// A removal that depends on the stop of its own resource is a sound teardown.
	sound := []*agentv1.PlanStep{stop, planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, stop.Command.StepId)}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: sound}, op); err != nil {
		t.Fatal(err)
	}

	// A removal with no stop at all tears a running engine out from under clients.
	alone := []*agentv1.PlanStep{planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT)}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: alone}, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("err=%v, want a plan that removes without stopping refused", err)
	}

	// A stop that is merely earlier in the list is not an ordering. Only a declared
	// dependency makes the engine stop first.
	unordered := []*agentv1.PlanStep{stop, planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT)}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: unordered}, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("err=%v, want an undeclared ordering refused", err)
	}

	// A stop of some other resource proves nothing about this one.
	other := planStep(t, op, node, uuid.NewString(), agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED)
	crossed := []*agentv1.PlanStep{other, planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, other.Command.StepId)}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: crossed}, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("err=%v, want another resource's stop refused", err)
	}
}

// A removal is destructive, so it can only be cancelled before it starts.
func TestValidatePlanBindsARemovalToItsBoundary(t *testing.T) {
	op, node := testOperation(), uuid.NewString()
	resource := uuid.NewString()
	stop := planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED)
	removal := planStep(t, op, node, resource, agentv1.Postcondition_POSTCONDITION_CONTAINER_ABSENT, stop.Command.StepId)
	removal.CancellationBoundary = agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP
	steps := []*agentv1.PlanStep{stop, removal}
	if err := validatePlan(&agentv1.ExecutionPlan{Version: 1, Steps: steps}, op); !errors.Is(err, ErrUnsupportedPlan) {
		t.Fatalf("err=%v, want a removal with a safe-stop boundary refused", err)
	}
}
