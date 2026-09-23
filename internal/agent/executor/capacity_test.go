package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	noderuntime "github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// measuredRuntime answers a usage question without a host. The challenge rules
// are what these tests measure, so the driver is replaced rather than driven.
type measuredRuntime struct {
	usage []noderuntime.ResourceUsage
	err   error
	asked [][]uuid.UUID
	// resources and drift are what one scan of the host finds.
	resources []noderuntime.Resource
	drift     []noderuntime.Drift
	// duringUsage runs inside the measurement, while the executor lock is
	// released. A test that needs something to happen in that window hooks here
	// rather than racing it with a sleep.
	duringUsage func()
}

func (m *measuredRuntime) Execute(context.Context, *agentv1.Command, noderuntime.Secrets) (noderuntime.Result, error) {
	return noderuntime.Result{}, errors.New("not used")
}
func (m *measuredRuntime) Recover(context.Context, *agentv1.Command, []*agentv1.Effect) (noderuntime.Result, error) {
	return noderuntime.Result{}, errors.New("not used")
}
func (m *measuredRuntime) Inventory(context.Context) ([]noderuntime.Resource, error) {
	return m.resources, nil
}
func (m *measuredRuntime) Drift(context.Context) ([]noderuntime.Drift, error) { return m.drift, nil }
func (m *measuredRuntime) InspectCluster(context.Context, uuid.UUID, []*agentv1.Effect) (noderuntime.ClusterInspection, error) {
	return noderuntime.ClusterInspection{}, nil
}
func (m *measuredRuntime) Usage(_ context.Context, resources []uuid.UUID) ([]noderuntime.ResourceUsage, error) {
	m.asked = append(m.asked, resources)
	if m.duringUsage != nil {
		m.duringUsage()
	}
	return m.usage, m.err
}

// measurable writes a fixture proc tree that answers one measurement.
func measurable(t *testing.T) capacity.Host {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meminfo"), []byte("MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stat := filepath.Join(dir, "stat")
	// A real kernel counter advances the whole time the host runs, so the fixture
	// advances too. A single rewrite on a timer is only inside the sample window
	// when the machine is idle: under load it lands before the first read or after
	// the second, both reads see one value, and the measurement fails for a reason
	// that has nothing to do with the test.
	//
	// Each step adds 1000 aggregate jiffies of which 250 are idle, so any two reads
	// give a positive interval and an idle share inside it.
	write := func(step int64) error {
		line := fmt.Sprintf("cpu  %d 0 %d %d 100 0 0 0\n", 100+500*step, 100+250*step, 700+250*step)
		// The reader is a plain ReadFile. A truncating rewrite can hand it an empty
		// or half-written line, so the new content is renamed over the old one.
		temporary := stat + ".next"
		if err := os.WriteFile(temporary, []byte(line), 0o600); err != nil {
			return err
		}
		return os.Rename(temporary, stat)
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	// The rewrite must not outlive the test. A write into a directory the testing
	// package is removing fails that removal, which fails the test that owns it.
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for step := int64(1); ; step++ {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			if write(step) != nil {
				return
			}
		}
	}()
	return capacity.Host{ProcRoot: dir, StoragePath: t.TempDir(), Sample: 40 * time.Millisecond}
}

// challenged builds an executor that can answer a capacity challenge.
func challenged(t *testing.T, native noderuntime.Runtime) (*Executor, context.Context, string) {
	t.Helper()
	identity := journal.Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	boot := uuid.NewString()
	e, err := New(j, native, Options{
		Workers: 1, QueueSize: 1, PollInterval: time.Second,
		Host: measurable(t), BootID: boot, StorageID: "pool-a", RuntimeVersion: "26.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := localSession(t, e)
	return e, ctx, boot
}

// request builds a challenge that names the executor's own session and host.
func request(t *testing.T, e *Executor, boot string, known ...*agentv1.KnownReservation) *agentv1.CapacityRequest {
	t.Helper()
	e.mu.Lock()
	session := e.session.id
	e.mu.Unlock()
	return &agentv1.CapacityRequest{
		ProbeId: uuid.NewString(), ProbeRevision: 1, SessionId: session, BootId: boot,
		JournalGeneration: e.journal.Identity().JournalGeneration,
		ExpiresUnixMs:     time.Now().Add(5 * time.Second).UnixMilli(),
		Known:             known,
	}
}

func answer(t *testing.T, e *Executor, ctx context.Context, req *agentv1.CapacityRequest) (*agentv1.CapacityReport, error) {
	t.Helper()
	message, err := e.Handle(ctx, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_CapacityRequest{CapacityRequest: req}})
	if err != nil {
		return nil, err
	}
	return message.GetCapacityReport(), nil
}

func TestCapacityMeasuresTheHostAndEchoesKnownReservations(t *testing.T) {
	resource, reservation := uuid.New(), uuid.New()
	native := &measuredRuntime{usage: []noderuntime.ResourceUsage{
		{ResourceID: resource, Used: noderuntime.Amount{CPUMillis: 250, MemoryBytes: 1 << 30, DiskBytes: 2 << 30}},
	}}
	e, ctx, boot := challenged(t, native)
	req := request(t, e, boot, &agentv1.KnownReservation{ReservationId: reservation.String(), ResourceId: resource.String()})
	report, err := answer(t, e, ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if report.ProbeId != req.ProbeId || report.ProbeRevision != 1 {
		t.Fatal("the report did not answer the challenge it was given")
	}
	if report.StorageId != "pool-a" || report.RuntimeVersion != "26.1" {
		t.Fatalf("storage=%q runtime=%q", report.StorageId, report.RuntimeVersion)
	}
	// Capacity must be measured, not declared.
	if report.Capacity.MemoryBytes != 16000000*1024 || report.Capacity.CpuMillicores <= 0 || report.Capacity.DiskBytes <= 0 {
		t.Fatalf("capacity=%+v", report.Capacity)
	}
	// The agent cannot learn a reservation identity from a host, so it echoes
	// the pair it was given against what it measured for that resource.
	if len(report.Reservations) != 1 || report.Reservations[0].ReservationId != reservation.String() {
		t.Fatalf("reservations=%+v", report.Reservations)
	}
	if report.Reservations[0].Used.DiskBytes != 2<<30 {
		t.Fatalf("itemized use=%+v", report.Reservations[0].Used)
	}
	if len(native.asked) != 1 || len(native.asked[0]) != 1 || native.asked[0][0] != resource {
		t.Fatalf("the driver was asked for %+v", native.asked)
	}
	// Itemized use can never exceed the total, which the far end refuses.
	if report.Used.DiskBytes < report.Reservations[0].Used.DiskBytes {
		t.Fatal("the report contradicts itself")
	}
}

// A resource this node does not hold is absent, never reported as zero.
func TestCapacityOmitsAResourceItDoesNotHold(t *testing.T) {
	e, ctx, boot := challenged(t, &measuredRuntime{})
	req := request(t, e, boot, &agentv1.KnownReservation{ReservationId: uuid.NewString(), ResourceId: uuid.NewString()})
	report, err := answer(t, e, ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Reservations) != 0 {
		t.Fatalf("reservations=%+v, want none measured", report.Reservations)
	}
}

func TestCapacityRefusesAChallengeForOtherState(t *testing.T) {
	e, ctx, boot := challenged(t, &measuredRuntime{})
	for name, change := range map[string]func(*agentv1.CapacityRequest){
		// A changed session invalidates capacity even when the boot matches.
		"another session":   func(r *agentv1.CapacityRequest) { r.SessionId = uuid.NewString() },
		"another boot":      func(r *agentv1.CapacityRequest) { r.BootId = uuid.NewString() },
		"another journal":   func(r *agentv1.CapacityRequest) { r.JournalGeneration = uuid.NewString() },
		"a closed window":   func(r *agentv1.CapacityRequest) { r.ExpiresUnixMs = time.Now().Add(-time.Second).UnixMilli() },
		"no probe revision": func(r *agentv1.CapacityRequest) { r.ProbeRevision = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			req := request(t, e, boot)
			change(req)
			if _, err := answer(t, e, ctx, req); err == nil {
				t.Fatal("the agent answered a challenge it should have refused")
			}
		})
	}
}

// A duplicate identity in the known list is a malformed frame.
func TestCapacityRefusesADuplicateKnownReservation(t *testing.T) {
	e, ctx, boot := challenged(t, &measuredRuntime{})
	resource := uuid.NewString()
	req := request(t, e, boot,
		&agentv1.KnownReservation{ReservationId: uuid.NewString(), ResourceId: resource},
		&agentv1.KnownReservation{ReservationId: uuid.NewString(), ResourceId: resource})
	if _, err := answer(t, e, ctx, req); !errors.Is(err, controlstream.ErrMessage) {
		t.Fatalf("err=%v, want ErrMessage", err)
	}
}

// A challenge replaced while the host is measured must never be answered.
func TestCapacityNeverAnswersAReplacedChallenge(t *testing.T) {
	native := &measuredRuntime{}
	e, ctx, boot := challenged(t, native)
	// The replacement happens inside the measurement, where the executor lock is
	// released. Hooking the driver puts it provably in that window, rather than
	// racing it with a sleep that a loaded machine can miss.
	native.duringUsage = func() {
		e.mu.Lock()
		if e.session != nil {
			e.session.probe = uuid.NewString()
		}
		e.mu.Unlock()
	}
	if _, err := answer(t, e, ctx, request(t, e, boot)); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("err=%v, want ErrStaleOwner", err)
	}
}

// An executor with no approved pool cannot answer, and must not invent one.
func TestCapacityRefusesWithoutAnApprovedPool(t *testing.T) {
	identity := journal.Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	boot := uuid.NewString()
	e, err := New(j, &measuredRuntime{}, Options{Workers: 1, QueueSize: 1, PollInterval: time.Second, Host: measurable(t), BootID: boot, RuntimeVersion: "26.1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := localSession(t, e)
	if _, err := answer(t, e, ctx, request(t, e, boot)); !errors.Is(err, operation.ErrStaleOwner) {
		t.Fatalf("err=%v, want a refusal rather than a declared pool", err)
	}
}

// A driver that cannot measure must fail the probe rather than report zero.
func TestCapacityFailsWhenTheDriverCannotMeasure(t *testing.T) {
	native := &measuredRuntime{err: errors.New("daemon unreachable")}
	e, ctx, boot := challenged(t, native)
	if _, err := answer(t, e, ctx, request(t, e, boot)); err == nil {
		t.Fatal("an unmeasurable driver reported a capacity")
	}
}
