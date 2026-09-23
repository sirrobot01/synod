// Package executor connects authenticated sessions to durable local execution.
package executor

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Options bounds executor concurrency, queue depth, and timing.
type Options struct {
	Workers           int
	QueueSize         int
	PollInterval      time.Duration
	InspectionTimeout time.Duration
	// Host is what a capacity challenge measures. An executor with no measurable
	// host refuses a challenge rather than reporting a figure it did not observe.
	Host capacity.Host
	// BootID and StorageID identify what was measured. The controller compares
	// the boot against the challenge it issued, and policy approves the storage.
	BootID    string
	StorageID string
	// RuntimeVersion is the driver version the daemon already negotiated.
	RuntimeVersion string
	// MinFreeBytes is the operator-declared floor for the local storage pool. New
	// work that would take the pool to or below it is refused. Zero declares no
	// floor, so the pool is only refused when it has no free space at all:
	// inventing a headroom figure here would reject work nobody asked to reject.
	MinFreeBytes int64
	// Probers check a live engine through the endpoint it publishes, by engine name.
	// An engine with no prober cannot be told about, which is an unavailable probe
	// rather than a failure.
	Probers map[string]engine.Prober
}

type epochGate struct {
	epoch  *agentv1.Epoch
	active bool
	ready  bool
	report *agentv1.BarrierResult
	unsent bool
}

type session struct {
	ctx         context.Context
	id          string
	gates       map[string]*epochGate
	sent        map[string]bool
	inventoryID string
	inventory   []runtime.Resource
	// drift is what the same scan found that does not belong. It travels on the
	// first page of the scan, because drift is a property of the host and not of
	// a page.
	drift      []runtime.Drift
	truncated  bool
	inspection bool
	// probe is the capacity challenge this session is answering. A replaced
	// challenge must never be answered, so the answer checks it again.
	probe string
	// secrets are the resolved credentials this session was sent, by purpose. They
	// are held here and nowhere else, so they end when the session does.
	secrets map[string]sessionSecret
}

type job struct {
	cluster string
	command string
	session *session
	gate    *epochGate
	barrier bool
}

// Executor runs controller commands against the local runtime under journal authority.
type Executor struct {
	journal        *journal.Journal
	runtime        runtime.Runtime
	options        Options
	mu             sync.Mutex
	session        *session
	active         map[string]bool
	jobs           chan job
	wake           chan struct{}
	running        bool
	started        bool
	inventoryBusy  bool
	inspectionBusy bool
	capacityBusy   bool
	failure        error
	nextCompaction time.Time
}

// New builds an executor on a journal and a runtime and applies default options.
func New(j *journal.Journal, native runtime.Runtime, options Options) (*Executor, error) {
	if j == nil || native == nil {
		return nil, errors.New("executor requires journal and runtime")
	}
	if options.Workers == 0 {
		options.Workers = 4
	}
	if options.QueueSize == 0 {
		options.QueueSize = 64
	}
	if options.PollInterval == 0 {
		options.PollInterval = time.Second
	}
	if options.InspectionTimeout == 0 {
		options.InspectionTimeout = 5 * time.Second
	}
	if options.Workers < 1 || options.Workers > 32 || options.QueueSize < 1 || options.QueueSize > 256 || options.PollInterval < 10*time.Millisecond || options.PollInterval > time.Minute || options.InspectionTimeout < time.Second || options.InspectionTimeout > time.Minute {
		return nil, errors.New("invalid executor limits")
	}
	return &Executor{journal: j, runtime: native, options: options, active: make(map[string]bool), jobs: make(chan job, options.QueueSize), wake: make(chan struct{}, 1)}, nil
}

// Run owns a fixed worker pool. The executable runs the borrowed transport separately.
func (e *Executor) Run(ctx context.Context, stream *transport.Client) error {
	if stream == nil {
		return errors.New("executor requires transport")
	}
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return errors.New("executor already running")
	}
	e.running, e.started = true, true
	e.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	for range e.options.Workers {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case task := <-e.jobs:
					if task.barrier {
						e.inspectBarrier(ctx, task)
					} else {
						e.execute(ctx, task)
					}
					e.mu.Lock()
					delete(e.active, task.cluster)
					e.mu.Unlock()
				}
			}
		})
	}
	defer func() {
		cancel()
		workers.Wait()
		e.mu.Lock()
		e.running = false
		e.session = nil
		clear(e.active)
		e.mu.Unlock()
	}()
	tick := time.NewTicker(e.options.PollInterval)
	defer tick.Stop()
	for {
		if err := e.schedule(ctx, stream); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		case <-e.wake:
		}
	}
}

func (e *Executor) schedule(ctx context.Context, stream *transport.Client) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failure != nil {
		return e.failure
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	now := time.Now()
	if !now.Before(e.nextCompaction) {
		count, err := e.journal.Compact(now)
		if err != nil {
			return err
		}
		e.nextCompaction = now.Add(time.Minute)
		if count == 256 {
			e.nextCompaction = now
		}
	}
	current := e.session
	if current != nil && current.ctx.Err() == nil {
		results, err := e.journal.Results()
		if err != nil {
			return err
		}
		for _, result := range results {
			if current.sent[result.CommandId] {
				continue
			}
			if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_TerminalResult{TerminalResult: result}}); err != nil {
				if !errors.Is(err, controlstream.ErrBusy) && !errors.Is(err, controlstream.ErrDisconnected) {
					return err
				}
				break
			}
			current.sent[result.CommandId] = true
		}
		for _, cluster := range slices.Sorted(maps.Keys(current.gates)) {
			gate := current.gates[cluster]
			if gate.unsent {
				if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_BarrierResult{BarrierResult: gate.report}}); err == nil {
					gate.unsent = false
				} else if !errors.Is(err, controlstream.ErrBusy) && !errors.Is(err, controlstream.ErrDisconnected) {
					return err
				}
			}
			if !gate.active && !gate.ready && !e.active[cluster] {
				select {
				case e.jobs <- job{cluster: cluster, barrier: true, session: current, gate: gate}:
					e.active[cluster] = true
				default:
				}
			}
		}
	}
	recoverable, err := e.journal.RecoveryRecords()
	if err != nil {
		return err
	}
	pending, err := e.journal.Pending()
	if err != nil {
		return err
	}
	for _, record := range append(recoverable, pending...) {
		cluster := record.Epoch.ClusterId
		if e.active[cluster] {
			continue
		}
		var gate *epochGate
		if record.State == journal.Queued {
			if current == nil || current.ctx.Err() != nil {
				continue
			}
			gate = current.gates[cluster]
			if gate == nil || !gate.active || !proto.Equal(gate.epoch, record.Epoch) {
				continue
			}
		}
		select {
		case e.jobs <- job{cluster: cluster, command: record.CommandID, session: current, gate: gate}:
			e.active[cluster] = true
		default:
			return nil
		}
	}
	return nil
}

func (e *Executor) fail(err error) {
	e.mu.Lock()
	if e.failure == nil {
		e.failure = err
	}
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
