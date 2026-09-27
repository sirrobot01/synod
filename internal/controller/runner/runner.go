// Package runner claims pending operations and runs one controller worker for each.
// It owns no execution policy. Every decision belongs to the worker it starts.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controller/agentstream"
	"github.com/sirrobot01/synod/internal/controller/worker"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/coordinator"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Config holds the settings of one controller runner.
type Config struct {
	Store          *postgres.Store
	Transport      worker.Transport
	Coordinator    *coordinator.Coordinator
	InstallationID uuid.UUID
	OwnerID        string
	Logger         *slog.Logger
	// ClaimTTL is the durable ownership lifetime. The runner renews at a third of it.
	ClaimTTL time.Duration
	// LeaseSeconds is the etcd session TTL of one elected term.
	LeaseSeconds int
	// MaxWorkers bounds concurrent operations in this process.
	MaxWorkers int
	// ScanInterval bounds how long the runner waits before it rescans without a notification.
	ScanInterval time.Duration
	// CampaignTimeout bounds one wait for elected ownership of a cluster.
	CampaignTimeout time.Duration
	// IdleBackoff is the pause before a worker retries while it holds its claim.
	IdleBackoff time.Duration
	// OutboxPage bounds one page of the pending outbox scan.
	OutboxPage int
	// Keyring opens the sealed credentials a plan names. A controller without one
	// runs plans that need no credential and refuses the ones that do.
	Keyring *secret.Keyring
	// Planner produces the plan for a claimed operation that has none.
	// *provision.Planner satisfies it. A controller without one runs only plans
	// something else installed.
	Planner Planner
	// EvidenceWait bounds one worker wait for committed agent evidence.
	EvidenceWait time.Duration
}

// Runner keeps at most one worker for each cluster in this process.
type Runner struct {
	config   Config
	receiver agentstream.Receiver
	slots    chan struct{}
	mu       sync.Mutex
	workers  map[uuid.UUID]*worker.Worker
	wg       sync.WaitGroup
}

// New checks the runner settings and applies defaults.
func New(config Config) (*Runner, error) {
	if config.Store == nil || config.Transport == nil || config.Coordinator == nil {
		return nil, errors.New("runner requires a store, a transport, and a coordinator")
	}
	if config.InstallationID == uuid.Nil || config.OwnerID == "" || len(config.OwnerID) > 256 {
		return nil, errors.New("runner requires an installation and an owner identity")
	}
	if config.Logger == nil {
		return nil, errors.New("runner requires a logger")
	}
	if config.ClaimTTL == 0 {
		config.ClaimTTL = time.Minute
	}
	if config.LeaseSeconds == 0 {
		config.LeaseSeconds = 10
	}
	if config.MaxWorkers == 0 {
		config.MaxWorkers = 16
	}
	if config.ScanInterval == 0 {
		config.ScanInterval = 30 * time.Second
	}
	if config.CampaignTimeout == 0 {
		config.CampaignTimeout = 30 * time.Second
	}
	if config.IdleBackoff == 0 {
		config.IdleBackoff = 2 * time.Second
	}
	if config.OutboxPage == 0 {
		config.OutboxPage = 1000
	}
	if config.ClaimTTL < 3*time.Second || config.ClaimTTL > 5*time.Minute || config.LeaseSeconds < 3 || config.LeaseSeconds > 300 || config.MaxWorkers < 1 || config.MaxWorkers > 256 || config.ScanInterval < time.Second || config.ScanInterval > 5*time.Minute || config.CampaignTimeout < time.Second || config.CampaignTimeout > 5*time.Minute || config.IdleBackoff < time.Second || config.IdleBackoff > time.Minute || config.OutboxPage < 1 || config.OutboxPage > 1000 {
		return nil, errors.New("invalid runner limits")
	}
	return &Runner{
		config: config, receiver: agentstream.Receiver{Store: config.Store},
		slots: make(chan struct{}, config.MaxWorkers), workers: make(map[uuid.UUID]*worker.Worker),
	}, nil
}

// Handle routes claim-bound evidence to the worker that owns its cluster.
// Every other message reaches the receiver, which commits it without claim context.
func (r *Runner) Handle(ctx context.Context, peer controlstream.Peer, message *agentv1.AgentMessage) error {
	if message == nil {
		return agentstream.ErrUnsupportedMessage
	}
	switch value := message.Message.(type) {
	case *agentv1.AgentMessage_BarrierResult:
		return r.deliver(ctx, value.BarrierResult.GetEpoch(), peer, message)
	case *agentv1.AgentMessage_AdmissionAck:
		return r.deliver(ctx, value.AdmissionAck.GetEpoch(), peer, message)
	case *agentv1.AgentMessage_CancellationResult:
		// Only the owning worker asked for this, so only it can read the answer.
		return r.deliver(ctx, value.CancellationResult.GetEpoch(), peer, message)
	case *agentv1.AgentMessage_TerminalResult:
		// A receipt outlives its worker. The receiver keeps it durable either way.
		if owner, ok := r.worker(value.TerminalResult.GetEpoch()); ok {
			return owner.Handle(ctx, peer, message)
		}
		return r.receiver.Handle(ctx, peer, message)
	default:
		return r.receiver.Handle(ctx, peer, message)
	}
}

// deliver drops claim-bound evidence that no worker owns.
// The transport closes an agent stream on any handler error, and a barrier or an
// acknowledgement for a finished run is ordinary. The next owner requests its own
// barrier and redispatches, so dropping the message loses no durable evidence.
func (r *Runner) deliver(ctx context.Context, epoch *agentv1.Epoch, peer controlstream.Peer, message *agentv1.AgentMessage) error {
	found, ok := r.worker(epoch)
	if !ok {
		r.config.Logger.Warn("dropped claim evidence with no owner", "cluster_id", epoch.GetClusterId(), "node_id", peer.NodeID)
		return nil
	}
	return found.Handle(ctx, peer, message)
}

func (r *Runner) worker(epoch *agentv1.Epoch) (*worker.Worker, bool) {
	if epoch == nil {
		return nil, false
	}
	clusterID, err := uuid.Parse(epoch.ClusterId)
	if err != nil || clusterID == uuid.Nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A reserved cluster holds a nil entry until its claim and worker exist.
	found, ok := r.workers[clusterID]
	return found, ok && found != nil
}

// Run scans for claimable operations until ctx ends, then waits for every worker to stop.
// A failed scan, listener, or generation write never ends the run. The next cycle retries.
// Shutdown stops management dispatch. It never cancels native work that an agent admitted.
func (r *Runner) Run(ctx context.Context) error {
	notices := r.listen(ctx)
	ticker := time.NewTicker(r.config.ScanInterval)
	defer ticker.Stop()
	for {
		if err := r.ensureGeneration(ctx); err != nil {
			if ctx.Err() == nil {
				r.config.Logger.Warn("read installation recovery generation failed", "error", err.Error())
			}
		} else if err := r.scan(ctx); err != nil && ctx.Err() == nil {
			r.config.Logger.Warn("controller scan failed", "error", err.Error())
		}
		select {
		case <-notices:
		case <-ticker.C:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			r.wg.Wait()
			return nil
		}
	}
}

// listen reports committed outbox events. It reconnects until ctx ends.
// A notification is a hint that shortens the wait. The ticker still bounds it.
func (r *Runner) listen(ctx context.Context) <-chan struct{} {
	notices := make(chan struct{}, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for ctx.Err() == nil {
			listener, err := r.config.Store.ListenOutbox(ctx, r.config.InstallationID)
			if err != nil {
				if ctx.Err() == nil {
					r.config.Logger.Warn("outbox listener failed", "error", err.Error())
				}
				select {
				case <-time.After(r.config.ScanInterval):
				case <-ctx.Done():
				}
				continue
			}
			for {
				committed, err := listener.Wait(ctx)
				if err != nil || !committed {
					break
				}
				select {
				case notices <- struct{}{}:
				default:
				}
			}
			listener.Close()
		}
	}()
	return notices
}

// ensureGeneration stores a first recovery generation when the installation has none.
// Recovery after a restore supplies its own generation instead.
// Another controller can win this race and its generation is equally valid,
// so a stale-owner result is success, not a reason to stop this runner.
// The call writes nothing once a generation exists.
func (r *Runner) ensureGeneration(ctx context.Context) error {
	err := r.config.Store.InitializeGeneration(ctx, r.config.InstallationID, uuid.New())
	if errors.Is(err, operation.ErrStaleOwner) {
		return nil
	}
	return err
}

// scan starts a worker for every claimable operation that this process does not already run.
func (r *Runner) scan(ctx context.Context) error {
	// No Store method marks an outbox row delivered, so a scan pages through every row.
	// One page would hide every operation admitted after the first page of rows.
	for after := int64(0); ; {
		events, err := r.config.Store.ListPendingOutbox(ctx, r.config.InstallationID, after, r.config.OutboxPage)
		if err != nil {
			return err
		}
		for _, event := range events {
			after = event.ID
			r.consider(ctx, event.OperationID)
		}
		if len(events) < r.config.OutboxPage {
			return nil
		}
	}
}

// consider starts a worker for one admitted operation when it can still run.
func (r *Runner) consider(ctx context.Context, operationID uuid.UUID) {
	op, err := r.config.Store.GetOperation(ctx, r.config.InstallationID, operationID)
	if err != nil {
		if ctx.Err() == nil {
			r.config.Logger.Warn("read pending operation failed", "operation_id", operationID, "error", err.Error())
		}
		return
	}
	switch {
	case op.Status == operation.Succeeded || op.Status == operation.Failed || op.Status == operation.Cancelled:
		return
	case op.PlanVersion == 0 && r.config.Planner == nil:
		// Admission leaves an operation pending until a plan exists, and this
		// controller cannot produce one.
		r.config.Logger.Debug("operation has no executable plan", "operation_id", op.ID, "cluster_id", op.ClusterID)
		return
	}
	r.start(ctx, op)
}

// start reserves a slot and a cluster, then drives the operation in its own goroutine.
func (r *Runner) start(ctx context.Context, op operation.Operation) {
	if !r.reserve(op.ClusterID) {
		return
	}
	select {
	case r.slots <- struct{}{}:
	default:
		r.release(op.ClusterID)
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-r.slots }()
		defer r.release(op.ClusterID)
		if err := r.drive(ctx, op); err != nil && ctx.Err() == nil {
			r.config.Logger.Info("operation run stopped", "operation_id", op.ID, "cluster_id", op.ClusterID, "reason", err.Error())
		}
	}()
}

// reserve claims the cluster slot before any campaign so one process runs one worker.
func (r *Runner) reserve(clusterID uuid.UUID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, running := r.workers[clusterID]; running {
		return false
	}
	// A nil entry holds the cluster while the campaign and the claim complete.
	r.workers[clusterID] = nil
	return true
}

func (r *Runner) publish(clusterID uuid.UUID, owner *worker.Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[clusterID] = owner
}

func (r *Runner) release(clusterID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, clusterID)
}

// drive campaigns for the cluster, claims the operation, and runs one worker.
func (r *Runner) drive(ctx context.Context, op operation.Operation) error {
	run, stop := context.WithCancel(ctx)
	defer stop()
	leader, err := r.elect(run, stop, op.ClusterID)
	if err != nil {
		return err
	}
	defer func() {
		closing, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := leader.Close(closing); err != nil {
			r.config.Logger.Warn("release elected ownership failed", "cluster_id", op.ClusterID, "error", err.Error())
		}
	}()
	generation, err := r.config.Store.RecoveryGeneration(run, r.config.InstallationID)
	if err != nil {
		return err
	}
	if generation == uuid.Nil {
		return errors.New("installation has no recovery generation")
	}
	claim, err := r.config.Store.AcquireClaim(run, postgres.ClaimRequest{
		ClaimID: uuid.New(), InstallationID: r.config.InstallationID, OperationID: op.ID, RecoveryGeneration: generation,
		OwnerID: r.config.OwnerID, TTL: r.config.ClaimTTL,
		LeaseID: leader.LeaseID, LeaseRevision: leader.Revision,
	})
	if err != nil {
		return fmt.Errorf("claim operation %s: %w", op.ID, err)
	}
	// An operation with no plan has not been planned yet. Planning happens under the
	// claim, so exactly one controller plans it and the reservations it takes belong
	// to the claim that will run them.
	if op.PlanVersion == 0 && r.config.Planner != nil {
		if err := r.config.Planner.Plan(run, claim); err != nil {
			return fmt.Errorf("plan operation %s: %w", op.ID, err)
		}
		// Re-read, because the worker runs the plan the store now holds rather than
		// the operation this loop picked up before it existed.
		op, err = r.config.Store.GetOperation(run, r.config.InstallationID, op.ID)
		if err != nil {
			return err
		}
	}
	owner, err := worker.New(worker.Config{
		Store: r.config.Store, Transport: r.config.Transport, Authority: leader, Claim: claim, Operation: op,
		EvidenceWait: r.config.EvidenceWait, Keyring: r.config.Keyring,
	})
	if err != nil {
		return err
	}
	r.publish(op.ClusterID, owner)
	r.config.Logger.Info("operation claimed", "operation_id", op.ID, "cluster_id", op.ClusterID, "execution_epoch", claim.ExecutionEpoch)
	var renewal sync.WaitGroup
	renewal.Add(1)
	go func() {
		defer renewal.Done()
		r.renew(run, stop, claim, leader)
	}()
	// Keep the claim while evidence is outstanding. Returning here would idle the
	// operation until the claim expired, then force a new epoch and a new drain round.
	for run.Err() == nil {
		err = owner.Run(run)
		if !errors.Is(err, worker.ErrEvidencePending) {
			break
		}
		select {
		case <-time.After(r.config.IdleBackoff):
		case <-run.Done():
		}
	}
	stop()
	renewal.Wait()

	return err
}

// elect bounds the wait for cluster ownership so a wedged etcd cannot hold a worker slot.
// The election session lives on the run context, so a timeout must end that context.
func (r *Runner) elect(run context.Context, stop context.CancelFunc, clusterID uuid.UUID) (*coordinator.Leadership, error) {
	type outcome struct {
		leader *coordinator.Leadership
		err    error
	}
	elected := make(chan outcome, 1)
	go func() {
		leader, err := r.config.Coordinator.Campaign(run, r.config.InstallationID, clusterID, r.config.OwnerID, r.config.LeaseSeconds)
		elected <- outcome{leader, err}
	}()
	timer := time.NewTimer(r.config.CampaignTimeout)
	defer timer.Stop()
	select {
	case result := <-elected:
		if result.err != nil {
			return nil, fmt.Errorf("campaign for cluster %s: %w", clusterID, result.err)
		}
		return result.leader, nil
	case <-timer.C:
	}
	// Ending the run context closes the election session and releases this slot.
	stop()
	if result := <-elected; result.leader != nil {
		closing, cancel := context.WithTimeout(context.WithoutCancel(run), 5*time.Second)
		defer cancel()
		if err := result.leader.Close(closing); err != nil {
			r.config.Logger.Warn("release late elected ownership failed", "cluster_id", clusterID, "error", err.Error())
		}
	}
	return nil, fmt.Errorf("campaign for cluster %s did not elect an owner in %s", clusterID, r.config.CampaignTimeout)
}

// renew holds durable ownership while a worker runs.
// A lost renewal or a lost election stops the worker. It never stops native work.
func (r *Runner) renew(ctx context.Context, stop context.CancelFunc, claim operation.Claim, leader *coordinator.Leadership) {
	ticker := time.NewTicker(r.config.ClaimTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-leader.Done:
			r.config.Logger.Warn("elected ownership ended", "operation_id", claim.OperationID, "cluster_id", claim.ClusterID)
			stop()
			return
		case <-ticker.C:
			if _, err := r.config.Store.RenewClaim(ctx, claim, r.config.ClaimTTL); err != nil {
				if ctx.Err() == nil {
					r.config.Logger.Warn("renew claim failed", "operation_id", claim.OperationID, "error", err.Error())
				}
				stop()
				return
			}
		}
	}
}

// Planner installs the durable plan for one claimed operation.
// *provision.Planner satisfies it.
type Planner interface {
	Plan(ctx context.Context, claim operation.Claim) error
}
