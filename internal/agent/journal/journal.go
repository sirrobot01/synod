// Package journal persists local command authority and recovery evidence.
package journal

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrAlreadyCompleted   = errors.New("command already completed")
	ErrRecoveryRequired   = errors.New("agent journal recovery required")
	ErrBusy               = errors.New("agent_busy")
	ErrInspectionRequired = errors.New("command requires runtime inspection")
	ErrNotFound           = errors.New("command not found")
	ErrClosed             = errors.New("agent journal closed")
)

// State is the durable state of one journalled command.
type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Terminal  State = "terminal"
	Compacted State = "compacted"
)

// Identity is the assigned identity that a journal belongs to.
type Identity struct {
	InstallationID     string `json:"installation_id"`
	NodeID             string `json:"node_id"`
	JournalGeneration  string `json:"journal_generation"`
	RecoveryGeneration string `json:"recovery_generation"`
}

// Options holds the journal settings and its record bounds.
type Options struct {
	// Logger overrides Pebble diagnostics. Nil preserves the component default.
	Logger          pebble.Logger
	Identity        Identity
	AllowCreate     bool
	EmptyInventory  bool
	MaxCommands     int
	MaxCommandBytes int
	MaxResultBytes  int
	// MaxRetainedBytes bounds live protobuf payloads, not database size.
	MaxRetainedBytes int64
	RecoveryWindow   time.Duration
	// MaxRecords includes all command tombstones.
	MaxRecords        int
	MaxApps           int
	MaxRecordBytes    int
	MaxInventoryBytes int
	MaxStoredBytes    int64
}

// Record is a detached snapshot. Mutating it does not change durable state.
type Record struct {
	CommandID    string
	InputHash    []byte
	Epoch        *agentv1.Epoch
	State        State
	Command      *agentv1.Command
	Result       *agentv1.TerminalResult
	Effects      []*agentv1.Effect
	Acknowledged bool
}

type commandRecord struct {
	ID             string            `json:"id"`
	Hash           []byte            `json:"hash"`
	Epoch          *agentv1.Epoch    `json:"epoch"`
	ResourceID     string            `json:"resource_id"`
	State          State             `json:"state"`
	Command        []byte            `json:"command,omitempty"`
	Result         []byte            `json:"result,omitempty"`
	Effects        []*agentv1.Effect `json:"effects,omitempty"`
	ResultSequence uint64            `json:"result_sequence,omitzero"`
	CommandLimit   int               `json:"command_limit,omitzero"`
	ResultLimit    int               `json:"result_limit,omitzero"`
	ReservedBytes  int64             `json:"reserved_bytes,omitzero"`
	AcknowledgedAt time.Time         `json:"acknowledged_at,omitzero"`
}

type appState struct {
	Prepared       *agentv1.Epoch `json:"prepared"`
	Active         bool           `json:"active"`
	Barrier        []byte         `json:"barrier,omitempty"`
	InventoryLimit int            `json:"inventory_limit,omitzero"`
	ReservedBytes  int64          `json:"reserved_bytes,omitzero"`
	Reconciled     bool           `json:"reconciled"`
}

type diskState struct {
	ExecutionBlocked bool                     `json:"execution_blocked,omitzero"`
	Version          int                      `json:"version"`
	Identity         Identity                 `json:"identity"`
	Apps             map[string]appState      `json:"apps"`
	Commands         map[string]commandRecord `json:"commands"`
}

// Journal serializes admission with authority changes and completion.
// A failed synchronous write poisons the handle until it is reopened.
type Journal struct {
	mu      sync.Mutex
	db      *pebble.DB
	state   diskState
	options Options
	failed  bool
	closed  bool
	usage   Usage
	sizes   map[string]int64
}

var stateKey = []byte("synod/agent/journal/v1")

// Open creates a journal only after an explicit empty-inventory bootstrap decision.
// An omitted expected journal generation accepts the generation already on disk.
func Open(path string, options Options) (*Journal, error) {
	options.MaxCommands = cmp.Or(options.MaxCommands, 64)
	options.MaxCommandBytes = cmp.Or(options.MaxCommandBytes, 1<<20)
	options.MaxResultBytes = cmp.Or(options.MaxResultBytes, 1<<20)
	options.MaxRetainedBytes = cmp.Or(options.MaxRetainedBytes, int64(256<<20))
	options.RecoveryWindow = cmp.Or(options.RecoveryWindow, 24*time.Hour)
	options.MaxRecords = cmp.Or(options.MaxRecords, 100000)
	options.MaxApps = cmp.Or(options.MaxApps, 1024)
	options.MaxRecordBytes = cmp.Or(options.MaxRecordBytes, 8<<20)
	options.MaxInventoryBytes = cmp.Or(options.MaxInventoryBytes, 1<<20)
	options.MaxStoredBytes = cmp.Or(options.MaxStoredBytes, int64(1<<30))
	if options.MaxRecords < 1 || options.MaxApps < 1 || options.MaxRecordBytes < 4096 || options.MaxRecordBytes > maxFormatRecordBytes || options.MaxInventoryBytes < 1 || options.MaxInventoryBytes > maxFormatRecordBytes || options.MaxStoredBytes < metadataReservation || options.MaxCommandBytes > maxFormatRecordBytes || options.MaxResultBytes > maxFormatRecordBytes || commandReservation(options.MaxCommandBytes, options.MaxResultBytes) > int64(options.MaxRecordBytes) || inventoryReservation(options.MaxInventoryBytes) > int64(options.MaxRecordBytes) {
		return nil, fmt.Errorf("invalid journal storage limits")
	}
	if options.MaxCommands < 1 || options.MaxCommandBytes < 1 || options.MaxResultBytes < 1 || options.MaxRetainedBytes < 1 || options.RecoveryWindow < 0 || int64(options.MaxResultBytes) > options.MaxRetainedBytes/2 {
		return nil, fmt.Errorf("journal limits must be positive")
	}
	for _, id := range []string{options.Identity.InstallationID, options.Identity.NodeID, options.Identity.RecoveryGeneration} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return nil, fmt.Errorf("journal identity must use canonical nonzero UUIDs: %w", ErrRecoveryRequired)
		}
	}
	if options.Identity.JournalGeneration != "" {
		parsed, err := uuid.Parse(options.Identity.JournalGeneration)
		if err != nil || parsed == uuid.Nil || parsed.String() != options.Identity.JournalGeneration {
			return nil, fmt.Errorf("invalid expected journal generation: %w", ErrRecoveryRequired)
		}
	}
	entries, pathErr := os.ReadDir(path)
	creationPathEmpty := errors.Is(pathErr, os.ErrNotExist) || (pathErr == nil && len(entries) == 0)
	// Try the existing database first. Even an explicit bootstrap must not replace it.
	db, err := pebble.Open(path, &pebble.Options{ErrorIfNotExists: true, Logger: options.Logger})
	created := false
	if errors.Is(err, pebble.ErrDBDoesNotExist) && options.AllowCreate && options.EmptyInventory && creationPathEmpty {
		db, err = pebble.Open(path, &pebble.Options{ErrorIfExists: true, Logger: options.Logger})
		created = err == nil
	}
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", errors.Join(ErrRecoveryRequired, err))
	}
	j := &Journal{db: db, options: options, sizes: make(map[string]int64)}
	if created {
		identity := options.Identity
		if identity.JournalGeneration == "" {
			identity.JournalGeneration = uuid.NewString()
		}
		j.state = diskState{Version: 2, Identity: identity, Apps: make(map[string]appState), Commands: make(map[string]commandRecord)}
		if err := j.initialize(false); err != nil {
			return nil, errors.Join(err, db.Close())
		}
		return j, nil
	}
	if err := j.load(); err != nil {
		return nil, fmt.Errorf("validate journal: %w", errors.Join(ErrRecoveryRequired, err, db.Close()))
	}
	return j, nil
}

func (j *Journal) validate() error {
	s := j.state
	want := j.options.Identity
	if (s.Version != 1 && s.Version != 2) || s.Apps == nil || s.Commands == nil || s.Identity.InstallationID != want.InstallationID || s.Identity.NodeID != want.NodeID || s.Identity.RecoveryGeneration != want.RecoveryGeneration || (want.JournalGeneration != "" && s.Identity.JournalGeneration != want.JournalGeneration) {
		return fmt.Errorf("the journal holds version %d of %+v and this agent is %+v: %w", s.Version, s.Identity, want, ErrRecoveryRequired)
	}
	id, err := uuid.Parse(s.Identity.JournalGeneration)
	if err != nil {
		return fmt.Errorf("parse the journal generation %q: %w: %w", s.Identity.JournalGeneration, ErrRecoveryRequired, err)
	}
	if id == uuid.Nil || id.String() != s.Identity.JournalGeneration {
		return fmt.Errorf("the journal generation %q is not a canonical non-nil UUID: %w", s.Identity.JournalGeneration, ErrRecoveryRequired)
	}
	for appID, app := range s.Apps {
		if err := j.checkTarget(app.Prepared); err != nil {
			return fmt.Errorf("check the prepared epoch of app %s: %w: %w", appID, ErrRecoveryRequired, err)
		}
		if appID != app.Prepared.AppId {
			return fmt.Errorf("app %s is filed under the prepared epoch of app %s: %w", appID, app.Prepared.AppId, ErrRecoveryRequired)
		}
		if len(app.Barrier) != 0 {
			var barrier agentv1.BarrierResult
			if err := proto.Unmarshal(app.Barrier, &barrier); err != nil {
				return fmt.Errorf("decode the barrier of app %s: %w: %w", appID, ErrRecoveryRequired, err)
			}
			if !proto.Equal(barrier.Epoch, app.Prepared) {
				return fmt.Errorf("the barrier of app %s answers another epoch: %w", appID, ErrRecoveryRequired)
			}
			if app.Active {
				if err := operation.BarrierDrained(&barrier); err != nil {
					return fmt.Errorf("app %s is active behind an undrained barrier: %w: %w", appID, ErrRecoveryRequired, err)
				}
			}
		}
		if app.Active && (!app.Reconciled || len(app.Barrier) == 0) {
			return fmt.Errorf("app %s is active but not reconciled or carries no barrier: %w", appID, ErrRecoveryRequired)
		}
	}
	for commandID, record := range s.Commands {
		for _, identity := range []string{commandID, record.ResourceID} {
			parsed, err := uuid.Parse(identity)
			if err != nil {
				return fmt.Errorf("parse %q on command %s: %w: %w", identity, commandID, ErrRecoveryRequired, err)
			}
			if parsed == uuid.Nil || parsed.String() != identity {
				return fmt.Errorf("%q on command %s is not a canonical non-nil UUID: %w", identity, commandID, ErrRecoveryRequired)
			}
		}
		if commandID != record.ID || len(record.Hash) != 32 {
			return fmt.Errorf("command %s is filed under %s with a %d byte input hash: %w", record.ID, commandID, len(record.Hash), ErrRecoveryRequired)
		}
		if err := j.checkTarget(record.Epoch); err != nil {
			return fmt.Errorf("check the epoch of command %s: %w: %w", commandID, ErrRecoveryRequired, err)
		}
		app, exists := s.Apps[record.Epoch.AppId]
		if !exists || record.Epoch.ExecutionEpoch > app.Prepared.ExecutionEpoch {
			return fmt.Errorf("command %s names execution epoch %d of app %s, which this journal has not prepared: %w",
				commandID, record.Epoch.ExecutionEpoch, record.Epoch.AppId, ErrRecoveryRequired)
		}
		view, err := record.view()
		if err != nil {
			return fmt.Errorf("read command %s: %w", commandID, err)
		}
		if record.State == Compacted {
			if len(record.Command) != 0 || len(record.Result) != 0 || record.AcknowledgedAt.IsZero() || len(record.Effects) != 0 || record.ResultSequence == 0 {
				return fmt.Errorf("compacted command %s still carries a command, a result, or effects: %w", commandID, ErrRecoveryRequired)
			}
			continue
		}
		if err := operation.ValidateCommand(view.Command); err != nil {
			return fmt.Errorf("validate the saved command %s: %w: %w", commandID, ErrRecoveryRequired, err)
		}
		if view.Command.CommandId != commandID || !bytes.Equal(view.Command.InputHash, record.Hash) || !proto.Equal(view.Command.Epoch, record.Epoch) || view.Command.Input.ResourceId != record.ResourceID {
			return fmt.Errorf("the saved command under %s does not match its record: %w", commandID, ErrRecoveryRequired)
		}
		if record.State != Queued && record.State != Running && record.State != Terminal {
			return fmt.Errorf("command %s is in state %q: %w", commandID, record.State, ErrRecoveryRequired)
		}
		if (record.State == Terminal) != (view.Result != nil) || (view.Result == nil && !record.AcknowledgedAt.IsZero()) {
			return fmt.Errorf("command %s is in state %q and its result is present=%t: %w", commandID, record.State, view.Result != nil, ErrRecoveryRequired)
		}
		if err := validateEffects(commandRecord{ID: record.ID, ResourceID: record.ResourceID}, record.Effects); err != nil {
			return fmt.Errorf("validate the recorded effects of command %s: %w: %w", commandID, ErrRecoveryRequired, err)
		}
		if record.State == Queued && len(record.Effects) != 0 {
			return fmt.Errorf("queued command %s already carries %d effects: %w", commandID, len(record.Effects), ErrRecoveryRequired)
		}
		if view.Result == nil && record.ResultSequence != 0 {
			return fmt.Errorf("command %s carries result sequence %d and no result: %w", commandID, record.ResultSequence, ErrRecoveryRequired)
		}
		if view.Result != nil {
			if err := validateEffects(commandRecord{ID: record.ID, ResourceID: record.ResourceID}, view.Result.Effects); err != nil {
				return fmt.Errorf("validate the result effects of command %s: %w: %w", commandID, ErrRecoveryRequired, err)
			}
			for _, effect := range view.Result.Effects {
				if view.Result.State == "succeeded" && (effect.Active || effect.OutcomeUnknown) {
					return fmt.Errorf("command %s succeeded with an active or unknown effect on %s: %w", commandID, effect.ResourceId, ErrRecoveryRequired)
				}
			}
			if view.Result.State != "succeeded" && view.Result.State != "failed" && view.Result.State != "cancelled" {
				return fmt.Errorf("the result of command %s is in state %q: %w", commandID, view.Result.State, ErrRecoveryRequired)
			}
			if view.Result.Sequence == 0 || view.Result.Sequence != record.ResultSequence || view.Result.CommandId != commandID || !proto.Equal(view.Result.Epoch, record.Epoch) || !bytes.Equal(view.Result.InputHash, record.Hash) {
				return fmt.Errorf("the saved result under %s does not match its record: %w", commandID, ErrRecoveryRequired)
			}
		}
	}
	return nil
}

func (j *Journal) checkTarget(epoch *agentv1.Epoch) error {
	if epoch == nil || epoch.InstallationId != j.state.Identity.InstallationID || epoch.NodeId != j.state.Identity.NodeID || epoch.RecoveryGeneration != j.state.Identity.RecoveryGeneration || epoch.ExecutionEpoch < 1 {
		return operation.ErrStaleOwner
	}
	id, err := uuid.Parse(epoch.AppId)
	if err != nil {
		return fmt.Errorf("parse the app of the epoch (%q): %w: %w", epoch.AppId, operation.ErrStaleOwner, err)
	}
	if id == uuid.Nil || id.String() != epoch.AppId {
		return fmt.Errorf("the epoch names app %q, which is not a canonical non-nil UUID: %w", epoch.AppId, operation.ErrStaleOwner)
	}
	return nil
}

func (j *Journal) ready() error {
	if j.closed {
		return ErrClosed
	}
	if j.failed {
		return ErrRecoveryRequired
	}
	return nil
}

func (r commandRecord) view() (Record, error) {
	v := Record{CommandID: r.ID, InputHash: bytes.Clone(r.Hash), State: r.State, Acknowledged: !r.AcknowledgedAt.IsZero()}
	if r.Epoch != nil {
		v.Epoch = proto.Clone(r.Epoch).(*agentv1.Epoch)
	}
	if len(r.Command) != 0 {
		v.Command = new(agentv1.Command)
		if err := proto.Unmarshal(r.Command, v.Command); err != nil {
			return Record{}, errors.Join(ErrRecoveryRequired, err)
		}
	}
	if len(r.Result) != 0 {
		v.Result = new(agentv1.TerminalResult)
		if err := proto.Unmarshal(r.Result, v.Result); err != nil {
			return Record{}, errors.Join(ErrRecoveryRequired, err)
		}
	}
	for _, effect := range r.Effects {
		if effect == nil {
			return Record{}, ErrRecoveryRequired
		}
		v.Effects = append(v.Effects, proto.Clone(effect).(*agentv1.Effect))
	}
	return v, nil
}

func (j *Journal) Identity() Identity {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state.Identity
}

// Get returns one command record, or ErrNotFound when it is absent.
func (j *Journal) Get(commandID string) (Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return Record{}, err
	}
	record, ok := j.state.Commands[commandID]
	if !ok {
		return Record{}, ErrNotFound
	}
	return record.view()
}

// Pending returns the queued and running records in command ID order.
func (j *Journal) Pending() ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return nil, err
	}
	var records []Record
	for _, id := range slices.Sorted(maps.Keys(j.state.Commands)) {
		r := j.state.Commands[id]
		if r.State != Queued && r.State != Running {
			continue
		}
		v, err := r.view()
		if err != nil {
			return nil, err
		}
		records = append(records, v)
	}
	return records, nil
}

// Results returns the terminal results that the controller has not acknowledged.
func (j *Journal) Results() ([]*agentv1.TerminalResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return nil, err
	}
	var results []*agentv1.TerminalResult
	for _, id := range slices.Sorted(maps.Keys(j.state.Commands)) {
		r := j.state.Commands[id]
		if r.State != Terminal || !r.AcknowledgedAt.IsZero() {
			continue
		}
		v, err := r.view()
		if err != nil {
			return nil, err
		}
		results = append(results, v.Result)
	}
	return results, nil
}

// Close is safe to call more than once.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	return j.db.Close()
}
