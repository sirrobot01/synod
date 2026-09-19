package journal

import (
	"bytes"
	"fmt"
	"maps"
	"time"

	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// Admit persists immutable input before any runtime effect can start.
func (j *Journal) Admit(command *agentv1.Command) (Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return Record{}, err
	}
	if j.state.ExecutionBlocked {
		return Record{}, ErrRecoveryRequired
	}
	commandLimit := j.options.MaxCommandBytes
	if command != nil {
		if previous, exists := j.state.Commands[command.CommandId]; exists {
			commandLimit = previous.CommandLimit
		}
	}
	if proto.Size(command) > commandLimit {
		return Record{}, ErrBusy
	}
	if err := operation.ValidateCommand(command); err != nil {
		return Record{}, err
	}
	if err := j.checkTarget(command.Epoch); err != nil {
		return Record{}, err
	}
	cluster, exists := j.state.Clusters[command.Epoch.ClusterId]
	if !exists || !cluster.Active {
		return Record{}, operation.ErrStaleOwner
	}
	if err := operation.CheckEpoch(command.Epoch, cluster.Prepared); err != nil {
		return Record{}, err
	}
	if previous, exists := j.state.Commands[command.CommandId]; exists {
		if !bytes.Equal(previous.Hash, command.InputHash) {
			return Record{}, operation.ErrCommandConflict
		}
		// An unstarted command can transfer authority only after the new barrier activates.
		if previous.State == Queued && !proto.Equal(previous.Epoch, command.Epoch) {
			encoded, err := proto.Marshal(command)
			if err != nil {
				return Record{}, err
			}
			if retainedBytes(j.state.Commands)-int64(len(previous.Command))+int64(len(encoded)) > j.options.MaxRetainedBytes {
				return Record{}, ErrBusy
			}
			previous.Command = encoded
			previous.Epoch = proto.Clone(command.Epoch).(*agentv1.Epoch)
			if len(encoded) > previous.CommandLimit {
				return Record{}, ErrBusy
			}
			if err := j.persist(map[string]commandRecord{previous.ID: previous}, nil); err != nil {
				return Record{}, err
			}
		}
		return previous.view()
	}
	encoded, err := proto.Marshal(command)
	if err != nil {
		return Record{}, err
	}
	if len(encoded) > j.options.MaxCommandBytes {
		return Record{}, ErrBusy
	}
	count := 0
	retained := retainedBytes(j.state.Commands)
	for existing := range maps.Values(j.state.Commands) {
		active := false
		for _, effect := range existing.Effects {
			active = active || effect.Active || effect.OutcomeUnknown
		}
		if existing.State == Queued || existing.State == Running || existing.AcknowledgedAt.IsZero() || active {
			count++

		}
		if existing.Epoch.ClusterId == command.Epoch.ClusterId && (existing.State == Queued || existing.State == Running || active) {
			return Record{}, ErrBusy
		}
	}
	// Reserve result space at admission so completion does not compete with later work.
	if count >= j.options.MaxCommands || retained+int64(len(encoded))+int64(2*j.options.MaxResultBytes) > j.options.MaxRetainedBytes {
		return Record{}, ErrBusy
	}
	r := commandRecord{ID: command.CommandId, Hash: bytes.Clone(command.InputHash), Epoch: proto.Clone(command.Epoch).(*agentv1.Epoch), ResourceID: command.Input.ResourceId, State: Queued, Command: encoded, CommandLimit: j.options.MaxCommandBytes, ResultLimit: j.options.MaxResultBytes, ReservedBytes: commandReservation(j.options.MaxCommandBytes, j.options.MaxResultBytes)}
	if err := j.persist(map[string]commandRecord{r.ID: r}, nil); err != nil {
		return Record{}, err
	}
	return r.view()
}

// Start grants execution only for an unstarted command under active authority.
// A running record survives restart and must be inspected instead of started again.
func (j *Journal) Start(commandID string) (Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return Record{}, err
	}
	if j.state.ExecutionBlocked {
		return Record{}, ErrRecoveryRequired
	}
	r, exists := j.state.Commands[commandID]
	if !exists {
		return Record{}, ErrNotFound
	}
	if r.State == Running {
		return Record{}, ErrInspectionRequired
	}
	if r.State != Queued {
		v, err := r.view()
		if err != nil {
			return Record{}, err
		}
		return v, ErrAlreadyCompleted
	}
	cluster := j.state.Clusters[r.Epoch.ClusterId]
	if !cluster.Active || operation.CheckEpoch(r.Epoch, cluster.Prepared) != nil {
		return Record{}, operation.ErrStaleOwner
	}
	// A late native observation can invalidate the slot after admission.
	for other := range maps.Values(j.state.Commands) {
		if other.ID == r.ID || other.Epoch.ClusterId != r.Epoch.ClusterId {
			continue
		}
		if other.State == Running {
			return Record{}, ErrBusy
		}
		for _, effect := range other.Effects {
			if effect.Active || effect.OutcomeUnknown {
				return Record{}, ErrBusy
			}
		}
	}
	r.State = Running
	if err := j.persist(map[string]commandRecord{r.ID: r}, nil); err != nil {
		return Record{}, err
	}
	return r.view()
}

// Observe records a complete current observation of the command's native effects.
func (j *Journal) Observe(commandID string, effects []*agentv1.Effect) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	r, exists := j.state.Commands[commandID]
	if !exists {
		return ErrNotFound
	}
	if r.State != Running && r.State != Terminal {
		return ErrInspectionRequired
	}
	if proto.Size(&agentv1.BarrierResult{Effects: effects}) > r.ResultLimit {
		return j.blockExecution(ErrBusy)
	}
	if err := validateEffects(r, effects); err != nil {
		return err
	}
	encoded, err := proto.Marshal(&agentv1.BarrierResult{Effects: effects})
	if err != nil {
		return err
	}
	if len(encoded) > r.ResultLimit {
		return j.blockExecution(ErrBusy)
	}
	r.Effects = proto.Clone(&agentv1.BarrierResult{Effects: effects}).(*agentv1.BarrierResult).Effects
	if err := j.persist(map[string]commandRecord{r.ID: r}, nil); err != nil {
		return j.blockExecution(err)
	}
	return nil
}

func validateEffects(record commandRecord, effects []*agentv1.Effect) error {
	for _, effect := range effects {
		if effect == nil || effect.CommandId != record.ID || effect.ResourceId != record.ResourceID {
			return fmt.Errorf("effect identity does not match command")
		}
	}
	// An observation can mark a task stopped, but cannot silently omit an unresolved task.
	for _, old := range record.Effects {
		if !old.Active && !old.OutcomeUnknown {
			continue
		}
		found := false
		for _, effect := range effects {
			if effect.NativeOperationId == old.NativeOperationId {
				found = true
				break
			}
		}
		if !found {
			return operation.ErrBarrierPending
		}
	}
	return nil
}

// Complete persists result delivery and residual effects as one synchronous transition.
func (j *Journal) Complete(result *agentv1.TerminalResult) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if result == nil {
		return operation.ErrCommandConflict
	}
	r, exists := j.state.Commands[result.CommandId]
	if !exists {
		return ErrNotFound
	}
	if !proto.Equal(result.Epoch, r.Epoch) || !bytes.Equal(result.InputHash, r.Hash) || result.Sequence == 0 {
		return operation.ErrCommandConflict
	}
	if result.State != "succeeded" && result.State != "failed" && result.State != "cancelled" {
		return fmt.Errorf("terminal result has an invalid state")
	}
	if proto.Size(result) > r.ResultLimit {
		return j.blockExecution(ErrBusy)
	}
	encoded, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	if len(encoded) > r.ResultLimit {
		return j.blockExecution(ErrBusy)
	}
	if r.State == Terminal {
		var previous agentv1.TerminalResult
		if proto.Unmarshal(r.Result, &previous) != nil {
			return ErrRecoveryRequired
		}
		if !proto.Equal(result, &previous) {
			return operation.ErrCommandConflict
		}
		return nil
	}
	if r.State == Compacted {
		return operation.ErrCommandConflict
	}
	if r.State == Queued && result.State != "cancelled" {
		return ErrInspectionRequired
	}
	if err := validateEffects(r, result.Effects); err != nil {
		return err
	}
	if result.State == "succeeded" {
		for _, effect := range result.Effects {
			if effect.Active || effect.OutcomeUnknown {
				return operation.ErrBarrierPending
			}
		}
	}
	r.State = Terminal
	r.ResultSequence = result.Sequence
	r.Result = encoded
	r.Effects = proto.Clone(result).(*agentv1.TerminalResult).Effects
	if err := j.persist(map[string]commandRecord{r.ID: r}, nil); err != nil {
		return j.blockExecution(err)
	}
	return nil
}

// Acknowledge marks one terminal result as delivered to the controller.
func (j *Journal) Acknowledge(ack *agentv1.ResultAck) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return err
	}
	if ack == nil {
		return operation.ErrCommandConflict
	}
	r, exists := j.state.Commands[ack.CommandId]
	if !exists {
		return ErrNotFound
	}
	if r.State == Compacted && proto.Equal(ack.Epoch, r.Epoch) && ack.Sequence == r.ResultSequence {
		return nil
	}
	if r.State != Terminal || !proto.Equal(ack.Epoch, r.Epoch) {
		return operation.ErrCommandConflict
	}
	var result agentv1.TerminalResult
	if err := proto.Unmarshal(r.Result, &result); err != nil {
		return ErrRecoveryRequired
	}
	if ack.Sequence != result.Sequence {
		return operation.ErrCommandConflict
	}
	if !r.AcknowledgedAt.IsZero() {
		return nil
	}
	r.AcknowledgedAt = time.Now().UTC()
	return j.persist(map[string]commandRecord{r.ID: r}, nil)
}

// Compact changes at most 256 records per call.
// It retains identity/hash tombstones and prepared epoch high-water marks.
// Unresolved barriers retain command payloads until their evidence is reconciled.
func (j *Journal) Compact(now time.Time) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.ready(); err != nil {
		return 0, err
	}
	protected := make(map[string]bool)
	for cluster := range maps.Values(j.state.Clusters) {
		var barrier agentv1.BarrierResult
		if err := proto.Unmarshal(cluster.Barrier, &barrier); err != nil {
			return 0, ErrRecoveryRequired
		}
		for _, effect := range barrier.Effects {
			if effect.CommandId != "" && (effect.Active || effect.OutcomeUnknown) {
				protected[effect.CommandId] = true
			}
		}
	}
	changes := make(map[string]commandRecord)
	count := 0
	for id, record := range j.state.Commands {
		if protected[id] {
			continue
		}
		if record.State != Terminal || record.AcknowledgedAt.IsZero() || now.Before(record.AcknowledgedAt.Add(j.options.RecoveryWindow)) {
			continue
		}
		unresolved := false
		for _, effect := range record.Effects {
			unresolved = unresolved || effect.Active || effect.OutcomeUnknown
		}
		if unresolved {
			continue
		}
		record.State = Compacted
		record.Command = nil
		record.Result = nil
		record.Effects = nil
		changes[id] = record
		count++
		if count == 256 {
			break
		}
	}
	if count == 0 {
		return 0, nil
	}
	if err := j.persist(changes, nil); err != nil {
		return 0, err
	}
	return count, nil
}

// Reserve both the terminal result and current effects until payload compaction.
// Acknowledged payloads still consume space until their recovery window ends.
func retainedBytes(commands map[string]commandRecord) int64 {
	var size int64
	for record := range maps.Values(commands) {
		size += int64(len(record.Command))
		if record.State != Compacted {
			size += 2 * int64(record.ResultLimit)
		}
	}
	return size
}
