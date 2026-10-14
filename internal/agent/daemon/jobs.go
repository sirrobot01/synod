package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	agentrestore "github.com/sirrobot01/synod/internal/agent/restore"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// executeJobs runs backups and restores one at a time, so a restore never races a backup of the same app.
// reload returns true when the agent must start again. The loop checks it between jobs.
func executeJobs(ctx context.Context, worker *agentbackup.Worker, restorer *agentrestore.Worker, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger, reload func() bool) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	defer worker.CloseIdle(0)
	// The loop retries a stuck record every 5 seconds. Its warning is logged once.
	stuck := uuid.Nil
	pending := func(id uuid.UUID, message string, err error) {
		waiting.Store(new(id))
		if stuck != id {
			stuck = id
			logger.Warn(message, "operation", id, "error", err)
		}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		if reload() {
			return
		}
		worker.CloseIdle(agentbackup.ConnectionIdle)
		if _, err := worker.Journal.AdmitScheduled(ctx, worker.Keyring, time.Now()); err != nil && !errors.Is(err, journal.ErrNotFound) {
			logger.Warn("scheduled backup not queued", "error", err)
		}
		records, err := worker.Journal.Pending(ctx)
		if err != nil {
			logger.Warn("job journal unreadable", "error", err)
		}
		// Pending puts recovery first. A pending restart prevents another app from stopping.
		for _, record := range records {
			if ctx.Err() != nil {
				return
			}
			if record.State == journal.Terminal {
				continue
			}
			waiting.Store(nil)
			var check proof.Request
			if json.Unmarshal(record.Input, &check) == nil && check.Kind.Valid() {
				if _, err := restorer.ExecuteProof(ctx, check); err != nil {
					pending(record.ID, "restore test or check cannot finish; other jobs wait until it does", err)
					break
				}
				continue
			}
			var job restore.Request
			if json.Unmarshal(record.Input, &job) == nil && job.Kind == "restore" {
				if _, err := restorer.Execute(ctx, job); err != nil {
					pending(record.ID, "restore cannot finish its recovery; other jobs wait until it does", err)
					break
				}
				continue
			}
			var request backup.Request
			if err := json.Unmarshal(record.Input, &request); err != nil || request.Validate() != nil {
				pending(record.ID, "job journal holds an unreadable job; other jobs wait", errors.Join(err, request.Validate()))
				break
			}
			if _, err := worker.Execute(ctx, request); err != nil {
				pending(record.ID, "backup cannot finish; other jobs wait until it does (often an app that did not restart)", err)
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-worker.Journal.Queued():
		}
	}
}

func reportJobs(ctx context.Context, cache *journal.Journal, stream *transport.Client, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	// Reports repeat every second. A refused one is logged once for each operation.
	refused := make(map[uuid.UUID]bool)
	invalid := func(id uuid.UUID, kind string, err error) {
		if !refused[id] {
			refused[id] = true
			logger.Error(kind+" result not reported: it fails validation, so the controller never receives it", "operation", id, "error", err)
		}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		records, err := cache.Records(ctx)
		if err != nil {
			logger.Warn("job journal unreadable", "error", err)
		}
		for _, record := range records {
			var check proof.Request
			if json.Unmarshal(record.Input, &check) == nil && check.Kind.Valid() {
				reportProof(stream, record, check, waiting, logger, invalid)
				continue
			}
			var job restore.Request
			if json.Unmarshal(record.Input, &job) == nil && job.Kind == "restore" {
				reportRestore(stream, record, job, waiting, logger, invalid)
				continue
			}
			var request backup.Request
			// A safety snapshot reaches the controller in its restore result.
			if json.Unmarshal(record.Input, &request) != nil || request.Validate() != nil || request.RestoreID != uuid.Nil {
				continue
			}
			event := backup.Event{ID: record.ID, Result: backup.Result{DestinationID: request.DestinationID}}
			if request.ScheduleID != uuid.Nil {
				event.Request = &request
			}
			switch record.State {
			case journal.Queued:
				event.Status = "queued"
			case journal.Running:
				event.Status = "running"
				event.Result, err = agentbackup.WaitingResult(record)
				if err != nil {
					continue
				}
				event.Result.DestinationID = request.DestinationID
				if id := waiting.Load(); id != nil && *id == record.ID {
					event.Status = "waiting"
				}
			case journal.Terminal:
				if json.Unmarshal(record.Result, &event.Result) != nil {
					continue
				}
				event.Status = "succeeded"
				if event.Result.FailureCode != "" {
					event.Status = "failed"
				}
			}
			if err := event.Validate(request); err != nil {
				invalid(record.ID, "backup", err)
				continue
			}
			raw, err := json.Marshal(event)
			if err != nil {
				continue
			}
			err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_BackupEvent{BackupEvent: &agentv1.BackupEvent{EventJson: raw}}})
			if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
				logger.Warn("backup result not sent to the controller; retrying", "operation", record.ID, "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-cache.Changed():
		}
	}
}

func reportRestore(stream *transport.Client, record journal.Record, request restore.Request, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger, invalid func(uuid.UUID, string, error)) {
	event := restore.Event{ID: record.ID}
	switch record.State {
	case journal.Queued:
		event.Status = "queued"
	case journal.Running:
		event.Status = "running"
		result, err := agentrestore.WaitingResult(record)
		if err != nil {
			return
		}
		event.Result = result
		if id := waiting.Load(); id != nil && *id == record.ID {
			event.Status = "waiting"
		} else {
			event.Result.RecoveryPending = nil
		}
	case journal.Terminal:
		if json.Unmarshal(record.Result, &event.Result) != nil {
			return
		}
		event.Status = "succeeded"
		if event.Result.FailureCode != "" {
			event.Status = "failed"
		}
	}
	if err := event.Validate(request); err != nil {
		invalid(record.ID, "restore", err)
		return
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_RestoreEvent{RestoreEvent: &agentv1.RestoreEvent{EventJson: raw}}})
	if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
		logger.Warn("restore result not sent to the controller; retrying", "operation", record.ID, "error", err)
	}
}

func reportProof(stream *transport.Client, record journal.Record, request proof.Request, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger, invalid func(uuid.UUID, string, error)) {
	event := proof.Event{ID: record.ID}
	if request.ScheduleID != uuid.Nil {
		event.Request = &request
	}
	switch record.State {
	case journal.Queued:
		event.Status = "queued"
	case journal.Running:
		event.Status = "running"
		result, err := agentrestore.WaitingProofResult(record)
		if err != nil {
			return
		}
		event.Result = result
		if id := waiting.Load(); id != nil && *id == record.ID {
			event.Status = "waiting"
		}
	case journal.Terminal:
		if json.Unmarshal(record.Result, &event.Result) != nil {
			return
		}
		event.Status = "succeeded"
		if event.Result.FailureCode != "" {
			event.Status = "failed"
		}
	}
	if err := event.Validate(request); err != nil {
		invalid(record.ID, string(request.Kind), err)
		return
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_ProofEvent{ProofEvent: &agentv1.ProofEvent{EventJson: raw}}})
	if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
		logger.Warn("restore test or check result not sent to the controller; retrying", "operation", record.ID, "error", err)
	}
}
