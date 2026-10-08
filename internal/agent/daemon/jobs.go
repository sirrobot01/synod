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
	for {
		if ctx.Err() != nil {
			return
		}
		if reload() {
			return
		}
		worker.CloseIdle(agentbackup.ConnectionIdle)
		if _, err := worker.Journal.AdmitScheduled(ctx, worker.Keyring, time.Now()); err != nil && !errors.Is(err, journal.ErrNotFound) {
			logger.Warn("backup_schedule_failed", "code", "schedule_admission_failed")
		}
		records, err := worker.Journal.Pending(ctx)
		if err != nil {
			logger.Warn("backup_journal_failed", "code", "journal_read_failed")
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
					waiting.Store(new(record.ID))
					logger.Warn("proof_recovery_pending", "operation_id", record.ID, "code", "inspect_drill_cleanup")
					break
				}
				continue
			}
			var job restore.Request
			if json.Unmarshal(record.Input, &job) == nil && job.Kind == "restore" {
				if _, err := restorer.Execute(ctx, job); err != nil {
					waiting.Store(new(record.ID))
					logger.Warn("restore_recovery_pending", "operation_id", record.ID, "code", "inspect_restore_recovery")
					break
				}
				continue
			}
			var request backup.Request
			if json.Unmarshal(record.Input, &request) != nil || request.Validate() != nil {
				logger.Warn("backup_journal_failed", "code", "journal_request_invalid")
				break
			}
			if _, err := worker.Execute(ctx, request); err != nil {
				waiting.Store(new(record.ID))
				logger.Warn("backup_recovery_pending", "operation_id", record.ID, "code", "inspect_backup_recovery")
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
	for {
		if ctx.Err() != nil {
			return
		}
		records, err := cache.Records(ctx)
		if err != nil {
			logger.Warn("backup_report_failed", "code", "journal_read_failed")
		}
		for _, record := range records {
			var check proof.Request
			if json.Unmarshal(record.Input, &check) == nil && check.Kind.Valid() {
				reportProof(stream, record, check, waiting, logger)
				continue
			}
			var job restore.Request
			if json.Unmarshal(record.Input, &job) == nil && job.Kind == "restore" {
				reportRestore(stream, record, job, waiting, logger)
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
			if event.Validate(request) != nil {
				logger.Warn("backup_report_failed", "operation_id", record.ID, "code", "result_invalid")
				continue
			}
			raw, err := json.Marshal(event)
			if err != nil {
				continue
			}
			err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_BackupEvent{BackupEvent: &agentv1.BackupEvent{EventJson: raw}}})
			if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
				logger.Warn("backup_report_failed", "code", "result_delivery_failed")
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

func reportRestore(stream *transport.Client, record journal.Record, request restore.Request, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger) {
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
	if event.Validate(request) != nil {
		logger.Warn("restore_report_failed", "operation_id", record.ID, "code", "result_invalid")
		return
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_RestoreEvent{RestoreEvent: &agentv1.RestoreEvent{EventJson: raw}}})
	if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
		logger.Warn("restore_report_failed", "code", "result_delivery_failed")
	}
}

func reportProof(stream *transport.Client, record journal.Record, request proof.Request, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger) {
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
	if event.Validate(request) != nil {
		logger.Warn("proof_report_failed", "operation_id", record.ID, "code", "result_invalid")
		return
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	err = stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_ProofEvent{ProofEvent: &agentv1.ProofEvent{EventJson: raw}}})
	if err != nil && !errors.Is(err, controlstream.ErrDisconnected) && !errors.Is(err, controlstream.ErrBusy) {
		logger.Warn("proof_report_failed", "code", "result_delivery_failed")
	}
}
