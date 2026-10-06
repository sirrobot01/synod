package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/controlstream"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func executeBackups(ctx context.Context, worker *agentbackup.Worker, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		records, err := worker.Journal.Records(ctx)
		if err != nil {
			logger.Warn("backup_journal_failed", "code", "journal_read_failed")
		}
		// Recovery runs before new work. A pending restart prevents another app from stopping.
		slices.SortStableFunc(records, func(a, b journal.Record) int {
			if a.State == journal.Running && b.State != journal.Running {
				return -1
			}
			if b.State == journal.Running && a.State != journal.Running {
				return 1
			}
			return 0
		})
		for _, record := range records {
			if ctx.Err() != nil {
				return
			}
			if record.State == journal.Terminal {
				continue
			}
			var request backup.Request
			if json.Unmarshal(record.Input, &request) != nil || request.Validate() != nil {
				logger.Warn("backup_journal_failed", "code", "journal_request_invalid")
				break
			}
			waiting.Store(nil)
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
		}
	}
}

func reportBackups(ctx context.Context, cache *journal.Journal, stream *transport.Client, waiting *atomic.Pointer[uuid.UUID], logger *slog.Logger) {
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
			var request backup.Request
			if json.Unmarshal(record.Input, &request) != nil || request.Validate() != nil {
				continue
			}
			event := backup.Event{ID: record.ID, Result: backup.Result{DestinationID: request.DestinationID}}
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
		}
	}
}
