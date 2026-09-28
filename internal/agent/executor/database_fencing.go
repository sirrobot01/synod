package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
)

type databaseFenceRuntime interface {
	EngineApps(context.Context) ([]uuid.UUID, error)
	FenceEngine(context.Context, uuid.UUID) error
}

// watchDatabaseWriters keeps fencing independent of engine commands and credentials.
func (e *Executor) watchDatabaseWriters(ctx context.Context) {
	native, ok := e.runtime.(databaseFenceRuntime)
	if !ok {
		return
	}
	defer func() {
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.options.InspectionTimeout)
		defer cancel()
		if err := e.fenceDatabaseWriters(stop, native, true); err != nil {
			e.fail(err)
		}
	}()
	for {
		check, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
		err := e.fenceDatabaseWriters(check, native, false)
		cancel()
		if err != nil && ctx.Err() == nil {
			e.fail(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (e *Executor) fenceDatabaseWriters(ctx context.Context, native databaseFenceRuntime, closing bool) error {
	apps, err := native.EngineApps(ctx)
	if err != nil {
		return err
	}
	for _, id := range apps {
		if e.options.Replication != nil {
			assignment, assigned := e.options.Replication.Lookup(id.String())
			if assigned && assignment.Database != nil {
				if assignment.Role == replication.RoleStandby && filepath.IsAbs(e.options.AppsDir) {
					_, err := os.Stat(filepath.Join(e.options.AppsDir, id.String(), "database-primary-epoch"))
					if errors.Is(err, os.ErrNotExist) {
						continue
					}
				}
				if !closing && assignment.Role == replication.RoleActive {
					if current, err := e.writerAssignment(id.String()); err == nil && current.Epoch == assignment.Epoch {
						continue
					}
				}
			}
		}
		if err := native.FenceEngine(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
