package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database"
	"github.com/sirrobot01/synod/internal/agent/replication"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// engineRuntime runs engine steps in the tier 1 copies on this node.
type engineRuntime interface {
	EngineExec(ctx context.Context, appID uuid.UUID, env []string, stdin string, args ...string) ([]byte, error)
	FenceEngine(ctx context.Context, appID uuid.UUID) error
}

// Supervisor timing. Each step is idempotent, so a missed round only delays it.
const (
	databaseInterval  = 5 * time.Second
	schemaCheckPeriod = 30 * time.Second
	primaryRepeat     = 5 * time.Minute
	slotCleanupPeriod = time.Minute
)

// The CA path inside a tier 1 container. The container copies the TLS files there.
const engineCAFile = "/tls/ca.crt"

var errDatabaseCredentials = errors.New("database credentials are not in this session")

// superviseDatabases runs one supervisor per tier 1 assignment. A changed assignment
// restarts its supervisor, so a promotion or a new primary starts from a clean state.
func (e *Executor) superviseDatabases(ctx context.Context) {
	native, ok := e.runtime.(engineRuntime)
	if !ok || e.options.Replication == nil {
		return
	}
	type running struct {
		assignment *agentv1.AppReplication
		cancel     context.CancelFunc
		done       chan struct{}
	}
	copies := map[string]running{}
	defer func() {
		for _, c := range copies {
			c.cancel()
			<-c.done
		}
	}()
	for {
		wanted := map[string]*agentv1.AppReplication{}
		for _, app := range e.options.Replication.Apps() {
			if app.Database != nil {
				wanted[app.AppId] = app
			}
		}
		for id, c := range copies {
			if next, ok := wanted[id]; !ok || !proto.Equal(next, c.assignment) {
				c.cancel()
				<-c.done
				delete(copies, id)
			}
		}
		for id, app := range wanted {
			if _, ok := copies[id]; ok {
				continue
			}
			child, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			copies[id] = running{assignment: app, cancel: cancel, done: done}
			go func() {
				defer close(done)
				e.superviseDatabase(child, native, app)
			}()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(databaseInterval):
		}
	}
}

// databaseState is what one supervisor remembers between rounds.
type databaseState struct {
	prepared, schemaChecked, slotsCleaned time.Time
	// applyErrors holds the last cumulative error count per database.
	applyErrors map[string]int64
	schemas     map[string]string
	complete    *agentv1.ReplicaStatus
}

// databaseSequence orders reports by wall time. The controller keeps the highest
// sequence it has seen, so a restarted agent must not count from one again.
func databaseSequence() uint64 {
	return uint64(time.Now().UnixMilli())
}

// superviseDatabase keeps one copy in its assigned role and reports its state.
func (e *Executor) superviseDatabase(ctx context.Context, native engineRuntime, app *agentv1.AppReplication) {
	state := &databaseState{applyErrors: map[string]int64{}}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(databaseInterval):
		}
		if report := e.databaseRound(ctx, native, app, state); report != nil {
			e.recordDatabase(report)
		}
	}
}

// databaseRound runs one round of engine steps. Fencing does not need session credentials.
func (e *Executor) databaseRound(ctx context.Context, native engineRuntime, app *agentv1.AppReplication, state *databaseState) *agentv1.ReplicaStatus {
	appID, err := uuid.Parse(app.AppId)
	if err != nil {
		return nil
	}
	x := func(ctx context.Context, env []string, stdin string, args ...string) ([]byte, error) {
		return native.EngineExec(ctx, appID, env, stdin, args...)
	}
	report := &agentv1.ReplicaStatus{AppId: app.AppId, Role: app.Role, Epoch: app.Epoch}
	if app.Role == replication.RoleActive {
		if current, err := e.writerAssignment(app.AppId); err != nil || current.Epoch != app.Epoch {
			// The lease is gone, so another copy may be promoted. Stop this primary.
			report.State, report.FailureCode = database.StateFailed, "writer_lease_expired"
			if err := native.FenceEngine(context.WithoutCancel(ctx), appID); err != nil {
				report.FailureCode = "fence_failed"
			}
			state.prepared = time.Time{}
			return report
		}
	}
	postgres, redis, err := e.databaseCredentials(app)
	if err != nil {
		return nil
	}
	if app.Role == replication.RoleActive {
		lock, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		unlock, err := e.options.Replication.LockApp(lock, app.AppId)
		cancel()
		if err != nil {
			return nil
		}
		defer unlock()
		// An assignment can change while cleanup or switchover holds the lock.
		if current, err := e.writerAssignment(app.AppId); err != nil || current.Epoch != app.Epoch {
			return nil
		}
		path := filepath.Join(e.options.AppsDir, app.AppId)
		if !filepath.IsAbs(e.options.AppsDir) {
			report.State, report.FailureCode = database.StateFailed, "primary_setup_failed"
			return report
		}
		_, pauseErr := os.Stat(filepath.Join(path, "database-write-pause"))
		if !errors.Is(pauseErr, os.ErrNotExist) {
			err = pauseErr
			if err == nil && app.Database.Engine == "postgres" {
				err = postgres.ResumeWrites(ctx, x)
			} else if err == nil {
				err = redis.ResumeWrites(ctx, x)
			}
			if err == nil {
				err = os.Remove(filepath.Join(path, "database-write-pause"))
			}
			if err != nil {
				report.State, report.FailureCode = database.StateFailed, "primary_setup_failed"
				return report
			}
			state.prepared = time.Time{}
		}
		if time.Since(state.prepared) > primaryRepeat {
			if err := os.MkdirAll(path, 0700); err != nil {
				report.State, report.FailureCode = database.StateFailed, "primary_setup_failed"
				return report
			}
			root, err := os.OpenRoot(path)
			if err == nil {
				// A former primary must be rebuilt before it can run as a standby.
				err = writeWriterFile(root, "database-primary-epoch", strconv.FormatUint(app.Epoch, 10))
				root.Close()
			}
			if err != nil {
				report.State, report.FailureCode = database.StateFailed, "primary_setup_failed"
				return report
			}
			if app.Database.Engine == "postgres" {
				if app.Epoch == 1 {
					// The first primary has no application databases to promote.
					err = postgres.EnsurePrimary(ctx, x)
				} else {
					// Remove the old subscription before this standby accepts writes.
					err = postgres.Promote(ctx, x)
				}
			} else {
				err = redis.EnsurePrimary(ctx, x)
			}
			if err != nil {
				report.State, report.FailureCode = database.StateFailed, "primary_setup_failed"
				return report
			}
			state.prepared = time.Now()
		}
		if app.Database.Engine == "postgres" && time.Since(state.slotsCleaned) > slotCleanupPeriod {
			if database.DropStaleSyncSlots(ctx, x) == nil {
				state.slotsCleaned = time.Now()
			}
		}
		report.State, report.Sequence, report.CompletedAt = database.StateStreaming, databaseSequence(), timestamppb.Now()
		return report
	}
	if _, err := os.Stat(filepath.Join(e.options.AppsDir, app.AppId, "database-primary-epoch")); !errors.Is(err, os.ErrNotExist) {
		report.State, report.FailureCode = database.StateFailed, database.StateFailed
		return report
	}
	primary := database.Endpoint{Host: app.Database.PrimaryHost, Port: uint16(app.Database.PrimaryPort), CAFile: engineCAFile}
	held := app.Database.SwitchoverOperationId != ""
	if held {
		// The previous supervisor has ended before this assignment can run.
		report.DatabaseHoldId = app.Database.SwitchoverOperationId
	}
	if app.Database.Engine == "redis" {
		status := database.RedisStatus{State: database.StateFailed}
		if !held {
			err = redis.EnsureReplica(ctx, x, primary)
		}
		if err == nil {
			if status, err = redis.Status(ctx, x); err != nil {
				status = database.RedisStatus{State: database.StateFailed}
			}
		}
		report.State, report.LagSeconds = status.State, status.LagSeconds
	} else {
		checkSchema := state.complete == nil || time.Since(state.schemaChecked) > schemaCheckPeriod
		report.State = database.StateStreaming
		for _, name := range app.Database.Databases {
			var status database.PostgresStatus
			if held {
				status, err = postgres.Status(ctx, x, name)
				if err != nil {
					status.State = database.StateFailed
				}
			} else {
				status = e.standbyDatabase(ctx, x, postgres, primary, name, checkSchema, state)
			}
			report.LagSeconds = max(report.LagSeconds, status.LagSeconds)
			if databaseStateRank(status.State) > databaseStateRank(report.State) {
				report.State = status.State
			}
		}
		if checkSchema {
			state.schemaChecked = time.Now()
		}
	}
	if report.State == database.StateDisconnected && state.complete != nil {
		report.Sequence = max(databaseSequence(), state.complete.Sequence+1)
		report.CompletedAt = timestamppb.Now()
		report.LagSeconds = state.complete.LagSeconds + max(0, time.Since(state.complete.CompletedAt.AsTime()).Seconds())
		return report
	}
	if report.State != database.StateStreaming {
		state.complete = nil
		report.FailureCode = report.State
		return report
	}
	report.Sequence, report.CompletedAt = databaseSequence(), timestamppb.Now()
	state.complete = proto.Clone(report).(*agentv1.ReplicaStatus)
	return report
}

// standbyDatabase brings one standby database to streaming. It copies when the
// subscription is missing, when the apply worker reports new errors, or when the
// schema differs from the primary. A streaming database takes the primary's sequences.
func (e *Executor) standbyDatabase(ctx context.Context, x database.Exec, config database.Postgres, primary database.Endpoint, name string, checkSchema bool, state *databaseState) database.PostgresStatus {
	status, err := config.Status(ctx, x, name)
	if err != nil {
		return database.PostgresStatus{State: database.StateFailed}
	}
	stalled := status.ApplyErrors > state.applyErrors[name]
	var local string
	if (status.State == database.StateStreaming || status.State == database.StateDisconnected) && (checkSchema || stalled || status.State == database.StateDisconnected) {
		local, err = config.LocalSchemaHash(ctx, x, name)
		if err != nil {
			return database.PostgresStatus{State: database.StateFailed}
		}
	}
	// A lost connection also increases PostgreSQL's apply error count. Keep the
	// last complete copy while the source is down, if its local schema is unchanged.
	if status.State == database.StateDisconnected && state.schemas[name] != "" && local == state.schemas[name] {
		return status
	}
	drifted := false
	if (checkSchema || stalled) && status.State == database.StateStreaming {
		remote, remoteErr := config.PrimarySchemaHash(ctx, x, primary, name)
		if remoteErr != nil {
			if state.schemas[name] != "" && local == state.schemas[name] {
				return database.PostgresStatus{State: database.StateDisconnected}
			}
			return database.PostgresStatus{State: database.StateFailed}
		}
		drifted = local != remote
	}
	if status.State == database.StateFailed || stalled || drifted {
		delete(state.schemas, name)
		if err := config.Copy(ctx, x, primary, name); err != nil {
			return database.PostgresStatus{State: database.StateFailed}
		}
		state.applyErrors[name] = 0
		return database.PostgresStatus{State: database.StateCopying}
	}
	if status.State == database.StateDisconnected {
		return database.PostgresStatus{State: database.StateFailed}
	}
	if checkSchema && status.State == database.StateStreaming {
		if err := config.SyncSequences(ctx, x, primary, name); err != nil {
			if errors.Is(err, database.ErrUpstreamUnavailable) && state.schemas[name] != "" && local == state.schemas[name] {
				return database.PostgresStatus{State: database.StateDisconnected}
			}
			return database.PostgresStatus{State: database.StateDrifted, LagSeconds: status.LagSeconds}
		}
		if state.schemas == nil {
			state.schemas = map[string]string{}
		}
		state.schemas[name] = local
	}
	state.applyErrors[name] = status.ApplyErrors
	return status
}

// databaseStateRank orders states so that one bad database marks the whole copy.
func databaseStateRank(state string) int {
	return slices.Index([]string{database.StateStreaming, database.StateDisconnected, database.StateCopying, database.StateDrifted, database.StateFailed}, state)
}

// databaseCredentials reads one app's credentials from session memory.
func (e *Executor) databaseCredentials(app *agentv1.AppReplication) (database.Postgres, database.Redis, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.session
	if current == nil || current.ctx.Err() != nil {
		return database.Postgres{}, database.Redis{}, errDatabaseCredentials
	}
	read := func(purpose string) (string, bool) {
		secret, ok := current.secrets["db:"+app.AppId+":"+purpose]
		return string(secret.material), ok
	}
	admin, okAdmin := read("admin")
	replica, okReplica := read("replication")
	if !okAdmin || !okReplica {
		return database.Postgres{}, database.Redis{}, errDatabaseCredentials
	}
	redis := database.Redis{AdminPassword: admin, ReplicationPassword: replica}
	if app.Database.Engine != "postgres" {
		return database.Postgres{}, redis, nil
	}
	application, ok := read("app")
	if !ok {
		return database.Postgres{}, database.Redis{}, errDatabaseCredentials
	}
	node, err := uuid.Parse(e.journal.Identity().NodeID)
	if err != nil {
		return database.Postgres{}, database.Redis{}, err
	}
	return database.Postgres{Node: node, Databases: app.Database.Databases, AdminPassword: admin, ReplicationPassword: replica, AppPassword: application}, redis, nil
}

func (e *Executor) recordDatabase(report *agentv1.ReplicaStatus) {
	if e.options.ReplicaStatus != nil {
		e.options.ReplicaStatus.Record(report)
	}
}
