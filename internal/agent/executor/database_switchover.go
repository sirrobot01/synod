package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type databaseSwitchoverProof struct {
	InputHash []byte                             `json:"input_hash"`
	Receipt   *agentv1.DatabaseSwitchoverReceipt `json:"receipt"`
}

// switchDatabase needs a target whose supervisor has acknowledged the move hold.
func (e *Executor) switchDatabase(ctx context.Context, command *agentv1.Command) (result runtime.Result, err error) {
	if e.options.Replication == nil || e.options.BootTime == nil || !filepath.IsAbs(e.options.AppsDir) {
		return result, runtime.ErrUnsupportedConfiguration
	}
	native, ok := e.runtime.(engineRuntime)
	if !ok {
		return result, runtime.ErrUnsupportedConfiguration
	}
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	appID, input := command.Epoch.AppId, command.Input.GetSwitchDatabase()
	unlock, err := e.options.Replication.LockApp(ctx, appID)
	if err != nil {
		return result, err
	}
	defer unlock()
	assignment, err := e.writerAssignment(appID)
	if err != nil || assignment.Epoch != input.WriterEpoch || assignment.Database == nil {
		return result, errWriterLease
	}
	root, err := os.OpenRoot(filepath.Join(e.options.AppsDir, appID))
	if err != nil {
		return result, err
	}
	defer root.Close()
	proofName := "database-switch-" + command.CommandId + ".json"
	if _, err := root.Stat(proofName); err == nil {
		return e.recoverDatabaseSwitch(ctx, command)
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	pg, redis, err := e.databaseCredentials(assignment)
	if err != nil {
		return result, err
	}
	started, err := e.options.BootTime()
	if err != nil {
		return result, err
	}
	expires := started + command.Input.Timeout.AsDuration()
	x := func(ctx context.Context, env []string, stdin string, args ...string) ([]byte, error) {
		return native.EngineExec(ctx, uuid.MustParse(appID), env, stdin, args...)
	}
	target := func(ctx context.Context, env []string, stdin string, args ...string) ([]byte, error) {
		if assignment.Database.Engine == "postgres" {
			remote := []string{"PGHOST=" + input.TargetHost, "PGPORT=" + strconv.Itoa(int(input.TargetPort)), "PGUSER=synod_admin", "PGPASSWORD=" + pg.AdminPassword, "PGSSLMODE=verify-ca", "PGSSLROOTCERT=" + engineCAFile, "PGCONNECT_TIMEOUT=5"}
			return x(ctx, append(remote, env...), stdin, args...)
		}
		if len(args) < 3 || args[0] != "redis-cli" || args[1] != "-s" || args[2] != database.RedisSocket {
			return nil, runtime.ErrUnsupportedConfiguration
		}
		remote := []string{"redis-cli", "--tls", "--cacert", engineCAFile, "-h", input.TargetHost, "-p", strconv.Itoa(int(input.TargetPort))}
		return x(ctx, env, stdin, append(remote, args[3:]...)...)
	}
	// Rollback is authorized by the current lease, even if the control session was
	// cancelled. A lost lease permits fencing only; it cannot restore writes.
	paused, stopped := false, false
	defer func() {
		if !paused || stopped {
			return
		}
		rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		current, leaseErr := e.writerAssignment(appID)
		if leaseErr != nil || current.Epoch != input.WriterEpoch {
			err = errors.Join(err, errWriterLease, native.FenceEngine(rollback, uuid.MustParse(appID)))
			return
		}
		var resumeErr error
		if assignment.Database.Engine == "postgres" {
			resumeErr = pg.ResumeWrites(rollback, x)
		} else {
			resumeErr = redis.ResumeWrites(rollback, x)
		}
		if resumeErr == nil {
			resumeErr = root.Remove("database-write-pause")
		}
		err = errors.Join(err, resumeErr)
	}()
	if err := writeWriterFile(root, "database-write-pause", strconv.FormatUint(input.WriterEpoch, 10)); err != nil {
		return result, err
	}
	paused = true
	var positions map[string]database.PostgresPosition
	var position database.RedisPosition
	if assignment.Database.Engine == "postgres" {
		positions, err = pg.PauseWrites(ctx, x)
	} else {
		position, err = redis.PauseWrites(ctx, x, command.Input.Timeout.AsDuration())
	}
	if err != nil {
		return result, err
	}
	targetPG := pg
	targetPG.Node = uuid.MustParse(input.TargetNodeId)
	for {
		current, err := e.writerAssignment(appID)
		if err != nil || current.Epoch != input.WriterEpoch {
			return result, errWriterLease
		}
		now, err := e.options.BootTime()
		if err != nil {
			return result, err
		}
		if now >= expires {
			return result, context.DeadlineExceeded
		}
		var ready bool
		if assignment.Database.Engine == "postgres" {
			ready, err = targetPG.CaughtUp(ctx, target, positions)
		} else {
			ready, err = redis.CaughtUp(ctx, target, position)
		}
		if err != nil {
			return result, err
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	if assignment.Database.Engine == "postgres" {
		for _, name := range assignment.Database.Databases {
			if err := targetPG.SyncSequences(ctx, target, database.Endpoint{Host: "127.0.0.1", Port: 5432, CAFile: engineCAFile}, name); err != nil {
				return result, err
			}
		}
	}
	if err := native.FenceEngine(ctx, uuid.MustParse(appID)); err != nil {
		return result, err
	}
	for {
		err := e.inspectDatabaseStop(ctx, uuid.MustParse(appID))
		if err == nil {
			break
		}
		if !errors.Is(err, runtime.ErrBusy) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	stopped = true
	// Redis can resume writes when its pause ends. A late stop proves no final copy.
	now, err := e.options.BootTime()
	if err != nil {
		return result, err
	}
	if now >= expires || ctx.Err() != nil {
		return result, context.DeadlineExceeded
	}
	encoded, err := json.Marshal(struct {
		Postgres map[string]database.PostgresPosition
		Redis    database.RedisPosition
	}{positions, position})
	if err != nil {
		return result, err
	}
	digest := sha256.Sum256(encoded)
	proof := databaseSwitchoverProof{InputHash: bytes.Clone(command.InputHash), Receipt: &agentv1.DatabaseSwitchoverReceipt{WriterEpoch: input.WriterEpoch, TargetNodeId: input.TargetNodeId, PositionHash: digest[:]}}
	encoded, err = json.Marshal(proof)
	if err != nil {
		return result, err
	}
	if err := writeWriterFile(root, proofName, string(encoded)); err != nil {
		return result, err
	}
	return e.recoverDatabaseSwitch(ctx, command)
}

func (e *Executor) recoverDatabaseSwitch(ctx context.Context, command *agentv1.Command) (runtime.Result, error) {
	if !filepath.IsAbs(e.options.AppsDir) {
		return runtime.Result{}, runtime.ErrUnsupportedConfiguration
	}
	root, err := os.OpenRoot(filepath.Join(e.options.AppsDir, command.Epoch.AppId))
	if err != nil {
		return runtime.Result{}, err
	}
	defer root.Close()
	data, err := root.ReadFile("database-switch-" + command.CommandId + ".json")
	if err != nil {
		return runtime.Result{}, err
	}
	var proof databaseSwitchoverProof
	if json.Unmarshal(data, &proof) != nil || !bytes.Equal(proof.InputHash, command.InputHash) {
		return runtime.Result{}, operation.ErrCommandConflict
	}
	if err := e.inspectDatabaseStop(ctx, uuid.MustParse(command.Epoch.AppId)); err != nil {
		return runtime.Result{}, err
	}
	effect := &agentv1.Effect{CommandId: command.CommandId, ResourceId: command.Input.ResourceId, NativeOperationId: "database-switch:" + command.CommandId, State: "database_switchover_ready", DatabaseSwitchover: proof.Receipt}
	if err := operation.CheckStepEffects(&agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_DATABASE_SWITCHOVER_READY}, []*agentv1.Effect{effect}); err != nil {
		return runtime.Result{}, err
	}
	return runtime.Result{Effects: []*agentv1.Effect{effect}}, nil
}

func (e *Executor) inspectDatabaseStop(ctx context.Context, appID uuid.UUID) error {
	inspection, err := e.runtime.InspectApp(ctx, appID, nil)
	if err != nil {
		return err
	}
	for _, resource := range inspection.Resources {
		if resource.Kind == "container" && !slices.Contains([]string{"created", "exited", "dead", "absent", "stopped"}, resource.State) {
			return runtime.ErrBusy
		}
	}
	return nil
}
