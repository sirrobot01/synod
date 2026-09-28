package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database"
	"github.com/sirrobot01/synod/internal/agent/database/enginefixture"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type switchoverTestRuntime struct {
	runtime.Runtime
	exec    database.Exec
	fence   func(context.Context) error
	inspect func(context.Context) (runtime.AppInspection, error)
}

func (r *switchoverTestRuntime) EngineExec(ctx context.Context, _ uuid.UUID, env []string, stdin string, args ...string) ([]byte, error) {
	return r.exec(ctx, env, stdin, args...)
}

func (r *switchoverTestRuntime) FenceEngine(ctx context.Context, _ uuid.UUID) error {
	return r.fence(ctx)
}

func (r *switchoverTestRuntime) InspectApp(ctx context.Context, _ uuid.UUID, _ []*agentv1.Effect) (runtime.AppInspection, error) {
	return r.inspect(ctx)
}

func TestDatabaseSwitchoverKeepsDurableProof(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	for _, kind := range []string{"postgres", "redis"} {
		t.Run(kind, func(t *testing.T) {
			network := "synod-switch-command-" + uuid.NewString()[:8]
			if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
			certificates := enginefixture.TLS(t)
			primaryName, standbyName := network+"-a", network+"-b"
			appID, targetID, resource := uuid.NewString(), uuid.NewString(), uuid.New()
			app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: kind}}
			if kind == "postgres" {
				app.Database.Databases = []string{"photos"}
			}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{app})
			started := time.Now()
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return time.Since(started), nil })
			pushDatabaseSecrets(t, e, session, appID)
			pg, redis, err := e.databaseCredentials(app)
			if err != nil {
				t.Fatal(err)
			}
			var primary, standby database.Exec
			var port uint32
			if kind == "postgres" {
				port = 5432
				primary = enginefixture.StartPostgres(t, network, primaryName, certificates, pg.AdminPassword)
				standby = enginefixture.StartPostgres(t, network, standbyName, certificates, pg.AdminPassword)
				if err := pg.EnsurePrimary(t.Context(), primary); err != nil {
					t.Fatal(err)
				}
				if _, err := primary(t.Context(), nil, "CREATE TABLE proof (id bigserial PRIMARY KEY); INSERT INTO proof DEFAULT VALUES;", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-"); err != nil {
					t.Fatal(err)
				}
				pg.Node = uuid.MustParse(targetID)
				if err := pg.Copy(t.Context(), standby, database.Endpoint{Host: primaryName, Port: 5432, CAFile: engineCAFile}, "photos"); err != nil {
					t.Fatal(err)
				}
			} else {
				port = 6379
				primary = enginefixture.StartRedis(t, network, primaryName, certificates, redis.AdminPassword, redis.ReplicationPassword, database.RedisSocket)
				standby = enginefixture.StartRedis(t, network, standbyName, certificates, redis.AdminPassword, redis.ReplicationPassword, database.RedisSocket)
				if _, err := primary(t.Context(), []string{"REDISCLI_AUTH=" + redis.AdminPassword}, "", "redis-cli", "-s", database.RedisSocket, "SET", "proof", "retained"); err != nil {
					t.Fatal(err)
				}
				if err := redis.EnsureReplica(t.Context(), standby, database.Endpoint{Host: primaryName, Port: 6379}); err != nil {
					t.Fatal(err)
				}
			}
			enginefixture.Wait(t, time.Minute, func() error {
				var state string
				var err error
				if kind == "postgres" {
					var status database.PostgresStatus
					status, err = pg.Status(t.Context(), standby, "photos")
					state = status.State
				} else {
					var status database.RedisStatus
					status, err = redis.Status(t.Context(), standby)
					state = status.State
				}
				if err != nil || state != database.StateStreaming {
					return fmt.Errorf("standby: %s %v", state, err)
				}
				return nil
			})
			host, err := enginefixture.Docker(t, "", "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", standbyName)
			if err != nil {
				t.Fatal(err)
			}
			identity := e.journal.Identity()
			command := &agentv1.Command{Epoch: &agentv1.Epoch{InstallationId: identity.InstallationID, NodeId: identity.NodeID, AppId: appID, RecoveryGeneration: identity.RecoveryGeneration, ExecutionEpoch: 1}, OperationId: uuid.NewString(), CommandId: uuid.NewString(), StepId: uuid.NewString(), DesiredRevision: 2, PlanVersion: 1, Input: &agentv1.CommandInput{ResourceId: resource.String(), PayloadVersion: 1, Timeout: durationpb.New(time.Minute), Payload: &agentv1.CommandInput_SwitchDatabase{SwitchDatabase: &agentv1.SwitchDatabase{WriterEpoch: 1, TargetNodeId: targetID, TargetHost: strings.TrimSpace(host), TargetPort: port}}}}
			hash, err := operation.CommandHash(command)
			if err != nil {
				t.Fatal(err)
			}
			command.InputHash = hash[:]
			if err := os.MkdirAll(filepath.Join(e.options.AppsDir, appID), 0700); err != nil {
				t.Fatal(err)
			}
			e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Since(started) + time.Minute}}
			e.runtime = &switchoverTestRuntime{exec: primary, fence: func(context.Context) error {
				_, err := enginefixture.Docker(t, "", "stop", "-t", "3", primaryName)
				return err
			}, inspect: func(context.Context) (runtime.AppInspection, error) {
				state, err := enginefixture.Docker(t, "", "inspect", "--format", "{{.State.Status}}", primaryName)
				return runtime.AppInspection{Resources: []runtime.Resource{{AppID: uuid.MustParse(appID), ResourceID: resource, Kind: "container", State: strings.TrimSpace(state)}}}, err
			}}
			result, err := e.switchDatabase(t.Context(), command)
			if err != nil || len(result.Effects) != 1 {
				t.Fatalf("switch: %+v %v", result, err)
			}
			step := &agentv1.PlanStep{Command: command, Postcondition: agentv1.Postcondition_POSTCONDITION_DATABASE_SWITCHOVER_READY}
			if err := operation.CheckStepEffects(step, result.Effects); err != nil {
				t.Fatal(err)
			}
			restarted := &Executor{runtime: e.runtime, options: Options{AppsDir: e.options.AppsDir}}
			recovered, err := restarted.recoverDatabaseSwitch(t.Context(), command)
			if err != nil || len(recovered.Effects) != 1 || !proto.Equal(recovered.Effects[0], result.Effects[0]) {
				t.Fatalf("lost reply recovery: %+v %v", recovered, err)
			}
			changed := proto.Clone(command).(*agentv1.Command)
			changed.InputHash[0] ^= 1
			if _, err := restarted.recoverDatabaseSwitch(t.Context(), changed); !errors.Is(err, operation.ErrCommandConflict) {
				t.Fatalf("another input used the proof: %v", err)
			}
			if kind == "postgres" {
				if err := pg.Promote(t.Context(), standby); err != nil {
					t.Fatal(err)
				}
				out, err := standby(t.Context(), nil, "INSERT INTO proof DEFAULT VALUES; SELECT count(*) FROM proof;", "psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-")
				if err != nil || strings.TrimSpace(string(out)) != "2" {
					t.Fatalf("promoted data: %q %v", out, err)
				}
			} else {
				if err := redis.EnsurePrimary(t.Context(), standby); err != nil {
					t.Fatal(err)
				}
				out, err := standby(t.Context(), []string{"REDISCLI_AUTH=" + redis.AdminPassword}, "", "redis-cli", "-s", database.RedisSocket, "GET", "proof")
				if err != nil || !strings.Contains(string(out), "retained") {
					t.Fatalf("promoted data: %q %v", out, err)
				}
			}
			// Remove the stopped source and its local proof before creating a fresh standby.
			e.options.Replication.Set(nil)
			cleanup := &agentv1.Command{Epoch: command.Epoch, CommandId: uuid.NewString(), Input: &agentv1.CommandInput{ResourceId: resource.String(), Payload: &agentv1.CommandInput_RemoveAppData{RemoveAppData: &agentv1.RemoveAppData{WriterEpoch: 2}}}}
			if _, err := e.removeAppData(t.Context(), cleanup); !errors.Is(err, runtime.ErrBusy) {
				t.Fatalf("old container still owns the data: %v", err)
			}
			if _, err := enginefixture.Docker(t, "", "rm", "-v", primaryName); err != nil {
				t.Fatal(err)
			}
			e.runtime.(*switchoverTestRuntime).inspect = func(context.Context) (runtime.AppInspection, error) {
				containers, err := enginefixture.Docker(t, "", "ps", "-aq", "--filter", "name=^/"+primaryName+"$")
				if err != nil {
					return runtime.AppInspection{}, err
				}
				if strings.TrimSpace(containers) != "" {
					return runtime.AppInspection{Resources: []runtime.Resource{{Kind: "container"}}}, nil
				}
				return runtime.AppInspection{}, nil
			}
			if _, err := e.removeAppData(t.Context(), cleanup); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(e.options.AppsDir, appID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old primary directory remains: %v", err)
			}
			if kind == "postgres" {
				primary = enginefixture.StartPostgres(t, network, primaryName, certificates, pg.AdminPassword)
				pg.Node = uuid.MustParse(identity.NodeID)
				if err := pg.Copy(t.Context(), primary, database.Endpoint{Host: standbyName, Port: 5432, CAFile: engineCAFile}, "photos"); err != nil {
					t.Fatal(err)
				}
				if _, err := standby(t.Context(), nil, "INSERT INTO proof DEFAULT VALUES;", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-"); err != nil {
					t.Fatal(err)
				}
				enginefixture.Wait(t, time.Minute, func() error {
					out, err := primary(t.Context(), nil, "SELECT count(*) FROM proof;", "psql", "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-")
					if err != nil || strings.TrimSpace(string(out)) != "3" {
						return fmt.Errorf("rejoined data: %q %v", out, err)
					}
					return nil
				})
				if _, err := primary(t.Context(), nil, "INSERT INTO proof DEFAULT VALUES;", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-"); err == nil {
					t.Fatal("rejoined standby accepted a write")
				}
			} else {
				primary = enginefixture.StartRedis(t, network, primaryName, certificates, redis.AdminPassword, redis.ReplicationPassword, database.RedisSocket)
				if err := redis.EnsureReplica(t.Context(), primary, database.Endpoint{Host: standbyName, Port: 6379}); err != nil {
					t.Fatal(err)
				}
				if _, err := standby(t.Context(), []string{"REDISCLI_AUTH=" + redis.AdminPassword}, "", "redis-cli", "-s", database.RedisSocket, "SET", "proof", "after-promotion"); err != nil {
					t.Fatal(err)
				}
				enginefixture.Wait(t, time.Minute, func() error {
					out, err := primary(t.Context(), []string{"REDISCLI_AUTH=" + redis.AdminPassword}, "", "redis-cli", "-s", database.RedisSocket, "GET", "proof")
					if err != nil || strings.TrimSpace(string(out)) != "after-promotion" {
						return fmt.Errorf("rejoined data: %q %v", out, err)
					}
					return nil
				})
				out, err := primary(t.Context(), []string{"REDISCLI_AUTH=" + redis.AdminPassword}, "", "redis-cli", "-s", database.RedisSocket, "SET", "forbidden", "write")
				if err == nil && !strings.Contains(string(out), "READONLY") {
					t.Fatalf("rejoined standby accepted a write: %q", out)
				}
			}

		})
	}
}

func TestDatabaseSwitchoverRollbackNeedsCurrentLease(t *testing.T) {
	for _, lostLease := range []bool{false, true} {
		t.Run(fmt.Sprint(lostLease), func(t *testing.T) {
			appID := uuid.NewString()
			app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{app})
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			pushDatabaseSecrets(t, e, session, appID)
			e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Minute}}
			if err := os.MkdirAll(filepath.Join(e.options.AppsDir, appID), 0700); err != nil {
				t.Fatal(err)
			}
			resumed, fenced := false, false
			e.runtime = &switchoverTestRuntime{exec: func(_ context.Context, _ []string, _ string, args ...string) ([]byte, error) {
				if slices.Contains(args, "UNPAUSE") {
					resumed = true
				}
				if slices.Contains(args, "PAUSE") && lostLease {
					delete(e.leases, appID)
				}
				if slices.Contains(args, "INFO") {
					if slices.Contains(args, "-h") {
						return []byte("role:slave\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_replid:" + strings.Repeat("a", 40) + "\nslave_repl_offset:0\n"), nil
					}
					return []byte("role:master\nmaster_replid:" + strings.Repeat("a", 40) + "\nmaster_repl_offset:10\n"), nil
				}
				return nil, nil
			}, fence: func(context.Context) error { fenced = true; return nil }}
			command := &agentv1.Command{Epoch: &agentv1.Epoch{AppId: appID}, CommandId: uuid.NewString(), Input: &agentv1.CommandInput{Timeout: durationpb.New(time.Second), Payload: &agentv1.CommandInput_SwitchDatabase{SwitchDatabase: &agentv1.SwitchDatabase{WriterEpoch: 1, TargetNodeId: uuid.NewString(), TargetHost: "192.0.2.1", TargetPort: 6379}}}}
			result, err := e.switchDatabase(t.Context(), command)
			if len(result.Effects) != 0 || resumed == lostLease || fenced != lostLease {
				t.Fatalf("rollback: effects %v, resumed %v, fenced %v, err %v", result.Effects, resumed, fenced, err)
			}
			if lostLease && !errors.Is(err, errWriterLease) || !lostLease && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("rollback cause: %v", err)
			}
		})
	}
}

func TestDatabasePrimaryKeepsPauseAndFencesDuringSwitchover(t *testing.T) {
	appID := uuid.NewString()
	app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{app})
	now := time.Duration(0)
	e, session := databaseExecutor(t, table, func() (time.Duration, error) { return now, nil })
	pushDatabaseSecrets(t, e, session, appID)
	e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Minute}}
	unlock, err := table.LockApp(t.Context(), appID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	native := &fakeEngine{}
	if report := e.databaseRound(t.Context(), native, app, &databaseState{}); report != nil || native.ran("RESET default_transaction_read_only") {
		t.Fatalf("supervisor changed the paused primary: %+v", report)
	}
	now = time.Minute
	report := e.databaseRound(t.Context(), native, app, &databaseState{})
	if report == nil || report.FailureCode != "writer_lease_expired" || native.fenced != 1 {
		t.Fatalf("switchover blocked fencing: %+v, fences %d", report, native.fenced)
	}
}

func TestDatabaseSwitchoverHoldObservesWithoutMutations(t *testing.T) {
	for _, kind := range []string{"postgres", "redis"} {
		t.Run(kind, func(t *testing.T) {
			app := &agentv1.AppReplication{AppId: uuid.NewString(), Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: kind, PrimaryHost: "192.0.2.10", PrimaryPort: 5432, SwitchoverOperationId: uuid.NewString()}}
			if kind == "postgres" {
				app.Database.Databases = []string{"photos"}
			}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{app})
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			pushDatabaseSecrets(t, e, session, app.AppId)
			native := &fakeEngine{status: "1|2|0|0.5|0", redisInfo: "role:slave\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_last_io_seconds_ago:1\n"}
			state := &databaseState{applyErrors: map[string]int64{}}
			report := e.databaseRound(t.Context(), native, app, state)
			if report == nil || report.DatabaseHoldId != app.Database.SwitchoverOperationId || report.State != database.StateStreaming || report.Sequence == 0 {
				t.Fatalf("hold acknowledgement: %+v", report)
			}
			// A failed check must not start a new copy while final positions are checked.
			native.status, native.redisInfo = "0|0|0|-1|0", "role:master\n"
			report = e.databaseRound(t.Context(), native, app, state)
			if report == nil || report.State == database.StateStreaming || report.FailureCode == "" {
				t.Fatalf("failed target remained eligible: %+v", report)
			}
			for _, mutation := range []string{"CREATE", "ALTER", "DROP", "setval", "pg_dump", "REPLICAOF", "CONFIG"} {
				if native.ran(mutation) {
					t.Fatalf("held target performed %s", mutation)
				}
			}
		})
	}
}

func TestDatabaseSwitchoverWaitsForStopBeforeSavingProof(t *testing.T) {
	for _, scenario := range []string{"delayed", "late", "inspection_failure"} {
		t.Run(scenario, func(t *testing.T) {
			appID := uuid.NewString()
			assignment := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{assignment})
			now := time.Duration(0)
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return now, nil })
			pushDatabaseSecrets(t, e, session, appID)
			e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Minute}}
			if err := os.MkdirAll(filepath.Join(e.options.AppsDir, appID), 0700); err != nil {
				t.Fatal(err)
			}
			command := &agentv1.Command{Epoch: &agentv1.Epoch{AppId: appID}, CommandId: uuid.NewString(), InputHash: make([]byte, 32), Input: &agentv1.CommandInput{ResourceId: uuid.NewString(), Timeout: durationpb.New(time.Second), Payload: &agentv1.CommandInput_SwitchDatabase{SwitchDatabase: &agentv1.SwitchDatabase{WriterEpoch: 1, TargetNodeId: uuid.NewString(), TargetHost: "192.0.2.1", TargetPort: 6379}}}}
			proof := filepath.Join(e.options.AppsDir, appID, "database-switch-"+command.CommandId+".json")
			inspections := 0
			resumed := false
			e.runtime = &switchoverTestRuntime{exec: func(_ context.Context, _ []string, _ string, args ...string) ([]byte, error) {
				if slices.Contains(args, "UNPAUSE") {
					resumed = true
				}
				if slices.Contains(args, "INFO") {
					if slices.Contains(args, "-h") {
						return []byte("role:slave\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_replid:" + strings.Repeat("a", 40) + "\nslave_repl_offset:10\n"), nil
					}
					return []byte("role:master\nmaster_replid:" + strings.Repeat("a", 40) + "\nmaster_repl_offset:10\n"), nil
				}
				return nil, nil
			}, fence: func(context.Context) error { return nil }, inspect: func(context.Context) (runtime.AppInspection, error) {
				inspections++
				if scenario == "inspection_failure" {
					return runtime.AppInspection{}, errors.New("inspection failed")
				}
				state := "running"
				if inspections >= 3 {
					state = "exited"
					if scenario == "late" {
						now = time.Second
					}
				} else if _, err := os.Stat(proof); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("proof saved before source quiescence")
				}
				return runtime.AppInspection{Resources: []runtime.Resource{{Kind: "container", State: state}}}, nil
			}}
			result, err := e.switchDatabase(t.Context(), command)
			if scenario == "delayed" {
				if err != nil || len(result.Effects) != 1 || inspections < 3 || resumed {
					t.Fatalf("delayed stop: %+v %v, inspections %d, resumed %v", result, err, inspections, resumed)
				}
			} else {
				if err == nil || len(result.Effects) != 0 {
					t.Fatalf("unproved stop succeeded: %+v %v", result, err)
				}
				if _, err := os.Stat(proof); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("failed stop retained success proof")
				}
				if scenario == "late" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("late stop: %v", err)
				}
				if resumed != (scenario == "inspection_failure") {
					t.Fatalf("wrong rollback: resumed %v", resumed)
				}
			}
		})
	}
}

func TestDatabasePrimaryRecoversAnInterruptedWritePause(t *testing.T) {
	for _, kind := range []string{"postgres", "redis"} {
		t.Run(kind, func(t *testing.T) {
			appID := uuid.NewString()
			assignment := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: kind}}
			if kind == "postgres" {
				assignment.Database.Databases = []string{"photos"}
			}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{assignment})
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			pushDatabaseSecrets(t, e, session, appID)
			e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Minute}}
			root := filepath.Join(e.options.AppsDir, appID)
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(root, "database-write-pause")
			if err := os.WriteFile(marker, []byte("1"), 0600); err != nil {
				t.Fatal(err)
			}
			native := &fakeEngine{redisInfo: "role:master\n"}
			state := &databaseState{prepared: time.Now()}
			report := e.databaseRound(t.Context(), native, assignment, state)
			if report == nil || report.State != database.StateStreaming {
				t.Fatalf("pause recovery: %+v", report)
			}
			resumed := native.ran("UNPAUSE")
			if kind == "postgres" {
				resumed = native.ran("pg_terminate_backend")
			}
			if !resumed {
				t.Fatal("recent setup skipped interrupted pause recovery")
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("pause marker remains: %v", err)
			}
		})
	}
}
