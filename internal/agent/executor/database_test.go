package executor

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/agent/replication"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeEngine records engine steps. It answers a subscription status query with status
// and gives both schema dumps the same text.
type fakeEngine struct {
	mu        sync.Mutex
	steps     []string
	status    string
	fenced    int
	redisInfo string
	failStep  string
	schema    string
}

func (f *fakeEngine) EngineExec(_ context.Context, _ uuid.UUID, _ []string, stdin string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.steps = append(f.steps, strings.Join(args, " ")+"\n"+stdin)
	if f.failStep != "" && strings.Contains(strings.Join(args, " ")+stdin, f.failStep) {
		return nil, errors.New("engine unavailable")
	}
	switch {
	case args[0] == "redis-cli" && slices.Contains(args, "INFO"):
		return []byte(f.redisInfo), nil
	case args[0] == "psql" && strings.Contains(stdin, "WITH sub AS"):
		return []byte(f.status), nil
	case args[0] == "psql" && strings.Contains(stdin, "pg_terminate_backend"):
		return []byte("t"), nil
	case args[0] == "pg_dump":
		if f.schema != "" {
			return []byte(f.schema), nil
		}
		return []byte("CREATE TABLE t ();\n"), nil
	}
	return nil, nil
}

func (f *fakeEngine) FenceEngine(context.Context, uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fenced++
	return nil
}

func (f *fakeEngine) ran(fragment string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.steps, func(step string) bool { return strings.Contains(step, fragment) })
}

func databaseExecutor(t *testing.T, table *replication.Table, now func() (time.Duration, error)) (*Executor, context.Context) {
	t.Helper()
	identity := journal.Identity{InstallationID: uuid.NewString(), NodeID: uuid.NewString(), RecoveryGeneration: uuid.NewString()}
	j, err := journal.Open(filepath.Join(t.TempDir(), "journal"), journal.Options{Identity: identity, AllowCreate: true, EmptyInventory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	e := &Executor{journal: j, options: Options{AppsDir: t.TempDir(), Replication: table, ReplicaStatus: &replication.Status{}, BootTime: now}}
	session, _ := localSession(t, e, replication.Capability)
	return e, session
}

func pushDatabaseSecrets(t *testing.T, e *Executor, session context.Context, appID string) {
	t.Helper()
	for _, purpose := range []string{"admin", "replication", "app"} {
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: &agentv1.SecretMaterial{CredentialId: uuid.NewString(), Version: 1, Purpose: "db:" + appID + ":" + purpose, Material: []byte(purpose + "-" + strings.Repeat("x", 20))}}}
		if _, err := e.Handle(session, message); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDatabasePrimaryIsFencedWithoutALease(t *testing.T) {
	appID := uuid.NewString()
	app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{app})
	e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 10 * time.Second, nil })
	engine := &fakeEngine{}
	state := &databaseState{applyErrors: map[string]int64{}}
	if report := e.databaseRound(t.Context(), engine, app, state); report == nil || report.FailureCode != "writer_lease_expired" || engine.fenced != 1 {
		t.Fatalf("fencing needed credentials: %+v", report)
	}
	engine.fenced = 0
	pushDatabaseSecrets(t, e, session, appID)
	report := e.databaseRound(t.Context(), engine, app, state)
	if report == nil || report.State != database.StateFailed || report.FailureCode != "writer_lease_expired" || engine.fenced != 1 || engine.ran("CREATE PUBLICATION") {
		t.Fatalf("unleased primary: %+v fenced %d", report, engine.fenced)
	}
	grant := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: &agentv1.WriterLease{SessionId: e.session.id, AppId: appID, Epoch: 1, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}}}
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	report = e.databaseRound(t.Context(), engine, app, state)
	if report == nil || report.State != database.StateStreaming || report.Sequence == 0 || engine.fenced != 1 || !engine.ran("CREATE PUBLICATION synod_pub") || !engine.ran("pg_drop_replication_slot") {
		t.Fatalf("leased primary: %+v", report)
	}
	if engine.ran("pg_subscription") {
		t.Fatal("initial setup tried to promote a database that does not exist")
	}
	app.Epoch = 2
	table.Set([]*agentv1.AppReplication{app})
	grant.GetWriterLease().Epoch = 2
	if _, err := e.Handle(session, grant); err != nil {
		t.Fatal(err)
	}
	report = e.databaseRound(t.Context(), engine, app, &databaseState{applyErrors: map[string]int64{}})
	if report == nil || report.State != database.StateStreaming || !engine.ran("pg_subscription") {
		t.Fatalf("later epoch did not promote the standby: %+v", report)
	}
}

func TestDatabaseStandbyCopiesThenStreams(t *testing.T) {
	appID := uuid.NewString()
	app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}, PrimaryHost: "192.0.2.10", PrimaryPort: 25100}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{app})
	e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
	pushDatabaseSecrets(t, e, session, appID)
	// No subscription exists yet.
	engine := &fakeEngine{status: "0|0|0|-1|0"}
	state := &databaseState{applyErrors: map[string]int64{}}
	report := e.databaseRound(t.Context(), engine, app, state)
	if report == nil || report.State != database.StateCopying || report.Sequence != 0 || !engine.ran("CREATE SUBSCRIPTION") || !engine.ran("host=192.0.2.10 port=25100") || engine.fenced != 0 {
		t.Fatalf("first copy: %+v", report)
	}
	engine.status = "1|2|0|0.5|0"
	report = e.databaseRound(t.Context(), engine, app, state)
	if report == nil || report.State != database.StateStreaming || report.Sequence == 0 || report.LagSeconds != 0.5 || report.FailureCode != "" {
		t.Fatalf("streaming: %+v", report)
	}
	// New apply errors mean the subscription stalled, so the standby copies again.
	engine.steps = nil
	engine.status = "1|2|0|3|4"
	report = e.databaseRound(t.Context(), engine, app, state)
	if report == nil || report.State != database.StateCopying || !engine.ran("CREATE SUBSCRIPTION") {
		t.Fatalf("stalled subscription: %+v", report)
	}
}

func TestReplicationTableAcceptsDatabaseAssignments(t *testing.T) {
	app := func(role, host string, port uint32) *agentv1.AppReplication {
		return &agentv1.AppReplication{AppId: uuid.NewString(), Epoch: 1, Role: role, Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}, PrimaryHost: host, PrimaryPort: port}}
	}
	if err := replication.Validate([]*agentv1.AppReplication{app(replication.RoleActive, "", 0), app(replication.RoleStandby, "192.0.2.10", 25100)}); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]*agentv1.AppReplication{
		"primary with an upstream":  app(replication.RoleActive, "192.0.2.10", 25100),
		"standby without a primary": app(replication.RoleStandby, "", 0),
		"snapshot peers": func() *agentv1.AppReplication {
			a := app(replication.RoleActive, "", 0)
			a.Peers = []*agentv1.ReplicaPeer{{NodeId: uuid.NewString(), Address: "192.0.2.1:1"}}
			return a
		}(),
		"unknown engine": func() *agentv1.AppReplication {
			a := app(replication.RoleActive, "", 0)
			a.Database.Engine = "mysql"
			return a
		}(),
		"redis database names": func() *agentv1.AppReplication {
			a := app(replication.RoleActive, "", 0)
			a.Database.Engine = "redis"
			return a
		}(),
		"bad database name": func() *agentv1.AppReplication {
			a := app(replication.RoleActive, "", 0)
			a.Database.Databases = []string{"x;drop"}
			return a
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := replication.Validate([]*agentv1.AppReplication{bad}); err == nil {
				t.Fatal("invalid database assignment accepted")
			}
		})
	}
}

func TestDatabaseStandbySurvivesUpstreamLoss(t *testing.T) {
	for _, kind := range []string{"postgres", "redis"} {
		t.Run(kind, func(t *testing.T) {
			appID := uuid.NewString()
			app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: kind, PrimaryHost: "192.0.2.10", PrimaryPort: 25100}}
			if kind == "postgres" {
				app.Database.Databases = []string{"photos"}
			}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{app})
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			pushDatabaseSecrets(t, e, session, appID)
			engine := &fakeEngine{status: "1|2|0|-1|1", redisInfo: "role:slave\nmaster_host:192.0.2.10\nmaster_port:25100\nmaster_link_status:down\nmaster_sync_in_progress:0\n"}
			state := &databaseState{applyErrors: map[string]int64{}}
			report := e.databaseRound(t.Context(), engine, app, state)
			if report.FailureCode == "" || report.Sequence != 0 {
				t.Fatalf("a copy with no complete evidence is eligible: %+v", report)
			}
			engine.status = "1|2|0|0.5|0"
			engine.redisInfo = strings.ReplaceAll(engine.redisInfo, "status:down", "status:up") + "master_last_io_seconds_ago:0.5\n"
			report = e.databaseRound(t.Context(), engine, app, state)
			if report.State != database.StateStreaming || report.FailureCode != "" || state.complete == nil {
				t.Fatalf("streaming: %+v", report)
			}
			sequence := report.Sequence
			state.complete.CompletedAt = timestamppb.New(time.Now().Add(-95 * time.Second))
			engine.status = "1|2|0|-1|1"
			engine.redisInfo = strings.ReplaceAll(engine.redisInfo, "status:up", "status:down")
			engine.steps = nil
			for range 2 {
				report = e.databaseRound(t.Context(), engine, app, state)
				if report.State != database.StateDisconnected || report.FailureCode != "" || report.Sequence <= sequence || report.LagSeconds < 95.5 || report.LagSeconds > 100 {
					t.Fatalf("disconnected after failover delay: %+v", report)
				}
			}
			if engine.ran("DROP DATABASE") || engine.ran("CREATE SUBSCRIPTION") {
				t.Fatal("source loss destroyed the complete copy")
			}
			// A new assignment has no proof for this source or epoch.
			report = e.databaseRound(t.Context(), engine, app, &databaseState{applyErrors: map[string]int64{}})
			if report.FailureCode == "" || report.Sequence != 0 {
				t.Fatalf("new assignment reused old evidence: %+v", report)
			}
			// A failed local check revokes the retained evidence.
			engine.failStep = "WITH sub AS"
			if kind == "redis" {
				engine.failStep = "INFO"
			}
			report = e.databaseRound(t.Context(), engine, app, state)
			if report.FailureCode == "" || state.complete != nil {
				t.Fatalf("failed local copy is eligible: %+v", report)
			}
			engine.failStep = ""
			report = e.databaseRound(t.Context(), engine, app, state)
			if report.FailureCode == "" || report.Sequence != 0 {
				t.Fatalf("failed copy restored old evidence: %+v", report)
			}
		})
	}
}

func TestDatabaseDisconnectedStandbyRejectsLocalSchemaChange(t *testing.T) {
	appID := uuid.NewString()
	app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: "postgres", Databases: []string{"photos"}, PrimaryHost: "192.0.2.10", PrimaryPort: 25100}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{app})
	e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
	pushDatabaseSecrets(t, e, session, appID)
	engine := &fakeEngine{status: "1|2|0|0.5|0"}
	state := &databaseState{applyErrors: map[string]int64{}}
	if report := e.databaseRound(t.Context(), engine, app, state); report.State != database.StateStreaming {
		t.Fatalf("streaming: %+v", report)
	}
	engine.status, engine.schema = "1|2|0|-1|0", "CREATE TABLE changed ();"
	report := e.databaseRound(t.Context(), engine, app, state)
	if report.FailureCode == "" || state.complete != nil {
		t.Fatalf("changed local schema is eligible: %+v", report)
	}
}

func TestDatabaseLeaseLossDoesNotNeedSessionCredentials(t *testing.T) {
	appID := uuid.NewString()
	app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{app})
	e, _ := databaseExecutor(t, table, func() (time.Duration, error) { return time.Minute, nil })
	e.session = nil
	e.leases = map[string]writerLease{appID: {epoch: 1, expires: time.Second}}
	native := &fakeEngine{}
	report := e.databaseRound(t.Context(), native, app, &databaseState{})
	if report == nil || report.FailureCode != "writer_lease_expired" || native.fenced != 1 {
		t.Fatalf("expired primary survived session loss: %+v, fences %d", report, native.fenced)
	}
}
