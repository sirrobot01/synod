package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type databaseFenceFixture struct {
	fakeEngine
	apps []uuid.UUID
}

func (f *databaseFenceFixture) EngineApps(context.Context) ([]uuid.UUID, error) { return f.apps, nil }

func TestDatabaseFencingReadsNativeInventoryWithoutCredentials(t *testing.T) {
	for _, scenario := range []string{"unassigned", "expired", "leased", "standby", "former_primary", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			id := uuid.New()
			assignment := &agentv1.AppReplication{AppId: id.String(), Epoch: 2, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
			if scenario == "standby" || scenario == "former_primary" {
				assignment.Role = replication.RoleStandby
			}
			table := &replication.Table{}
			if scenario != "unassigned" {
				table.Set([]*agentv1.AppReplication{assignment})
			}
			e, _ := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			e.session = nil
			if scenario == "leased" || scenario == "shutdown" {
				e.leases = map[string]writerLease{id.String(): {epoch: 2, expires: time.Minute}}
			}
			if scenario == "former_primary" {
				if err := os.MkdirAll(filepath.Join(e.options.AppsDir, id.String()), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(e.options.AppsDir, id.String(), "database-primary-epoch"), []byte("1"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			native := &databaseFenceFixture{apps: []uuid.UUID{id}}
			if err := e.fenceDatabaseWriters(t.Context(), native, scenario == "shutdown"); err != nil {
				t.Fatal(err)
			}
			want := 1
			if scenario == "leased" || scenario == "standby" {
				want = 0
			}
			if native.fenced != want || len(native.steps) != 0 {
				t.Fatalf("fences=%d, want=%d, SQL steps=%v", native.fenced, want, native.steps)
			}
		})
	}
}

func TestFormerDatabasePrimaryNeedsARebuild(t *testing.T) {
	id := uuid.NewString()
	assignment := &agentv1.AppReplication{AppId: id, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{assignment})
	e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
	pushDatabaseSecrets(t, e, session, id)
	e.leases = map[string]writerLease{id: {epoch: 1, expires: time.Minute}}
	native := &fakeEngine{redisInfo: "role:master\n"}
	report := e.databaseRound(t.Context(), native, assignment, &databaseState{})
	if report == nil || report.FailureCode != "" {
		t.Fatalf("primary setup: %+v", report)
	}
	data, err := os.ReadFile(filepath.Join(e.options.AppsDir, id, "database-primary-epoch"))
	if err != nil || string(data) != "1" {
		t.Fatalf("primary marker: %q %v", data, err)
	}
	assignment.Role, assignment.Epoch = replication.RoleStandby, 2
	table.Set([]*agentv1.AppReplication{assignment})
	native.steps = nil
	report = e.databaseRound(t.Context(), native, assignment, &databaseState{})
	if report == nil || report.FailureCode == "" || len(native.steps) != 0 {
		t.Fatalf("old primary ran before rebuild: %+v, steps %v", report, native.steps)
	}
}

func TestDatabasePrimaryRechecksLeaseAfterDataLock(t *testing.T) {
	id := uuid.NewString()
	assignment := &agentv1.AppReplication{AppId: id, Epoch: 1, Role: replication.RoleActive, Database: &agentv1.DatabaseAssignment{Engine: "redis"}}
	table := &replication.Table{}
	table.Set([]*agentv1.AppReplication{assignment})
	checked := make(chan struct{}, 1)
	e, session := databaseExecutor(t, table, func() (time.Duration, error) {
		select {
		case checked <- struct{}{}:
		default:
		}
		return 0, nil
	})
	pushDatabaseSecrets(t, e, session, id)
	e.leases = map[string]writerLease{id: {epoch: 1, expires: time.Minute}}
	unlock, err := table.LockApp(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	native := &fakeEngine{}
	done := make(chan *agentv1.ReplicaStatus, 1)
	go func() { done <- e.databaseRound(t.Context(), native, assignment, &databaseState{}) }()
	<-checked
	table.Set(nil)
	unlock()
	if report := <-done; report != nil || len(native.steps) != 0 {
		t.Fatalf("withdrawn primary changed data: %+v, %v", report, native.steps)
	}
	if _, err := os.Stat(filepath.Join(e.options.AppsDir, id, "database-primary-epoch")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("primary marker written after withdrawal: %v", err)
	}
}
