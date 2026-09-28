package executor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database"
	"github.com/sirrobot01/synod/internal/agent/database/enginefixture"
	"github.com/sirrobot01/synod/internal/agent/replication"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type databaseTestEngine struct {
	exec database.Exec
}

func (d databaseTestEngine) EngineExec(ctx context.Context, _ uuid.UUID, env []string, stdin string, args ...string) ([]byte, error) {
	return d.exec(ctx, env, stdin, args...)
}

func (d databaseTestEngine) FenceEngine(context.Context, uuid.UUID) error {
	return fmt.Errorf("unexpected fence of the standby")
}

func TestDatabaseSupervisorPromotesAfterUpstreamLoss(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	for _, kind := range []string{"postgres", "redis"} {
		t.Run(kind, func(t *testing.T) {
			network := "synod-supervisor-" + uuid.NewString()[:8]
			if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
			certificates := enginefixture.TLS(t)
			primaryName, standbyName := network+"-a", network+"-b"
			appID := uuid.NewString()
			app := &agentv1.AppReplication{AppId: appID, Epoch: 1, Role: replication.RoleStandby, Database: &agentv1.DatabaseAssignment{Engine: kind, PrimaryHost: primaryName}}
			if kind == "postgres" {
				app.Database.Databases, app.Database.PrimaryPort = []string{"photos", "notes"}, 5432
			} else {
				app.Database.PrimaryPort = 6379
			}
			table := &replication.Table{}
			table.Set([]*agentv1.AppReplication{app})
			e, session := databaseExecutor(t, table, func() (time.Duration, error) { return 0, nil })
			pushDatabaseSecrets(t, e, session, appID)
			pg, redis, err := e.databaseCredentials(app)
			if err != nil {
				t.Fatal(err)
			}
			var primary, standby database.Exec
			var env, command []string
			var write, read, after string
			if kind == "postgres" {
				primary = enginefixture.StartPostgres(t, network, primaryName, certificates, pg.AdminPassword)
				standby = enginefixture.StartPostgres(t, network, standbyName, certificates, pg.AdminPassword)
				if err := pg.EnsurePrimary(t.Context(), primary); err != nil {
					t.Fatal(err)
				}
				command = []string{"psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-"}
				write = "CREATE TABLE proof (id bigserial PRIMARY KEY, value text); INSERT INTO proof(value) VALUES ('retained');"
				read = "SELECT value FROM proof WHERE id=1;"
				after = "INSERT INTO proof(value) VALUES ('after'); SELECT count(*) FROM proof;"
			} else {
				primary = enginefixture.StartRedis(t, network, primaryName, certificates, redis.AdminPassword, redis.ReplicationPassword, database.RedisSocket)
				standby = enginefixture.StartRedis(t, network, standbyName, certificates, redis.AdminPassword, redis.ReplicationPassword, database.RedisSocket)
				env = []string{"REDISCLI_AUTH=" + redis.AdminPassword}
				command = []string{"redis-cli", "--raw", "-s", database.RedisSocket}
				write, read, after = "SET proof retained\n", "GET proof\n", "SET after yes\nGET after\n"
			}
			if _, err := primary(t.Context(), env, write, command...); err != nil {
				t.Fatal(err)
			}
			native := databaseTestEngine{exec: standby}
			state := &databaseState{applyErrors: map[string]int64{}}
			enginefixture.Wait(t, time.Minute, func() error {
				report := e.databaseRound(t.Context(), native, app, state)
				if report == nil || report.State != database.StateStreaming || report.FailureCode != "" {
					return fmt.Errorf("standby setup: %+v", report)
				}
				out, err := standby(t.Context(), env, read, command...)
				if err != nil || !strings.Contains(string(out), "retained") {
					return fmt.Errorf("replicated data: %q %v", out, err)
				}
				return nil
			})
			if _, err := enginefixture.Docker(t, "", "stop", "-t", "3", primaryName); err != nil {
				t.Fatal(err)
			}
			// The configured minimum failover delay is 60 seconds.
			for deadline := time.Now().Add(61 * time.Second); time.Now().Before(deadline); {
				time.Sleep(5 * time.Second)
				report := e.databaseRound(t.Context(), native, app, state)
				if report == nil || report.State != database.StateDisconnected || report.FailureCode != "" || report.Sequence == 0 || report.LagSeconds < 5 {
					t.Fatalf("complete standby lost during outage: %+v", report)
				}
			}
			app.Epoch, app.Role = 2, replication.RoleActive
			app.Database.PrimaryHost, app.Database.PrimaryPort = "", 0
			table.Set([]*agentv1.AppReplication{app})
			grant := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_WriterLease{WriterLease: &agentv1.WriterLease{SessionId: e.session.id, AppId: appID, Epoch: 2, Seconds: 30, ExpiresAtBootNanos: uint64(30 * time.Second)}}}
			if _, err := e.Handle(session, grant); err != nil {
				t.Fatal(err)
			}
			report := e.databaseRound(t.Context(), native, app, &databaseState{applyErrors: map[string]int64{}})
			if report == nil || report.State != database.StateStreaming || report.FailureCode != "" {
				t.Fatalf("promotion: %+v", report)
			}
			out, err := standby(t.Context(), env, read+after, command...)
			if err != nil || !strings.Contains(string(out), "retained") || (kind == "postgres" && !strings.Contains(string(out), "2")) || (kind == "redis" && !strings.Contains(string(out), "yes")) {
				t.Fatalf("promoted writes and retained data: %q %v", out, err)
			}
		})
	}
}
