package database

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database/enginefixture"
)

func TestPostgresWritePauseCatchupAndResume(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	network := "synod-pg-switch-" + uuid.NewString()[:8]
	if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
	certificates := enginefixture.TLS(t)
	config := Postgres{Node: uuid.New(), Databases: []string{"photos", "notes"}, AdminPassword: "admin-" + uuid.NewString(), ReplicationPassword: "replica-" + uuid.NewString(), AppPassword: "app-" + uuid.NewString()}
	primaryName, standbyName := network+"-a", network+"-b"
	primary := enginefixture.StartPostgres(t, network, primaryName, certificates, config.AdminPassword)
	standby := enginefixture.StartPostgres(t, network, standbyName, certificates, config.AdminPassword)
	if err := config.EnsurePrimary(t.Context(), primary); err != nil {
		t.Fatal(err)
	}
	endpoint := Endpoint{Host: primaryName, Port: 5432, CAFile: "/tls/ca.crt"}
	for _, name := range config.Databases {
		if _, err := appQuery(t.Context(), primary, config, name, "CREATE TABLE proof (id integer PRIMARY KEY); INSERT INTO proof VALUES (1);"); err != nil {
			t.Fatal(err)
		}
		if err := config.Copy(t.Context(), standby, endpoint, name); err != nil {
			t.Fatal(err)
		}
		enginefixture.Wait(t, time.Minute, func() error {
			status, err := config.Status(t.Context(), standby, name)
			if err != nil || status.State != StateStreaming {
				return fmt.Errorf("copy: %+v %v", status, err)
			}
			return nil
		})
	}
	// A transaction opened before the pause must end without committing its write.
	client := make(chan error, 1)
	go func() {
		_, err := primary(t.Context(), []string{"PGAPPNAME=old-client"}, "BEGIN; INSERT INTO proof VALUES (2); SELECT pg_sleep(30); COMMIT;", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-")
		client <- err
	}()
	enginefixture.Wait(t, 5*time.Second, func() error {
		if query(t, primary, "postgres", "SELECT count(*) FROM pg_stat_activity WHERE application_name='old-client' AND wait_event='PgSleep'") != "1" {
			return errors.New("client has not started its transaction")
		}
		return nil
	})
	positions, err := config.PauseWrites(t.Context(), primary)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-client:
		if err == nil {
			t.Fatal("the old client committed after the pause")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the old client remained connected")
	}
	for _, name := range config.Databases {
		if _, err := appQuery(t.Context(), primary, config, name, "INSERT INTO proof VALUES (3)"); err == nil {
			t.Fatal("a new client wrote to a paused database")
		}
	}
	enginefixture.Wait(t, 10*time.Second, func() error {
		ready, err := config.CaughtUp(t.Context(), standby, positions)
		if err != nil || !ready {
			return fmt.Errorf("final positions: %v %v", ready, err)
		}
		return nil
	})
	if got := query(t, standby, "photos", "SELECT count(*) FROM proof"); got != "1" {
		t.Fatalf("uncommitted client data was copied: %s", got)
	}
	// A stopped apply worker blocks switchover even when the other database is ready.
	query(t, standby, "notes", "ALTER SUBSCRIPTION "+config.subscription()+" DISABLE")
	// Connections opened during the pause keep that default until they reconnect.
	go func() {
		_, err := primary(t.Context(), []string{"PGAPPNAME=paused-client"}, "SELECT pg_sleep(30);", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-U", "app", "-d", "photos", "-f", "-")
		client <- err
	}()
	enginefixture.Wait(t, 5*time.Second, func() error {
		if query(t, primary, "postgres", "SELECT count(*) FROM pg_stat_activity WHERE application_name='paused-client' AND wait_event='PgSleep'") != "1" {
			return errors.New("read-only client has not connected")
		}
		return nil
	})
	if err := config.ResumeWrites(t.Context(), primary); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-client:
		if err == nil {
			t.Fatal("rollback kept a read-only client")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rollback did not end the read-only client")
	}
	if _, err := appQuery(t.Context(), primary, config, "notes", "INSERT INTO proof VALUES (4)"); err != nil {
		t.Fatalf("write after rollback: %v", err)
	}
	positions, err = config.PauseWrites(t.Context(), primary)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := config.CaughtUp(t.Context(), standby, positions); err != nil || ready {
		t.Fatalf("a stopped subscription passed catch-up: %v %v", ready, err)
	}
	query(t, standby, "notes", "ALTER SUBSCRIPTION "+config.subscription()+" ENABLE")
	enginefixture.Wait(t, 10*time.Second, func() error {
		ready, err := config.CaughtUp(t.Context(), standby, positions)
		if err != nil || !ready {
			return fmt.Errorf("resumed apply worker: %v %v", ready, err)
		}
		return nil
	})
	// Matching WAL progress cannot hide a schema difference.
	query(t, standby, "photos", "CREATE TABLE local_only (id integer)")
	if ready, err := config.CaughtUp(t.Context(), standby, positions); err != nil || ready {
		t.Fatalf("a changed schema passed catch-up: %v %v", ready, err)
	}
	query(t, standby, "photos", "DROP TABLE local_only")
	if _, err := enginefixture.Docker(t, "", "stop", "-t", "3", primaryName); err != nil {
		t.Fatal(err)
	}
	if err := config.Promote(t.Context(), standby); err != nil {
		t.Fatal(err)
	}
	if got, err := appQuery(t.Context(), standby, config, "notes", "INSERT INTO proof VALUES (5); SELECT count(*) FROM proof"); err != nil || got != "3" {
		t.Fatalf("promoted data: %q %v", got, err)
	}
}

func TestRedisWritePauseCatchupAndResume(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	network := "synod-redis-switch-" + uuid.NewString()[:8]
	if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
	certificates := enginefixture.TLS(t)
	config := Redis{AdminPassword: "admin-" + uuid.NewString(), ReplicationPassword: "replica-" + uuid.NewString()}
	primaryName, standbyName := network+"-a", network+"-b"
	primary := enginefixture.StartRedis(t, network, primaryName, certificates, config.AdminPassword, config.ReplicationPassword, RedisSocket)
	standby := enginefixture.StartRedis(t, network, standbyName, certificates, config.AdminPassword, config.ReplicationPassword, RedisSocket)
	if _, err := config.cli(t.Context(), primary, "", "SET", "proof", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := config.EnsureReplica(t.Context(), standby, Endpoint{Host: primaryName, Port: 6379}); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, time.Minute, func() error {
		status, err := config.Status(t.Context(), standby)
		if err != nil || status.State != StateStreaming {
			return fmt.Errorf("replica: %+v %v", status, err)
		}
		return nil
	})
	position, err := config.PauseWrites(t.Context(), primary, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 5*time.Second, func() error {
		ready, err := config.CaughtUp(t.Context(), standby, position)
		if err != nil || !ready {
			return fmt.Errorf("applied offset: %v %v", ready, err)
		}
		return nil
	})
	other := position
	other.ReplicationID = strings.Repeat("f", 40)
	if ready, err := config.CaughtUp(t.Context(), standby, other); err != nil || ready {
		t.Fatalf("another history passed catch-up: %v %v", ready, err)
	}
	other = position
	other.Offset++
	if ready, err := config.CaughtUp(t.Context(), standby, other); err != nil || ready {
		t.Fatalf("a future offset passed catch-up: %v %v", ready, err)
	}
	writer := make(chan error, 1)
	go func() {
		_, err := config.cli(t.Context(), primary, "", "SET", "blocked", "resumed")
		writer <- err
	}()
	select {
	case err := <-writer:
		t.Fatalf("write was not paused: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := config.ResumeWrites(t.Context(), primary); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-writer:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rollback left the client paused")
	}
	position, err = config.PauseWrites(t.Context(), primary, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 5*time.Second, func() error {
		ready, err := config.CaughtUp(t.Context(), standby, position)
		if err != nil || !ready {
			return fmt.Errorf("final offset: %v %v", ready, err)
		}
		return nil
	})
	if _, err := enginefixture.Docker(t, "", "stop", "-t", "3", primaryName); err != nil {
		t.Fatal(err)
	}
	if err := config.EnsurePrimary(t.Context(), standby); err != nil {
		t.Fatal(err)
	}
	if out, err := config.cli(t.Context(), standby, "", "GET", "blocked"); err != nil || !strings.Contains(string(out), "resumed") {
		t.Fatalf("promoted data: %q %v", out, err)
	}
}
