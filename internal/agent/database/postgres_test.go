package database

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/database/enginefixture"
)

func query(t *testing.T, x Exec, database, sql string) string {
	t.Helper()
	out, err := psql(t.Context(), x, database, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return strings.TrimSpace(string(out))
}

// appQuery runs SQL as the application role over TLS, as a client does.
func appQuery(ctx context.Context, x Exec, config Postgres, database, sql string) (string, error) {
	out, err := x(ctx, []string{"PGPASSWORD=" + config.AppPassword, "PGSSLMODE=verify-ca", "PGSSLROOTCERT=/tls/ca.crt", "PGCHANNELBINDING=require"}, sql, "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At", "-h", "127.0.0.1", "-U", "app", "-d", database, "-f", "-")
	return strings.TrimSpace(string(out)), err
}

func TestPostgresReplicatesAcrossSchemaChangesAndPromotes(t *testing.T) {
	if os.Getenv("SYNOD_TEST_DOCKER_HOST") == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated Docker fixture")
	}
	network := "synod-database-" + uuid.NewString()[:8]
	if _, err := enginefixture.Docker(t, "", "network", "create", network); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = enginefixture.Docker(t, "", "network", "rm", network) })
	tls := enginefixture.TLS(t)
	passwords := func() (string, string, string) {
		return "admin-" + uuid.NewString(), "replica-" + uuid.NewString(), "app-" + uuid.NewString()
	}
	admin, replica, application := passwords()
	primaryName, standbyName := network+"-a", network+"-b"
	primaryConfig := Postgres{Node: uuid.New(), Databases: []string{"photos", "notes"}, AdminPassword: admin, ReplicationPassword: replica, AppPassword: application}
	standbyConfig := primaryConfig
	standbyConfig.Node = uuid.New()
	primary := enginefixture.StartPostgres(t, network, primaryName, tls, primaryConfig.AdminPassword)
	standby := enginefixture.StartPostgres(t, network, standbyName, tls, standbyConfig.AdminPassword)
	ctx := t.Context()
	if status, err := standbyConfig.Status(ctx, standby, "photos"); err != nil || status.State != StateFailed {
		t.Fatalf("missing database must request its first copy: %+v %v", status, err)
	}

	if err := primaryConfig.EnsurePrimary(ctx, primary); err != nil {
		t.Fatal(err)
	}
	// A repeat changes nothing.
	if err := primaryConfig.EnsurePrimary(ctx, primary); err != nil {
		t.Fatal(err)
	}
	// The app role creates a table without a primary key and updates it at once.
	if _, err := appQuery(ctx, primary, primaryConfig, "photos", "CREATE TABLE assets (id bigserial PRIMARY KEY, name text); CREATE TABLE tags (asset bigint, tag text); INSERT INTO assets (name) SELECT 'a' || g FROM generate_series(1, 500) g; INSERT INTO tags VALUES (1, 'x'); UPDATE tags SET tag = 'y';"); err != nil {
		t.Fatalf("app writes on the primary: %v", err)
	}

	endpoint := Endpoint{Host: primaryName, Port: 5432, CAFile: "/tls/ca.crt"}
	for _, database := range standbyConfig.Databases {
		if err := standbyConfig.Copy(ctx, standby, endpoint, database); err != nil {
			t.Fatal(err)
		}
	}
	streaming := func(database string) func() error {
		return func() error {
			status, err := standbyConfig.Status(ctx, standby, database)
			if err != nil {
				return err
			}
			if status.State != StateStreaming {
				return fmt.Errorf("state %s", status.State)
			}
			return nil
		}
	}
	for _, database := range standbyConfig.Databases {
		enginefixture.Wait(t, 60*time.Second, streaming(database))
	}
	if got := query(t, standby, "photos", "SELECT count(*) FROM assets; SELECT tag FROM tags;"); got != "500\ny" {
		t.Fatalf("first copy: %q", got)
	}
	if _, err := appQuery(ctx, standby, standbyConfig, "photos", "INSERT INTO assets (name) VALUES ('client')"); err == nil {
		t.Fatal("a client wrote to the standby")
	}
	// A slow apply worker does not change the answer, so the test waits for each change.
	if _, err := appQuery(ctx, primary, primaryConfig, "photos", "DELETE FROM tags; INSERT INTO assets (name) VALUES ('live')"); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 20*time.Second, func() error {
		if got := query(t, standby, "photos", "SELECT count(*) FROM assets; SELECT count(*) FROM tags;"); got != "501\n0" {
			return fmt.Errorf("live change: %q", got)
		}
		return nil
	})

	// A schema change stalls the subscription and changes the hash. A re-copy repairs both.
	primaryHash, err := standbyConfig.PrimarySchemaHash(ctx, standby, endpoint, "photos")
	if err != nil {
		t.Fatal(err)
	}
	if local, err := standbyConfig.LocalSchemaHash(ctx, standby, "photos"); err != nil || local != primaryHash {
		t.Fatalf("hashes differ before the change: %s %s %v", local, primaryHash, err)
	}
	if _, err := appQuery(ctx, primary, primaryConfig, "photos", "ALTER TABLE assets ADD COLUMN size integer; CREATE TABLE albums (title text); INSERT INTO assets (name, size) VALUES ('sized', 7); INSERT INTO albums VALUES ('trip'); UPDATE albums SET title = 'holiday';"); err != nil {
		t.Fatalf("migration on the primary: %v", err)
	}
	primaryHash, err = standbyConfig.PrimarySchemaHash(ctx, standby, endpoint, "photos")
	if err != nil {
		t.Fatal(err)
	}
	if local, err := standbyConfig.LocalSchemaHash(ctx, standby, "photos"); err != nil || local == primaryHash {
		t.Fatalf("drift not detected: %v", err)
	}
	if err := standbyConfig.Copy(ctx, standby, endpoint, "photos"); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 60*time.Second, streaming("photos"))
	if local, err := standbyConfig.LocalSchemaHash(ctx, standby, "photos"); err != nil || local != primaryHash {
		t.Fatalf("hashes differ after the re-copy: %v", err)
	}
	if got := query(t, standby, "photos", "SELECT count(*) FROM assets WHERE size = 7; SELECT title FROM albums;"); got != "1\nholiday" {
		t.Fatalf("re-copy: %q", got)
	}
	if err := DropStaleSyncSlots(ctx, primary); err != nil {
		t.Fatal(err)
	}

	// Sequences run ahead on the standby. After promotion no id is reused.
	if err := standbyConfig.SyncSequences(ctx, standby, endpoint, "photos"); err != nil {
		t.Fatal(err)
	}
	last := query(t, primary, "photos", "SELECT max(id) FROM assets")
	if _, err := appQuery(ctx, primary, primaryConfig, "photos", "INSERT INTO assets (name) SELECT 'late' || g FROM generate_series(1, 20) g"); err != nil {
		t.Fatal(err)
	}
	issued := query(t, primary, "photos", "SELECT max(id) FROM assets")
	if _, err := enginefixture.Docker(t, "", "stop", "-t", "5", primaryName); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 10*time.Second, func() error {
		status, err := standbyConfig.Status(ctx, standby, "photos")
		if err != nil || status.State != "disconnected" {
			return fmt.Errorf("complete standby after upstream loss: %+v %v", status, err)
		}
		return nil
	})
	if err := standbyConfig.Promote(ctx, standby); err != nil {
		t.Fatalf("promote without the primary: %v", err)
	}
	next, err := appQuery(ctx, standby, standbyConfig, "photos", "INSERT INTO assets (name) VALUES ('after') RETURNING id")
	if err != nil {
		t.Fatalf("write after promotion: %v", err)
	}
	var nextID, issuedID, lastID int
	fmt.Sscan(next, &nextID)
	fmt.Sscan(issued, &issuedID)
	fmt.Sscan(last, &lastID)
	if nextID <= issuedID || nextID <= lastID {
		t.Fatalf("id %d after promotion reuses a value up to %d", nextID, issuedID)
	}
	if got := query(t, standby, "notes", "SELECT count(*) FROM pg_subscription"); got != "0" {
		t.Fatalf("subscriptions remain after promotion: %s", got)
	}
	if got := query(t, standby, "notes", "SELECT count(*) FROM pg_publication WHERE pubname = 'synod_pub'"); got != "1" {
		t.Fatalf("the new primary publishes nothing: %s", got)
	}

	// The old primary returns and rejoins as a standby of the new one.
	if _, err := enginefixture.Docker(t, "", "start", primaryName); err != nil {
		t.Fatal(err)
	}
	enginefixture.Wait(t, 60*time.Second, func() error {
		_, err := primary(ctx, nil, "", "pg_isready", "-U", "synod_admin", "-h", "127.0.0.1")
		return err
	})
	rejoin := primaryConfig
	back := Endpoint{Host: standbyName, Port: 5432, CAFile: "/tls/ca.crt"}
	for _, database := range rejoin.Databases {
		if err := rejoin.Copy(ctx, primary, back, database); err != nil {
			t.Fatal(err)
		}
	}
	enginefixture.Wait(t, 60*time.Second, func() error {
		status, err := rejoin.Status(ctx, primary, "photos")
		if err != nil || status.State != StateStreaming {
			return fmt.Errorf("rejoin: %+v %v", status, err)
		}
		return nil
	})
	if got := query(t, primary, "photos", "SELECT count(*) FROM assets WHERE name = 'after'"); got != "1" {
		t.Fatalf("rejoined copy lacks the new primary's write: %s", got)
	}
}
