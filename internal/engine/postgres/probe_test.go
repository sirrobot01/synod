package postgres_test

import (
	"context"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/engine/postgres"
)

func TestApplicationProbeRejectsPrivilegeDrift(t *testing.T) {
	endpoint, password := live(t)
	admin, err := pgx.Connect(t.Context(), os.Getenv("SYNOD_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	if _, err := admin.Exec(t.Context(), `CREATE ROLE synod_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, sql := range []string{`DROP DATABASE IF EXISTS app WITH (FORCE)`, `DROP ROLE synod_app`, `GRANT CONNECT,TEMP ON DATABASE postgres,template1 TO PUBLIC`} {
			if _, err := admin.Exec(context.Background(), sql); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := admin.Exec(t.Context(), `CREATE DATABASE app OWNER synod_app`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(t.Context(), `REVOKE CONNECT,TEMP ON DATABASE postgres,template1 FROM PUBLIC`); err != nil {
		t.Fatal(err)
	}
	endpoint.Database, endpoint.Role = "app", "synod_app"
	if observed := postgres.New().Probe(t.Context(), endpoint, password, 1); !observed.Healthy() {
		t.Fatalf("application observation=%+v", observed)
	}
	for _, test := range []struct{ name, change, restore string }{
		{"superuser", `ALTER ROLE synod_app SUPERUSER`, `ALTER ROLE synod_app NOSUPERUSER`},
		{"create role", `ALTER ROLE synod_app CREATEROLE`, `ALTER ROLE synod_app NOCREATEROLE`},
		{"create database", `ALTER ROLE synod_app CREATEDB`, `ALTER ROLE synod_app NOCREATEDB`},
		{"replication", `ALTER ROLE synod_app REPLICATION`, `ALTER ROLE synod_app NOREPLICATION`},
		{"bypass row security", `ALTER ROLE synod_app BYPASSRLS`, `ALTER ROLE synod_app NOBYPASSRLS`},
		{"server file access", `GRANT pg_read_server_files TO synod_app`, `REVOKE pg_read_server_files FROM synod_app`},
		{"other database access", `GRANT CONNECT ON DATABASE postgres TO synod_app`, `REVOKE CONNECT ON DATABASE postgres FROM synod_app`},
		{"other database ownership", `CREATE DATABASE other OWNER synod_app`, `DROP DATABASE other`},
		{"database ownership", `ALTER DATABASE app OWNER TO postgres`, `ALTER DATABASE app OWNER TO synod_app`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := admin.Exec(t.Context(), test.change); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := admin.Exec(context.Background(), test.restore); err != nil {
					t.Fatal(err)
				}
			}()
			observed := postgres.New().Probe(t.Context(), endpoint, password, 1)
			if observed.Healthy() || observed.Endpoint.Reason != "application_privileges_invalid" {
				t.Fatalf("privilege drift observation=%+v", observed)
			}
		})
	}
}

// live points the probe at the real PostgreSQL fixture the scripts start.
func live(t *testing.T) (engine.Endpoint, []byte) {
	t.Helper()
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	if password == "" {
		// The fixture uses trust authentication, which accepts any password. A
		// value is still supplied because an empty one means "nobody can ask",
		// which is a different thing this probe reports differently.
		password = "accepted-under-trust"
	}
	return engine.Endpoint{PostgresRole: "primary",
		Address: host, Port: uint16(number),
		Database: parsed.Path[1:], Role: parsed.User.Username(),
	}, []byte(password)
}

// A real database answering through its published endpoint is healthy, and every
// probe says why separately.
func TestProbeReportsARealDatabaseHealthy(t *testing.T) {
	endpoint, password := live(t)
	observed := postgres.New().Probe(t.Context(), endpoint, password, 7)
	if !observed.Healthy() {
		t.Fatalf("observation=%+v, want a live database healthy", observed)
	}
	if observed.ObservedRevision != 7 || observed.ObservedAt.IsZero() {
		t.Fatalf("observation=%+v, want the revision and freshness it was taken at", observed)
	}
}

// A container that runs but whose database never answers is the failure this check
// exists to catch. Reachability and liveness pass; readiness does not.
func TestProbeSeparatesAListeningPortFromAReadyDatabase(t *testing.T) {
	// A socket that accepts connections and speaks nothing.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	address := listener.Addr().(*net.TCPAddr)
	endpoint := engine.Endpoint{PostgresRole: "primary",
		Address: "127.0.0.1", Port: uint16(address.Port), Database: "app", Role: "synod_admin",
	}
	observed := postgres.New().Probe(t.Context(), endpoint, []byte("password"), 1)
	if observed.Healthy() {
		t.Fatal("a port with no database behind it was reported healthy")
	}
	if observed.Reachable.State != engine.ProbePassed || observed.ProcessLive.State != engine.ProbePassed {
		t.Fatalf("observation=%+v, want reachability and liveness to pass", observed)
	}
	if observed.Ready.State != engine.ProbeFailed {
		t.Fatalf("readiness=%+v, want it to fail where the others passed", observed.Ready)
	}
}

// Nothing listening means nothing further can be told, so the later probes stay
// unavailable rather than being reported as failures of their own.
func TestProbeLeavesLaterChecksUnavailableWhenNothingListens(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	endpoint := engine.Endpoint{PostgresRole: "primary", Address: "127.0.0.1", Port: uint16(port), Database: "app", Role: "synod_admin"}
	observed := postgres.New().Probe(t.Context(), endpoint, []byte("password"), 1)
	if observed.Reachable.State != engine.ProbeFailed {
		t.Fatalf("reachable=%+v, want a refused connection", observed.Reachable)
	}
	for name, probe := range map[string]engine.Probe{
		"liveness": observed.ProcessLive, "readiness": observed.Ready, "endpoint": observed.Endpoint,
	} {
		if probe.State != engine.ProbeUnavailable {
			t.Fatalf("%s=%+v, want it unavailable rather than failed", name, probe)
		}
	}
}

// Without a credential the database cannot be asked anything, which is an unavailable
// probe and not a failing database.
func TestProbeWithoutACredentialIsUnavailableNotFailed(t *testing.T) {
	endpoint, _ := live(t)
	observed := postgres.New().Probe(t.Context(), endpoint, nil, 1)
	if observed.Reachable.State != engine.ProbePassed {
		t.Fatalf("reachable=%+v, want the fixture reachable", observed.Reachable)
	}
	if observed.Ready.State != engine.ProbeUnavailable {
		t.Fatalf("readiness=%+v, want it unavailable without a credential", observed.Ready)
	}
	if observed.Healthy() {
		t.Fatal("an engine nobody could ask was reported healthy")
	}
}

// A probe reason is a stable code and never carries a credential or a host path.
func TestProbeReasonsCarryNoDetail(t *testing.T) {
	endpoint := engine.Endpoint{PostgresRole: "primary", Address: "127.0.0.1", Port: 1, Database: "app", Role: "someone"}
	observed := postgres.New().Probe(t.Context(), endpoint, []byte("a-secret-password"), 1)
	for _, probe := range []engine.Probe{observed.Reachable, observed.ProcessLive, observed.Ready, observed.Endpoint} {
		if probe.Reason == "" {
			continue
		}
		for _, leak := range []string{"a-secret-password", "127.0.0.1", "someone"} {
			if strings.Contains(probe.Reason, leak) {
				t.Fatalf("reason %q carries %q", probe.Reason, leak)
			}
		}
	}
}

func TestProbeUsesDeclaredModeDespiteAmbientSettings(t *testing.T) {
	endpoint, password := live(t)
	for name, value := range map[string]string{
		"PGHOST": "192.0.2.250,192.0.2.251", "PGPORT": "1", "PGUSER": "wrong_user", "PGDATABASE": "missing_database",
		"PGPASSWORD": "wrong_password", "PGSSLMODE": "require", "PGSSLCERT": "/missing/client.crt", "PGSSLKEY": "/missing/client.key",
		"PGSSLROOTCERT": "/missing/ca.crt", "PGTARGETSESSIONATTRS": "read-only", "PGOPTIONS": "-c default_transaction_read_only=on", "PGSSLNEGOTIATION": "direct",
	} {
		t.Setenv(name, value)
	}
	if observed := postgres.New().Probe(t.Context(), endpoint, password, 1); !observed.Healthy() {
		t.Fatalf("ambient settings replaced declared probe: %+v", observed)
	}
}
