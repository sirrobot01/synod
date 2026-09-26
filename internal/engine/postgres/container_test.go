package postgres

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type bootstrapFixture struct {
	host, name, address, password, verifier string
	config                                  *agentv1.PostgresContainerConfig
	command                                 []string
}

func bootstrapContainer(t *testing.T, injectedStartup string) *bootstrapFixture {
	t.Helper()
	host := os.Getenv("SYNOD_TEST_DOCKER_HOST")
	if host == "" {
		t.Skip("set SYNOD_TEST_DOCKER_HOST for the isolated PostgreSQL startup fixture")
	}
	f := &bootstrapFixture{host: host, name: "synod-bootstrap-" + uuid.NewString(), password: "application-test-" + uuid.NewString()}
	arch := strings.TrimSpace(f.docker(t, "info", "--format", "{{.Architecture}}"))
	image := ""
	switch arch {
	case "aarch64", "arm64":
		image = "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0"
	case "x86_64", "amd64":
		image = "postgres@sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232"
	default:
		t.Fatalf("unqualified PostgreSQL fixture architecture %q", arch)
	}
	salt := []byte("native-test-salt")
	salted, err := pbkdf2.Key(sha256.New, f.password, salt, 4096, 32)
	if err != nil {
		t.Fatal(err)
	}
	clientKey := hmac.New(sha256.New, salted)
	clientKey.Write([]byte("Client Key"))
	storedKey := sha256.Sum256(clientKey.Sum(nil))
	serverKey := hmac.New(sha256.New, salted)
	serverKey.Write([]byte("Server Key"))
	f.verifier = "SCRAM-SHA-256$4096:" + base64.StdEncoding.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(storedKey[:]) + ":" + base64.StdEncoding.EncodeToString(serverKey.Sum(nil))
	f.config = &agentv1.PostgresContainerConfig{Port: 5432, SuperuserCredentialId: uuid.NewString(), SuperuserCredentialVersion: 1, ApplicationCredentialId: uuid.NewString(), ApplicationCredentialVersion: 1}
	args := []string{"--host", host, "run", "--detach", "--name", f.name, "--label", "io.synod.fixture=postgres-bootstrap",
		"--publish", "127.0.0.1::5432", "--tmpfs", ConfigDirectory + ":rw,noexec,nosuid,nodev,size=1m",
		"--env", "POSTGRES_PASSWORD", "--env", "SYNOD_APP_VERIFIER", image}
	startup := ContainerCommand(f.config, uuid.NewString(), uuid.NewString(), uuid.NewString())
	f.command = startup
	if injectedStartup != "" {
		startup[2] = injectedStartup
	}
	args = append(args, startup...)
	command := exec.CommandContext(t.Context(), "docker", args...)
	command.Env = append(os.Environ(), "POSTGRES_PASSWORD=admin-fixture-password", "SYNOD_APP_VERIFIER="+f.verifier)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start bootstrap fixture: %v (%d output bytes)", err, len(output))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "docker", "--host", host, "rm", "--force", "--volumes", f.name).CombinedOutput(); err != nil {
			t.Errorf("remove bootstrap fixture: %v (%d output bytes)", err, len(output))
		}
	})
	f.address = strings.TrimSpace(f.docker(t, "port", f.name, "5432/tcp"))
	if injectedStartup == "" {
		f.waitPhase(t, "ready")
	}
	return f
}

func (f *bootstrapFixture) docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", append([]string{"--host", f.host}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker fixture command failed: %v (%d output bytes)", err, len(output))
	}
	return string(output)
}

func (f *bootstrapFixture) waitPhase(t *testing.T, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		output, err := exec.CommandContext(ctx, "docker", "--host", f.host, "exec", f.name, "cat", BootstrapCompletePath).Output()
		if err == nil && strings.TrimSpace(string(output)) == phase {
			f.address = strings.TrimSpace(f.docker(t, "port", f.name, "5432/tcp"))
			return
		}
		if err == nil && strings.TrimSpace(string(output)) == "failed" {
			t.Fatal("bootstrap failed before becoming ready")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("bootstrap did not reach %s", phase)
		case <-ticker.C:
		}
	}
}

func (f *bootstrapFixture) adminSQL(t *testing.T, query string) string {
	t.Helper()
	return f.docker(t, "exec", "--user", "postgres", f.name, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", ConfigDirectory, "-U", "synod_admin", "-d", "postgres", "-At", "-c", query)
}

func (f *bootstrapFixture) connect(t *testing.T) *pgx.Conn {
	t.Helper()
	host, port, err := net.SplitHostPort(f.address)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig("sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.Host, config.Port = host, uint16(number)
	config.User, config.Password, config.Database = "synod_app", f.password, "app"
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var conn *pgx.Conn
	for ctx.Err() == nil {
		conn, err = pgx.ConnectConfig(ctx, config)
		if err == nil {
			t.Cleanup(func() { conn.Close(context.Background()) })
			return conn
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("application connection did not become ready")
	return nil
}

func TestPostgresBootstrapCreatesLeastPrivilegeApplicationAndPreservesRestart(t *testing.T) {
	f := bootstrapContainer(t, "")
	conn := f.connect(t)
	if _, err := conn.Exec(t.Context(), `CREATE TABLE application_rows(value integer); INSERT INTO application_rows VALUES(90)`); err != nil {
		t.Fatal("application cannot create and write its own table")
	}
	for _, query := range []string{`CREATE ROLE forbidden`, `CREATE DATABASE forbidden`, `SET ROLE synod_admin`, `SELECT pg_read_file('/etc/passwd')`} {
		_, err := conn.Exec(t.Context(), query)
		if failure, ok := errors.AsType[*pgconn.PgError](err); !ok || failure.Code != "42501" {
			t.Fatalf("application administrative action did not return insufficient_privilege: %s", query)
		}
	}
	wrong := conn.Config().Copy()
	wrong.Password = "admin-fixture-password"
	if unexpected, err := pgx.ConnectConfig(t.Context(), wrong); err == nil {
		unexpected.Close(t.Context())
		t.Fatal("administrative password authenticated the application role")
	} else if failure, ok := errors.AsType[*pgconn.PgError](err); !ok || failure.Code != "28P01" {
		t.Fatal("cross-credential login did not return authentication failure")
	}
	var user, database string
	if err := conn.QueryRow(t.Context(), `SELECT current_user,current_database()`).Scan(&user, &database); err != nil || user != "synod_app" || database != "app" {
		t.Fatal("application identity mismatch")
	}
	before := f.adminSQL(t, `SELECT app_oid,database_oid,encode(verifier_fingerprint,'hex'),phase FROM synod_control.bootstrap`)
	conn.Close(t.Context())
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "ready")
	after := f.adminSQL(t, `SELECT app_oid,database_oid,encode(verifier_fingerprint,'hex'),phase FROM synod_control.bootstrap`)
	if before != after {
		t.Fatal("restart changed native identity or credential fingerprint")
	}
	conn = f.connect(t)
	var value int
	if err := conn.QueryRow(t.Context(), `SELECT value FROM application_rows`).Scan(&value); err != nil || value != 90 {
		t.Fatal("restart did not preserve application data")
	}
	logs := f.docker(t, "logs", f.name)
	if strings.Contains(logs, f.password) || strings.Contains(logs, f.verifier) || strings.Contains(logs, "admin-fixture-password") {
		t.Fatal("container logs disclosed credential material")
	}
	process := f.docker(t, "exec", "--user", "postgres", f.name, "sh", "-c", "tr '\\0' '\\n' < /proc/1/environ")
	if strings.Contains(process, "POSTGRES_PASSWORD=") || strings.Contains(process, "SYNOD_APP_VERIFIER=") {
		t.Fatal("final PostgreSQL process retained startup secrets")
	}
}

func TestPostgresBootstrapRefusesCompletedReceiptDrift(t *testing.T) {
	for _, test := range []struct{ name, change string }{
		{"missing database", `DROP DATABASE app`},
		{"recreated database", `DROP DATABASE app;`},
		{"role privilege", `ALTER ROLE synod_app CREATEDB`},
		{"role password", `ALTER ROLE synod_app PASSWORD NULL`},
		{"credential version", `UPDATE synod_control.bootstrap SET app_version=app_version+1`},
		{"missing cluster binding", `ALTER TABLE synod_control.bootstrap ALTER COLUMN cluster_id DROP NOT NULL; UPDATE synod_control.bootstrap SET cluster_id=NULL`},
		{"public receipt", `GRANT USAGE ON SCHEMA synod_control TO PUBLIC`},
		{"role membership", `CREATE ROLE unexpected_membership; GRANT unexpected_membership TO synod_app`},
		{"missing receipt", `DROP SCHEMA synod_control CASCADE`},
		{"administrative database access", `GRANT CONNECT ON DATABASE postgres TO synod_app`},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := bootstrapContainer(t, "")
			f.adminSQL(t, test.change)
			if test.name == "recreated database" {
				f.adminSQL(t, `CREATE DATABASE app OWNER synod_app`)
			}
			f.docker(t, "restart", "--time", "10", f.name)
			f.waitPhase(t, "failed")
			command := exec.CommandContext(t.Context(), "docker", "--host", f.host, "exec", f.name, "pg_isready", "-h", "127.0.0.1", "-p", "5432", "-t", "1")
			if failure, ok := errors.AsType[*exec.ExitError](command.Run()); !ok || failure.ExitCode() != 2 {
				t.Fatal("failed bootstrap did not report an absent TCP server")
			}
			state := f.docker(t, "inspect", "--format", "{{.State.Running}} {{.State.Restarting}}", f.name)
			if strings.TrimSpace(state) != "true false" {
				t.Fatalf("failed wrapper did not remain inert: %s", state)
			}
			processes := f.docker(t, "top", f.name, "-eo", "pid,comm")
			if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "psql\n") {
				t.Fatal("terminal failed phase retained a database process")
			}
			f.docker(t, "stop", "--time", "5", f.name)
		})
	}
}

func TestPostgresBootstrapResumesPreparedReceipt(t *testing.T) {
	f := bootstrapContainer(t, "")
	// Restore the exact durable state before the separate CREATE DATABASE phase.
	f.adminSQL(t, `DROP DATABASE app`)
	f.adminSQL(t, `BEGIN; ALTER ROLE synod_app NOLOGIN; UPDATE synod_control.bootstrap SET phase='prepared',database_oid=NULL; COMMIT`)
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "ready")
	conn := f.connect(t)
	var name string
	if err := conn.QueryRow(t.Context(), `SELECT current_user`).Scan(&name); err != nil || name != "synod_app" {
		t.Fatal("prepared receipt did not resume application access")
	}
}

func TestPostgresBootstrapResumesActualInterruptedSetup(t *testing.T) {
	for _, test := range []struct {
		name, boundary string
		databaseExists bool
		kill           bool
	}{
		{name: "prepared commit killed", boundary: "COMMIT;\nSELECT 'CREATE DATABASE", kill: true},
		{name: "database created stopped", boundary: "\\gexec\nBEGIN;", databaseExists: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The injected delay changes no production branch. Its file is removed
			// before interruption, so the same container resumes without that delay.
			startup := strings.Replace(postgresStartup, "sql_pid=''\ntemporary=0", `
if [ ! -e /var/lib/synod-postgres/test-once ]; then
    touch /var/lib/synod-postgres/test-once /var/lib/synod-postgres/test-pause
fi
sql_pid=''
temporary=0`, 1)
			boundary, after, found := strings.Cut(test.boundary, "\n")
			if !found {
				t.Fatal("invalid test boundary")
			}
			startup = strings.Replace(startup, test.boundary, boundary+"\nSELECT pg_catalog.pg_sleep(25) WHERE (pg_catalog.pg_stat_file('/var/lib/synod-postgres/test-pause',true)).size IS NOT NULL;\n"+after, 1)
			f := bootstrapContainer(t, startup)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			var roleOID string
			for ctx.Err() == nil {
				query := `SELECT app_oid FROM synod_control.bootstrap WHERE phase='prepared' AND EXISTS
 (SELECT FROM pg_stat_activity WHERE query LIKE 'SELECT pg_catalog.pg_sleep(25)%' AND wait_event='PgSleep')
 AND EXISTS (SELECT FROM pg_authid WHERE rolname='synod_app' AND NOT rolcanlogin)`
				if test.databaseExists {
					query += ` AND EXISTS (SELECT FROM pg_database WHERE datname='app')`
				} else {
					query += ` AND NOT EXISTS (SELECT FROM pg_database WHERE datname='app')`
				}
				command := exec.CommandContext(ctx, "docker", "--host", f.host, "exec", "--user", "postgres", f.name,
					"psql", "-X", "-h", ConfigDirectory, "-U", "synod_admin", "-d", "postgres", "-At", "-c", query)
				output, err := command.Output()
				if err == nil && strings.TrimSpace(string(output)) != "" {
					roleOID = strings.TrimSpace(string(output))
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if roleOID == "" {
				t.Fatal("startup did not reach its durable interruption boundary")
			}
			command := exec.CommandContext(ctx, "docker", "--host", f.host, "exec", f.name, "pg_isready", "-h", "127.0.0.1", "-p", "5432", "-t", "1")
			if failure, ok := errors.AsType[*exec.ExitError](command.Run()); !ok || failure.ExitCode() != 2 {
				t.Fatal("incomplete bootstrap exposed a TCP server")
			}
			f.docker(t, "exec", f.name, "rm", "/var/lib/synod-postgres/test-pause")
			if test.kill {
				f.docker(t, "kill", "--signal", "KILL", f.name)
			} else {
				f.docker(t, "stop", "--time", "10", f.name)
			}
			state := f.docker(t, "inspect", "--format", "{{.State.Running}} {{.State.Restarting}}", f.name)
			if strings.TrimSpace(state) != "false false" {
				t.Fatal("container stop left bootstrap processes active")
			}
			f.docker(t, "start", f.name)
			f.waitPhase(t, "ready")
			if current := strings.TrimSpace(f.adminSQL(t, `SELECT app_oid FROM synod_control.bootstrap WHERE phase='completed'`)); current != roleOID {
				t.Fatal("resumed bootstrap replaced its prepared role")
			}
			f.connect(t)
		})
	}
}

func TestPostgresBootstrapRefusesStorageLossOnContainerRestart(t *testing.T) {
	f := bootstrapContainer(t, "")
	f.docker(t, "stop", "--time", "10", f.name)
	// Only the fixture's anonymous volume is mounted into this one-shot helper.
	image := strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.Config.Image}}", f.name))
	f.docker(t, "run", "--rm", "--volumes-from", f.name, "--entrypoint", "/bin/bash", image, "-ec", `find /var/lib/postgresql/data -mindepth 1 -delete`)
	f.docker(t, "start", f.name)
	f.waitPhase(t, "failed")
	files := f.docker(t, "exec", f.name, "find", DataDirectory, "-mindepth", "1", "-maxdepth", "1", "-print")
	if strings.TrimSpace(files) != "" {
		t.Fatal("restart initialized a lost data directory")
	}
	marker := f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/initialized-attempt")
	if strings.TrimSpace(marker) != "attempted" {
		t.Fatalf("initialization history missing: %q", marker)
	}
}

// A member refuses a volume that holds a different complete database. The bootstrap receipt
// in the database binds it to its installation, cluster, member, and credentials, and every
// start checks that receipt before the engine serves. The member's own data, moved the same
// way, still starts, so the refusal comes from the receipt and not from the copy.
func TestPostgresBootstrapRefusesSwappedDatabase(t *testing.T) {
	for _, source := range []string{"own data", "another cluster", "foreign database"} {
		t.Run(source, func(t *testing.T) {
			f := bootstrapContainer(t, "")
			f.adminSQL(t, `CREATE TABLE swap_marker(value int); INSERT INTO swap_marker VALUES (175)`)
			image := strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.Config.Image}}", f.name))
			var donor, donorVolume string
			switch source {
			case "own data":
				donorVolume = "synod-swap-copy-" + uuid.NewString()
				f.docker(t, "volume", "create", "--label", "io.synod.fixture=postgres-bootstrap", donorVolume)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					exec.CommandContext(ctx, "docker", "--host", f.host, "volume", "rm", "--force", donorVolume).Run()
				})
				f.docker(t, "stop", "--time", "10", f.name)
				f.docker(t, "run", "--rm", "--volumes-from", f.name, "--volume", donorVolume+":/donor", "--entrypoint", "/bin/bash", image,
					"-ec", `cp -a /var/lib/postgresql/data/. /donor/`)
			case "another cluster":
				other := bootstrapContainer(t, "")
				other.docker(t, "stop", "--time", "10", other.name)
				donor = other.name
			default:
				// A plain PostgreSQL cluster with its own superuser and no Synod receipt.
				donor = "synod-swap-donor-" + uuid.NewString()
				f.docker(t, "run", "--detach", "--name", donor, "--label", "io.synod.fixture=postgres-bootstrap", "--env", "POSTGRES_PASSWORD=foreign-fixture-password", image)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					exec.CommandContext(ctx, "docker", "--host", f.host, "rm", "--force", "--volumes", donor).Run()
				})
				deadline := time.Now().Add(60 * time.Second)
				for {
					command := exec.CommandContext(t.Context(), "docker", "--host", f.host, "exec", donor, "pg_isready", "-h", "127.0.0.1", "-t", "1")
					if command.Run() == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("foreign donor did not become ready")
					}
					time.Sleep(500 * time.Millisecond)
				}
				f.docker(t, "stop", "--time", "10", donor)
			}
			if donorVolume == "" {
				donorVolume = strings.TrimSpace(f.docker(t, "inspect", "--format", `{{range .Mounts}}{{if eq .Destination "`+DataDirectory+`"}}{{.Name}}{{end}}{{end}}`, donor))
			}
			if donorVolume == "" {
				t.Fatal("donor has no data volume")
			}
			f.docker(t, "stop", "--time", "10", f.name)
			// Replace the member's data with the donor's complete data directory.
			f.docker(t, "run", "--rm", "--volumes-from", f.name, "--volume", donorVolume+":/donor:ro", "--entrypoint", "/bin/bash", image,
				"-ec", `find /var/lib/postgresql/data -mindepth 1 -delete && cp -a /donor/. /var/lib/postgresql/data/`)
			f.docker(t, "start", f.name)
			if source == "own data" {
				f.waitPhase(t, "ready")
				if value := strings.TrimSpace(f.adminSQL(t, `SELECT value FROM swap_marker`)); value != "175" {
					t.Fatalf("own data after the move: %q", value)
				}
				return
			}
			f.waitPhase(t, "failed")
			// The refused start served nothing and changed nothing in the swapped data.
			marker := f.docker(t, "run", "--rm", "--volumes-from", f.name, "--entrypoint", "/bin/bash", image, "-ec", `test -s /var/lib/postgresql/data/PG_VERSION && echo present`)
			if strings.TrimSpace(marker) != "present" {
				t.Fatal("the refused start changed the swapped data directory")
			}
		})
	}
}

func TestPostgresBootstrapTerminalFailureStaysFailedAcrossRestart(t *testing.T) {
	startup := strings.Replace(postgresStartup, "sql_pid=''\ntemporary=0", `
if [ ! -e /var/lib/synod-postgres/test-once ]; then
    touch /var/lib/synod-postgres/test-once /var/lib/synod-postgres/test-fail
fi
sql_pid=''
temporary=0`, 1)
	startup = strings.Replace(startup, "psql --no-psqlrc", "printf 'attempted\\n' >> /var/lib/synod-postgres/test-sql-attempts\npsql --no-psqlrc", 1)
	startup = strings.Replace(startup, "COMMIT;\nSELECT 'CREATE DATABASE", `COMMIT;
COPY (SELECT phase || ':' || app_oid::text FROM synod_control.bootstrap) TO '/var/lib/synod-postgres/test-prepared-proof';
SELECT 1 / CASE WHEN (pg_catalog.pg_stat_file('/var/lib/synod-postgres/test-fail',true)).size IS NOT NULL THEN 0 ELSE 1 END;
SELECT 'CREATE DATABASE`, 1)
	f := bootstrapContainer(t, startup)
	f.waitPhase(t, "failed")
	proof := strings.TrimSpace(f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/test-prepared-proof"))
	_, roleID, found := strings.Cut(proof, "prepared:")
	if !found || roleID == "" {
		t.Fatal("terminal failure did not follow the committed prepared receipt")
	}
	if tombstone := f.docker(t, "exec", f.name, "cat", BootstrapFailurePath); strings.TrimSpace(tombstone) != "failed" {
		t.Fatal("terminal phase did not have a durable failure tombstone")
	}
	f.docker(t, "exec", f.name, "rm", InitializationDirectory+"/test-fail")
	f.docker(t, "update", "--restart", "unless-stopped", f.name)
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "failed")
	attempts := f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/test-sql-attempts")
	if attempts != "attempted\n" {
		t.Fatal("restart entered SQL after terminal bootstrap failure")
	}
	processes := f.docker(t, "top", f.name, "-eo", "pid,comm")
	if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "psql\n") {
		t.Fatal("terminal failure restart started a database process")
	}
	f.docker(t, "stop", "--time", "5", f.name)
}
