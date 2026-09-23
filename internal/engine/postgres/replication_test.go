package postgres

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresReplicationCredentialPermitsOnlyPhysicalAccess(t *testing.T) {
	f := bootstrapContainer(t, "")
	config := f.connect(t).Config().Copy()
	config.User, config.Password = "synod_repl", f.replicationPassword
	_, err := pgconn.ConnectConfig(t.Context(), &config.Config)
	if denied, ok := errors.AsType[*pgconn.PgError](err); !ok || denied.Code != "28000" {
		t.Fatalf("replication SQL session was not rejected by HBA: %v", err)
	}
	config.RuntimeParams["replication"] = "true"
	config.Password = f.password
	_, err = pgconn.ConnectConfig(t.Context(), &config.Config)
	if denied, ok := errors.AsType[*pgconn.PgError](err); !ok || denied.Code != "28P01" {
		t.Fatalf("application password authenticated replication: %v", err)
	}
	config.Password = f.replicationPassword
	conn, err := pgconn.ConnectConfig(t.Context(), &config.Config)
	if err != nil {
		t.Fatal("replication authentication failed")
	}
	defer conn.Close(context.WithoutCancel(t.Context()))
	identified, err := conn.Exec(t.Context(), "IDENTIFY_SYSTEM").ReadAll()
	if err != nil || len(identified) != 1 || len(identified[0].Rows) != 1 || len(identified[0].Rows[0]) != 4 || len(identified[0].Rows[0][0]) == 0 {
		t.Fatalf("physical identity query failed: %v", err)
	}
	conn.Close(context.WithoutCancel(t.Context()))
	if got := strings.TrimSpace(f.adminSQL(t, `SELECT rolreplication AND rolcanlogin AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolinherit AND NOT rolbypassrls AND rolvaliduntil IS NULL FROM pg_roles WHERE rolname='synod_repl'`)); got != "t" {
		t.Fatal("replication role has the wrong privileges")
	}
	if got := strings.TrimSpace(f.adminSQL(t, `SELECT current_setting('wal_level')='replica' AND current_setting('max_wal_senders')::int=10 AND current_setting('max_replication_slots')::int=10 AND (SELECT setting::int=256 FROM pg_settings WHERE name='wal_keep_size') AND (SELECT setting::int=1024 FROM pg_settings WHERE name='max_slot_wal_keep_size')`)); got != "t" {
		t.Fatal("primary did not apply its WAL settings")
	}
	before := f.adminSQL(t, `SELECT replication_oid::text || ':' || replication_credential_id::text || ':' || replication_version::text || ':' || encode(replication_verifier_fingerprint,'hex') FROM synod_control.bootstrap`)

	// The backup client receives plaintext. The primary only stores its SCRAM verifier.
	name := "synod-replication-client-" + uuid.NewString()
	image := strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.Config.Image}}", f.name))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", "--host", f.host, "rm", "--force", "--volumes", name).CombinedOutput()
		if err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove backup client: %v (%d bytes)", err, len(output))
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	backup := exec.CommandContext(ctx, "docker", "--host", f.host, "run", "--name", name, "--label", "io.synod.fixture=postgres-replication", "--network", "container:"+f.name, "--user", "postgres", "--env", "PGPASSWORD", "--entrypoint", "bash", image, "-ec", `pg_basebackup -h 127.0.0.1 -p 5432 -U synod_repl -D /tmp/backup -X stream --checkpoint=fast --no-password
pg_verifybackup /tmp/backup`)
	backup.Env = append(os.Environ(), "PGPASSWORD="+f.replicationPassword)
	if output, err := backup.CombinedOutput(); err != nil {
		t.Fatalf("native physical backup failed: %v (%d output bytes)", err, len(output))
	}
	metadata := f.docker(t, "inspect", "--format", "{{json .Config.Env}}", f.name)
	if strings.Contains(metadata, f.password) || strings.Contains(metadata, f.replicationPassword) {
		t.Fatal("primary metadata contains a plaintext application or replication password")
	}
	f.docker(t, "restart", f.name)
	f.waitPhase(t, "ready")
	f.connect(t)
	after := f.adminSQL(t, `SELECT replication_oid::text || ':' || replication_credential_id::text || ':' || replication_version::text || ':' || encode(replication_verifier_fingerprint,'hex') FROM synod_control.bootstrap`)
	if before != after {
		t.Fatal("restart replaced the replication identity")
	}
}

func TestPostgresStartupRefusesReplicationRoleDrift(t *testing.T) {
	for name, change := range map[string]string{
		"missing role":   `DROP ROLE synod_repl`,
		"replaced role":  `DROP ROLE synod_repl; CREATE ROLE synod_repl LOGIN REPLICATION`,
		"superuser":      `ALTER ROLE synod_repl SUPERUSER`,
		"no replication": `ALTER ROLE synod_repl NOREPLICATION`,
		"password":       `ALTER ROLE synod_repl PASSWORD 'changed-fixture-password'`,
		"expiry":         `ALTER ROLE synod_repl VALID UNTIL 'infinity'`,
		"membership":     `GRANT synod_app TO synod_repl`,
		"ownership":      `CREATE DATABASE extra OWNER synod_repl`,
	} {
		t.Run(name, func(t *testing.T) {
			f := bootstrapContainer(t, "")
			f.connect(t)
			f.adminSQL(t, change)
			f.docker(t, "restart", f.name)
			f.waitPhase(t, "failed")
			f.docker(t, "exec", f.name, "test", "-f", BootstrapFailurePath)
			f.recoverSetup(t, uuid.NewString(), "refused")
		})
	}
}
