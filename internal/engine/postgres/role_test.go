package postgres

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/engine"
)

func TestPostgresProbeRequiresNativeRole(t *testing.T) {
	f := bootstrapContainer(t, "")
	f.connect(t)
	host, port, err := net.SplitHostPort(f.address)
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := engine.Endpoint{Address: host, Port: uint16(number), Database: "app", Role: "synod_app", PostgresRole: "primary"}
	observed := New().Probe(t.Context(), endpoint, []byte(f.password), 1)
	if !observed.Healthy() || observed.Postgres == nil || observed.Postgres.Role != "primary" || observed.Postgres.ReadOnly {
		t.Fatalf("primary proof: %+v", observed)
	}
	endpoint.PostgresRole = "replica"
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 1)
	if observed.Healthy() || observed.Ready.Reason != reasonRoleMismatch || observed.Postgres == nil || observed.Postgres.Role != "primary" {
		t.Fatalf("primary passed replica check: %+v", observed)
	}
	endpoint.PostgresRole = "primary"
	f.adminSQL(t, `ALTER ROLE synod_app SET default_transaction_read_only = on`)
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 1)
	if observed.Healthy() || observed.Ready.Reason != reasonReadOnlyMismatch || observed.Postgres == nil || !observed.Postgres.ReadOnly {
		t.Fatalf("read-only primary passed writer check: %+v", observed)
	}
	f.adminSQL(t, `ALTER ROLE synod_app RESET default_transaction_read_only`)
	f.docker(t, "exec", "--user", "postgres", f.name, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", ConfigDirectory, "-U", "synod_admin", "-d", "app", "-c", `REVOKE EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() FROM PUBLIC`)
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 1)
	if observed.Healthy() || observed.Ready.State != engine.ProbeUnavailable || observed.Ready.Reason != reasonRoleUnavailable || observed.Postgres != nil {
		t.Fatalf("unavailable role passed readiness: %+v", observed)
	}
	f.docker(t, "exec", "--user", "postgres", f.name, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", ConfigDirectory, "-U", "synod_admin", "-d", "app", "-c", `GRANT EXECUTE ON FUNCTION pg_catalog.pg_is_in_recovery() TO PUBLIC`)

	// Clone a real physical standby inside the isolated Docker fixture network.
	f.docker(t, "exec", f.name, "sh", "-c", "printf 'host replication synod_admin all trust\\n' >> "+ConfigDirectory+"/pg_hba.conf")
	f.adminSQL(t, `SELECT pg_reload_conf()`)
	peer := strings.TrimSpace(f.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", f.name))
	image := strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.Config.Image}}", f.name))
	replica := "synod-role-" + uuid.NewString()
	f.docker(t, "run", "--detach", "--name", replica, "--label", "io.synod.fixture=postgres-role", "--publish", "127.0.0.1::5432", "--user", "postgres", "--entrypoint", "bash", image, "-ceu", `pg_basebackup -h "$1" -p 5432 -U synod_admin -D /tmp/standby -R -X stream --checkpoint=fast
printf 'host all all all scram-sha-256\n' >> /tmp/standby/pg_hba.conf
exec postgres -D /tmp/standby -p 5432 -c listen_addresses=0.0.0.0`, "_", peer)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "docker", "--host", f.host, "rm", "--force", "--volumes", replica).CombinedOutput(); err != nil {
			t.Errorf("remove standby: %v (%d bytes)", err, len(output))
		}
	})
	address := strings.TrimSpace(f.docker(t, "port", replica, "5432/tcp"))
	host, port, err = net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	number, err = strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Address, endpoint.Port, endpoint.PostgresRole = host, uint16(number), "replica"
	deadline := time.Now().Add(45 * time.Second)
	for {
		observed = New().Probe(t.Context(), endpoint, []byte(f.password), 2)
		if observed.Healthy() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("standby unavailable: %+v; logs: %s", observed, f.docker(t, "logs", "--tail", "20", replica))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if observed.Postgres == nil || observed.Postgres.Role != "replica" || !observed.Postgres.ReadOnly {
		t.Fatalf("standby proof: %+v", observed)
	}
	endpoint.PostgresRole = "primary"
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 2)
	if observed.Healthy() || observed.Ready.Reason != reasonRoleMismatch {
		t.Fatalf("standby passed writer check: %+v", observed)
	}
	f.docker(t, "exec", "--user", "postgres", replica, "pg_ctl", "-D", "/tmp/standby", "-w", "promote")
	endpoint.PostgresRole = "replica"
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 3)
	if observed.Healthy() || observed.Ready.Reason != reasonRoleMismatch {
		t.Fatalf("promoted server passed replica check: %+v", observed)
	}
	endpoint.PostgresRole = "primary"
	observed = New().Probe(t.Context(), endpoint, []byte(f.password), 3)
	if !observed.Healthy() || observed.Postgres == nil || observed.Postgres.Role != "primary" || observed.Postgres.ReadOnly {
		t.Fatalf("promoted writer: %+v", observed)
	}
}

func TestPostgresProbeRejectsAnUnknownRole(t *testing.T) {
	for _, role := range []string{"", "writer", "PRIMARY", "standby"} {
		observed := New().Probe(t.Context(), engine.Endpoint{PostgresRole: role}, nil, 1)
		if observed.Healthy() || observed.Reachable.Reason != reasonRoleInvalid || observed.Postgres != nil {
			t.Fatalf("invalid role %q: %+v", role, observed)
		}
	}
}
