package postgres

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func (f *bootstrapFixture) recoverSetup(t *testing.T, commandID string, outcome string) {
	t.Helper()
	wantSuccess := outcome == "validated"
	program, err := RecoveryCommand(f.config, f.command[5], f.command[6], f.command[7], commandID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	args := append([]string{"--host", f.host, "exec", "--user", "postgres", f.name}, program...)
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if (err == nil) != wantSuccess {
		t.Fatalf("recovery success=%v, want %v (%d output bytes)", err == nil, wantSuccess, len(output))
	}
	if ctx.Err() != nil {
		t.Fatal("recovery did not stop")
	}
	processes := f.docker(t, "top", f.name, "-eo", "pid,comm")
	if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "psql\n") {
		t.Fatal("recovery left native database work active")
	}
	if wantSuccess {
		f.waitPhase(t, "recovered")
		got := f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/recovery-"+commandID)
		if got != "validated:"+commandID+"\n" {
			t.Fatal("recovery receipt does not bind the command")
		}
		f.docker(t, "exec", f.name, "test", "!", "-e", BootstrapFailurePath)
	} else {
		f.docker(t, "exec", f.name, "test", "-f", BootstrapFailurePath)
		if outcome == "" {
			f.docker(t, "exec", f.name, "test", "!", "-e", InitializationDirectory+"/recovery-"+commandID)
		} else if got := f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/recovery-"+commandID); got != outcome+":"+commandID+"\n" {
			t.Fatal("refusal receipt does not bind the command")
		}
	}
}

func TestPostgresRecoveryFinishesPreparedReceipt(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		t.Run(map[bool]string{false: "asynchronous", true: "synchronous"}[synchronous], func(t *testing.T) {
			startup := strings.Replace(postgresStartup, "COMMIT;\nSELECT 'CREATE DATABASE", `COMMIT;
COPY (SELECT app_oid FROM synod_control.bootstrap) TO '/var/lib/synod-postgres/test-role';
SELECT 1 / CASE WHEN (SELECT phase FROM synod_control.bootstrap)='prepared' THEN 0 ELSE 1 END;
SELECT 'CREATE DATABASE`, 1)
			f := bootstrapContainerWithPolicy(t, startup, synchronous)
			f.waitPhase(t, "failed")
			before := strings.TrimSpace(f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/test-role"))
			// COPY refuses an existing target. The proof file is only a fixture fault.
			f.docker(t, "exec", f.name, "rm", InitializationDirectory+"/test-role")
			f.recoverSetup(t, uuid.NewString(), "validated")
			f.docker(t, "restart", "--time", "10", f.name)
			f.waitPhase(t, "ready")
			if after := strings.TrimSpace(f.adminSQL(t, "SELECT app_oid FROM synod_control.bootstrap")); before != after {
				t.Fatal("recovery replaced the prepared application role")
			}
			f.connect(t)
		})
	}
}

func TestPostgresRecoveryVerifiesCompletedData(t *testing.T) {
	startup := strings.Replace(postgresStartup, "sql_pid=''\ncleanup\ntemporary=0", "sql_pid=''\nif [ -e /var/lib/synod-postgres/test-fail ]; then false; fi\ncleanup\ntemporary=0", 1)
	f := bootstrapContainer(t, startup)
	f.waitPhase(t, "ready")
	conn := f.connect(t)
	if _, err := conn.Exec(t.Context(), "CREATE TABLE kept(value integer); INSERT INTO kept VALUES(91)"); err != nil {
		t.Fatal(err)
	}
	before := f.adminSQL(t, "SELECT app_oid,replication_oid,database_oid,encode(verifier_fingerprint,'hex'),encode(replication_verifier_fingerprint,'hex') FROM synod_control.bootstrap")
	conn.Close(t.Context())
	f.docker(t, "exec", f.name, "touch", InitializationDirectory+"/test-fail")
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "failed")
	f.docker(t, "exec", f.name, "rm", InitializationDirectory+"/test-fail")
	f.recoverSetup(t, uuid.NewString(), "validated")
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "ready")
	if after := f.adminSQL(t, "SELECT app_oid,replication_oid,database_oid,encode(verifier_fingerprint,'hex'),encode(replication_verifier_fingerprint,'hex') FROM synod_control.bootstrap"); before != after {
		t.Fatal("recovery changed native identity")
	}
	var value int
	if err := f.connect(t).QueryRow(t.Context(), "SELECT value FROM kept").Scan(&value); err != nil || value != 91 {
		t.Fatal("recovery lost application data")
	}
}

func TestPostgresRecoveryRefusesUnsafeReceipt(t *testing.T) {
	for _, test := range []struct{ name, change string }{
		{"missing database", "DROP DATABASE app"},
		{"wrong identity", "UPDATE synod_control.bootstrap SET cluster_id='00000000-0000-0000-0000-000000000001'"},
		{"missing receipt", "DROP DATABASE app; DROP ROLE synod_app; DROP SCHEMA synod_control CASCADE"},
		{"missing version", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := bootstrapContainer(t, "")
			for query := range strings.SplitSeq(test.change, ";") {
				if query = strings.TrimSpace(query); query != "" {
					f.adminSQL(t, query)
				}
			}
			// Force a known failure without allowing ordinary startup to create a new receipt.
			f.docker(t, "exec", f.name, "sh", "-c", "printf 'failed\\n' > "+BootstrapFailurePath)
			f.docker(t, "restart", "--time", "10", f.name)
			f.waitPhase(t, "failed")
			if test.name == "missing version" {
				f.docker(t, "exec", f.name, "rm", DataVersionFile)
			}
			outcome := "refused"
			if test.name == "missing version" {
				outcome = ""
			}
			f.recoverSetup(t, uuid.NewString(), outcome)
			f.docker(t, "restart", "--time", "10", f.name)
			f.waitPhase(t, "failed")
		})
	}
}

func TestPostgresRecoveryRejectsInvalidCommandIdentity(t *testing.T) {
	for _, id := range []string{"", uuid.Nil.String(), "../outside", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"} {
		if _, err := RecoveryCommand(nil, "", "", "", id); err == nil {
			t.Errorf("accepted command identity %q", id)
		}
	}
}

func TestPostgresRecoveryCancellationPreservesFailure(t *testing.T) {
	startup := strings.Replace(postgresStartup, "COMMIT;\nSELECT 'CREATE DATABASE", `COMMIT;
SELECT 1 / CASE WHEN (SELECT phase FROM synod_control.bootstrap)='prepared' THEN 0 ELSE 1 END;
SELECT 'CREATE DATABASE`, 1)
	f := bootstrapContainer(t, startup)
	f.waitPhase(t, "failed")
	id := uuid.NewString()
	program, err := RecoveryCommand(f.config, f.command[5], f.command[6], f.command[7], id)
	if err != nil {
		t.Fatal(err)
	}
	program[2] = strings.Replace(program[2], "psql --no-psqlrc", "echo $$ > /run/synod-postgres/test-recovery-pid\npsql --no-psqlrc", 1)
	program[2] = strings.Replace(program[2], "BEGIN;\nDO $prepare$", "COPY (SELECT 1) TO '/run/synod-postgres/test-recovery-sql';\nSELECT pg_catalog.pg_sleep(25);\nBEGIN;\nDO $prepare$", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	args := append([]string{"--host", f.host, "exec", "--user", "postgres", f.name}, program...)
	command := exec.CommandContext(ctx, "docker", args...)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			cancel()
			command.Wait()
		}
	}()
	// The file is created only after the temporary server starts.
	f.docker(t, "exec", f.name, "sh", "-c", "until test -s /run/synod-postgres/test-recovery-sql; do sleep 0.05; done; kill -TERM $(cat /run/synod-postgres/test-recovery-pid)")
	if err := command.Wait(); err == nil || ctx.Err() != nil {
		t.Fatal("cancelled recovery did not stop with failure")
	}
	f.waitPhase(t, "failed")
	f.docker(t, "exec", f.name, "test", "-f", BootstrapFailurePath)
	if got := f.docker(t, "exec", f.name, "cat", InitializationDirectory+"/recovery-"+id); got != "refused:"+id+"\n" {
		t.Fatal("cancelled recovery did not record refusal")
	}
	processes := f.docker(t, "top", f.name, "-eo", "pid,comm")
	if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "psql\n") {
		t.Fatal("cancelled recovery left native work active")
	}
	f.recoverSetup(t, uuid.NewString(), "validated")
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "ready")
	f.connect(t)
}

func TestPostgresRecoveryRefusesExistingNativeServer(t *testing.T) {
	f := bootstrapContainer(t, "")
	f.docker(t, "exec", f.name, "sh", "-c", "printf 'failed\\n' > "+BootstrapFailurePath)
	f.docker(t, "restart", "--time", "10", f.name)
	f.waitPhase(t, "failed")
	f.docker(t, "exec", "--user", "postgres", f.name, "pg_ctl", "-D", DataDirectory, "-w", "-t", "10", "-o", "-c listen_addresses='' -c unix_socket_directories="+ConfigDirectory, "start")
	id := uuid.NewString()
	program, err := RecoveryCommand(f.config, f.command[5], f.command[6], f.command[7], id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	args := append([]string{"--host", f.host, "exec", "--user", "postgres", f.name}, program...)
	if err := exec.CommandContext(ctx, "docker", args...).Run(); err == nil || ctx.Err() != nil {
		t.Fatal("recovery did not refuse the existing server")
	}
	if got := strings.TrimSpace(f.adminSQL(t, "SELECT 7")); got != "7" {
		t.Fatal("refused recovery stopped the existing server")
	}
	f.docker(t, "exec", f.name, "test", "!", "-e", InitializationDirectory+"/recovery-"+id)
	f.waitPhase(t, "failed")
	f.docker(t, "exec", "--user", "postgres", f.name, "pg_ctl", "-D", DataDirectory, "-m", "fast", "-w", "-t", "10", "stop")
}
