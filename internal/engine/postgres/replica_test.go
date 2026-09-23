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
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func replicaContainer(t *testing.T, primary *bootstrapFixture, source ReplicaSource, edits ...func(*bootstrapFixture)) *bootstrapFixture {
	t.Helper()
	f := &bootstrapFixture{
		host: primary.host, name: "synod-replica-" + uuid.NewString(), password: primary.password,
		verifier: primary.verifier, replicationPassword: primary.replicationPassword, replicationVerifier: primary.replicationVerifier,
		config: proto.Clone(primary.config).(*agentv1.PostgresContainerConfig),
	}
	for _, edit := range edits {
		edit(f)
	}
	var err error
	f.command, err = ReplicaCommand(f.config, source, primary.command[5], primary.command[6], uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	image := strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{.Config.Image}}", primary.name))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, "docker", "--host", f.host, "rm", "--force", "--volumes", f.name).CombinedOutput(); err != nil {
			t.Errorf("remove replica: %v (%d output bytes)", err, len(output))
		}
	})
	args := []string{"--host", f.host, "run", "--detach", "--name", f.name, "--label", "io.synod.fixture=postgres-replica",
		"--publish", "127.0.0.1::5432", "--tmpfs", ConfigDirectory + ":rw,noexec,nosuid,nodev,size=1m",
		"--env", "SYNOD_APP_VERIFIER", "--env", "SYNOD_REPL_VERIFIER", "--env", "SYNOD_REPL_PASSWORD", image}
	args = append(args, f.command...)
	command := exec.CommandContext(t.Context(), "docker", args...)
	command.Env = append(os.Environ(), "SYNOD_APP_VERIFIER="+f.verifier, "SYNOD_REPL_VERIFIER="+f.replicationVerifier, "SYNOD_REPL_PASSWORD="+f.replicationPassword)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create replica: %v (%d output bytes)", err, len(output))
	}
	f.address = strings.TrimSpace(f.docker(t, "port", f.name, "5432/tcp"))
	f.waitPhase(t, "clone_pending")
	return f
}

func TestPostgresNativeReplicaCloneStreamsAndPreservesRestart(t *testing.T) {
	primary := bootstrapContainer(t, "", "replication:escaped\\password-90")
	writer := primary.connect(t)
	if _, err := writer.Exec(t.Context(), "CREATE TABLE replica_rows(value integer); INSERT INTO replica_rows VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
	replica := replicaContainer(t, primary, source)
	id := uuid.NewString()
	program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
	if output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("clone replica: %v (%d output bytes)", err, len(output))
	}
	replica.waitPhase(t, "cloned")
	before := replica.docker(t, "exec", replica.name, "cat", ReplicaReceiptFile)
	if !strings.Contains(before, "\n"+id+"\n") {
		t.Fatal("clone receipt lost command identity")
	}
	replica.docker(t, "restart", replica.name)
	replica.waitPhase(t, "ready")
	reader := replica.connect(t)
	var inRecovery, readOnly bool
	if err := reader.QueryRow(t.Context(), "SELECT pg_is_in_recovery(), current_setting('transaction_read_only')::boolean").Scan(&inRecovery, &readOnly); err != nil || !inRecovery || !readOnly {
		t.Fatalf("replica role: recovery=%v read_only=%v err=%v", inRecovery, readOnly, err)
	}
	if _, err := reader.Exec(t.Context(), "INSERT INTO replica_rows VALUES(99)"); err == nil {
		t.Fatal("replica accepted a write")
	} else if pgerr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgerr.Code != "25006" {
		t.Fatalf("replica write refusal: %v", err)
	}
	if _, err := writer.Exec(t.Context(), "INSERT INTO replica_rows VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var count int
		if err := reader.QueryRow(t.Context(), "SELECT count(*) FROM replica_rows").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("primary write did not reach replica")
		}
		time.Sleep(50 * time.Millisecond)
	}
	reader.Close(context.WithoutCancel(t.Context()))
	replica.docker(t, "restart", replica.name)
	replica.waitPhase(t, "ready")
	var count int
	if err := replica.connect(t).QueryRow(t.Context(), "SELECT count(*) FROM replica_rows").Scan(&count); err != nil || count != 2 {
		t.Fatalf("restart lost replica data: count=%d err=%v", count, err)
	}
	if after := replica.docker(t, "exec", replica.name, "cat", ReplicaReceiptFile); after != before {
		t.Fatal("restart changed clone identity")
	}
	logs := replica.docker(t, "logs", replica.name)
	if strings.Contains(logs, replica.password) || strings.Contains(logs, replica.replicationPassword) || strings.Contains(logs, replica.verifier) || strings.Contains(logs, replica.replicationVerifier) {
		t.Fatal("replica logs disclosed secret material")
	}
	process := replica.docker(t, "exec", "--user", "postgres", replica.name, "sh", "-c", "tr '\\0' '\\n' < /proc/1/environ")
	if strings.Contains(process, "SYNOD_REPL_PASSWORD=") || strings.Contains(process, "SYNOD_APP_VERIFIER=") || strings.Contains(process, "SYNOD_REPL_VERIFIER=") {
		t.Fatal("final standby retained startup secrets")
	}
}

func TestPostgresReplicaCommandBindsAndValidatesSource(t *testing.T) {
	config := &agentv1.PostgresContainerConfig{Port: 5432, ApplicationCredentialId: uuid.NewString(), ApplicationCredentialVersion: 1, SuperuserCredentialId: uuid.NewString(), SuperuserCredentialVersion: 1, ReplicationCredentialId: uuid.NewString(), ReplicationCredentialVersion: 1}
	installation, clusterID, resource := uuid.NewString(), uuid.NewString(), uuid.NewString()
	source := ReplicaSource{Address: "192.0.2.20", Port: 5432, ResourceID: uuid.NewString()}
	baseline, err := ReplicaCommand(config, source, installation, clusterID, resource)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []ReplicaSource{
		{Address: "bad host", Port: 5432, ResourceID: source.ResourceID},
		{Address: "0.0.0.0", Port: 5432, ResourceID: source.ResourceID},
		{Address: "224.0.0.1", Port: 5432, ResourceID: source.ResourceID},
		{Address: "fe80::1%eth0", Port: 5432, ResourceID: source.ResourceID},
		{Address: source.Address, ResourceID: source.ResourceID},
		{Address: source.Address, Port: source.Port, ResourceID: resource},
		{Address: source.Address, Port: source.Port, ResourceID: uuid.Nil.String()},
	} {
		if _, err := ReplicaCommand(config, invalid, installation, clusterID, resource); err == nil {
			t.Fatalf("accepted invalid source %+v", invalid)
		}
	}
	for _, changed := range []ReplicaSource{
		{Address: "192.0.2.21", Port: source.Port, ResourceID: source.ResourceID},
		{Address: source.Address, Port: 5433, ResourceID: source.ResourceID},
		{Address: source.Address, Port: source.Port, ResourceID: uuid.NewString()},
	} {
		command, err := ReplicaCommand(config, changed, installation, clusterID, resource)
		if err != nil || command[len(command)-1] == baseline[len(baseline)-1] {
			t.Fatal("source change preserved configuration identity")
		}
	}
	config.SynchronousReplication = true
	changedPolicy, err := ReplicaCommand(config, source, installation, clusterID, resource)
	if err != nil || changedPolicy[len(changedPolicy)-1] == baseline[len(baseline)-1] {
		t.Fatal("replication policy change preserved native configuration identity")
	}
	if _, err := CloneReplicaCommand(config, source, installation, clusterID, resource, "invalid"); err == nil {
		t.Fatal("accepted invalid command identity")
	}
}

func TestPostgresNativeCloneRefusesSourceAndCredentialMismatch(t *testing.T) {
	for _, name := range []string{"source identity", "credential version", "password"} {
		t.Run(name, func(t *testing.T) {
			primary := bootstrapContainer(t, "")
			primary.connect(t)
			source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
			if name == "source identity" {
				source.ResourceID = uuid.NewString()
			}
			replica := replicaContainer(t, primary, source, func(f *bootstrapFixture) {
				if name == "credential version" {
					f.config.ReplicationCredentialVersion++
				}
				if name == "password" {
					f.replicationPassword = "wrong-replication-password"
				}
			})
			program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
			if _, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err == nil || ctx.Err() != nil {
				t.Fatal("unsafe clone did not reach a bounded refusal")
			}
			replica.waitPhase(t, "failed")
			replica.docker(t, "exec", replica.name, "test", "-f", DataDirectory+"/.synod-clone-intent")
			replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
			processes := replica.docker(t, "top", replica.name, "-eo", "pid,comm")
			if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "pg_basebackup\n") || strings.Contains(processes, "psql\n") {
				t.Fatal("refused clone retained native work")
			}
			replica.docker(t, "restart", replica.name)
			replica.waitPhase(t, "failed")
			replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
		})
	}
}

func TestPostgresNativeClonePublicationSurvivesProcessLoss(t *testing.T) {
	for _, boundary := range []string{"before rename", "after rename"} {
		t.Run(boundary, func(t *testing.T) {
			primary := bootstrapContainer(t, "")
			primary.connect(t)
			source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
			replica := replicaContainer(t, primary, source)
			id := uuid.NewString()
			program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], id)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "before rename" {
				program[2] = strings.Replace(program[2], `mv -T "$PGDATA" "$target"`, "kill -KILL $$", 1)
			} else {
				program[2] = strings.Replace(program[2], `printf 'cloned\n' > "$private/bootstrap-complete"`, "kill -KILL $$", 1)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
			if _, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err == nil || ctx.Err() != nil {
				t.Fatal("clone publication fault did not stop the native program")
			}
			replica.docker(t, "exec", replica.name, "test", "!", "-e", InitializationDirectory+"/replica-failed")
			replica.docker(t, "restart", replica.name)
			if boundary == "before rename" {
				replica.waitPhase(t, "failed")
				replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
				replica.docker(t, "exec", replica.name, "test", "-f", DataDirectory+"/.synod-clone/data/.synod-replica-receipt")
				return
			}
			replica.waitPhase(t, "ready")
			var recovery bool
			if err := replica.connect(t).QueryRow(t.Context(), "SELECT pg_is_in_recovery()").Scan(&recovery); err != nil || !recovery {
				t.Fatal("published clone did not recover as a standby")
			}
			if receipt := replica.docker(t, "exec", replica.name, "cat", ReplicaReceiptFile); !strings.Contains(receipt, "\n"+id+"\n") {
				t.Fatal("published clone lost its command identity")
			}
		})
	}
}

func TestPostgresNativeReplicaRefusesRestartDrift(t *testing.T) {
	for name, change := range map[string][]string{
		"missing standby":       {"rm", ReplicaDataDirectory + "/standby.signal"},
		"missing receipt":       {"rm", ReplicaReceiptFile},
		"wrong digest":          {"sed", "-i", "2cchanged", ReplicaReceiptFile},
		"wrong system identity": {"sed", "-i", "4c123456789", ReplicaReceiptFile},
		"changed configuration": {"sh", "-c", "printf 'port=5433\\n' >> " + ReplicaDataDirectory + "/postgresql.auto.conf"},
		"empty volume":          {"rm", "-rf", ReplicaDataDirectory, DataDirectory + "/.synod-clone", DataDirectory + "/.synod-clone-intent"},
		"missing attempt":       {"rm", InitializationDirectory + "/replica-attempt"},
		"missing volume intent": {"rm", DataDirectory + "/.synod-clone-intent"},
		"missing data":          {"mv", ReplicaDataDirectory, DataDirectory + "/retained"},
		"source role":           nil,
	} {
		t.Run(name, func(t *testing.T) {
			primary := bootstrapContainer(t, "")
			primary.connect(t)
			source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
			replica := replicaContainer(t, primary, source)
			program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			replica.docker(t, append([]string{"exec", "--user", "postgres", replica.name}, program...)...)
			if name == "source role" {
				replica.docker(t, "restart", replica.name)
				replica.waitPhase(t, "ready")
				reader := replica.connect(t)
				primary.adminSQL(t, "ALTER ROLE synod_app CREATEDB")
				deadline := time.Now().Add(15 * time.Second)
				for {
					var drifted bool
					if err := reader.QueryRow(t.Context(), "SELECT rolcreatedb FROM pg_roles WHERE rolname='synod_app'").Scan(&drifted); err != nil {
						t.Fatal(err)
					}
					if drifted {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("role change did not reach standby")
					}
					time.Sleep(50 * time.Millisecond)
				}
				reader.Close(context.WithoutCancel(t.Context()))
			} else {
				replica.docker(t, append([]string{"exec", replica.name}, change...)...)
			}
			replica.docker(t, "restart", replica.name)
			replica.waitPhase(t, "failed")
			processes := replica.docker(t, "top", replica.name, "-eo", "pid,comm")
			if strings.Contains(processes, "postgres\n") {
				t.Fatal("replica left a server running after failed verification")
			}
		})
	}
}

func TestPostgresNativeCloneCancellationStopsBackup(t *testing.T) {
	primary := bootstrapContainer(t, "")
	primary.connect(t)
	source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
	replica := replicaContainer(t, primary, source)
	program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	program[2] = strings.Replace(program[2], "failure_stage=copy", `failure_stage=copy
    printf '%s\n' "$$" > "$private/test-clone-pid"`, 1)
	program[2] = strings.Replace(program[2], "--checkpoint=fast --no-password", "--checkpoint=fast --no-password --max-rate=32", 1)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
	command := exec.CommandContext(ctx, "docker", args...)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if processes := replica.docker(t, "top", replica.name, "-eo", "pid,comm"); strings.Contains(processes, "pg_basebackup\n") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("clone did not start physical backup")
		}
		time.Sleep(50 * time.Millisecond)
	}
	replica.docker(t, "exec", "--user", "postgres", replica.name, "sh", "-c", `kill -TERM "$(cat /run/synod-postgres/test-clone-pid)"`)
	if err := command.Wait(); err == nil || ctx.Err() != nil {
		t.Fatal("cancelled clone did not stop in its bound")
	}
	replica.waitPhase(t, "failed")
	processes := replica.docker(t, "top", replica.name, "-eo", "pid,comm")
	if strings.Contains(processes, "postgres\n") || strings.Contains(processes, "pg_basebackup\n") {
		t.Fatal("cancelled clone retained native work")
	}
	replica.docker(t, "exec", replica.name, "test", "-f", DataDirectory+"/.synod-clone-intent")
	replica.docker(t, "exec", replica.name, "test", "-d", DataDirectory+"/.synod-clone")
	replica.docker(t, "restart", replica.name)
	replica.waitPhase(t, "failed")
}

func TestPostgresNativeCloneRejectsAdditionalTablespaces(t *testing.T) {
	primary := bootstrapContainer(t, "")
	primary.connect(t)
	primary.docker(t, "exec", "--user", "postgres", primary.name, "mkdir", "/tmp/extra-tablespace")
	primary.adminSQL(t, "CREATE TABLESPACE extra LOCATION '/tmp/extra-tablespace'")
	source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
	replica := replicaContainer(t, primary, source)
	program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
	if _, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err == nil || ctx.Err() != nil {
		t.Fatal("additional tablespace did not receive a bounded refusal")
	}
	replica.waitPhase(t, "failed")
	replica.docker(t, "exec", replica.name, "test", "!", "-e", "/tmp/extra-tablespace")
	replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
	if reason := replica.docker(t, "exec", replica.name, "cat", InitializationDirectory+"/replica-failed"); reason != "failed:archive\n" {
		t.Fatalf("tablespace refusal: %q", reason)
	}
}

func TestPostgresNativeCloneRejectsChangedContainerIdentity(t *testing.T) {
	primary := bootstrapContainer(t, "")
	primary.connect(t)
	source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
	replica := replicaContainer(t, primary, source)
	source.ResourceID = uuid.NewString()
	program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
	if _, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err == nil || ctx.Err() != nil {
		t.Fatal("changed clone identity did not receive a bounded refusal")
	}
	replica.waitPhase(t, "failed")
	replica.docker(t, "exec", replica.name, "test", "!", "-e", DataDirectory+"/.synod-clone-intent")
	replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
}

func TestPostgresNativeCloneRefusesUnsafeArchiveAndFailedVerification(t *testing.T) {
	for _, name := range []string{"parent path", "symbolic link", "checksum", "replica receipt"} {
		t.Run(name, func(t *testing.T) {
			primary := bootstrapContainer(t, "")
			primary.connect(t)
			if name == "replica receipt" {
				primary.docker(t, "exec", "--user", "postgres", primary.name, "sh", "-c", "printf 'existing-member\\n' > "+DataDirectory+"/.synod-replica-receipt")
			}
			source := ReplicaSource{Address: strings.TrimSpace(primary.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", primary.name)), Port: 5432, ResourceID: primary.command[7]}
			replica := replicaContainer(t, primary, source)
			program, err := CloneReplicaCommand(replica.config, source, replica.command[5], replica.command[6], replica.command[7], uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			wantStage := "archive"
			switch name {
			case "parent path":
				program[2] = strings.Replace(program[2], "failure_stage=archive", `failure_stage=archive
    printf 'outside\n' > "$stage/escape"
    tar --create --file="$stage/archives/base.tar" --transform='s,^,../,' --directory="$stage" escape`, 1)
			case "symbolic link":
				program[2] = strings.Replace(program[2], "failure_stage=archive", `failure_stage=archive
    ln -s /tmp/synod-escape "$stage/escape"
    tar --create --file="$stage/archives/base.tar" --directory="$stage" escape`, 1)
			case "checksum":
				wantStage = "backup_verification"
				program[2] = strings.Replace(program[2], "failure_stage=backup_verification", `failure_stage=backup_verification
    printf 'corrupt\n' >> "$stage/data/PG_VERSION"`, 1)
			case "replica receipt":
				wantStage = "backup_verification"
			}
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			args := append([]string{"--host", replica.host, "exec", "--user", "postgres", replica.name}, program...)
			if _, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err == nil || ctx.Err() != nil {
				t.Fatal("unsafe backup did not receive a bounded refusal")
			}
			replica.waitPhase(t, "failed")
			replica.docker(t, "exec", replica.name, "test", "!", "-e", ReplicaDataDirectory)
			replica.docker(t, "exec", replica.name, "test", "!", "-e", "/tmp/synod-escape")
			if reason := replica.docker(t, "exec", replica.name, "cat", InitializationDirectory+"/replica-failed"); reason != "failed:"+wantStage+"\n" {
				t.Fatalf("backup refusal: %q", reason)
			}
		})
	}
}
