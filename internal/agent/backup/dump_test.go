package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
)

// dockerCommand checks native tool results in disposable containers only.
func dockerCommand(ctx context.Context, api *client.Client, id, command string) ([]byte, error) {
	exec, err := api.ExecCreate(ctx, id, client.ExecCreateOptions{Cmd: []string{"sh", "-ec", command}, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return nil, err
	}
	stream, err := api.ExecAttach(ctx, exec.ID, client.ExecAttachOptions{})
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	stop := context.AfterFunc(ctx, stream.Close)
	defer stop()
	var output, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&output, &stderr, stream.Reader); err != nil {
		return nil, err
	}
	for deadline := time.Now().Add(time.Second); ; {
		result, err := api.ExecInspect(ctx, exec.ID, client.ExecInspectOptions{})
		if err != nil {
			return nil, err
		}
		if !result.Running {
			if result.ExitCode != 0 {
				return nil, fmt.Errorf("fixture command failed (%d): %s", result.ExitCode, stderr.String())
			}
			return output.Bytes(), nil
		}
		if time.Now().After(deadline) {
			return nil, errors.New("fixture command did not exit")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWorkerDumpsAndRestoresRunningDatabases(t *testing.T) {
	for _, db := range []struct {
		name, image, client, seed, check string
		env, cmd                         []string
	}{
		{name: "postgres", image: "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0", env: []string{"POSTGRES_USER=app_admin", "POSTGRES_PASSWORD=private-dump-password"}, client: `psql -v ON_ERROR_STOP=1 -U app_admin -d postgres -At`, seed: `CREATE TABLE proof(id integer PRIMARY KEY, value text); INSERT INTO proof VALUES (1, 'private-dump-value'); CREATE ROLE proof_user LOGIN;`, check: `SELECT value FROM proof WHERE id=1;`},
		{name: "mysql", image: "mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242", env: []string{"MYSQL_ROOT_PASSWORD_FILE=/tmp/synod-password"}, client: `MYSQL_PWD=$(cat "$MYSQL_ROOT_PASSWORD_FILE") mysql -u root -N`, seed: `CREATE DATABASE proof; CREATE TABLE proof.data(id INT PRIMARY KEY, value TEXT); INSERT INTO proof.data VALUES (1, 'private-dump-value'); CREATE USER 'proof_user'@'%' IDENTIFIED BY 'proof_password'; GRANT SELECT ON proof.* TO 'proof_user'@'%';`, check: `SELECT value FROM proof.data WHERE id=1;`},
		{name: "mariadb", image: "mariadb@sha256:6422478cb8e159f080fb1d8ccf65101e26fe51385787fde7d16c3b165a331f15", env: []string{"MARIADB_ROOT_PASSWORD=private-dump-password"}, client: `MYSQL_PWD=$MARIADB_ROOT_PASSWORD mariadb -u root -N`, seed: `CREATE DATABASE proof; CREATE TABLE proof.data(id INT PRIMARY KEY, value TEXT); INSERT INTO proof.data VALUES (1, 'private-dump-value'); CREATE USER 'proof_user'@'%' IDENTIFIED BY 'proof_password'; GRANT SELECT ON proof.* TO 'proof_user'@'%';`, check: `SELECT value FROM proof.data WHERE id=1;`},
		{name: "redis", image: "redis@sha256:d30960f73a599496d8b2802c97758bab6b1cd421fd06337f837779c47a57e1f3", cmd: []string{"redis-server", "--requirepass", "private-dump-password"}, client: `REDISCLI_AUTH=private-dump-password redis-cli --raw`, seed: `SET proof private-dump-value`, check: `GET proof`},
	} {
		t.Run(db.name, func(t *testing.T) {
			fixture := newWorkerFixture(t, backup.Auto, ":")
			api := fixture.worker.Docker
			project := strings.TrimPrefix(fixture.request.SourceKey, "compose:")
			labels := map[string]string{"synod.test": project, "com.docker.compose.project": project, "com.docker.compose.project.working_dir": fixture.project, "com.docker.compose.project.config_files": filepath.Join(fixture.project, "compose.yaml"), "com.docker.compose.service": "database"}
			create := func(env []string, selected map[string]string) string {
				t.Helper()
				c, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Config: &container.Config{Image: db.image, Env: env, Cmd: db.cmd, Labels: selected}, HostConfig: &container.HostConfig{NetworkMode: "none"}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if _, err := api.ContainerRemove(context.Background(), c.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
						t.Error(err)
					}
				})
				if db.name == "mysql" {
					dockerFile(t, api, c.ID, "/tmp", "synod-password", []byte("private-dump-password"))
				}
				return c.ID
			}
			start := func(id, command string) {
				t.Helper()
				if _, err := api.ContainerStart(t.Context(), id, client.ContainerStartOptions{}); err != nil {
					t.Fatal(err)
				}
				for deadline := time.Now().Add(90 * time.Second); ; {
					process := map[string]string{"postgres": "postgres", "mysql": "mysqld", "mariadb": "mariadbd", "redis": "redis-server"}[db.name]
					if _, err := dockerCommand(t.Context(), api, id, `[ "$(cat /proc/1/comm)" = "`+process+`" ]; `+command); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("database did not become ready")
					}
					time.Sleep(200 * time.Millisecond)
				}
			}
			input := func(query string) string {
				return "printf '%s\\n' " + "'" + strings.ReplaceAll(query, "'", "'\"'\"'") + "' | " + db.client
			}
			ready := input("SELECT 1;")
			if db.name == "redis" {
				ready = db.client + " PING"
			}
			id := create(db.env, labels)
			start(id, ready)
			if _, err := dockerCommand(t.Context(), api, id, input(db.seed)); err != nil {
				t.Fatal(err)
			}
			before, err := api.ContainerInspect(t.Context(), id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var writes atomic.Int64
			writerCtx, stopWrites := context.WithCancel(t.Context())
			writerDone := make(chan struct{})
			writerErrors := make(chan error, 1)
			go func() {
				defer close(writerDone)
				for writerCtx.Err() == nil {
					query := "INSERT INTO proof VALUES (" + fmt.Sprint(writes.Load()+2) + ", 'concurrent');"
					if db.name == "mysql" || db.name == "mariadb" {
						query = strings.Replace(query, "INTO proof VALUES", "INTO proof.data VALUES", 1)
					}
					if db.name == "redis" {
						query = "INCR concurrent"
					}
					if _, err := dockerCommand(writerCtx, api, id, input(query)); err != nil {
						if writerCtx.Err() == nil {
							writerErrors <- err
						}
						return
					}
					writes.Add(1)
				}
			}()
			defer func() { stopWrites(); <-writerDone }()
			for writes.Load() < 3 {
				select {
				case <-writerDone:
					t.Fatal("writer stopped")
				case <-time.After(20 * time.Millisecond):
				}
			}
			completedBefore := writes.Load()
			result, err := fixture.worker.Execute(t.Context(), fixture.request)
			stopWrites()
			<-writerDone
			select {
			case err := <-writerErrors:
				t.Fatalf("backup interrupted the database writer: %v", err)
			default:
			}
			// The deferred read also needs a closed channel, so this is safe.
			if err != nil || result.FailureCode != "" || result.SnapshotID != fixture.request.ID {
				t.Fatalf("backup: %+v %v", result, err)
			}
			if writes.Load() <= completedBefore {
				t.Fatal("database received no writes during backup")
			}
			after, err := api.ContainerInspect(t.Context(), id, client.ContainerInspectOptions{})
			if err != nil || after.Container.State.StartedAt != before.Container.State.StartedAt {
				t.Fatal("database was restarted")
			}
			r, err := repository.Open(t.Context(), fixture.location, fixture.credentials)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close(context.Background())
			saved, err := r.Snapshot(t.Context(), result.SnapshotID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Method != backup.Dump || len(saved.Dumps) != 1 || saved.Dumps[0].ContainerID != id || saved.Dumps[0].Database != db.name {
				t.Fatalf("dump metadata missing: %+v", saved.Dumps)
			}
			found := false
			for _, path := range saved.App.Definition.Paths {
				if path.ContainerId == id && path.Status == "Dumped" {
					found = true
					for _, copied := range saved.Paths {
						if copied.Source == path.Source {
							t.Fatal("raw database data was copied")
						}
					}
				}
			}
			if !found {
				t.Fatal("database directory is not Dumped")
			}
			target := filepath.Join(t.TempDir(), "restored")
			if err := r.RestoreFiles(t.Context(), result.SnapshotID, target); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(target, saved.Dumps[0].Archive))
			if err != nil || len(data) == 0 {
				t.Fatal("dump was not restored")
			}
			public, _ := json.Marshal(result)
			if bytes.Contains(public, []byte("private-dump")) {
				t.Fatal("result contains private data")
			}
			restoreEnv := db.env
			if db.name == "postgres" {
				restoreEnv = []string{"POSTGRES_USER=synod_restore", "POSTGRES_PASSWORD=private-dump-password"}
			}
			restoredID := create(restoreEnv, map[string]string{"synod.test": project})
			restoreClient := db.client
			if db.name == "postgres" {
				restoreClient = strings.ReplaceAll(restoreClient, "app_admin", "synod_restore")
			}
			if db.name != "redis" {
				start(restoredID, "printf 'SELECT 1;\\n' | "+restoreClient)
			}
			path, name := "/tmp", "synod.sql"
			if db.name == "redis" {
				path, name = "/data", "dump.rdb"
			}
			dockerFile(t, api, restoredID, path, name, data)
			if db.name == "redis" {
				start(restoredID, ready)
			} else if _, err := dockerCommand(t.Context(), api, restoredID, restoreClient+" < /tmp/synod.sql"); err != nil {
				t.Fatal(err)
			}
			value, err := dockerCommand(t.Context(), api, restoredID, input(db.check))
			if err != nil || strings.TrimSpace(string(value)) != "private-dump-value" {
				t.Fatalf("restored content differs: %q %v", value, err)
			}
			countQuery := "SELECT count(*) FROM proof WHERE id>1;"
			if db.name == "mysql" || db.name == "mariadb" {
				countQuery = "SELECT count(*) FROM proof.data WHERE id>1;"
			}
			if db.name == "redis" {
				countQuery = "GET concurrent"
			}
			value, err = dockerCommand(t.Context(), api, restoredID, input(countQuery))
			var restoredWrites int64
			if _, scanErr := fmt.Sscan(string(value), &restoredWrites); err != nil || scanErr != nil || restoredWrites < completedBefore {
				t.Fatalf("lost completed writes: got %q need %d: %v", value, completedBefore, err)
			}
			if db.name != "redis" {
				query := "SELECT count(*) FROM mysql.user WHERE User='proof_user';"
				if db.name == "postgres" {
					query = "SELECT count(*) FROM pg_roles WHERE rolname='proof_user';"
				}
				value, err := dockerCommand(t.Context(), api, restoredID, input(query))
				if err != nil || strings.TrimSpace(string(value)) != "1" {
					t.Fatalf("database user was not restored: %q %v", value, err)
				}
			}
			// Stop quiesces the file container, but never stops the native database.
			request := fixture.request
			request.ID, request.Method, request.Initialize = uuid.New(), backup.Stop, false
			stopped, err := fixture.worker.Execute(t.Context(), request)
			if err != nil || stopped.FailureCode != "" || stopped.DowntimeMillis <= 0 {
				t.Fatalf("Stop dump: %+v %v", stopped, err)
			}
			after, err = api.ContainerInspect(t.Context(), id, client.ContainerInspectOptions{})
			if err != nil || after.Container.State.StartedAt != before.Container.State.StartedAt {
				t.Fatal("Stop restarted the database")
			}
			stopTarget := filepath.Join(t.TempDir(), "stop-restore")
			if err := r.RestoreFiles(t.Context(), stopped.SnapshotID, stopTarget); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(stopTarget, saved.Dumps[0].Archive)); err != nil || !bytes.Contains(data, []byte("private-dump-value")) {
				t.Fatal("Stop lost the native dump")
			}
			if db.name == "redis" {
				if _, err := dockerCommand(t.Context(), api, id, db.client+" CONFIG SET requirepass changed-password"); err != nil {
					t.Fatal(err)
				}
				request.ID = uuid.New()
				failed, err := fixture.worker.Execute(t.Context(), request)
				if err != nil || failed.FailureCode != "database_dump_failed" || failed.SnapshotID != uuid.Nil {
					t.Fatalf("bad credentials accepted: %+v %v", failed, err)
				}
				if _, err := r.Snapshot(t.Context(), request.ID); !errors.Is(err, repository.ErrSnapshotMissing) {
					t.Fatal("failed dump published a snapshot")
				}
				app, err := api.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
				if err != nil || !app.Container.State.Running {
					t.Fatal("failed dump left the app stopped")
				}
				if err := (backup.Event{ID: request.ID, Status: "failed", Result: failed}).Validate(request); err != nil {
					t.Fatal("failed result cannot reach controller")
				}
				if _, err := api.ContainerStop(t.Context(), id, client.ContainerStopOptions{}); err != nil {
					t.Fatal(err)
				}
				request.ID = uuid.New()
				failed, err = fixture.worker.Execute(t.Context(), request)
				if err != nil || failed.FailureCode != "database_unavailable" {
					t.Fatalf("stopped database accepted: %+v %v", failed, err)
				}
			}

		})
	}
}

func TestDumpRejectsPartialOutputAndCancels(t *testing.T) {
	for _, command := range []string{"printf private-data; printf private-password >&2; exit 1", "printf private-password >&2; exit 0", "exec sleep 300"} {
		t.Run(command[:5], func(t *testing.T) {
			fixture := newWorkerFixture(t, backup.Live, ":")
			c, err := fixture.worker.Docker.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
			if err != nil {
				t.Fatal(err)
			}
			d, _, err := newDump(fixture.worker.Docker, c.Container, "mysql")
			if err != nil {
				t.Fatal(err)
			}
			d.command = command
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			reader, err := d.GetReader(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.ReadAll(reader)
			reader.Close()
			if !errors.Is(err, errDump) || !d.failed.Load() {
				t.Fatalf("failed command accepted: %v", err)
			}
			for deadline := time.Now().Add(2 * time.Second); ; {
				output, err := dockerCommand(t.Context(), fixture.worker.Docker, fixture.id, "ps -o args")
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(output), "sleep 300") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("cancel left the dump process running")
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

func TestDumpCancellationKeepsPostgresRunning(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Auto, ":")
	api := fixture.worker.Docker
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Config: &container.Config{
		Image: "postgres@sha256:051f7b7b3abdd564d5d1bd1e8c4b9c1b6e77087d1dd22020ede611c096a272e0",
		Env:   []string{"POSTGRES_USER=app_admin", "POSTGRES_PASSWORD=private-dump-password"},
	}, HostConfig: &container.HostConfig{NetworkMode: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		api.ContainerRemove(context.Background(), created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	})
	if _, err := api.ContainerStart(t.Context(), created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	const sql = "psql -v ON_ERROR_STOP=1 -U app_admin -d postgres -qAt -c "
	for deadline := time.Now().Add(90 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if _, err := dockerCommand(t.Context(), api, created.ID, `[ "$(cat /proc/1/comm)" = postgres ]; `+sql+`'SELECT 1'`); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PostgreSQL did not start")
		}
	}
	if _, err := dockerCommand(t.Context(), api, created.ID, sql+`'CREATE TABLE blocked(id integer)'`); err != nil {
		t.Fatal(err)
	}
	lockCtx, unlock := context.WithCancel(t.Context())
	lockDone := make(chan error, 1)
	go func() {
		_, err := dockerCommand(lockCtx, api, created.ID, sql+`'BEGIN; LOCK blocked IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(6); COMMIT'`)
		lockDone <- err
		close(lockDone)
	}()
	defer func() { unlock(); <-lockDone }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		output, err := dockerCommand(t.Context(), api, created.ID, sql+`"SELECT count(*) FROM pg_locks WHERE relation='blocked'::regclass AND mode='AccessExclusiveLock' AND granted"`)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(output)) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the fixture did not lock its table")
		}
	}
	inspected, err := api.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dump, _, err := newDump(api, inspected.Container, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader, err := dump.GetReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, reader); done <- err }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		output, err := dockerCommand(t.Context(), api, created.ID, sql+`"SELECT count(*) FROM pg_stat_activity WHERE application_name='pg_dump' AND wait_event_type='Lock'"`)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(output)) == "1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pg_dump did not wait for the locked table")
		}
	}
	cancel()
	reader.Close()
	if err := <-done; err == nil {
		t.Fatal("cancelled dump succeeded")
	}
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		output, err := dockerCommand(t.Context(), api, created.ID, sql+`"SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('pg_dump','pg_dumpall')"`)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(output)) == "0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel did not release the database dump connections")
		}
	}
	if err := <-lockDone; err != nil {
		t.Fatalf("dump cancellation interrupted another database connection: %v", err)
	}
	if _, err := dockerCommand(t.Context(), api, created.ID, `for file in /proc/[0-9]*/comm; do read -r name < "$file" || continue; [ "$name" != pg_dump ] && [ "$name" != pg_dumpall ] || exit 1; done`); err != nil {
		t.Fatalf("cancel left a dump process: %v", err)
	}
}

func TestDumpDeadlineStopsTheCommandWithoutClosingStdin(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Live, ":")
	for _, signal := range []string{"INT", "TERM"} {
		t.Run(signal, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := time.Now()
			created, err := fixture.worker.Docker.ExecCreate(ctx, fixture.id, client.ExecCreateOptions{Cmd: []string{"sh", "-c", dumpSupervisor, "synod-dump", "printf ready; exec sleep 300", signal, "1"}, AttachStdin: true, AttachStdout: true, AttachStderr: true})
			if err != nil {
				t.Fatal(err)
			}
			attached, err := fixture.worker.Docker.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer attached.Close()
			stop := context.AfterFunc(ctx, attached.Close)
			defer stop()
			var output bytes.Buffer
			if _, err := stdcopy.StdCopy(&output, io.Discard, attached.Reader); err != nil || ctx.Err() != nil || output.String() != "ready" || time.Since(started) < 500*time.Millisecond {
				t.Fatalf("deadline did not stop a started command: %v %v", err, ctx.Err())
			}
			for {
				status, err := fixture.worker.Docker.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if !status.Running {
					if status.ExitCode == 0 {
						t.Fatal("deadline reported success")
					}
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestDumpRefusesMissingTimeout(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Live, ":")
	api := fixture.worker.Docker
	if _, err := dockerCommand(t.Context(), api, fixture.id, "mkdir /tmp/only-shell; ln -s /bin/sh /tmp/only-shell/sh"); err != nil {
		t.Fatal(err)
	}
	c, err := api.ContainerInspect(t.Context(), fixture.id, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dump, _, err := newDump(api, c.Container, "mysql")
	if err != nil {
		t.Fatal(err)
	}
	dump.env = []string{"PATH=/tmp/only-shell"}
	dump.command = "printf should-not-run"
	reader, err := dump.GetReader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if !errors.Is(err, errDump) || len(data) != 0 || !dump.failed.Load() {
		t.Fatalf("dump ran without its deadline tool: %q %v", data, err)
	}
}

func dockerFile(t *testing.T, api *client.Client, id, path, name string, data []byte) {
	t.Helper()
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: int64(len(data)), Uid: 999, Gid: 999}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := api.CopyToContainer(t.Context(), id, client.CopyToContainerOptions{DestinationPath: path, Content: &archive}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRefusesDumpWithoutDatabase(t *testing.T) {
	fixture := newWorkerFixture(t, backup.Dump, ":")
	result, err := fixture.worker.Execute(t.Context(), fixture.request)
	if err != nil || result.FailureCode != "database_not_detected" || result.SnapshotID != uuid.Nil {
		t.Fatalf("Dump accepted a file app: %+v %v", result, err)
	}
	if err := (backup.Event{ID: fixture.request.ID, Status: "failed", Result: result}).Validate(fixture.request); err != nil {
		t.Fatal(err)
	}
	r, err := repository.Open(t.Context(), fixture.location, fixture.credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if _, err := r.Snapshot(t.Context(), fixture.request.ID); !errors.Is(err, repository.ErrSnapshotMissing) {
		t.Fatal("invalid Dump published a snapshot")
	}
}
