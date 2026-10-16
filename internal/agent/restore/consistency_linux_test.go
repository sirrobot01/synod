package restore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/backup"
)

// The directories must be on disposable snapshot-capable filesystems visible to
// both this process and Docker. Separate ZFS datasets must belong to one pool.
func TestFilesystemSnapshotRestoresLinkedDatabaseAndFiles(t *testing.T) {
	top := os.Getenv("SYNOD_TEST_SNAPSHOT_DIR")
	if top == "" {
		t.Skip("set SYNOD_TEST_SNAPSHOT_DIR for whole-app snapshot recovery")
	}
	f := newFixture(t, false)
	f.worker.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	api := f.worker.Docker
	databaseTop := os.Getenv("SYNOD_TEST_DATABASE_DIR")
	if databaseTop == "" {
		databaseTop = top
	}
	data, err := os.MkdirTemp(databaseTop, "synod-database-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(data) })
	linked := filepath.Join(f.project, "linked")
	if err := os.Mkdir(linked, 0755); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"synod.test": f.name, "com.docker.compose.project": f.name, "com.docker.compose.service": "db", "com.docker.compose.project.working_dir": f.project, "com.docker.compose.project.config_files": filepath.Join(f.project, "compose.yaml")}
	created, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: f.name + "-db", Config: &container.Config{Image: postgresImage, Env: []string{"POSTGRES_USER=app_admin", "POSTGRES_PASSWORD=private-test-password"}, Labels: labels}, HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: []mount.Mount{{Type: mount.TypeBind, Source: data, Target: "/var/lib/postgresql/data"}, {Type: mount.TypeBind, Source: linked, Target: "/linked"}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		api.ContainerRemove(context.Background(), created.ID, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	})
	if _, err := api.ContainerStart(t.Context(), created.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	query(t, api, f.name+"-db", "CREATE TABLE linked(id integer PRIMARY KEY, value text NOT NULL)")
	original, err := api.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Each committed row refers to an already durable, immutable file. A capture
	// may contain an extra file from an unfinished transaction, but no missing one.
	var committed atomic.Int64
	writerContext, cancelWriter := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		for id := int64(1); ; id++ {
			select {
			case <-writerContext.Done():
				done <- nil
				return
			default:
			}
			value := fmt.Sprintf("linked-%d", id)
			file, err := os.Create(filepath.Join(linked, strconv.FormatInt(id, 10)))
			if err == nil {
				_, err = file.WriteString(value)
				if err == nil {
					err = file.Sync()
				}
				closeErr := file.Close()
				if err == nil {
					err = closeErr
				}
			}
			if err == nil {
				err = execCommand(writerContext, api, created.ID, []string{"psql", "-v", "ON_ERROR_STOP=1", "-U", "app_admin", "-d", "postgres", "-c", fmt.Sprintf("INSERT INTO linked VALUES (%d, '%s')", id, value)}, nil, nil)
			}
			if err != nil {
				if writerContext.Err() != nil {
					err = nil
				}
				done <- err
				return
			}
			committed.Store(id)
		}
	}()
	t.Cleanup(func() {
		cancelWriter()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	for deadline := time.Now().Add(time.Minute); committed.Load() < 10; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("writer did not commit its initial rows")
		}
	}
	before := committed.Load()
	request := backup.Request{ID: uuid.New(), InstallationID: f.worker.InstallationID, NodeID: f.worker.NodeID, AppID: f.app.ID, SourceKey: f.app.Definition.Key, DestinationID: f.destinationID, Method: backup.Auto, RequestedAt: time.Now().UTC()}
	result, err := f.worker.Execute(t.Context(), request)
	if err != nil || result.FailureCode != "" || committed.Load() <= before {
		t.Fatalf("backup did not complete while writes continued: %+v, %v", result, err)
	}
	after := committed.Load()
	t.Logf("backup completed while writes advanced from %d to %d", before, after)
	current, err := api.ContainerInspect(t.Context(), created.ID, client.ContainerInspectOptions{})
	if err != nil || !current.Container.State.Running || current.Container.State.Paused || current.Container.State.StartedAt != original.Container.State.StartedAt {
		t.Fatalf("backup interrupted the source database: %v", err)
	}
	saved, err := f.repo.Snapshot(t.Context(), result.SnapshotID)
	if err != nil || saved.Consistency != backup.ConsistencyFilesystem || len(saved.Dumps) != 1 {
		t.Fatalf("snapshot did not establish whole-app consistency: %+v, %v", saved.Captures, err)
	}
	if files, err := databaseFiles(saved, created.ID); err != nil || !files {
		t.Fatalf("snapshot cannot restore the captured database files: %t, %v", files, err)
	}

	// The restore uses the captured database files. The older dump must not
	// replace them. Keep the source writing while its isolated copy starts.
	id := uuid.New()
	t.Cleanup(func() { removeLabelled(context.Background(), api, id, true) })
	directory := t.TempDir()
	p, err := makePlan(t.Context(), f.repo, saved, id, directory, f.worker.Architecture, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := isolateDrill(p, id, directory); err != nil {
		t.Fatal(err)
	}
	if err := loadImages(t.Context(), api, f.repo, p, f.worker.Architecture); err != nil {
		t.Fatal(err)
	}
	helper, err := prepareData(t.Context(), api, f.repo, p, id, directory)
	if helper != "" {
		t.Cleanup(func() {
			api.ContainerRemove(context.Background(), helper, client.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareDatabases(t.Context(), api, f.repo, p, id); err != nil {
		t.Fatal(err)
	}
	// Start only the restored database. It has no published port or host access.
	service := p.Definition.Services["db"]
	var restoredMounts []mount.Mount
	for _, volume := range service.Volumes {
		restoredMounts = append(restoredMounts, mount.Mount{Type: mount.TypeBind, Source: volume.Source, Target: volume.Target})
	}
	restored, err := api.ContainerCreate(t.Context(), client.ContainerCreateOptions{Name: service.ContainerName, Config: &container.Config{Image: service.Image, Env: []string{"POSTGRES_USER=app_admin", "POSTGRES_PASSWORD=private-test-password"}, Labels: service.Labels}, HostConfig: &container.HostConfig{NetworkMode: "none", Mounts: restoredMounts}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerStart(t.Context(), restored.ID, client.ContainerStartOptions{}); err != nil {
		t.Fatal(err)
	}
	counts := strings.Split(query(t, api, restored.ID, "SELECT count(*), count(*) FILTER (WHERE pg_read_file('/linked/' || id) <> value) FROM linked"), "|")
	rows, err := strconv.ParseInt(counts[0], 10, 64)
	if err != nil || len(counts) != 2 || rows < before || rows > after || counts[1] != "0" {
		t.Fatalf("restored rows and files disagree: %v; committed range %d..%d", counts, before, after)
	}
	t.Logf("restored %d linked rows and files; writes continued from %d to %d during backup", rows, before, after)
}
