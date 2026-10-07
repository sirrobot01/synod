package repository

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

// clockOffset moves the repository clock. New blobs get the moved time as their timestamp.
var clockOffset atomic.Int64

type clockOptions struct {
	Path string `json:"path"`
}

type clockStorage struct {
	blob.Storage
	options clockOptions
}

func init() {
	blob.AddSupportedStorage("synod-test-clock", clockOptions{}, func(ctx context.Context, options *clockOptions, create bool) (blob.Storage, error) {
		st, err := filesystem.New(ctx, &filesystem.Options{Path: options.Path, FileMode: 0600, DirectoryMode: 0700}, create)
		if err != nil {
			return nil, err
		}
		return &clockStorage{Storage: st, options: *options}, nil
	})
}

func shifted(t time.Time) time.Time { return t.Add(time.Duration(clockOffset.Load())) }

func (s *clockStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{Type: "synod-test-clock", Config: &s.options}
}

func (s *clockStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if opts.SetModTime.IsZero() {
		opts.SetModTime = shifted(time.Now())
	}
	return s.Storage.PutBlob(ctx, id, data, opts)
}

func TestMaintenanceReclaimsExpiredContentWithOneOwner(t *testing.T) {
	clockOffset.Store(0)
	t.Cleanup(func() { clockOffset.Store(0) })
	now := func() time.Time { return shifted(time.Now()) }
	advance := func(d time.Duration) { clockOffset.Add(int64(d)) }
	const password = "maintenance-test-password"
	st := &clockStorage{options: clockOptions{Path: t.TempDir()}}
	base, err := filesystem.New(t.Context(), &filesystem.Options{Path: st.options.Path, FileMode: 0600, DirectoryMode: 0700}, true)
	if err != nil {
		t.Fatal(err)
	}
	st.Storage = base
	defer st.Close(context.Background())
	if err := repo.Initialize(t.Context(), st, nil, password); err != nil {
		t.Fatal(err)
	}
	connect := func(host string) *Repository {
		t.Helper()
		r, err := open(t.Context(), st, Credentials{Password: password}, host, now)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	packBytes := func() int64 {
		t.Helper()
		var total int64
		if err := st.ListBlobs(t.Context(), "p", func(m blob.Metadata) error { total += m.Length; return nil }); err != nil {
			t.Fatal(err)
		}
		return total
	}
	catalog := testCatalog()
	app, destination := catalog.Apps[0], uuid.New()
	writer := connect("synod")
	defer func() { writer.Close(context.Background()) }()
	contents := map[string][]byte{}
	upload := func(name string, size int) *snapshot.Manifest {
		t.Helper()
		source := t.TempDir()
		data := make([]byte, size)
		rand.Read(data)
		contents[name] = data
		if err := os.WriteFile(filepath.Join(source, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		root, err := localfs.NewEntry(source)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		prepared, err := writer.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: app.NodeID.String(), UserName: "synod", Path: app.ID.String()}, false)
		if err != nil {
			t.Fatal(err)
		}
		return prepared
	}
	publish := func(prepared *snapshot.Manifest) uuid.UUID {
		t.Helper()
		at := now().UTC()
		metadata := backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: destination, App: app, Method: backup.Stop, Architecture: "arm64", StartedAt: at.Add(-time.Minute), CompletedAt: at}
		if _, err := writer.PublishSnapshot(t.Context(), prepared, metadata); err != nil {
			t.Fatal(err)
		}
		return metadata.ID
	}
	maintain := func(node uuid.UUID, revision int64) (MaintenanceState, error) {
		r := connect(node.String())
		defer r.Close(context.Background())
		return r.maintain(t.Context(), catalog.InstallationID, node, revision)
	}

	expired := publish(upload("expired", 16<<20))
	advance(time.Minute)
	kept := publish(upload("kept", 1<<20))
	if removed, err := writer.ApplyRetention(t.Context(), catalog.InstallationID, app.ID, destination, backup.Retention{}, kept); err != nil || len(removed) != 1 || removed[0] != expired {
		t.Fatalf("retention removed %v: %v", removed, err)
	}
	before := packBytes()

	owner, other := uuid.New(), uuid.New()
	if state, err := maintain(owner, 1); err != nil || state != MaintenanceClaimed {
		t.Fatalf("first claim: %s, %v", state, err)
	}
	if state, err := maintain(owner, 1); err != nil || state != MaintenanceHandingOver {
		t.Fatalf("run during handover: %s, %v", state, err)
	}
	if _, err := maintain(other, 1); !errors.Is(err, ErrMaintenanceNotOwner) {
		t.Fatalf("second device with the same plan revision: %v", err)
	}

	// A backup copies its files before maintenance and publishes them after it.
	advance(MaintenanceHandover)
	inFlight := upload("in-flight", 1<<20)
	advance(3 * time.Hour)
	if state, err := maintain(owner, 1); err != nil || state != MaintenanceCompleted {
		t.Fatalf("maintenance: %s, %v", state, err)
	}
	concurrent := publish(inFlight)
	// Daily backups of another app write the index blobs that let Kopia advance its index epochs.
	// Kopia drops deleted content and frees its pack only after an epoch compaction.
	churn := app
	churn.ID = uuid.New()
	reclaimedOn := 0
	var churned uuid.UUID
	for day := 1; day <= 30 && reclaimedOn == 0; day++ {
		advance(24 * time.Hour)
		var state MaintenanceState
		var maintained error
		running := make(chan struct{})
		go func() {
			defer close(running)
			state, maintained = maintain(owner, 1)
		}()
		at := now().UTC()
		metadata := backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: destination, App: churn, Method: backup.Stop, Architecture: "arm64", StartedAt: at.Add(-time.Minute), CompletedAt: at}
		if _, err := writer.PublishSnapshot(t.Context(), upload("churn", 64<<10), metadata); err != nil {
			t.Fatalf("backup during maintenance on day %d: %v", day, err)
		}
		<-running
		if maintained != nil || state != MaintenanceCompleted {
			t.Fatalf("maintenance on day %d: %s, %v", day, state, maintained)
		}
		if _, err := writer.ApplyRetention(t.Context(), catalog.InstallationID, churn.ID, destination, backup.Retention{}, metadata.ID); err != nil {
			t.Fatal(err)
		}
		churned = metadata.ID
		if before-packBytes() >= 14<<20 {
			reclaimedOn = day
		}
	}
	t.Logf("expired content reclaimed after %d days", reclaimedOn)
	after := packBytes()
	if before-after < 14<<20 {
		t.Fatalf("maintenance reclaimed %d bytes of %d packed bytes, want most of the expired 16 MiB", before-after, before)
	}

	reopened, err := Open(t.Context(), Location{Kind: "local", Path: st.options.Path}, Credentials{Password: password})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	for name, id := range map[string]uuid.UUID{"kept": kept, "in-flight": concurrent, "churn": churned} {
		target := filepath.Join(t.TempDir(), "restored")
		if err := reopened.RestoreFiles(t.Context(), id, target); err != nil {
			t.Fatalf("restore %s snapshot after maintenance: %v", name, err)
		}
		data, err := os.ReadFile(filepath.Join(target, name))
		if err != nil || string(data) != string(contents[name]) {
			t.Fatalf("restored %s content differs: %v", name, err)
		}
	}
	if _, err := reopened.Snapshot(t.Context(), expired); !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("expired snapshot remains: %v", err)
	}

	// A newer plan moves ownership. The old owner stops, and the new owner waits for the handover.
	if state, err := maintain(other, 2); err != nil || state != MaintenanceClaimed {
		t.Fatalf("handover claim: %s, %v", state, err)
	}
	if _, err := maintain(owner, 1); !errors.Is(err, ErrMaintenanceNotOwner) {
		t.Fatalf("old owner with a stale plan: %v", err)
	}
	if state, err := maintain(other, 2); err != nil || state != MaintenanceHandingOver {
		t.Fatalf("new owner during handover: %s, %v", state, err)
	}
	wrong := connect(owner.String())
	defer wrong.Close(context.Background())
	if _, err := wrong.maintain(t.Context(), catalog.InstallationID, other, 2); err == nil {
		t.Fatal("maintenance ran with another device's connection")
	}
}
