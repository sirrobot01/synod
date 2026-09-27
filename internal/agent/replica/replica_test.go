package replica

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

type fakeFreezer struct {
	freezeErr      error
	onFreeze       func()
	freezes, thaws int
}

func (f *fakeFreezer) Freeze(context.Context) error {
	f.freezes++
	if f.onFreeze != nil {
		f.onFreeze()
	}
	return f.freezeErr
}

func (f *fakeFreezer) Thaw(context.Context) error {
	f.thaws++
	return nil
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
}

func random(t *testing.T, n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

type fixture struct {
	live, snapshot string
	snapshotter    *Snapshotter
	freezer        *fakeFreezer
	replica        Replica
}

func newFixture(t *testing.T) *fixture {
	dir := t.TempDir()
	f := &fixture{live: filepath.Join(dir, "live"), snapshot: filepath.Join(dir, "snapshot"), freezer: &fakeFreezer{}, replica: Replica{Root: filepath.Join(dir, "standby")}}
	for _, d := range []string{f.live, f.snapshot} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	f.snapshotter = &Snapshotter{Live: f.live, Snapshot: f.snapshot, Freezer: f.freezer}
	return f
}

// fetchFrom serves blocks from the active device's snapshot and counts them.
func (f *fixture) fetchFrom(count *int) Fetch {
	return func(ref BlockRef) ([]byte, error) {
		*count++
		data, err := os.ReadFile(filepath.Join(f.snapshot, filepath.FromSlash(ref.Path)))
		if err != nil {
			return nil, err
		}
		end := min(len(data), (ref.Index+1)*BlockSize)
		return data[ref.Index*BlockSize : end], nil
	}
}

type entry struct {
	mode    fs.FileMode
	modTime time.Time
	link    string
	data    []byte
}

// tree reads a directory for comparison. Directory times are ignored because
// the walk itself can change them on some file systems.
func tree(t *testing.T, root string) map[string]entry {
	t.Helper()
	out := map[string]entry{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		e := entry{mode: info.Mode()}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			e.link, err = os.Readlink(path)
		case info.Mode().IsRegular():
			e.modTime = info.ModTime().UTC()
			e.data, err = os.ReadFile(path)
		}
		out[filepath.ToSlash(rel)] = e
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSnapshotRoundTrip(t *testing.T) {
	f := newFixture(t)
	big := random(t, 2*BlockSize+BlockSize/2)
	write(t, filepath.Join(f.live, "config", "app.db"), big)
	write(t, filepath.Join(f.live, "config", "empty"), nil)
	write(t, filepath.Join(f.live, "config", "nested", "config.xml"), []byte("<config/>"))
	if err := os.Chmod(filepath.Join(f.live, "config", "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/config.xml", filepath.Join(f.live, "config", "link")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(f.live, "config", "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if f.freezer.freezes != 1 || f.freezer.thaws != 1 {
		t.Fatalf("freezes %d, thaws %d", f.freezer.freezes, f.freezer.thaws)
	}
	if !reflect.DeepEqual(m.Skipped, []string{"config/pipe"}) {
		t.Fatalf("skipped: %v", m.Skipped)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(m, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	if fetched != 4 {
		t.Fatalf("fetched %d blocks, want 4", fetched)
	}
	want := tree(t, f.live)
	delete(want, "config/pipe")
	if got := tree(t, filepath.Join(f.replica.Root, replicaDir)); !reflect.DeepEqual(got, want) {
		t.Fatalf("replica differs from live:\n%v\n%v", got, want)
	}
	if current, err := f.replica.Current(); err != nil || current.Sequence != 1 {
		t.Fatalf("current: %+v %v", current, err)
	}
}

func TestIncrementalSnapshot(t *testing.T) {
	f := newFixture(t)
	db := random(t, 3*BlockSize)
	write(t, filepath.Join(f.live, "config", "app.db"), db)
	write(t, filepath.Join(f.live, "config", "app.db-journal"), []byte("hot"))
	write(t, filepath.Join(f.live, "config", "old"), []byte("old"))
	first, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(first, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	// Change one block, grow the file into a new block, delete two files, and add one.
	copy(db[BlockSize+10:], []byte("changed"))
	db = append(db, []byte("tail")...)
	write(t, filepath.Join(f.live, "config", "app.db"), db)
	for _, name := range []string{"app.db-journal", "old"} {
		if err := os.Remove(filepath.Join(f.live, "config", name)); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(f.live, "config", "new"), []byte("new"))
	second, err := f.snapshotter.Take(t.Context(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh snapshotter hashes every block. Reused hashes must agree with it.
	fresh := &Snapshotter{Live: f.live, Snapshot: t.TempDir(), Freezer: &fakeFreezer{}}
	full, err := fresh.Take(t.Context(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := range second.Files {
		if second.Files[i].Path != full.Files[i].Path || !reflect.DeepEqual(second.Files[i].Blocks, full.Files[i].Blocks) {
			t.Fatalf("reused hashes differ for %s", second.Files[i].Path)
		}
	}
	missing := Missing(first, second)
	want := []BlockRef{{"config/app.db", 1}, {"config/app.db", 3}, {"config/new", 0}}
	if !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing %v, want %v", missing, want)
	}
	fetched = 0
	if err := f.replica.Apply(second, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	if fetched != len(want) {
		t.Fatalf("fetched %d blocks, want %d", fetched, len(want))
	}
	if got, want := tree(t, filepath.Join(f.replica.Root, replicaDir)), tree(t, f.live); !reflect.DeepEqual(got, want) {
		t.Fatalf("replica differs from live")
	}
	if _, err := os.Lstat(filepath.Join(f.snapshot, "config", "app.db-journal")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("a deleted journal stayed in the snapshot")
	}
}

func TestFreezeTimeoutProducesNoManifest(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.live, "config", "app.db"), []byte("data"))
	f.freezer.freezeErr = ErrFreezeTimeout
	if m, err := f.snapshotter.Take(t.Context(), 1, 1); !errors.Is(err, ErrFreezeTimeout) || m != nil {
		t.Fatalf("manifest %v, error %v", m, err)
	}
	f.freezer.freezeErr = nil
	if _, err := f.snapshotter.Take(t.Context(), 1, 1); err != nil {
		t.Fatalf("the next snapshot failed: %v", err)
	}
}

func TestFailedFrozenCopyThaws(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a file without permissions")
	}
	f := newFixture(t)
	path := filepath.Join(f.live, "config", "app.db")
	write(t, path, []byte("data"))
	f.freezer.onFreeze = func() {
		write(t, path, []byte("changed while frozen"))
		if err := os.Chmod(path, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.snapshotter.Take(t.Context(), 1, 1); err == nil {
		t.Fatal("unreadable file accepted")
	}
	if f.freezer.thaws != 1 {
		t.Fatalf("thaws %d, want 1", f.freezer.thaws)
	}
}

func TestApplyRefusesBadInput(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.live, "config", "app.db"), random(t, BlockSize+1))
	first, err := f.snapshotter.Take(t.Context(), 2, 5)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(first, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	before := tree(t, filepath.Join(f.replica.Root, replicaDir))
	write(t, filepath.Join(f.live, "config", "app.db"), random(t, BlockSize+1))
	next, err := f.snapshotter.Take(t.Context(), 2, 6)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := func(ref BlockRef) ([]byte, error) {
		data, err := f.fetchFrom(&fetched)(ref)
		if len(data) > 0 {
			data = bytes.Clone(data)
			data[0] ^= 1
		}
		return data, err
	}
	if err := f.replica.Apply(next, corrupt); !errors.Is(err, ErrBlock) {
		t.Fatalf("corrupt block: %v", err)
	}
	if got := tree(t, filepath.Join(f.replica.Root, replicaDir)); !reflect.DeepEqual(got, before) {
		t.Fatal("a failed apply changed the replica")
	}
	for _, stale := range []struct{ epoch, sequence uint64 }{{2, 5}, {2, 4}, {1, 9}} {
		old := *next
		old.Epoch, old.Sequence = stale.epoch, stale.sequence
		if err := f.replica.Apply(&old, f.fetchFrom(&fetched)); !errors.Is(err, ErrStale) {
			t.Fatalf("epoch %d sequence %d: %v", stale.epoch, stale.sequence, err)
		}
	}
	if err := f.replica.Apply(next, f.fetchFrom(&fetched)); err != nil {
		t.Fatalf("valid apply after refusals: %v", err)
	}
}

func TestValidateRefusesUnsafePaths(t *testing.T) {
	dir := File{Path: "config", Type: TypeDir, Mode: 0o700}
	for name, files := range map[string][]File{
		"parent escape":      {{Path: "../x", Type: TypeDir}},
		"absolute":           {{Path: "/etc", Type: TypeDir}},
		"unclean":            {dir, {Path: "config/../config/x", Type: TypeFile}},
		"missing parent":     {{Path: "config/x", Type: TypeFile}},
		"through symlink":    {{Path: "config", Type: TypeSymlink, Link: "/etc"}, {Path: "config/passwd", Type: TypeFile}},
		"duplicate":          {dir, dir},
		"device mode":        {{Path: "dev", Type: TypeDir, Mode: fs.ModeDevice}},
		"block count":        {dir, {Path: "config/x", Type: TypeFile, Size: 1}},
		"unknown type":       {{Path: "x", Type: "socket"}},
		"negative owner":     {{Path: "x", Type: TypeDir, UID: -1}},
		"symlink without to": {{Path: "x", Type: TypeSymlink}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := (&Manifest{Files: files}).Validate(); err == nil {
				t.Fatal("unsafe manifest accepted")
			}
		})
	}
}

func TestRecoverInterruptedSwap(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.live, "config", "app.db"), []byte("one"))
	first, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(first, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	root := f.replica.Root
	rename := func(from, to string) {
		t.Helper()
		if err := os.Rename(filepath.Join(root, from), filepath.Join(root, to)); err != nil {
			t.Fatal(err)
		}
	}
	// Crash after the old replica moved aside: roll back.
	write(t, filepath.Join(root, newManifestFile), []byte("{}"))
	rename(replicaDir, oldReplicaDir)
	if err := f.replica.Recover(); err != nil {
		t.Fatal(err)
	}
	if current, err := f.replica.Current(); err != nil || current.Sequence != 1 || tree(t, filepath.Join(root, replicaDir))["config/app.db"].data == nil {
		t.Fatalf("rollback: %+v %v", current, err)
	}
	// Crash after staging moved into place: roll forward.
	write(t, filepath.Join(root, "staging", "config", "app.db"), []byte("two"))
	rename(replicaDir, oldReplicaDir)
	rename("staging", replicaDir)
	write(t, filepath.Join(root, newManifestFile), []byte(`{"epoch":1,"sequence":2}`))
	if err := f.replica.Recover(); err != nil {
		t.Fatal(err)
	}
	if current, err := f.replica.Current(); err != nil || current.Sequence != 2 {
		t.Fatalf("roll forward: %+v %v", current, err)
	}
	if _, err := os.Lstat(filepath.Join(root, oldReplicaDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("the old replica remained")
	}
}

func TestPromote(t *testing.T) {
	f := newFixture(t)
	target := filepath.Join(t.TempDir(), "live")
	if err := f.replica.Promote(target); !errors.Is(err, ErrNoReplica) {
		t.Fatalf("no replica: %v", err)
	}
	write(t, filepath.Join(f.live, "config", "app.db"), []byte("data"))
	m, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(m, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	if err := f.replica.Promote(f.live); err == nil {
		t.Fatal("promote over an existing live directory")
	}
	if err := f.replica.Promote(target); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(target, "config", "app.db")); err != nil || string(data) != "data" {
		t.Fatalf("promoted data: %q %v", data, err)
	}
	if current, err := f.replica.Current(); err != nil || current != nil {
		t.Fatalf("a promoted replica is still current: %+v %v", current, err)
	}
}

func TestOwnersSurviveRoundTrip(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing owners needs root")
	}
	f := newFixture(t)
	file := filepath.Join(f.live, "config", "app.db")
	link := filepath.Join(f.live, "config", "link")
	write(t, file, []byte("data"))
	if err := os.Symlink("app.db", link); err != nil {
		t.Fatal(err)
	}
	for path, owner := range map[string]int{filepath.Dir(file): 1000, file: 1234, link: 4321} {
		if err := os.Lchown(path, owner, owner+1); err != nil {
			t.Fatal(err)
		}
	}
	m, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(m, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	for rel, owner := range map[string]int{"config": 1000, "config/app.db": 1234, "config/link": 4321} {
		info, err := os.Lstat(filepath.Join(f.replica.Root, replicaDir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if st := info.Sys().(*syscall.Stat_t); int(st.Uid) != owner || int(st.Gid) != owner+1 {
			t.Fatalf("%s owner %d:%d, want %d:%d", rel, st.Uid, st.Gid, owner, owner+1)
		}
	}
}

func TestDiscardForcesAFullApply(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.live, "config", "app.db"), random(t, 2*BlockSize))
	m, err := f.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	fetched := 0
	if err := f.replica.Apply(m, f.fetchFrom(&fetched)); err != nil {
		t.Fatal(err)
	}
	if err := f.replica.Discard(); err != nil {
		t.Fatal(err)
	}
	if current, err := f.replica.Current(); err != nil || current != nil {
		t.Fatalf("discarded replica is current: %+v %v", current, err)
	}
	if missing := Missing(nil, m); len(missing) != 2 {
		t.Fatalf("missing %v", missing)
	}
	fetched = 0
	if err := f.replica.Apply(m, f.fetchFrom(&fetched)); err != nil || fetched != 2 {
		t.Fatalf("full apply: fetched %d, %v", fetched, err)
	}
}

func TestStoppedSnapshotCopiesFinalWritesWithoutFreezing(t *testing.T) {
	f := newFixture(t)
	write(t, filepath.Join(f.live, "config", "app.db"), []byte("before"))
	write(t, filepath.Join(f.snapshot, "config", "stale"), []byte("remove"))
	write(t, filepath.Join(f.live, "config", "app.db"), []byte("all committed writes"))
	f.snapshotter.Freezer = nil
	m, err := f.snapshotter.TakeStopped(t.Context(), 3, 8)
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 3 || m.Sequence != 8 || m.Frozen != 0 {
		t.Fatalf("manifest: %+v", m)
	}
	blocks := 0
	if err := f.replica.Apply(m, f.fetchFrom(&blocks)); err != nil {
		t.Fatal(err)
	}
	if got := tree(t, filepath.Join(f.replica.Root, "replica")); !reflect.DeepEqual(got, tree(t, f.live)) {
		t.Fatal("final replica differs from stopped files")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.snapshotter.TakeStopped(ctx, 3, 9); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled snapshot: %v", err)
	}
}
