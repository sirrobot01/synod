package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestLVMSnapshotSpaceLimits(t *testing.T) {
	for _, tc := range []struct {
		name, attributes, size, free, extent string
		want                                 int64
	}{
		{"small", "-wi-a-----", "536870912.00", "1073741824.00", "4194304.00", 16},
		{"large", "owi-aos---", "2199023255552.00", "4294967296.00", "4194304.00", 256},
		{"thin-origin", "Vwi-a-tz--", "536870912.00", "1073741824.00", "4194304.00", 16},
		{"insufficient-free-space", "-wi-a-----", "536870912", "100000000", "4194304", 0},
		{"snapshot-origin", "swi-a-s---", "536870912", "1073741824", "4194304", 0},
		{"inactive", "-wi-------", "536870912", "1073741824", "4194304", 0},
		{"partial", "-wi-a---p-", "536870912", "1073741824", "4194304", 0},
		{"invalid-number", "-wi-a-----", "NaN", "1073741824", "4194304", 0},
		{"zero-extent", "-wi-a-----", "536870912", "1073741824", "0", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := logicalVolume{Attributes: tc.attributes, Size: tc.size, Free: tc.free, Extent: tc.extent}
			got, ok := v.cowExtents()
			if got != tc.want || ok != (tc.want != 0) {
				t.Fatalf("cowExtents() = %d, %t; want %d", got, ok, tc.want)
			}
		})
	}
}

func TestLVMSnapshotOwnershipAndValidity(t *testing.T) {
	name := snapshotPrefix + uuid.NewString()
	valid := logicalVolume{Path: "/dev/test/" + name, UUID: "snapshot-uuid", Name: name, Group: "test", Attributes: "swi-aos---", OriginUUID: "source-uuid", Tags: lvmSnapshotTag, DataPercent: "1.25"}
	for _, tc := range []struct {
		name         string
		change       func(*logicalVolume)
		owned, valid bool
	}{
		{"valid", func(*logicalVolume) {}, true, true},
		{"full", func(v *logicalVolume) { v.DataPercent = "100.00" }, true, false},
		{"invalid", func(v *logicalVolume) { v.Attributes = "swi-I-s---" }, true, false},
		{"unknown-status", func(v *logicalVolume) { v.DataPercent = "" }, true, false},
		{"unowned", func(v *logicalVolume) { v.Tags = "user-backup" }, false, false},
		{"origin", func(v *logicalVolume) { v.Attributes = "owi-aos---" }, false, false},
		{"merging", func(v *logicalVolume) { v.Attributes = "Swi-aos---" }, false, false},
		{"thin-pool", func(v *logicalVolume) { v.Pool = "pool" }, false, false},
		{"different-path", func(v *logicalVolume) { v.Path = "/dev/test/source" }, false, false},
		{"unrelated-name", func(v *logicalVolume) { v.Name = "user-snapshot"; v.Path = "/dev/test/user-snapshot" }, false, false},
		{"unknown-origin", func(v *logicalVolume) { v.OriginUUID = "" }, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volume := valid
			tc.change(&volume)
			if volume.ownedSnapshot() != tc.owned || volume.validSnapshot() != tc.valid {
				t.Fatalf("ownership = %t, validity = %t; want %t, %t", volume.ownedSnapshot(), volume.validSnapshot(), tc.owned, tc.valid)
			}
		})
	}
}

// This test creates its own loop-backed VG. It needs root, LVM, ext4, and XFS tools.
// Set SYNOD_TEST_LVM=1 only on a disposable Linux test runner.
func TestLiveCopyReadsLVMSnapshot(t *testing.T) {
	if os.Getenv("SYNOD_TEST_LVM") != "1" {
		t.Skip("set SYNOD_TEST_LVM=1 on a disposable Linux runner")
	}
	for _, variant := range []string{"ext4", "xfs", "thin-ext4"} {
		t.Run(variant, func(t *testing.T) {
			ctx := t.Context()
			command := func(name string, args ...string) string {
				t.Helper()
				out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
				if err != nil {
					t.Fatalf("%s %v: %v: %s", name, args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			disk, err := os.CreateTemp(t.TempDir(), "lvm-disk")
			if err != nil {
				t.Fatal(err)
			}
			if err := disk.Truncate(2 << 30); err != nil {
				t.Fatal(err)
			}
			disk.Close()
			loop := command("losetup", "--find", "--show", disk.Name())
			t.Cleanup(func() {
				if out, err := exec.Command("losetup", "--detach", loop).CombinedOutput(); err != nil {
					t.Errorf("detach loop: %v: %s", err, out)
				}
			})
			group := "synodtest" + strings.ReplaceAll(uuid.NewString(), "-", "")
			command("vgcreate", group, loop)
			t.Cleanup(func() {
				if out, err := exec.Command("vgremove", "--yes", group).CombinedOutput(); err != nil {
					t.Errorf("remove test VG: %v: %s", err, out)
				}
			})
			if variant == "thin-ext4" {
				command("lvcreate", "--type", "thin-pool", "--size", "1G", "--name", "pool", group)
				command("lvcreate", "--virtualsize", "512M", "--thinpool", group+"/pool", "--name", "source")
			} else {
				command("lvcreate", "--size", "512M", "--name", "source", group)
			}
			device := "/dev/" + group + "/source"
			fstype := strings.TrimPrefix(variant, "thin-")
			command("mkfs."+fstype, device)
			top := t.TempDir()
			if err := syscall.Mount(device, top, fstype, 0, ""); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := syscall.Unmount(top, 0); err != nil {
					t.Errorf("unmount source: %v", err)
				}
			})
			var paths []*agentv1.DiscoveredPath
			for _, name := range []string{"data", "other"} {
				directory := filepath.Join(top, name)
				if err := os.Mkdir(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "state"), []byte("at-snapshot"), 0600); err != nil {
					t.Fatal(err)
				}
				paths = append(paths, &agentv1.DiscoveredPath{Source: directory, Kind: "bind", Status: "Included"})
			}
			alias := t.TempDir()
			if err := syscall.Mount(top, alias, "", syscall.MS_BIND, ""); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := syscall.Unmount(alias, 0); err != nil {
					t.Errorf("unmount alias: %v", err)
				}
			})
			paths = append(paths, &agentv1.DiscoveredPath{Source: filepath.Join(alias, "data"), Kind: "bind", Status: "Included"})
			app := &agentv1.DiscoveredApp{Key: "lvm-test", Name: "lvm-test", Paths: paths}
			tree, err := NewTree(ctx, app, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tree.live = true
			snapshots, err := tree.useSnapshots(ctx, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				tree.Close()
				if err := snapshots.Close(); err != nil {
					t.Errorf("remove snapshots: %v", err)
				}
			})
			if len(snapshots.lvm) != 1 {
				t.Fatalf("got %d LVM snapshots, want one shared by all roots and mount aliases", len(snapshots.lvm))
			}
			for _, root := range tree.roots {
				if !root.snapshot {
					t.Fatal("root uses live files")
				}
				if err := os.WriteFile(filepath.Join(root.source, "state"), []byte("after-snapshot"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := root.root.WriteFile("forbidden", []byte("write"), 0600); !errors.Is(err, syscall.EROFS) {
					t.Fatalf("snapshot permits writes: %v", err)
				}
			}
			root, err := tree.Root(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			repo, err := repository.Create(ctx, repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "lvm-test-password"})
			if err != nil {
				t.Fatal(err)
			}
			defer repo.Close(context.Background())
			prepared, err := repo.UploadFiles(ctx, root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.Check(ctx); err != nil {
				t.Fatal(err)
			}
			metadata := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(), Paths: tree.Paths, App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}}
			metadata.Consistency, metadata.Captures = tree.captureEvidence(backup.Live, nil, nil)
			if metadata.Consistency != backup.ConsistencyFilesystem || len(metadata.Captures) != 3 {
				t.Fatalf("missing filesystem capture evidence: %+v", metadata.Captures)
			}
			for _, capture := range metadata.Captures {
				if capture.Point == "" || capture.Point != metadata.Captures[0].Point {
					t.Fatal("mount aliases do not share the capture point")
				}
			}
			if _, err := repo.PublishSnapshot(ctx, prepared, metadata); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "restored")
			if err := repo.RestoreFiles(ctx, metadata.ID, target); err != nil {
				t.Fatal(err)
			}
			for _, path := range tree.Paths {
				if data, err := os.ReadFile(filepath.Join(target, path.Archive, "state")); err != nil || string(data) != "at-snapshot" {
					t.Fatalf("restored %q, want captured content: %v", data, err)
				}
			}
			tree.Close()
			if err := snapshots.Close(); err != nil {
				t.Fatal(err)
			}

			// Overflow must fail the check even when included files are unchanged.
			churn, err := os.Create(filepath.Join(top, "outside-included-paths"))
			if err != nil {
				t.Fatal(err)
			}
			defer churn.Close()
			block := bytes.Repeat([]byte("A"), 1<<20)
			for range 96 {
				if _, err := churn.Write(block); err != nil {
					t.Fatal(err)
				}
			}
			if err := churn.Sync(); err != nil {
				t.Fatal(err)
			}
			all, err := mounts()
			if err != nil {
				t.Fatal(err)
			}
			full := &fsSnapshots{name: snapshotPrefix + uuid.NewString(), mounts: all, views: make(map[string]string)}
			t.Cleanup(func() {
				if err := full.Close(); err != nil {
					t.Errorf("remove full snapshot: %v", err)
				}
			})
			if _, ok := full.view(ctx, paths[0].Source); !ok {
				t.Fatal("overflow fixture has no snapshot")
			}
			block[0] = 'B'
			for i := range 96 {
				if _, err := churn.WriteAt(block, int64(i)*int64(len(block))); err != nil {
					t.Fatalf("source write failed during snapshot overflow: %v", err)
				}
			}
			if err := churn.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := full.Check(ctx); err == nil {
				t.Fatal("full snapshot passed validation")
			}
			if err := full.Close(); err != nil {
				t.Fatal(err)
			}

			// Stale cleanup removes an abandoned owned snapshot, not a user's snapshot.
			unowned := snapshotPrefix + uuid.NewString()
			command("lvcreate", "--snapshot", "--size", "64M", "--name", unowned, device)
			stale := &fsSnapshots{name: snapshotPrefix + uuid.NewString(), mounts: all, views: make(map[string]string)}
			t.Cleanup(func() { stale.Close() })
			if _, ok := stale.view(ctx, paths[0].Source); !ok {
				t.Fatal("stale fixture has no snapshot")
			}
			if err := removeStaleLVM(ctx); err != nil {
				t.Fatal(err)
			}
			volumes, err := logicalVolumes(ctx)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, volume := range volumes {
				if volume.Group != group {
					continue
				}
				if volume.ownedSnapshot() {
					t.Fatal("owned snapshot remains after stale cleanup")
				}
				found = found || volume.Name == unowned
			}
			if !found {
				t.Fatal("stale cleanup removed an unowned snapshot")
			}

			// A mount failure must remove the LV that was already created.
			failed := &fsSnapshots{views: make(map[string]string)}
			wrong := "xfs"
			if fstype == "xfs" {
				wrong = "ext4"
			}
			if _, ok := failed.lvmView(ctx, mountEntry{root: "/", point: top, fstype: wrong, source: device}); ok {
				t.Fatal("mounted the snapshot as the wrong filesystem")
			}
			if err := failed.Close(); err != nil {
				t.Fatal(err)
			}
			volumes, err = logicalVolumes(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, volume := range volumes {
				if volume.Group == group && volume.ownedSnapshot() {
					t.Fatal("failed mount left an LVM snapshot")
				}
			}

			// A nested mount is not present in the snapshot of its parent volume.
			nested := filepath.Join(paths[0].Source, "nested")
			if err := os.Mkdir(nested, 0700); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mount("tmpfs", nested, "tmpfs", 0, "size=1m"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := syscall.Unmount(nested, 0); err != nil {
					t.Errorf("unmount nested fixture: %v", err)
				}
			})
			all, err = mounts()
			if err != nil {
				t.Fatal(err)
			}
			incomplete := &fsSnapshots{mounts: all, views: make(map[string]string)}
			if _, ok := incomplete.view(ctx, paths[0].Source); ok {
				t.Fatal("accepted a snapshot that omits a nested filesystem")
			}
		})
	}
}
