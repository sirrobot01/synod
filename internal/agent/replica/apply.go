package replica

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

// Names inside Replica.Root.
const (
	replicaDir      = "replica"
	oldReplicaDir   = "replica.old"
	stagingDir      = "staging"
	manifestFile    = "replica.manifest"
	newManifestFile = "replica.manifest.new"
)

var (
	// ErrStale reports a snapshot that is not newer than the current replica.
	ErrStale = errors.New("replica: snapshot is not newer than the current replica")
	// ErrBlock reports a received block whose length or hash does not match the manifest.
	ErrBlock = errors.New("replica: block does not match the manifest")
	// ErrNoReplica reports that no complete replica exists.
	ErrNoReplica = errors.New("replica: no complete replica")
)

// Fetch returns the content of one block that the standby lacks.
type Fetch func(BlockRef) ([]byte, error)

// Replica is the standby copy of one app's volumes. Root holds the complete
// replica, its manifest, and a staging directory for the next snapshot.
type Replica struct {
	Root string
}

// Current returns the manifest of the complete replica, or nil when none exists.
func (r Replica) Current() (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(r.Root, manifestFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("replica: read manifest: %w", err)
	}
	return &m, nil
}

// Apply builds next in staging from the current replica and fetched blocks.
// It checks every block against the manifest. Then it replaces the current
// replica. An interrupted Apply leaves the previous replica or the new one,
// never a mix; Recover finishes the swap.
func (r Replica) Apply(next *Manifest, fetch Fetch) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if err := r.Recover(); err != nil {
		return err
	}
	current, err := r.Current()
	if err != nil {
		return err
	}
	if current != nil && (next.Epoch < current.Epoch || (next.Epoch == current.Epoch && next.Sequence <= current.Sequence)) {
		return ErrStale
	}
	root, err := os.OpenRoot(r.Root)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := root.RemoveAll(stagingDir); err != nil {
		return err
	}
	if err := root.Mkdir(stagingDir, 0o700); err != nil {
		return err
	}
	staging, err := root.OpenRoot(stagingDir)
	if err != nil {
		return err
	}
	defer staging.Close()
	var previous *os.Root
	have := map[string]File{}
	if current != nil {
		if previous, err = root.OpenRoot(replicaDir); err != nil {
			return err
		}
		defer previous.Close()
		for _, f := range current.Files {
			have[f.Path] = f
		}
	}
	for _, f := range next.Files {
		switch f.Type {
		case TypeDir:
			err = staging.Mkdir(f.Path, 0o700)
		case TypeSymlink:
			if err = staging.Symlink(f.Link, f.Path); err == nil {
				err = lchown(staging, f)
			}
		case TypeFile:
			err = writeFile(staging, previous, have[f.Path], f, fetch)
		}
		if err != nil {
			return fmt.Errorf("replica: build %s: %w", f.Path, err)
		}
	}
	// Directories last, deepest first, so that later writes do not change their times.
	for _, f := range slices.Backward(next.Files) {
		if f.Type != TypeDir {
			continue
		}
		if err := setFileMetadata(staging, f); err != nil {
			return fmt.Errorf("replica: build %s: %w", f.Path, err)
		}
		if err := syncPath(staging, f.Path); err != nil {
			return err
		}
	}
	if err := syncPath(root, stagingDir); err != nil {
		return err
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err := writeSynced(root, newManifestFile, data); err != nil {
		return err
	}
	return r.swap(root, current != nil)
}

// swap renames staging into place. Recover depends on this order.
func (r Replica) swap(root *os.Root, hasCurrent bool) error {
	if hasCurrent {
		if err := root.Rename(replicaDir, oldReplicaDir); err != nil {
			return err
		}
	} else if err := root.RemoveAll(replicaDir); err != nil {
		return err
	}
	if err := root.Rename(stagingDir, replicaDir); err != nil {
		return err
	}
	if err := root.Rename(newManifestFile, manifestFile); err != nil {
		return err
	}
	if err := syncPath(root, "."); err != nil {
		return err
	}
	return root.RemoveAll(oldReplicaDir)
}

// Recover finishes or rolls back a swap that a crash interrupted, and removes staging.
func (r Replica) Recover() error {
	if err := os.MkdirAll(r.Root, 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(r.Root)
	if err != nil {
		return err
	}
	defer root.Close()
	exists := func(name string) bool { _, err := root.Lstat(name); return err == nil }
	switch {
	case exists(oldReplicaDir) && exists(replicaDir):
		// Staging was already renamed into place. Finish the swap.
		if exists(newManifestFile) {
			if err := root.Rename(newManifestFile, manifestFile); err != nil {
				return err
			}
		}
		if err := root.RemoveAll(oldReplicaDir); err != nil {
			return err
		}
	case exists(oldReplicaDir):
		if err := root.Rename(oldReplicaDir, replicaDir); err != nil {
			return err
		}
		if err := root.RemoveAll(newManifestFile); err != nil {
			return err
		}
	default:
		if err := root.RemoveAll(newManifestFile); err != nil {
			return err
		}
	}
	if err := root.RemoveAll(stagingDir); err != nil {
		return err
	}
	return syncPath(root, ".")
}

// Discard forgets the complete replica, so the next Apply receives every block.
// Use it when the local copy no longer matches its manifest.
func (r Replica) Discard() error {
	err := os.Remove(filepath.Join(r.Root, manifestFile))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Promote moves the complete replica to live, which must not exist yet.
// The app can then start on this device from the latest snapshot.
func (r Replica) Promote(live string) error {
	if err := r.Recover(); err != nil {
		return err
	}
	current, err := r.Current()
	if err != nil {
		return err
	}
	if current == nil {
		return ErrNoReplica
	}
	if _, err := os.Lstat(live); !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("replica: live directory %s already exists", live)
	}
	if err := os.Rename(filepath.Join(r.Root, replicaDir), live); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(r.Root, manifestFile)); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(live))
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func writeFile(staging, previous *os.Root, old, f File, fetch Fetch) error {
	out, err := staging.OpenFile(f.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	var source *os.File
	if previous != nil && old.Type == TypeFile {
		if source, err = previous.Open(f.Path); err == nil {
			defer source.Close()
		}
	}
	buf := make([]byte, BlockSize)
	for i, want := range f.Blocks {
		n := blockLen(f.Size, i)
		var block []byte
		// Reuse the local copy of an unchanged block, but only if it still matches.
		if source != nil && i < len(old.Blocks) && old.Blocks[i] == want {
			if m, _ := source.ReadAt(buf[:n], int64(i)*BlockSize); m == n && sha256.Sum256(buf[:n]) == want {
				block = buf[:n]
			}
		}
		if block == nil {
			if block, err = fetch(BlockRef{Path: f.Path, Index: i}); err != nil {
				return err
			}
			if len(block) != n || sha256.Sum256(block) != want {
				return fmt.Errorf("%w: %s block %d", ErrBlock, f.Path, i)
			}
		}
		if _, err := out.WriteAt(block, int64(i)*BlockSize); err != nil {
			return err
		}
	}
	if err := out.Truncate(f.Size); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return setFileMetadata(staging, f)
}

func setFileMetadata(root *os.Root, f File) error {
	if err := lchown(root, f); err != nil {
		return err
	}
	if err := root.Chmod(f.Path, f.Mode); err != nil {
		return err
	}
	return root.Chtimes(f.Path, f.ModTime, f.ModTime)
}

// lchown changes the owner only when it differs, so an unprivileged test can apply its own files.
func lchown(root *os.Root, f File) error {
	info, err := root.Lstat(f.Path)
	if err != nil {
		return err
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) == f.UID && int(st.Gid) == f.GID {
		return nil
	}
	return root.Lchown(f.Path, f.UID, f.GID)
}

func writeSynced(root *os.Root, name string, data []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func syncPath(root *os.Root, name string) error {
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
