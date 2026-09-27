package replica

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"
)

// BlockSize is the unit of change detection and transfer.
const BlockSize = 1 << 20

// File types in a manifest.
const (
	TypeDir     = "dir"
	TypeFile    = "file"
	TypeSymlink = "symlink"
)

// Hash is the SHA-256 of one block.
type Hash [sha256.Size]byte

func (h Hash) MarshalText() ([]byte, error) { return []byte(hex.EncodeToString(h[:])), nil }

func (h *Hash) UnmarshalText(text []byte) error {
	if hex.DecodedLen(len(text)) != len(h) {
		return fmt.Errorf("replica: block hash must be %d hex characters", 2*len(h))
	}
	_, err := hex.Decode(h[:], text)
	return err
}

// File describes one entry of a snapshot. Path is slash-separated and relative
// to the volume root, with the volume name as its first element.
type File struct {
	Path    string      `json:"path"`
	Type    string      `json:"type"`
	Mode    fs.FileMode `json:"mode"`
	UID     int         `json:"uid"`
	GID     int         `json:"gid"`
	Size    int64       `json:"size,omitempty"`
	ModTime time.Time   `json:"mod_time"`
	Link    string      `json:"link,omitempty"`
	Blocks  []Hash      `json:"blocks,omitempty"`
}

// Manifest describes one complete snapshot. Files are in walk order, so a
// directory comes before its entries.
type Manifest struct {
	Epoch     uint64        `json:"epoch"`
	Sequence  uint64        `json:"sequence"`
	CreatedAt time.Time     `json:"created_at"`
	Frozen    time.Duration `json:"frozen_ns"`
	Files     []File        `json:"files"`
	// Skipped lists sockets, pipes, and devices. They are not copied.
	Skipped []string `json:"skipped,omitempty"`
}

// BlockRef names one block of one file.
type BlockRef struct {
	Path  string `json:"path"`
	Index int    `json:"index"`
}

// Validate refuses a manifest that could write outside the replica or that is inconsistent.
func (m *Manifest) Validate() error {
	seen := make(map[string]string, len(m.Files))
	for _, f := range m.Files {
		native := filepath.FromSlash(f.Path)
		if !filepath.IsLocal(native) || filepath.ToSlash(filepath.Clean(native)) != f.Path {
			return fmt.Errorf("replica: path %q is not a clean local path", f.Path)
		}
		if _, ok := seen[f.Path]; ok {
			return fmt.Errorf("replica: path %q appears twice", f.Path)
		}
		// Every parent must be a directory listed earlier. So no write can pass through a symlink.
		if parent := filepath.ToSlash(filepath.Dir(native)); parent != "." && seen[parent] != TypeDir {
			return fmt.Errorf("replica: parent of %q is not an earlier directory", f.Path)
		}
		seen[f.Path] = f.Type
		if f.Mode&^(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 {
			return fmt.Errorf("replica: %q has unsupported mode bits", f.Path)
		}
		switch f.Type {
		case TypeDir:
			if f.Size != 0 || f.Link != "" || len(f.Blocks) != 0 {
				return fmt.Errorf("replica: directory %q has file content", f.Path)
			}
		case TypeSymlink:
			if f.Link == "" || f.Size != 0 || len(f.Blocks) != 0 {
				return fmt.Errorf("replica: symlink %q is invalid", f.Path)
			}
		case TypeFile:
			if f.Size < 0 || f.Link != "" || int64(len(f.Blocks)) != (f.Size+BlockSize-1)/BlockSize {
				return fmt.Errorf("replica: file %q has %d blocks for %d bytes", f.Path, len(f.Blocks), f.Size)
			}
		default:
			return fmt.Errorf("replica: %q has unknown type %q", f.Path, f.Type)
		}
		if f.UID < 0 || f.GID < 0 {
			return fmt.Errorf("replica: %q has a negative owner", f.Path)
		}
	}
	return nil
}

// Missing lists the blocks of next that current does not hold at the same path and index.
// A nil current means that the standby has no replica yet.
func Missing(current, next *Manifest) []BlockRef {
	have := map[string][]Hash{}
	if current != nil {
		for _, f := range current.Files {
			if f.Type == TypeFile {
				have[f.Path] = f.Blocks
			}
		}
	}
	var missing []BlockRef
	for _, f := range next.Files {
		old := have[f.Path]
		for i, h := range f.Blocks {
			if i >= len(old) || old[i] != h {
				missing = append(missing, BlockRef{Path: f.Path, Index: i})
			}
		}
	}
	return missing
}

// blockLen is the length of block i in a file of size bytes.
func blockLen(size int64, i int) int {
	return int(min(BlockSize, size-int64(i)*BlockSize))
}
