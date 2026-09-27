package replica

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Verify checks the complete replica against its stored manifest without changing it.
func (r Replica) Verify(ctx context.Context) error {
	manifest, err := r.Current()
	if err != nil {
		return err
	}
	if manifest == nil {
		return ErrNoReplica
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(r.Root, replicaDir))
	if err != nil {
		return err
	}
	defer root.Close()
	files := make(map[string]File, len(manifest.Files))
	for _, file := range manifest.Files {
		files[file.Path] = file
	}
	buffer := make([]byte, BlockSize)
	err = fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}
		want, ok := files[path]
		if !ok {
			return fmt.Errorf("%w: unexpected file %s", ErrBlock, path)
		}
		info, err := root.Lstat(path)
		if err != nil {
			return err
		}
		switch want.Type {
		case TypeDir:
			if !info.IsDir() {
				return ErrBlock
			}
		case TypeSymlink:
			if info.Mode()&fs.ModeSymlink == 0 {
				return ErrBlock
			}
			link, err := root.Readlink(path)
			if err != nil {
				return err
			}
			if link != want.Link {
				return ErrBlock
			}
		case TypeFile:
			if !info.Mode().IsRegular() || info.Size() != want.Size {
				return ErrBlock
			}
			file, err := root.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			for index, hash := range want.Blocks {
				if err := ctx.Err(); err != nil {
					return err
				}
				block := buffer[:blockLen(want.Size, index)]
				n, err := file.ReadAt(block, int64(index)*BlockSize)
				if err != nil {
					return err
				}
				if n != len(block) || sha256.Sum256(block) != hash {
					return ErrBlock
				}
			}
		}
		delete(files, path)
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) != 0 {
		return ErrBlock
	}
	return nil
}
