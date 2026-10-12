package repository

import (
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// ErrBlobIncomplete means a blob write ended before its stated length.
var ErrBlobIncomplete = errors.New("repository file write is incomplete")

// WriteBlobFile writes one repository file below root through a temporary file and a rename.
// Kopia commits packs through a later index file. A pack is not synced on its own. Before any
// other file is written, the filesystem is synced, so an index never names a pack that a crash
// could lose. Systems without syncfs sync each file.
func WriteBlobFile(root *os.Root, file string, data io.Reader, length int64, modified time.Time) (os.FileInfo, error) {
	pack := deferredSync && (strings.HasPrefix(file, "p/") || strings.HasPrefix(file, "q/"))
	if deferredSync && !pack {
		if err := syncFilesystem(root); err != nil {
			return nil, err
		}
	}
	directory := path.Dir(file)
	if err := root.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	temporary := path.Join(directory, ".synod-"+uuid.NewString()+".tmp")
	f, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	defer root.Remove(temporary)
	written, err := io.Copy(f, data)
	if err == nil && written != length {
		err = ErrBlobIncomplete
	}
	if err == nil && !modified.IsZero() {
		timestamp := syscall.NsecToTimeval(int64(modified.Nanosecond()))
		timestamp.Sec = modified.Unix()
		err = syscall.Futimes(int(f.Fd()), []syscall.Timeval{timestamp, timestamp})
	}
	if err == nil && !pack {
		err = f.Sync()
	}
	if err == nil && pack {
		err = startWriteback(f)
	}
	if err := errors.Join(err, f.Close()); err != nil {
		return nil, err
	}
	if err := root.Rename(temporary, file); err != nil {
		return nil, err
	}
	if !pack {
		if err := syncDirectory(root, directory); err != nil {
			return nil, err
		}
	}
	return root.Stat(file)
}

// RemoveBlobFile removes one repository file. A missing file is already removed.
func RemoveBlobFile(root *os.Root, file string) error {
	if err := root.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	err := syncDirectory(root, path.Dir(file))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func syncDirectory(root *os.Root, directory string) error {
	parent, err := root.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}
