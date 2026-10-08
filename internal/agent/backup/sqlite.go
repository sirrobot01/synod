package backup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var ErrSQLitePlatform = errors.New("live SQLite copies require a Linux agent; use Stop on this device")
var errSQLite = errors.New("SQLite copy failed; check that the database is readable and valid, then try again")

const sqliteHeader = "SQLite format 3\x00"

func (f *liveFile) capture(ctx context.Context) (_ *fileCopy, retErr error) {
	// Closing an unrelated descriptor can release this process's SQLite locks.
	// Keep header reads and source backup connections separate from each other.
	f.tree.sqliteMu.Lock()
	if copy := f.tree.sqlite[f.absolute]; copy != nil {
		info, err := copy.Stat()
		f.tree.sqliteMu.Unlock()
		if err != nil {
			return nil, errSQLite
		}
		return &fileCopy{ReaderAt: copy, size: info.Size()}, nil
	}
	source, err := (&sourceFile{f.sourceEntry}).open(ctx)
	if err != nil {
		f.tree.sqliteMu.Unlock()
		return nil, err
	}
	var header [16]byte
	_, err = source.ReadAt(header[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		source.Close()
		f.tree.sqliteMu.Unlock()
		return nil, ErrSource
	}
	if err != nil || string(header[:]) != sqliteHeader {
		f.tree.sqliteMu.Unlock()
		return f.tree.stable(ctx, source)
	}
	defer f.tree.sqliteMu.Unlock()
	defer source.Close()
	defer func() {
		if retErr != nil {
			f.tree.sqliteErr = retErr
		}
	}()
	if len(f.tree.sqlite) >= 256 {
		return nil, ErrSourceLimit
	}
	parent, err := openDirectory(f.source.root, filepath.Dir(f.path))
	if err != nil {
		return nil, ErrSource
	}
	defer parent.Close()
	directory, err := parent.Open(".")
	if err != nil {
		return nil, ErrSource
	}
	defer directory.Close()
	uri, err := sqliteSourceURI(directory, filepath.Base(f.path))
	if err != nil {
		return nil, err
	}
	copy, err := copySQLite(ctx, uri)
	if err != nil {
		return nil, errSQLite
	}
	current, err := parent.Lstat(filepath.Base(f.path))
	if err != nil || !os.SameFile(current, f.FileInfo) {
		copy.Close()
		return nil, ErrSource
	}
	if f.tree.sqlite == nil {
		f.tree.sqlite = make(map[string]*os.File)
	}
	f.tree.sqlite[f.absolute] = copy
	info, err := copy.Stat()
	if err != nil {
		return nil, errSQLite
	}
	return &fileCopy{ReaderAt: copy, size: info.Size()}, nil
}

func copySQLite(ctx context.Context, uri string) (_ *os.File, retErr error) {
	source, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, err
	}
	defer source.Close()
	source.SetMaxOpenConns(1)
	connection, err := source.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	var journal string
	if err := connection.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return nil, err
	}
	if journal == "wal" {
		if _, err := connection.ExecContext(ctx, "BEGIN"); err != nil {
			return nil, err
		}
		defer connection.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		// Hold one WAL read point while writers continue on their own connections.
		if _, err := connection.ExecContext(ctx, "SELECT rootpage FROM sqlite_schema LIMIT 1"); err != nil {
			return nil, err
		}
	}
	var checked string
	if err := connection.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&checked); err != nil {
		return nil, err
	}
	if checked != "ok" {
		return nil, errSQLite
	}
	directory, err := os.MkdirTemp("", "synod-sqlite-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	file, err := os.CreateTemp(directory, "copy-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			file.Close()
		}
	}()
	destination := url.URL{Scheme: "file", Path: file.Name(), RawQuery: url.Values{"_pragma": {"journal_mode(MEMORY)", "trusted_schema(OFF)", "temp_store(MEMORY)"}}.Encode()}
	err = connection.Raw(func(raw any) (retErr error) {
		native, ok := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		})
		if !ok {
			return errSQLite
		}
		copying, err := native.NewBackup(destination.String())
		if err != nil {
			return err
		}
		finished := false
		defer func() {
			if !finished {
				retErr = errors.Join(retErr, copying.Finish())
			}
		}()
		// Open the destination transaction before unlinking its private staging file.
		if _, err := copying.Step(0); err != nil {
			return fmt.Errorf("prepare SQLite copy: %w", err)
		}
		if err := os.Remove(file.Name()); err != nil {
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := copying.Step(256)
			if err != nil {
				code, ok := errors.AsType[*sqlite.Error](err)
				if !ok || (code.Code()&255 != sqlite3.SQLITE_BUSY && code.Code()&255 != sqlite3.SQLITE_LOCKED) {
					return fmt.Errorf("copy SQLite pages: %w", err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
			if !more {
				break
			}
		}
		err = copying.Finish()
		finished = true
		if err != nil {
			return fmt.Errorf("finish SQLite copy: %w", err)
		}

		return ctx.Err()
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (t *Tree) sqliteSidecar(source *sourceRoot, parent *os.Root, name string) (bool, error) {
	// A snapshot holds a database and its sidecars from one moment, so they are copied together.
	if !t.live || source.snapshot {
		return false, nil
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		base, found := strings.CutSuffix(name, suffix)
		if !found || base == "" {
			continue
		}
		t.sqliteMu.Lock()
		defer t.sqliteMu.Unlock()
		info, err := parent.Lstat(base)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, ErrSource
		}
		if !info.Mode().IsRegular() {
			return false, nil
		}
		file, err := parent.OpenFile(base, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return false, ErrSource
		}
		defer file.Close()
		var header [16]byte
		_, err = file.ReadAt(header[:], 0)
		if err != nil && !errors.Is(err, io.EOF) {
			return false, ErrSource
		}
		return err == nil && string(header[:]) == sqliteHeader, nil
	}
	return false, nil
}
