package backup

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"io"
	"modernc.org/sqlite"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSQLiteCopyIncludesUncheckpointedWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data?#.sqlite")
	uri := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE state(value TEXT); INSERT INTO state VALUES ('private-sqlite-state');"); err != nil {
		t.Fatal(err)
	}
	copy, err := copySQLite(t.Context(), uri.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	if _, err := os.Stat(copy.Name()); !os.IsNotExist(err) {
		t.Fatal("plaintext staging file remains named")
	}
	restored := filepath.Join(t.TempDir(), "restored.sqlite")
	output, err := os.Create(restored)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, io.NewSectionReader(copy, 0, 1<<30)); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	restoreURI := url.URL{Scheme: "file", Path: restored}
	recovered, err := sql.Open("sqlite", restoreURI.String())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	var value string
	if err := recovered.QueryRowContext(context.Background(), "SELECT value FROM state").Scan(&value); err != nil || value != "private-sqlite-state" {
		t.Fatalf("restored WAL differs: %q %v", value, err)
	}
}

func TestSQLitePinnedSource(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux agent source paths")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "data?#.sqlite")
	original := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", original.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; CREATE TABLE state(value TEXT); INSERT INTO state VALUES ('pinned');"); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	uri, err := sqliteSourceURI(parent, filepath.Base(path))
	if err != nil {
		t.Fatal(err)
	}
	copy, err := copySQLite(t.Context(), uri)
	if err != nil {
		t.Fatal(err)
	}
	copy.Close()
}

func TestSQLiteWriterProcess(t *testing.T) {
	path := os.Getenv("SYNOD_SQLITE_WRITER")
	if path == "" {
		return
	}
	uri := url.URL{Scheme: "file", Path: path, RawQuery: "_pragma=busy_timeout(1000)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); io.Copy(io.Discard, os.Stdin); cancel() }()
	defer func() { os.Stdin.Close(); <-done }()
	for id := 1; ctx.Err() == nil; id++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO state(id,value) VALUES (?,?)", id, strings.Repeat("state", 512)); err != nil {
			if ctx.Err() != nil {
				return
			}
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, id)
	}
}

func TestTreeCopiesLiveSQLiteAndSkipsSidecars(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux agent source paths")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "data?#.sqlite")
	uri := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; CREATE TABLE state(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO state VALUES (0,'initial');"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestSQLiteWriterProcess$")
	command.Env = append(os.Environ(), "SYNOD_SQLITE_WRITER="+path)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int64
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			if id, err := strconv.ParseInt(scan.Text(), 10, 64); err == nil {
				writes.Store(id)
			}
		}
	}()
	stop := sync.OnceFunc(func() {
		stdin.Close()
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("SQLite writer: %v %s", err, stderr.String())
			}
		case <-time.After(5 * time.Second):
			command.Process.Kill()
			<-done
			t.Error("SQLite writer did not stop")
		}
		<-scanned
	})
	defer stop()
	for deadline := time.Now().Add(10 * time.Second); writes.Load() < 32; {
		if time.Now().After(deadline) {
			t.Fatal("SQLite writer did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	for _, name := range []string{"plain", "plain-wal"} {
		data := name
		if name == "plain" {
			data = "SQLite format 3"
		}
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	app := &agentv1.DiscoveredApp{Key: "compose:sqlite", Name: "sqlite", Kind: "compose", Paths: []*agentv1.DiscoveredPath{
		{Source: directory, Status: "Included", Kind: "project"},
		{Source: path, Status: "Included", Kind: "bind"},
		{Source: path + "-wal", Status: "Included", Kind: "bind"},
	}}
	tree, err := NewTree(t.Context(), app, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	tree.live = true
	r, err := repository.Create(t.Context(), repository.Location{Kind: "local", Path: t.TempDir()}, repository.Credentials{Password: "sqlite-test-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	started := time.Now().UTC()
	before := writes.Load()
	root, err := tree.Root(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: app.Key}, false)
	if err != nil {
		t.Fatal(err)
	}
	if writes.Load() <= before {
		t.Fatal("SQLite writes stopped during backup")
	}
	stop()
	if len(tree.sqlite) != 1 {
		t.Fatalf("overlapping paths made %d copies", len(tree.sqlite))
	}
	saved := backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: inventory.App{ID: uuid.New(), NodeID: uuid.New(), Definition: app}, Method: backup.Live, Architecture: "arm64", StartedAt: started, CompletedAt: time.Now().UTC(), Paths: tree.Paths, Exclusions: tree.Exclusions()}
	if _, err := r.PublishSnapshot(t.Context(), prepared, saved); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := r.RestoreFiles(t.Context(), saved.ID, restored); err != nil {
		t.Fatal(err)
	}
	var previous []byte
	found := 0
	for _, mapping := range tree.Paths {
		file := filepath.Join(restored, mapping.Archive)
		if mapping.Source == directory {
			for _, suffix := range []string{"-wal", "-shm", "-journal"} {
				if _, err := os.Stat(filepath.Join(file, filepath.Base(path)+suffix)); !os.IsNotExist(err) {
					t.Fatalf("SQLite sidecar copied: %s %v", suffix, err)
				}
			}
			if data, err := os.ReadFile(filepath.Join(file, "plain-wal")); err != nil || string(data) != "plain-wal" {
				t.Fatal("ordinary file with a sidecar suffix was excluded")
			}
			file = filepath.Join(file, filepath.Base(path))
		} else if mapping.Source != path {
			t.Fatal("standalone sidecar mapping remains")
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil && !bytes.Equal(data, previous) {
			t.Fatal("overlapping SQLite copies differ")
		}
		previous = data
		found++
		location := url.URL{Scheme: "file", Path: file}
		restoredDB, err := sql.Open("sqlite", location.String())
		if err != nil {
			t.Fatal(err)
		}
		var count, maximum int64
		var integrity string
		err = restoredDB.QueryRowContext(t.Context(), "SELECT count(*),max(id) FROM state").Scan(&count, &maximum)
		if err != nil || maximum < before || count != maximum+1 {
			t.Fatalf("restored committed writes differ: %d/%d before %d: %v", count, maximum, before, err)
		}
		if err := restoredDB.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
			t.Fatalf("invalid SQLite restore: %s %v", integrity, err)
		}
		restoredDB.Close()
	}
	if found != 2 {
		t.Fatalf("restored SQLite mappings: %d", found)
	}
}

func TestSQLiteCopyRejectsCorruptionAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.db")
	if err := os.WriteFile(path, append([]byte(sqliteHeader), make([]byte, 4080)...), 0600); err != nil {
		t.Fatal(err)
	}
	uri := url.URL{Scheme: "file", Path: path}
	if file, err := copySQLite(t.Context(), uri.String()+"?mode=ro"); err == nil {
		file.Close()
		t.Fatal("corrupt SQLite database accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if file, err := copySQLite(ctx, uri.String()+"?mode=ro"); err == nil {
		file.Close()
		t.Fatal("cancelled SQLite copy accepted")
	}
}

func TestSQLitePinnedSourceRefusesLinksAndKeepsRenamedDirectory(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux agent source paths")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "state.db")
	uri := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE state(value TEXT); INSERT INTO state VALUES ('safe');"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	parent, err := os.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	pinned, err := sqliteSourceURI(parent, "state.db")
	if err != nil {
		t.Fatal(err)
	}
	moved := directory + "-moved"
	if err := os.Rename(directory, moved); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(moved)
	if err := os.Symlink(t.TempDir(), directory); err != nil {
		t.Fatal(err)
	}
	copied, err := copySQLite(t.Context(), pinned)
	if err != nil {
		t.Fatal(err)
	}
	copied.Close()
	if err := os.Symlink(filepath.Join(moved, "state.db"), filepath.Join(moved, "link.db")); err != nil {
		t.Fatal(err)
	}
	linked, err := sqliteSourceURI(parent, "link.db")
	if err != nil {
		t.Fatal(err)
	}
	if copied, err := copySQLite(t.Context(), linked); err == nil {
		copied.Close()
		t.Fatal("SQLite source followed a file link")
	}
}

func TestSQLitePinnedSourceRefusesSidecarLinks(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux agent source paths")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		t.Run(suffix, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "state.db")
			uri := url.URL{Scheme: "file", Path: path}
			db, err := sql.Open("sqlite", uri.String())
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; CREATE TABLE state(value TEXT); INSERT INTO state VALUES ('safe');"); err != nil {
				t.Fatal(err)
			}
			connection, err := db.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := connection.Raw(func(raw any) error { _, err := raw.(sqlite.FileControl).FileControlPersistWAL("main", 1); return err }); err != nil {
				t.Fatal(err)
			}
			connection.Close()
			db.Close()
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("private-outside-data"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path + suffix); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, path+suffix); err != nil {
				t.Fatal(err)
			}
			parent, err := os.Open(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			pinned, err := sqliteSourceURI(parent, "state.db")
			if err != nil {
				t.Fatal(err)
			}
			if file, err := copySQLite(t.Context(), pinned); err == nil {
				file.Close()
				t.Fatal("SQLite sidecar link was followed")
			}
			if data, err := os.ReadFile(outside); err != nil || string(data) != "private-outside-data" {
				t.Fatal("outside file changed")
			}
		})
	}
}

func TestSQLiteCopyCancelsWhileSourceIsLocked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.db")
	uri := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(t.Context(), "CREATE TABLE state(value TEXT); BEGIN EXCLUSIVE; INSERT INTO state VALUES ('uncommitted');"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "ROLLBACK")
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	uri.RawQuery = "mode=ro&_pragma=busy_timeout(1000)"
	started := time.Now()
	if file, err := copySQLite(ctx, uri.String()); err == nil {
		file.Close()
		t.Fatal("locked source produced a successful copy")
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("SQLite copy did not end after cancellation")
	}
}
