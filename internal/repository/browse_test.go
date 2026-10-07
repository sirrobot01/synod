package repository

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestBrowseUsesOriginalPathsAndDumps(t *testing.T) {
	file := func(name, content string) kfs.Entry {
		return virtualfs.StreamingFileFromReader(name, io.NopCloser(strings.NewReader(content)))
	}
	root := virtualfs.NewStaticDirectory("root", []kfs.Entry{
		virtualfs.NewStaticDirectory("paths", []kfs.Entry{
			virtualfs.NewStaticDirectory("app", []kfs.Entry{file("config.yml", "private-config"), file(".env", "stale-env"), virtualfs.NewStaticDirectory("sub", []kfs.Entry{file("deep", "private-deep")})}),
			file("env", "SECRET=private-env"),
		}),
		virtualfs.NewStaticDirectory("databases", []kfs.Entry{file("web.sql", "private-dump")}),
	})
	r, err := Create(t.Context(), Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}, Credentials{Password: "browse-password"})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	prepared, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "fixture", UserName: "synod", Path: "browse"}, false)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := r.PublishSnapshot(t.Context(), prepared, backup.Snapshot{ID: uuid.New(), InstallationID: uuid.New(), DestinationID: uuid.New(), App: testCatalog().Apps[0], Method: backup.Dump, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(),
		Paths: []backup.Path{{Source: "/srv/app", Archive: "paths/app", Kind: "bind"}, {Source: "/srv/app/.env", Archive: "paths/env", Kind: "env"}},
		Dumps: []backup.DatabaseDump{{ContainerID: "web", Database: "postgres", Archive: "databases/web.sql", Format: "sql"}}})
	if err != nil {
		t.Fatal(err)
	}
	names := func(path string) []string {
		t.Helper()
		entries, truncated, err := r.List(t.Context(), saved, path, 100)
		if err != nil || truncated {
			t.Fatalf("list %s: %v", path, err)
		}
		var result []string
		for _, entry := range entries {
			result = append(result, entry.Name+":"+entry.Type)
		}
		return result
	}
	for path, expected := range map[string]string{
		"/":                 "@databases:directory srv:directory",
		"/srv":              "app:directory",
		"/srv/app":          ".env:file config.yml:file sub:directory",
		"@databases":        "web.sql:file",
		"/srv/app/sub/deep": "deep:file",
	} {
		if actual := strings.Join(names(path), " "); actual != expected {
			t.Errorf("list %s = %q, want %q", path, actual, expected)
		}
	}
	if entries, truncated, err := r.List(t.Context(), saved, "/srv/app", 1); err != nil || !truncated || len(entries) != 1 {
		t.Fatal("listing limit was not applied", err)
	}
	for path, content := range map[string]string{"/srv/app/sub/deep": "private-deep", "/srv/app/.env": "SECRET=private-env", "@databases/web.sql": "private-dump"} {
		archive, _, err := Locate(saved, path)
		if err != nil {
			t.Fatal(err)
		}
		reader, size, err := r.OpenFile(t.Context(), saved.ID, archive)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(reader)
		reader.Close()
		if err != nil || string(data) != content || size != int64(len(content)) {
			t.Fatalf("read %s = %q (%d): %v", path, data, size, err)
		}
	}
	for _, source := range []string{"/", "/srv", "/srv/app", "@databases"} {
		var output bytes.Buffer
		if err := r.ArchivePath(t.Context(), saved, source, "download", &output); err != nil {
			t.Fatal(err)
		}
		files := make(map[string]string)
		reader := tar.NewReader(&output)
		for {
			header, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if header.Typeflag == tar.TypeReg {
				data, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				files[header.Name] = string(data)
			}
		}
		switch source {
		case "/":
			if files["download/srv/app/.env"] != "SECRET=private-env" || files["download/@databases/web.sql"] != "private-dump" {
				t.Fatalf("root download omitted saved files: %v", files)
			}
		case "/srv":
			if files["download/app/.env"] != "SECRET=private-env" {
				t.Fatal("parent download omitted override")
			}
		case "/srv/app":
			if files["download/.env"] != "SECRET=private-env" || files["download/sub/deep"] != "private-deep" {
				t.Fatal("directory download omitted saved files")
			}
		case "@databases":
			if files["download/web.sql"] != "private-dump" {
				t.Fatal("dump directory omitted dump")
			}
		}
	}
	for _, path := range []string{"/etc", "/srv/other", "/srv/app/missing", "@databases/other.sql", "relative", "/srv/../etc", "/srv/app/sub/../../x"} {
		if _, _, err := r.List(t.Context(), saved, path, 10); !errors.Is(err, ErrPathMissing) && !errors.Is(err, backup.ErrInvalid) {
			t.Errorf("list %s: %v", path, err)
		}
	}
	if archive, _, _ := Locate(saved, "/srv"); archive != "" {
		t.Fatal("a parent of saved paths has no saved content")
	}
	if _, _, err := r.OpenFile(t.Context(), saved.ID, "paths/app"); !errors.Is(err, backup.ErrInvalid) {
		t.Fatal("opened a directory as a file", err)
	}
}
