package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func testCatalog() Catalog {
	node := uuid.New()
	return Catalog{Version: 1, InstallationID: uuid.New(), Generation: 1, WrittenAt: time.Now().UTC().Truncate(time.Millisecond), Devices: []Device{{ID: node, Name: "private-catalog-marker", Architecture: "arm64"}}, Apps: []inventory.App{{ID: uuid.New(), NodeID: node, NodeName: "private-catalog-marker", ObservedAt: time.Now().UTC().Truncate(time.Millisecond), Present: true, Definition: &agentv1.DiscoveredApp{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "web", Name: "web", Image: "nginx:stable"}}}}}}
}

func TestEncryptedCatalogNeedsNoLocalConnectionState(t *testing.T) {
	location := Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}
	credentials := Credentials{Password: "private-repository-password"}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := r.ReadCatalog(t.Context()); !errors.Is(err, ErrCatalogMissing) {
		t.Fatalf("empty repository: %v", err)
	}
	config := r.repo.(repo.DirectRepository).ConfigFilename()
	if _, err := os.Stat(config); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary connection file remains")
	}
	catalog := testCatalog()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	entries, err := r.repo.FindManifests(t.Context(), map[string]string{"type": "synod.catalog"})
	if err != nil || len(entries) != 1 {
		t.Fatalf("retry wrote duplicate catalog: %d %v", len(entries), err)
	}
	if another, err := Create(t.Context(), location, credentials); !errors.Is(err, repo.ErrAlreadyInitialized) {
		if another != nil {
			another.Close(t.Context())
		}
		t.Fatalf("reinitialized existing data: %v", err)
	}
	wrong := credentials
	wrong.Password = "wrong-private-password"
	if another, err := Open(t.Context(), location, wrong); !errors.Is(err, repo.ErrInvalidPassword) {
		if another != nil {
			another.Close(t.Context())
		}
		t.Fatalf("wrong password: %v", err)
	} else if strings.Contains(err.Error(), wrong.Password) {
		t.Fatal("password reached error")
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadCatalog(t.Context())
	if err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("catalog after reopen differs: %v", err)
	}
	if err := filepath.WalkDir(location.Path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte("private-catalog-marker")) || bytes.Contains(data, []byte(credentials.Password)) {
			t.Errorf("repository contains plaintext in %s", entry.Name())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	changed := catalog
	changed.Generation = 2
	changed.WrittenAt = catalog.WrittenAt.Add(time.Second)
	if err := r.WriteCatalog(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteCatalog(t.Context(), catalog); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("stale generation: %v", err)
	}
	conflict := changed
	conflict.InstallationID = uuid.New()
	if err := r.WriteCatalog(t.Context(), conflict); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("another installation: %v", err)
	}
	conflict = changed
	conflict.WrittenAt = changed.WrittenAt.Add(time.Second)
	if err := r.WriteCatalog(t.Context(), conflict); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("changed retry: %v", err)
	}
	invalid := changed
	invalid.Devices = nil
	if err := r.WriteCatalog(t.Context(), invalid); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("invalid catalog: %v", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	changed.Generation = 3
	if err := r.WriteCatalog(cancelled, changed); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write: %v", err)
	}
	got, err = r.ReadCatalog(t.Context())
	if err != nil || got.Generation != 2 {
		t.Fatalf("failed write changed catalog: %d %v", got.Generation, err)
	}
}

func TestProviderErrorsAndCredentialsStayPrivate(t *testing.T) {
	credentials := Credentials{Password: "private-password", SSHKey: "private-key", AccessKeyID: "private-id", SecretAccessKey: "private-secret", SessionToken: "private-token"}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, credentials), "private-") {
			t.Fatal("credential formatting exposed values")
		}
	}
	for _, location := range []Location{{Kind: "local", Path: "relative"}, {Kind: "s3", S3: &S3Location{Endpoint: "https://user:secret@example.com", Bucket: "bucket"}}, {Kind: "sftp", Path: "/repo", SFTP: &SFTPLocation{Host: "example.com", Port: 22, User: "user"}}} {
		if r, err := Open(t.Context(), location, credentials); !errors.Is(err, ErrLocation) {
			if r != nil {
				r.Close(t.Context())
			}
			t.Fatalf("invalid location: %v", err)
		}
	}
}

func TestConflictingPublishedCatalogsFailClosed(t *testing.T) {
	r, err := Create(t.Context(), Location{Kind: "local", Path: t.TempDir()}, Credentials{Password: "test-password"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	catalog := testCatalog()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	catalog.WrittenAt = catalog.WrittenAt.Add(time.Second)
	err = repo.WriteSession(t.Context(), r.repo, repo.WriteSessionOptions{Purpose: "inject a competing writer"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.PutManifest(ctx, map[string]string{"type": "synod.catalog", "generation": "1", "installation": catalog.InstallationID.String()}, catalog)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadCatalog(t.Context()); !errors.Is(err, ErrCatalogConflict) {
		t.Fatalf("conflicting generation was selected: %v", err)
	}
}

func TestConnectRequiresInitializationPermissionAndKeepsExistingKeys(t *testing.T) {
	location := Location{Kind: "local", Path: t.TempDir()}
	credentials := Credentials{Password: "private-connect-password"}
	if r, err := Connect(t.Context(), location, credentials, false); !errors.Is(err, ErrUninitialized) {
		if r != nil {
			r.Close(t.Context())
		}
		t.Fatalf("unassigned initializer accepted: %v", err)
	}
	r, err := Connect(t.Context(), location, credentials, true)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		r.Close(t.Context())
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	wrong := credentials
	wrong.Password = "wrong-connect-password"
	if r, err := Connect(t.Context(), location, wrong, true); !errors.Is(err, repo.ErrInvalidPassword) {
		if r != nil {
			r.Close(t.Context())
		}
		t.Fatalf("wrong password could recreate the repository: %v", err)
	}
	r, err = Connect(t.Context(), location, credentials, false)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	got, err := r.ReadCatalog(t.Context())
	if err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("connect changed the repository keys: %v", err)
	}
}
