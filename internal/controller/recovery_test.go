package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/store"
)

func TestInitRecoversMovedRepositoryWithoutOldControllerFiles(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "lost-controller")
	if err := runInit(t.Context(), InitOptions{Directory: old, SystemdDir: filepath.Join(root, "absent")}, io.Discard); err != nil {
		t.Fatal(err)
	}
	installation := uuid.MustParse(strings.TrimSpace(readFile(t, old, "installation-id")))
	oldCA, oldKey, oldAdmin := readFile(t, old, "ca.crt"), readFile(t, old, "secret.key"), readFile(t, old, "admin.token")
	location := repository.Location{Kind: "local", Path: filepath.Join(root, "repository")}
	credentials := repository.Credentials{Password: "private-disaster-recovery-password"}
	r, err := repository.Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	node, dest := uuid.New(), uuid.New()
	catalog := repository.Catalog{Version: 1, InstallationID: installation, Generation: 50, WrittenAt: time.Now(), Devices: []repository.Device{{ID: node, Name: "saved", Architecture: "amd64"}}, Destinations: []repository.CatalogDestination{{ID: dest, Name: "survivor", NodeID: node, Location: location, Credentials: credentials}}}
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(old); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(root, "moved-repository")
	if err := os.Rename(location.Path, moved); err != nil {
		t.Fatal(err)
	}
	password := filepath.Join(root, "password")
	if err := os.WriteFile(password, []byte(credentials.Password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "recovered")
	options := InitOptions{Directory: dir, FromRepository: moved, PasswordFile: password, SystemdDir: filepath.Join(root, "absent")}
	var out bytes.Buffer
	if err := runInit(t.Context(), options, &out); err != nil {
		t.Fatal(err)
	}
	if readFile(t, dir, "ca.crt") == oldCA || readFile(t, dir, "secret.key") == oldKey || readFile(t, dir, "admin.token") == oldAdmin || strings.TrimSpace(readFile(t, dir, "installation-id")) != installation.String() {
		t.Fatal("recovery did not preserve the installation with new keys")
	}
	if strings.Contains(out.String(), credentials.Password) {
		t.Fatal("recovery printed the password")
	}
	if kit := readFile(t, dir, "recovery-kit.txt"); len(credentials.Password) >= 16 && !strings.Contains(kit, credentials.Password) {
		t.Fatal("the recovered controller did not keep the repository password in its recovery kit")
	}
	var token jointoken.Token
	for line := range strings.Lines(out.String()) {
		if strings.HasPrefix(strings.TrimSpace(line), "synod_join_") {
			token, err = jointoken.Decode(strings.TrimSpace(line))
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if token.InstallationID != installation || token.NodeID != node {
		t.Fatal("initial recovery token lost the repository owner's ID")
	}
	metadata, err := store.Open(t.Context(), filepath.Join(dir, "synod.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metadata.Close()
	listing, err := metadata.ListDestinations(t.Context())
	if err != nil || listing.Revision != 51 || len(listing.Destinations) != 1 || listing.Destinations[0].Location.Path != moved {
		t.Fatalf("moved destination was not recovered: %v", err)
	}
	if err := runInit(t.Context(), options, io.Discard); err == nil {
		t.Fatal("recovery replaced an existing controller")
	}
	if _, err := os.Stat(filepath.Join(dir, "synod.db")); err != nil {
		t.Fatal("refused recovery deleted the controller")
	}
	options.Directory = filepath.Join(root, "wrong-password")
	if err := os.WriteFile(password, []byte("wrong"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runInit(t.Context(), options, io.Discard); err == nil {
		t.Fatal("wrong password accepted")
	}
	if _, err := os.Stat(options.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("wrong password left partial controller files")
	}
}

func TestRecoverySelectsDestinationAndRollsBackInvalidImport(t *testing.T) {
	root := t.TempDir()
	location := repository.Location{Kind: "local", Path: filepath.Join(root, "repository")}
	credentials := repository.Credentials{Password: "private-recovery-password"}
	r, err := repository.Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	catalog := repository.Catalog{Version: 1, InstallationID: uuid.New(), Generation: 1, WrittenAt: time.Now()}
	for range 2 {
		catalog.Destinations = append(catalog.Destinations, repository.CatalogDestination{ID: uuid.New(), Name: "same", Location: repository.Location{Kind: "s3", S3: &repository.S3Location{Endpoint: "https://example.com", Bucket: "backups"}}, Credentials: repository.Credentials{Password: credentials.Password, AccessKeyID: "private-access", SecretAccessKey: "private-key"}})
	}
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	connection := filepath.Join(root, "connection.json")
	raw, _ := json.Marshal(struct {
		Location    repository.Location    `json:"location"`
		Credentials repository.Credentials `json:"credentials"`
	}{location, credentials})
	if err := os.WriteFile(connection, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	options := InitOptions{Directory: filepath.Join(root, "new-controller"), FromRepository: connection, SystemdDir: filepath.Join(root, "systemd")}
	if _, _, _, err := readRecovery(t.Context(), options); err == nil || !strings.Contains(err.Error(), "--repository-destination") {
		t.Fatalf("ambiguous destination: %v", err)
	}
	options.RepositoryDestination = catalog.Destinations[0].ID.String()
	recovered, node, _, err := readRecovery(t.Context(), options)
	if err != nil || node == uuid.Nil || len(recovered.Devices) != 1 || recovered.Destinations[0].Location.Kind != "local" || recovered.Destinations[0].NodeID != node {
		t.Fatalf("recovery from downloaded remote storage: %v", err)
	}
	if err := os.Mkdir(options.SystemdDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runInit(t.Context(), options, io.Discard); err == nil {
		t.Fatal("invalid duplicate names imported")
	}
	if _, err := os.Stat(options.Directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed import left a partial controller")
	}
	if files, err := os.ReadDir(options.SystemdDir); err != nil || len(files) != 0 {
		t.Fatal("failed import installed a service")
	}
	options.InstallationID = uuid.NewString()
	if _, _, _, err := readRecovery(t.Context(), options); err == nil {
		t.Fatal("conflicting installation ID accepted")
	}
	options.InstallationID = ""
	if err := os.Chmod(connection, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := readRecovery(t.Context(), options); err == nil {
		t.Fatal("public credentials file accepted")
	}
}
