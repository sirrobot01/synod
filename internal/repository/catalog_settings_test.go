package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestRecoveryCatalogRestoresPrivateSettingsFromRepository(t *testing.T) {
	catalog := testCatalog()
	target := CatalogDestination{ID: uuid.New(), Name: "other-copy", NodeID: catalog.Devices[0].ID, Location: Location{Kind: "local", Path: "/other-copy"}, Credentials: Credentials{Password: "private-other-repository-password"}}
	catalog.Destinations = []CatalogDestination{target}
	settings := backup.DefaultSettings()
	settings.Destinations = []uuid.UUID{target.ID}
	settings.At, settings.TimeZone = "01:45", "Africa/Lagos"
	catalog.Defaults, catalog.AutoProtect = &settings, true
	catalog.Protection = []backup.Selection{{AppID: catalog.Apps[0].ID, Enabled: true, Settings: settings}}
	compressed, err := EncodeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCatalog(compressed)
	a, _ := json.Marshal(catalog)
	b, _ := json.Marshal(decoded)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("catalog transfer changed settings: %v", err)
	}
	location := Location{Kind: "local", Path: filepath.Join(t.TempDir(), "repository")}
	credentials := Credentials{Password: "private-current-password"}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { r.Close(context.Background()) }()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := r.ReadCatalog(t.Context())
	b, _ = json.Marshal(restored)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("repository lost recovery settings: %v", err)
	}
	broken := catalog
	broken.Destinations = nil
	if _, err := EncodeCatalog(broken); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatalf("unresolved recovery destination accepted: %v", err)
	}
}

func TestCatalogTransferBoundsDecompression(t *testing.T) {
	if _, err := DecodeCatalog([]byte("not gzip")); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatal("invalid compressed catalog accepted")
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(bytes.Repeat([]byte(" "), MaxCatalogBytes+1)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCatalog(compressed.Bytes()); !errors.Is(err, ErrCatalogInvalid) {
		t.Fatal("oversized decoded catalog accepted")
	}
}
