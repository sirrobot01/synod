package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirrobot01/synod/internal/backup"
)

// TestM1HostProof runs only with the private fixture file from scripts/proof-m1.py.
// The owner must approve that script before it changes the real hosts.
func TestM1HostProof(t *testing.T) {
	file := os.Getenv("SYNOD_M1_PROOF_SPEC")
	if file == "" {
		t.Skip("run the approved scripts/proof-m1.py proof")
	}
	var spec struct {
		Stage          string      `json:"stage"`
		InstallationID uuid.UUID   `json:"installation_id"`
		RepositoryPath string      `json:"repository_path"`
		Endpoint       string      `json:"endpoint"`
		Bucket         string      `json:"bucket"`
		Credentials    Credentials `json:"credentials"`
		Copies         []struct {
			SnapshotID    uuid.UUID `json:"snapshot_id"`
			AppID         uuid.UUID `json:"app_id"`
			DestinationID uuid.UUID `json:"destination_id"`
			Kind          string    `json:"kind"`
			Architecture  string    `json:"architecture"`
			Project       string    `json:"project"`
			State         string    `json:"state"`
			Environment   string    `json:"environment"`
		} `json:"copies"`
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal("read private proof settings")
	}
	defer clear(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&spec) != nil || !strings.HasPrefix(spec.Bucket, "synod-m1-") {
		t.Fatal("invalid private proof settings")
	}
	endpoint, err := url.Parse(spec.Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") {
		t.Fatal("invalid proof S3 endpoint")
	}
	api, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(spec.Credentials.AccessKeyID, spec.Credentials.SecretAccessKey, ""), Secure: endpoint.Scheme == "https", Region: "us-east-1"})
	if err != nil {
		t.Fatal("open proof S3 endpoint")
	}
	if spec.Stage == "prepare" {
		if err := api.MakeBucket(t.Context(), spec.Bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			t.Fatal("create the proof bucket")
		}
		return
	}
	if spec.Stage != "verify" || spec.InstallationID == uuid.Nil || len(spec.Copies) != 4 || !filepath.IsAbs(spec.RepositoryPath) {
		t.Fatal("invalid proof copy selection")
	}
	seen := make(map[uuid.UUID]bool)
	combinations := make(map[string]bool)
	for _, copy := range spec.Copies {
		if copy.SnapshotID == uuid.Nil || copy.AppID == uuid.Nil || copy.DestinationID == uuid.Nil || seen[copy.SnapshotID] || copy.State == "" || copy.Environment == "" || !filepath.IsAbs(copy.Project) || (copy.Kind != "device" && copy.Kind != "s3") || (copy.Architecture != "amd64" && copy.Architecture != "arm64") {
			t.Fatal("invalid proof snapshot")
		}
		seen[copy.SnapshotID] = true
		key := copy.Architecture + "/" + copy.Kind
		if combinations[key] {
			t.Fatal("proof lacks one architecture and destination combination")
		}
		combinations[key] = true
	}
	for _, copy := range spec.Copies {
		t.Run(copy.Architecture+"/"+copy.Kind, func(t *testing.T) {
			location := Location{Kind: "local", Path: spec.RepositoryPath}
			keys := Credentials{Password: spec.Credentials.Password}
			if copy.Kind == "s3" {
				location = Location{Kind: "s3", S3: &S3Location{Endpoint: spec.Endpoint, Bucket: spec.Bucket, Region: "us-east-1"}}
				keys = spec.Credentials
			}
			r, err := Open(t.Context(), location, keys)
			if err != nil {
				t.Fatal("open the saved repository:", err)
			}
			defer r.Close(context.Background())
			catalog, err := r.ReadCatalog(t.Context())
			if err != nil || catalog.InstallationID != spec.InstallationID || len(catalog.Destinations) != 2 || len(catalog.Protection) != 2 {
				t.Fatal("repository did not preserve complete recovery choices")
			}
			metadata, err := r.Snapshot(t.Context(), copy.SnapshotID)
			if err != nil || metadata.App.ID != copy.AppID || metadata.DestinationID != copy.DestinationID || metadata.Architecture != copy.Architecture || metadata.Method != backup.Stop || metadata.DowntimeMillis <= 0 {
				t.Fatal("snapshot lost its source, destination, method, or downtime")
			}
			target := filepath.Join(t.TempDir(), "restore")
			if err := r.RestoreFiles(t.Context(), copy.SnapshotID, target); err != nil {
				t.Fatal("restore the proof files:", err)
			}
			found := false
			for _, path := range metadata.Paths {
				if path.Kind != "project" || path.Source != copy.Project {
					continue
				}
				found = true
				for name, want := range map[string]string{"state": copy.State, ".env": copy.Environment} {
					data, err := os.ReadFile(filepath.Join(target, path.Archive, name))
					if err != nil || string(data) != want {
						t.Fatalf("restored %s differs from the stopped source", name)
					}
				}
				compose, err := os.ReadFile(filepath.Join(target, path.Archive, "compose.yaml"))
				if err != nil || !bytes.Contains(compose, []byte("services:")) {
					t.Fatal("snapshot lost the Compose definition")
				}
			}
			if !found {
				t.Fatal("snapshot lost the project directory")
			}
			definition, err := os.ReadFile(filepath.Join(target, "definition", "inspect.json"))
			if err != nil || !bytes.Contains(definition, []byte("com.docker.compose.project")) {
				t.Fatal("snapshot lost its private container definition")
			}
		})
	}
	needles := [][]byte{[]byte(spec.Credentials.Password), []byte(spec.Credentials.SecretAccessKey)}
	for _, copy := range spec.Copies {
		needles = append(needles, []byte(copy.State), []byte(copy.Environment))
	}
	if err := filepath.WalkDir(spec.RepositoryPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, needle := range needles {
			if bytes.Contains(data, needle) {
				t.Error("device repository contains a plaintext private marker")
			}
		}
		return nil
	}); err != nil {
		t.Fatal("inspect encrypted device repository files")
	}
	objects := 0
	for object := range api.ListObjects(t.Context(), spec.Bucket, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil || object.Size > 64<<20 {
			t.Fatal("list bounded proof S3 objects")
		}
		reader, err := api.GetObject(t.Context(), spec.Bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal("read proof S3 object")
		}
		data, err := io.ReadAll(io.LimitReader(reader, 64<<20+1))
		closeErr := reader.Close()
		if err != nil || closeErr != nil || len(data) > 64<<20 {
			t.Fatal("read bounded proof S3 object")
		}
		for _, needle := range needles {
			if bytes.Contains(data, needle) {
				t.Error("S3 repository contains a plaintext private marker")
			}
		}
		objects++
	}
	if objects == 0 {
		t.Fatal("S3 repository has no encrypted objects")
	}
}
