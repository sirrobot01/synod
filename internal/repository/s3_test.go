package repository

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirrobot01/synod/internal/backup"
)

func TestMinIOEncryptedCatalog(t *testing.T) {
	endpoint := os.Getenv("SYNOD_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("run scripts/test-minio.sh for the S3 proof")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	keys := Credentials{Password: "private-repository-password", AccessKeyID: os.Getenv("SYNOD_TEST_S3_ACCESS_KEY"), SecretAccessKey: os.Getenv("SYNOD_TEST_S3_SECRET_KEY")}
	api, err := minio.New(parsed.Host, &minio.Options{Creds: credentials.NewStaticV4(keys.AccessKeyID, keys.SecretAccessKey, ""), Secure: parsed.Scheme == "https", Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "synod-test-" + uuid.NewString()
	if err := api.MakeBucket(t.Context(), bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatal("create the test bucket:", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for object := range api.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err != nil {
				t.Error(object.Err)
				continue
			}
			if err := api.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
				t.Error(err)
			}
		}
		if err := api.RemoveBucket(ctx, bucket); err != nil {
			t.Error(err)
		}
	})
	location := Location{Kind: "s3", S3: &S3Location{Endpoint: endpoint, Bucket: bucket, Prefix: "synod/", Region: "us-east-1"}}
	r, err := Create(t.Context(), location, keys)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		r.Close(t.Context())
		t.Fatal(err)
	}

	source := t.TempDir()
	content := bytes.Repeat([]byte("private-S3-snapshot-content\n"), 4096)
	if err := os.WriteFile(filepath.Join(source, "state"), content, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := localfs.NewEntry(source)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	copied, err := r.UploadFiles(t.Context(), root, snapshot.SourceInfo{Host: "test", UserName: "test", Path: "test-app"}, false)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := r.PublishSnapshot(t.Context(), copied, backup.Snapshot{ID: uuid.New(), InstallationID: catalog.InstallationID, DestinationID: uuid.New(), App: catalog.Apps[0], Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, keys)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadCatalog(t.Context())
	if err != nil || !reflect.DeepEqual(got, catalog) {
		r.Close(t.Context())
		t.Fatalf("S3 catalog after reopen: %v", err)
	}

	target := filepath.Join(t.TempDir(), "restore")
	if err := r.RestoreFiles(t.Context(), saved.ID, target); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(target, "state"))
	if err != nil || !bytes.Equal(restored, content) {
		t.Fatalf("S3 snapshot restore differs: %v", err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for object := range api.ListObjects(t.Context(), bucket, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		body, err := api.GetObject(t.Context(), bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(body)
		body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if bytes.Contains(data, []byte("private-catalog-marker")) || bytes.Contains(data, []byte(keys.Password)) || bytes.Contains(data, []byte("private-S3-snapshot-content")) {
			t.Fatal("S3 holds plaintext catalog data")
		}
	}
	wrong := keys
	wrong.Password = "wrong-private-password"
	if r, err := Open(t.Context(), location, wrong); !errors.Is(err, repo.ErrInvalidPassword) {
		if r != nil {
			r.Close(t.Context())
		}
		t.Fatalf("S3 wrong password: %v", err)
	}
	wrong = keys
	wrong.SecretAccessKey = "wrong-private-secret"
	if r, err := Open(t.Context(), location, wrong); err == nil {
		r.Close(t.Context())
		t.Fatal("S3 accepted wrong credentials")
	} else if strings.Contains(err.Error(), wrong.SecretAccessKey) {
		t.Fatal("S3 credentials reached an error")
	}
}
