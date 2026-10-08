package image

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

func TestImageArchiveRejectsCorruptLayersAndWrongIdentity(t *testing.T) {
	img, err := random.Image(4096, 2)
	if err != nil {
		t.Fatal(err)
	}
	config, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config.OS, config.Architecture = "linux", "arm64"
	img, err = mutate.ConfigFile(img, config)
	if err != nil {
		t.Fatal(err)
	}
	id, err := img.ConfigName()
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := name.NewDigest("synod.invalid/image@" + id.String())
	var original bytes.Buffer
	if err := tarball.Write(ref, img, &original); err != nil {
		t.Fatal(err)
	}
	for _, test := range []string{"good", "corrupt layer", "wrong image", "wrong architecture", "truncated", "cancelled"} {
		t.Run(test, func(t *testing.T) {
			data := bytes.Clone(original.Bytes())
			if test == "corrupt layer" {
				reader := bytes.NewReader(data)
				tr := tar.NewReader(reader)
				for {
					h, err := tr.Next()
					if err != nil {
						t.Fatal(err)
					}
					if h.Size > 1024 {
						offset := len(data) - reader.Len()
						data[offset+int(h.Size/2)] ^= 255
						break
					}
				}
			}
			if test == "truncated" {
				data = data[:len(data)/2]
			}
			file, digest, size, err := StageArchive(t.Context(), func(w io.Writer) error { _, err := w.Write(data); return err })
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if size != int64(len(data)) || len(digest) != 71 {
				t.Fatal("archive identity missing")
			}
			if _, err := os.Stat(file.Name()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("plaintext archive was left linked")
			}
			expected, architecture := id.String(), "arm64"
			if test == "wrong image" {
				expected = "sha256:wrong"
			}
			if test == "wrong architecture" {
				architecture = "amd64"
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test == "cancelled" {
				cancel()
			}
			_, err = CheckArchive(ctx, file, expected, architecture)
			if (err == nil) != (test == "good") {
				t.Fatalf("archive verification: %v", err)
			}
		})
	}
}
