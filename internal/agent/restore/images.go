package restore

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"os"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/image"
)

// loadImages verifies every target image before loading any of them. It never uses a registry.
func loadImages(ctx context.Context, api *client.Client, r *repository.Repository, p *plan, architecture string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	var files []*os.File
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	images := make(map[string]v1.Image)
	var ids []string
	for _, saved := range p.Snapshot.Images {
		if saved.Validate() != nil {
			return &failure{Code: "image_archive_missing", Resource: saved.Reference}
		}
		for _, variant := range saved.Variants {
			if variant.Architecture != architecture {
				continue
			}
			if _, ok := images[variant.ID]; ok {
				continue
			}
			input, size, err := r.OpenFile(ctx, p.Snapshot.ID, variant.Archive)
			if err != nil {
				return &failure{Code: "image_archive_invalid", Resource: saved.Reference, Cause: err}
			}
			if size != variant.Size {
				input.Close()
				return &failure{Code: "image_archive_invalid", Resource: saved.Reference}
			}
			file, digest, copied, err := image.StageArchive(ctx, func(out io.Writer) error { _, err := io.Copy(out, input); return err })
			closeErr := input.Close()
			if file != nil {
				files = append(files, file)
			}
			if err != nil || closeErr != nil || digest != variant.Digest || copied != variant.Size {
				return &failure{Code: "image_archive_invalid", Resource: saved.Reference, Cause: errors.Join(err, closeErr)}
			}
			img, err := image.CheckArchive(ctx, file, variant.ID, architecture)
			if err != nil {
				return &failure{Code: "image_archive_invalid", Resource: saved.Reference, Cause: err}
			}
			images[variant.ID] = img
			ids = append(ids, variant.ID)
		}
	}
	for _, id := range ids {
		// Re-encode the checked image without tags or unrelated archive entries.
		// An image archive must not replace an existing tag on the target device.
		ref, err := name.NewDigest("synod.invalid/image@" + id)
		if err != nil {
			return &failure{Code: "image_archive_invalid", Resource: id, Cause: err}
		}
		reader, writer := io.Pipe()
		done := make(chan error, 1)
		go func() { err := tarball.Write(ref, images[id], writer); writer.CloseWithError(err); done <- err }()
		response, err := api.ImageLoad(ctx, reader)
		if err == nil {
			decoder := jsontext.NewDecoder(response)
			for {
				var message struct {
					Error string `json:"error"`
				}
				readErr := json.UnmarshalDecode(decoder, &message)
				if errors.Is(readErr, io.EOF) {
					break
				}
				if readErr != nil || message.Error != "" {
					err = errors.New("Docker rejected the saved image")
					break
				}
			}
			err = errors.Join(err, response.Close())
		}
		reader.CloseWithError(err)
		err = errors.Join(err, <-done)
		if err != nil {
			return &failure{Code: "image_load_failed", Resource: id, Cause: err}
		}
		inspected, err := api.ImageInspect(ctx, id)
		if err != nil || inspected.ID != id || inspected.Os != "linux" || inspected.Architecture != architecture {
			return &failure{Code: "image_load_failed", Resource: id, Cause: err}
		}
	}
	return nil
}
