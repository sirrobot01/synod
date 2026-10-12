package backup

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	kfs "github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/image"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type imageArchives struct {
	images  []backup.Image
	entries []kfs.Entry
	files   []*os.File
}

func (a *imageArchives) Close() {
	for _, entry := range a.entries {
		entry.Close()
	}
	for _, file := range a.files {
		file.Close()
	}
}

// captureImages reuses immutable repository content before asking a registry or Docker.
func (w *Worker) captureImages(ctx context.Context, r *repository.Repository, app *agentv1.DiscoveredApp, prior *backup.Snapshot) (*imageArchives, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if prior != nil {
		for _, saved := range prior.Images {
			if !saved.LocalOnly && saved.Validate() == nil && slices.ContainsFunc(app.Containers, func(c *agentv1.DiscoveredContainer) bool {
				return c.ImageId == saved.SourceID && slices.ContainsFunc(c.RepoDigests, func(reference string) bool {
					ref, err := name.NewDigest(reference)
					return err == nil && ref.Name() == saved.Reference
				})
			}) {
				w.images.Store(saved.Reference, image.Resolution{Image: saved.Reference, Architectures: saved.Architectures})
			}
		}
	}
	images, err := w.imageEvidence(ctx, app)
	if err != nil {
		return nil, err
	}
	result := &imageArchives{images: images}
	ready := false
	defer func() {
		if !ready {
			result.Close()
		}
	}()
	savedFiles := make(map[string]backup.ImageVariant)
	for i := range result.images {
		current := &result.images[i]
		c := app.Containers[i]
		sourceArchitecture, _, _ := strings.Cut(strings.TrimPrefix(c.Platform, "linux/"), "/")
		if !slices.Contains(current.Architectures, sourceArchitecture) {
			return nil, &image.Error{Code: image.InvalidManifest}
		}
		current.SourceArchitecture = sourceArchitecture
		for _, architecture := range current.Architectures {
			var previous *backup.ImageVariant
			if prior != nil {
				for _, saved := range prior.Images {
					if saved.SourceID != current.SourceID || saved.Reference != current.Reference {
						continue
					}
					for _, variant := range saved.Variants {
						if variant.Architecture == architecture && variant.Validate() == nil {
							previous = new(variant)
							break
						}
					}
				}
			}
			if previous != nil {
				if existing, ok := savedFiles[previous.Archive]; ok {
					if existing != *previous {
						return nil, backup.ErrInvalid
					}
				} else {
					entry, err := r.SnapshotEntry(ctx, prior.ID, previous.Archive)
					if err != nil {
						return nil, err
					}
					if !entry.Mode().IsRegular() || entry.Size() != previous.Size {
						entry.Close()
						return nil, backup.ErrInvalid
					}
					result.entries = append(result.entries, entry)
					savedFiles[previous.Archive] = *previous
				}
				current.Variants = append(current.Variants, *previous)
				continue
			}
			var img v1.Image
			// id is the config digest of this platform. It names the image in both Docker
			// image stores. The containerd store gives a running image an index or manifest
			// digest instead, so the source variant takes its id from the saved archive.
			id := ""
			if !current.LocalOnly {
				reference := current.Reference
				if evidence, ok := w.images.Load(current.Reference); ok && evidence.(image.Resolution).Manifests[architecture] != "" {
					reference = evidence.(image.Resolution).Manifests[architecture]
				}
				ref, err := name.NewDigest(reference)
				if err != nil {
					return nil, err
				}
				img, err = remote.Image(ref, remote.WithContext(ctx), remote.WithPlatform(v1.Platform{OS: "linux", Architecture: architecture}), remote.WithUserAgent("synod"))
				if err != nil {
					return nil, err
				}
				config, err := img.ConfigName()
				if err != nil {
					return nil, err
				}
				id = config.String()
			}
			if architecture != sourceArchitecture {
				if existing, ok := savedFiles["images/"+strings.TrimPrefix(id, "sha256:")+".tar"]; ok {
					if existing.Architecture != architecture {
						return nil, backup.ErrInvalid
					}
					current.Variants = append(current.Variants, existing)
					continue
				}
			}
			file, digest, size, err := image.StageArchive(ctx, func(output io.Writer) error {
				if architecture != sourceArchitecture {
					ref, err := name.NewDigest("synod.invalid/image@" + id)
					if err != nil {
						return err
					}
					return tarball.Write(ref, img, output)
				}
				// A multi-platform image must give one platform. An older daemon refuses the
				// platform option; its classic store holds one platform for the image ID.
				input, err := w.Docker.ImageSave(ctx, []string{current.SourceID}, client.ImageSaveWithPlatforms(ocispec.Platform{OS: "linux", Architecture: architecture}))
				if err != nil {
					if input, err = w.Docker.ImageSave(ctx, []string{current.SourceID}); err != nil {
						return err
					}
				}
				_, copyErr := io.Copy(output, input)
				return errors.Join(copyErr, input.Close())
			})
			if file != nil {
				result.files = append(result.files, file)
			}
			if err != nil {
				return nil, err
			}
			if architecture == sourceArchitecture {
				saved, err := image.ArchiveConfig(ctx, file)
				if err != nil {
					return nil, err
				}
				if id != "" && saved != id {
					return nil, &image.Error{Code: image.InvalidManifest}
				}
				id = saved
			}
			archive := "images/" + strings.TrimPrefix(id, "sha256:") + ".tar"
			if existing, ok := savedFiles[archive]; ok {
				if existing.Architecture != architecture {
					return nil, backup.ErrInvalid
				}
				current.Variants = append(current.Variants, existing)
				continue
			}
			if _, err := image.CheckArchive(ctx, file, id, architecture); err != nil {
				return nil, err
			}
			variant := backup.ImageVariant{Architecture: architecture, ID: id, Archive: archive, Digest: digest, Size: size}
			if err := variant.Validate(); err != nil {
				return nil, err
			}
			current.Variants = append(current.Variants, variant)
			savedFiles[archive] = variant
			result.entries = append(result.entries, &definitionFile{virtualfs.StreamingFileFromReader(path.Base(archive), io.NopCloser(io.NewSectionReader(file, 0, size)))})
		}
	}
	kfs.Sort(result.entries)
	ready = true
	return result, nil
}
