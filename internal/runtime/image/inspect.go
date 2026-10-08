// Package image reads the platforms an image supports.
package image

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/moby/moby/client"
)

type ErrorCode string

const (
	InvalidReference    ErrorCode = "invalid_image_reference"
	Unavailable         ErrorCode = "image_inspection_unavailable"
	InvalidManifest     ErrorCode = "invalid_image_manifest"
	UnsupportedPlatform ErrorCode = "unsupported_image_platform"
)

type Error struct {
	Code  ErrorCode
	Cause error
}

func (e *Error) Error() string { return string(e.Code) }
func (e *Error) Unwrap() error { return e.Cause }

// Inspector uses anonymous registry access unless Keychain is set.
// A Docker fallback only proves that daemon's cached image.
type Inspector struct {
	Transport http.RoundTripper
	Keychain  authn.Keychain
	Docker    *client.Client
}

// Resolution is a pinned image and the Linux architectures it runs on.
// LocalOnly reports evidence that only one Docker daemon's cached image supplied.
type Resolution struct {
	Image         string
	Architectures []string
	Manifests     map[string]string
	LocalOnly     bool
}

// Resolve reads registry metadata without downloading image layers.
// It returns no resolution on failure. Calls have a 20-second upper bound.
func (i Inspector) Resolve(ctx context.Context, image string) (Resolution, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ref, err := name.ParseReference(image)
	if err != nil {
		return Resolution{}, &Error{Code: InvalidReference, Cause: err}
	}
	options := []remote.Option{remote.WithContext(ctx), remote.WithUserAgent("synod")}
	if i.Transport != nil {
		options = append(options, remote.WithTransport(i.Transport))
	}
	if i.Keychain != nil {
		options = append(options, remote.WithAuthFromKeychain(i.Keychain))
	}
	descriptor, err := remote.Get(ref, options...)
	if err != nil {
		if i.Docker == nil || ctx.Err() != nil {
			return Resolution{}, &Error{Code: Unavailable, Cause: err}
		}
		cached, localErr := i.Docker.ImageInspect(ctx, image)
		if localErr != nil {
			return Resolution{}, &Error{Code: Unavailable, Cause: errors.Join(err, localErr)}
		}
		if cached.Os != "linux" || (cached.Architecture != "amd64" && cached.Architecture != "arm64") || (cached.Variant != "" && !(cached.Architecture == "arm64" && cached.Variant == "v8") && !(cached.Architecture == "amd64" && cached.Variant == "v1")) {
			return Resolution{}, &Error{Code: UnsupportedPlatform}
		}
		if _, err := v1.NewHash(cached.ID); err != nil {
			return Resolution{}, &Error{Code: InvalidManifest, Cause: err}
		}
		return Resolution{Image: cached.ID, Architectures: []string{cached.Architecture}, LocalOnly: true}, nil
	}
	pinned := ref.Context().Digest(descriptor.Digest.String()).Name()
	architectures := map[string]struct{}{}
	manifests := make(map[string]string)
	// A bounded queue supports nested OCI indexes without recursive registry walks.
	queue := []*remote.Descriptor{descriptor}
	seen := map[string]bool{}
	for n := 0; n < len(queue); n++ {
		if len(queue) > 128 {
			return Resolution{}, &Error{Code: InvalidManifest, Cause: fmt.Errorf("image index exceeds 128 manifests")}
		}
		current := queue[n]
		digest := current.Digest.String()
		if seen[digest] {
			continue
		}
		seen[digest] = true
		switch current.MediaType {
		case types.OCIImageIndex, types.DockerManifestList:
			index, err := current.ImageIndex()
			if err != nil {
				return Resolution{}, &Error{Code: InvalidManifest, Cause: err}
			}
			manifest, err := index.IndexManifest()
			if err != nil {
				return Resolution{}, &Error{Code: InvalidManifest, Cause: err}
			}
			for _, child := range manifest.Manifests {
				// Attestations and non-Linux descriptors do not run.
				if child.Platform != nil && (child.Platform.OS != "linux" || (child.Platform.Architecture != "amd64" && child.Platform.Architecture != "arm64")) {
					continue
				}
				if child.Platform != nil && child.Platform.Variant != "" && !(child.Platform.Architecture == "arm64" && child.Platform.Variant == "v8") && !(child.Platform.Architecture == "amd64" && child.Platform.Variant == "v1") {
					continue
				}
				if child.MediaType != types.OCIImageIndex && child.MediaType != types.DockerManifestList && child.MediaType != types.OCIManifestSchema1 && child.MediaType != types.DockerManifestSchema2 {
					continue
				}
				if len(queue) >= 128 {
					return Resolution{}, &Error{Code: InvalidManifest, Cause: fmt.Errorf("image index exceeds 128 manifests")}
				}
				next, err := remote.Get(ref.Context().Digest(child.Digest.String()), options...)
				if err != nil {
					return Resolution{}, &Error{Code: Unavailable, Cause: err}
				}
				if next.MediaType != child.MediaType {
					return Resolution{}, &Error{Code: InvalidManifest, Cause: fmt.Errorf("child media type differs from index")}
				}
				if child.Platform != nil {
					next.Platform = child.Platform
				}
				queue = append(queue, next)
			}
		case types.OCIManifestSchema1, types.DockerManifestSchema2:
			manifestImage, err := current.Image()
			if err != nil {
				return Resolution{}, &Error{Code: InvalidManifest, Cause: err}
			}
			config, err := manifestImage.ConfigFile()
			if err != nil {
				return Resolution{}, &Error{Code: InvalidManifest, Cause: err}
			}
			if p := current.Platform; p != nil && (p.OS != config.OS || p.Architecture != config.Architecture) {
				return Resolution{}, &Error{Code: InvalidManifest, Cause: fmt.Errorf("image config differs from index platform")}
			}
			if config.OS == "linux" && (config.Architecture == "amd64" || config.Architecture == "arm64") {
				if config.Variant == "" || (config.Architecture == "arm64" && config.Variant == "v8") || (config.Architecture == "amd64" && config.Variant == "v1") {
					architectures[config.Architecture] = struct{}{}
					if manifests[config.Architecture] == "" {
						manifests[config.Architecture] = ref.Context().Digest(current.Digest.String()).Name()
					}
				}
			}
		default:
			return Resolution{}, &Error{Code: InvalidManifest, Cause: fmt.Errorf("unsupported manifest media type")}
		}
	}
	if len(architectures) == 0 {
		return Resolution{}, &Error{Code: UnsupportedPlatform}
	}
	return Resolution{Image: pinned, Manifests: manifests, Architectures: slices.Sorted(maps.Keys(architectures))}, nil
}
