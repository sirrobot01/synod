package backup

import (
	"context"
	"errors"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/runtime/image"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// imageEvidence resolves the running digest, never the current value of a mutable tag.
func (w *Worker) imageEvidence(ctx context.Context, app *agentv1.DiscoveredApp) ([]backup.Image, error) {
	resolved := make(map[string]image.Resolution)
	var result []backup.Image
	for _, c := range app.Containers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reference := c.ImageId
		if len(c.RepoDigests) > 0 {
			reference = c.RepoDigests[0]
		}
		evidence, ok := resolved[reference]
		if !ok {
			architecture, linux := strings.CutPrefix(c.Platform, "linux/")
			architecture, variant, _ := strings.Cut(architecture, "/")
			if !linux || (architecture != "amd64" && architecture != "arm64") || (variant != "" && !(architecture == "arm64" && variant == "v8") && !(architecture == "amd64" && variant == "v1")) {
				return nil, &image.Error{Code: image.UnsupportedPlatform}
			}
			if _, err := v1.NewHash(c.ImageId); err != nil {
				return nil, &image.Error{Code: image.InvalidManifest}
			}
			evidence = image.Resolution{Image: c.ImageId, Architectures: []string{architecture}, LocalOnly: true}
			if len(c.RepoDigests) > 0 {
				remote, err := (image.Inspector{}).Resolve(ctx, reference)
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if err == nil {
					evidence = remote
				} else if failure, ok := errors.AsType[*image.Error](err); !ok || failure.Code != image.Unavailable {
					return nil, err
				}
				// An offline registry must not prevent a local backup. The saved
				// evidence then proves only the running image and its architecture.
			}
			resolved[reference] = evidence
		}
		result = append(result, backup.Image{ContainerID: c.Id, Reference: evidence.Image, Architectures: evidence.Architectures, LocalOnly: evidence.LocalOnly})
	}
	return result, nil
}
