package docker

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"path"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/engine/redis"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// checkStartStorage checks engine data after container ownership is verified.
func (r *Runtime) checkStartStorage(ctx context.Context, command *agentv1.Command, item container.InspectResponse) error {
	encoded, err := base64.RawStdEncoding.DecodeString(item.Config.Labels[labelConfig])
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	var stored agentv1.EnsureContainer
	if err := proto.Unmarshal(encoded, &stored); err != nil {
		return &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	var marker string
	switch stored.Engine {
	case "postgres":
		if stored.Postgres != nil && stored.Postgres.Patroni != nil {
			return runtime.ErrUnsupportedConfiguration
		}
		// A pooler or router holds no data. Its protected files are checked instead.
		if stored.Route != nil {
			return r.inspectSource(ctx, item, routeSource(stored.Route))
		}
		// The marker must refer to the directory the image will use inside the owned mount.
		dataDirectory := ""
		for _, entry := range item.Config.Env {
			if value, ok := strings.CutPrefix(entry, "PGDATA="); ok {
				dataDirectory = value
			}
		}
		if dataDirectory != postgres.DataDirectory {
			return runtime.ErrUnsupportedConfiguration
		}
		marker = postgres.DataVersionFile
	case "redis":
		// Redis names its own directory in the generated configuration, so there is no
		// environment variable to confirm. The marker is Synod's own file.
		marker = redis.DataMarkerFile
	default:
		return nil
	}
	started, err := time.Parse(time.RFC3339Nano, item.State.StartedAt)
	if err != nil {
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if command.Input.GetStartContainer().AllowInitialization && started.IsZero() && item.State.Status == container.StateCreated {
		return nil
	}
	found, err := r.api.ContainerStatPath(ctx, item.ID, client.ContainerStatPathOptions{Path: marker})
	if err != nil {
		if errdefs.IsNotFound(err) {
			// A path lookup also returns 404 when the container itself disappeared.
			current, inspectErr := r.api.ContainerInspect(ctx, item.ID, client.ContainerInspectOptions{})
			if inspectErr != nil {
				return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: inspectErr}
			}
			if current.Container.State == nil || current.Container.State.Running || current.Container.State.Restarting || len(current.Container.ExecIDs) != 0 {
				return runtime.ErrOutcomeUnknown
			}
			return runtime.ErrStorageLost
		}
		return &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if !found.Stat.Mode.IsRegular() || found.Stat.Size == 0 || found.Stat.LinkTarget != "" {
		return runtime.ErrStorageLost
	}
	return nil
}

// containerRecord reads one small regular file from a container. It rejects links,
// large files, and extra archive entries.
func (r *Runtime) containerRecord(ctx context.Context, containerID, filename string) (string, error) {
	copy, err := r.api.CopyFromContainer(ctx, containerID, client.CopyFromContainerOptions{SourcePath: filename})
	if err != nil {
		return "", err
	}
	defer copy.Content.Close()
	if !copy.Stat.Mode.IsRegular() || copy.Stat.LinkTarget != "" || copy.Stat.Size < 1 || copy.Stat.Size > 256 {
		return "", runtime.ErrOutcomeUnknown
	}
	archive := tar.NewReader(io.LimitReader(copy.Content, 8192))
	header, err := archive.Next()
	if err != nil || header.Typeflag != tar.TypeReg || header.Name != path.Base(filename) || header.Linkname != "" || header.Size != copy.Stat.Size {
		return "", runtime.ErrOutcomeUnknown
	}
	content, err := io.ReadAll(io.LimitReader(archive, 257))
	if err != nil || int64(len(content)) != header.Size {
		return "", runtime.ErrOutcomeUnknown
	}
	if _, err := archive.Next(); !errors.Is(err, io.EOF) {
		return "", runtime.ErrOutcomeUnknown
	}
	return string(content), nil
}
