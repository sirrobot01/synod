package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
)

// maxEngineOutput bounds what one engine step may return, such as a schema dump.
const maxEngineOutput = 32 << 20

// EngineApps lists running, owned tier 1 apps without running database commands.
func (r *Runtime) EngineApps(ctx context.Context) ([]uuid.UUID, error) {
	filters := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String(), "synod.tier=1")
	found, err := r.api.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
	if err != nil {
		return nil, err
	}
	var apps []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for _, item := range found.Items {
		if len(item.Names) != 1 {
			return nil, runtime.ErrOwnershipConflict
		}
		identity, err := r.inventoryIdentity(item.Labels, "container", strings.TrimPrefix(item.Names[0], "/"), "synod-instance-")
		if err != nil {
			return nil, err
		}
		if !seen[identity.AppID] {
			apps = append(apps, identity.AppID)
			seen[identity.AppID] = true
		}
	}
	return apps, nil
}

// EngineExec runs one program in the running tier 1 copy of an app on this node.
// It returns standard output, and an error with standard error when the program fails.
func (r *Runtime) EngineExec(ctx context.Context, appID uuid.UUID, env []string, stdin string, args ...string) ([]byte, error) {
	if appID == uuid.Nil || len(args) == 0 {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	filters := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String(), "synod.tier=1", "synod.app_id="+appID.String())
	found, err := r.api.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if len(found.Items) != 1 {
		return nil, fmt.Errorf("%w: %d running copies of app %s", runtime.ErrNotApplied, len(found.Items), appID)
	}
	created, err := r.api.ExecCreate(ctx, found.Items[0].ID, client.ExecCreateOptions{Cmd: args, Env: env, AttachStdin: true, AttachStdout: true, AttachStderr: true})
	if err != nil {
		return nil, err
	}
	attached, err := r.api.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
	if err != nil {
		return nil, err
	}
	defer attached.Close()
	written := make(chan error, 1)
	go func() {
		_, err := io.Copy(attached.Conn, strings.NewReader(stdin))
		written <- errors.Join(err, attached.CloseWrite())
	}()
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&limitedWriter{buffer: &stdout, limit: maxEngineOutput}, &limitedWriter{buffer: &stderr, limit: 64 << 10}, attached.Reader); err != nil {
		return nil, err
	}
	if err := <-written; err != nil {
		return nil, err
	}
	inspected, err := r.api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
	if err != nil {
		return nil, err
	}
	if inspected.Running || inspected.ExitCode != 0 {
		return stdout.Bytes(), fmt.Errorf("engine step %s exited with %d: %s", args[0], inspected.ExitCode, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// limitedWriter refuses output beyond its limit instead of holding it in memory.
type limitedWriter struct {
	buffer *bytes.Buffer
	limit  int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buffer.Len()+len(p) > w.limit {
		return 0, fmt.Errorf("engine step output exceeds %d bytes", w.limit)
	}
	return w.buffer.Write(p)
}

// FenceEngine stops every tier 1 copy of an app on this node. The supervisor calls it
// when this node's primary loses its writer lease.
func (r *Runtime) FenceEngine(ctx context.Context, appID uuid.UUID) error {
	filters := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String(), "synod.tier=1", "synod.app_id="+appID.String())
	found, err := r.api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return err
	}
	for _, item := range found.Items {
		if len(item.Names) != 1 {
			return runtime.ErrOwnershipConflict
		}
		identity, err := r.inventoryIdentity(item.Labels, "container", strings.TrimPrefix(item.Names[0], "/"), "synod-instance-")
		if err != nil {
			return err
		}
		if err := r.FenceApp(ctx, runtime.AppContainer{AppID: appID.String(), ResourceID: identity.ResourceID.String()}); err != nil {
			return err
		}
	}
	return nil
}
