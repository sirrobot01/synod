package restore

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/repository"
)

const helperImage = "alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"
const helperPath = "/synod-restore"

// prepareData writes only fresh bind paths and volumes labelled for this operation.
func prepareData(ctx context.Context, api *client.Client, r *repository.Repository, p *plan, id uuid.UUID, directory string) (string, error) {
	labels := map[string]string{operationLabel: id.String(), "io.synod.restore.role": "data"}
	mounts := []mount.Mount{{Type: mount.TypeBind, Source: directory, Target: helperPath}}
	for i, root := range p.Data {
		if root.Volume == "" {
			continue
		}
		if _, err := api.VolumeInspect(ctx, root.Volume, client.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
			return "", &failure{Code: "app_conflict", Resource: root.Volume, Cause: err}
		}
		created, err := api.VolumeCreate(ctx, client.VolumeCreateOptions{Name: root.Volume, Labels: labels})
		if err != nil || created.Volume.Labels[operationLabel] != id.String() {
			return "", &failure{Code: "volume_restore_failed", Resource: root.Volume, Cause: err}
		}
		mounts = append(mounts, mount.Mount{Type: mount.TypeVolume, Source: root.Volume, Target: fmt.Sprintf("/synod-volume-%d", i), VolumeOptions: &mount.VolumeOptions{NoCopy: true}})
	}
	helper, err := api.ContainerCreate(ctx, client.ContainerCreateOptions{Name: "synod-restore-" + id.String() + "-data", Config: &container.Config{Image: helperImage, Cmd: []string{"sleep", "1800"}, User: "0", Labels: labels}, HostConfig: &container.HostConfig{NetworkMode: "none", ReadonlyRootfs: true, Mounts: mounts, CapDrop: []string{"ALL"}, CapAdd: []string{"DAC_OVERRIDE", "FOWNER"}}})
	if err != nil {
		return "", &failure{Code: "file_restore_failed", Cause: err}
	}
	for i, root := range p.Data {
		target, name := helperPath, filepath.ToSlash(root.Relative)
		if root.Volume != "" {
			target, name = fmt.Sprintf("/synod-volume-%d", i), "."
		}
		if root.Archive != "" {
			if err := copyArchive(ctx, api, r, helper.ID, target, p.Snapshot.ID, root.Archive, name, true); err != nil {
				return helper.ID, &failure{Code: "file_restore_failed", Resource: root.Source, Cause: err}
			}
			continue
		}
		var content bytes.Buffer
		writer := tar.NewWriter(&content)
		header := &tar.Header{Name: name, Mode: int64(root.Mode.Perm()), Uid: int(root.UserID), Gid: int(root.GroupID), ModTime: time.Now().UTC(), Typeflag: tar.TypeReg}
		if root.Mode.IsDir() {
			header.Typeflag = tar.TypeDir
		}
		if err := writer.WriteHeader(header); err != nil {
			return helper.ID, err
		}
		if err := writer.Close(); err != nil {
			return helper.ID, err
		}
		if _, err := api.CopyToContainer(ctx, helper.ID, client.CopyToContainerOptions{DestinationPath: target, Content: &content, CopyUIDGID: true}); err != nil {
			return helper.ID, &failure{Code: "file_restore_failed", Resource: root.Source, Cause: err}
		}
	}
	return helper.ID, nil
}

// copyArchive joins both sides of the stream before it returns.
func copyArchive(ctx context.Context, api *client.Client, r *repository.Repository, containerID, target string, snapshotID uuid.UUID, source, name string, preserveOwners bool) error {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := r.Archive(ctx, snapshotID, source, name, writer)
		writer.CloseWithError(err)
		done <- err
	}()
	_, err := api.CopyToContainer(ctx, containerID, client.CopyToContainerOptions{DestinationPath: target, Content: reader, CopyUIDGID: preserveOwners})
	reader.CloseWithError(err)
	return errors.Join(err, <-done)
}
