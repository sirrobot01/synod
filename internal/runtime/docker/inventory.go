package docker

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
)

// Inventory returns only this installation and node's resources. It never adopts them.
// A malformed owned identity forces the caller to keep recovery mode active.
func (r *Runtime) Inventory(ctx context.Context) ([]runtime.Resource, error) {
	filters := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String())
	containers, err := r.api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	volumes, err := r.api.VolumeList(ctx, client.VolumeListOptions{Filters: filters})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	networks, err := r.api.NetworkList(ctx, client.NetworkListOptions{Filters: filters})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if len(containers.Items)+len(volumes.Items)+len(networks.Items) > 10000 {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	resources := make([]runtime.Resource, 0, len(containers.Items)+len(volumes.Items)+len(networks.Items))
	for _, item := range containers.Items {
		if len(item.Names) != 1 {
			return nil, runtime.ErrOwnershipConflict
		}
		resource, err := r.inventoryIdentity(item.Labels, "container", strings.TrimPrefix(item.Names[0], "/"), "synod-instance-")
		if err != nil {
			return nil, err
		}
		resource.NativeID = item.ID
		resource.State = string(item.State)
		resources = append(resources, resource)
	}
	for _, item := range volumes.Items {
		resource, err := r.inventoryIdentity(item.Labels, "volume", item.Name, "synod-volume-")
		if err != nil {
			return nil, err
		}
		resource.NativeID = item.Name
		resource.State = "present"
		resources = append(resources, resource)
	}
	for _, item := range networks.Items {
		resource, err := r.inventoryIdentity(item.Labels, "network", item.Name, "synod-network-")
		if err != nil {
			return nil, err
		}
		resource.NativeID = item.ID
		resource.State = "present"
		resources = append(resources, resource)
	}
	return resources, nil
}

func (r *Runtime) inventoryIdentity(labels map[string]string, kind, name, prefix string) (runtime.Resource, error) {
	if labels[labelInstallation] != r.options.InstallationID.String() || labels[labelNode] != r.options.NodeID.String() || labels[labelKind] != kind {
		return runtime.Resource{}, runtime.ErrOwnershipConflict
	}
	clusterID, err := uuid.Parse(labels[labelCluster])
	if err != nil || clusterID == uuid.Nil {
		return runtime.Resource{}, runtime.ErrOwnershipConflict
	}
	resourceID, err := uuid.Parse(labels[labelResource])
	if err != nil || resourceID == uuid.Nil || labels[labelResource] != resourceID.String() || name != prefix+resourceID.String() {
		return runtime.Resource{}, &runtime.Error{Kind: runtime.ErrOwnershipConflict, Cause: fmt.Errorf("invalid owned %s identity", kind)}
	}
	return runtime.Resource{ClusterID: clusterID, ResourceID: resourceID, Kind: kind, Name: name}, nil
}
