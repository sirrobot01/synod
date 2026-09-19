package docker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// maxHostObjects bounds what one scan will look at, matching the inventory guard. A
// host holding more objects than this is not one this driver can describe, and a
// partial description of it would be worse than saying so.
const maxHostObjects = 10000

// Stable codes naming what disagrees about a mislabelled object. They never carry a
// label value or a host path, because a drift report travels to an operator's screen.
const (
	driftKind            = "kind_label_disagrees"
	driftCluster         = "cluster_label_unreadable"
	driftResource        = "resource_label_unreadable"
	driftName            = "name_disagrees_with_label"
	driftIdentityUnknown = "identity_unreadable"
	driftAmbiguousName   = "container_has_several_names"
)

// Stable codes naming an owned container that is no longer what this node created.
const (
	driftConfigUnreadable = "recorded_configuration_unreadable"
	driftConfigHash       = "recorded_configuration_hash_disagrees"
	driftHostConfig       = "host_configuration_widened"
	driftRestartPolicy    = "restart_policy_changed"
	driftResources        = "resource_limits_changed"
	driftPorts            = "port_bindings_changed"
)

// Drift reports objects on the host that this node did not expect to find as they are.
//
// Specification 33 requires reporting unowned containers and unexpected changes
// without deleting them, and keeps adoption of arbitrary external databases outside
// v0.1. This reads and classifies. Every call it makes is a list; there is no removal,
// no relabelling, and no write of any kind, so the report cannot act on what it finds.
//
// Classification comes from labels alone. The driver cannot know what the controller
// has a record of, so it never decides that something was lost track of; it says only
// whether an object carries this node's labels, somebody else's, or none.
func (r *Runtime) Drift(ctx context.Context) ([]runtime.Drift, error) {
	// No label filter. The whole point is to see what Inventory cannot.
	containers, err := r.api.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	volumes, err := r.api.VolumeList(ctx, client.VolumeListOptions{})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	networks, err := r.api.NetworkList(ctx, client.NetworkListOptions{})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	if len(containers.Items)+len(volumes.Items)+len(networks.Items) > maxHostObjects {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	var found []runtime.Drift
	for _, item := range containers.Items {
		name := ""
		if len(item.Names) == 1 {
			name = strings.TrimPrefix(item.Names[0], "/")
		}
		drift, ok := r.classify(item.Labels, "container", name, "synod-instance-")
		if !ok {
			// The labels agree. The container itself may still have been changed
			// since this node created it.
			detail, err := r.changedContainer(ctx, item.ID)
			if err != nil {
				return nil, err
			}
			if detail == "" {
				continue
			}
			drift.Class, drift.Detail = runtime.DriftChanged, detail
		}
		// A container with several names has no name this node can check against
		// its labels, and that is the disagreement worth reporting.
		if len(item.Names) != 1 && drift.Class == runtime.DriftMislabelled {
			drift.Detail = driftAmbiguousName
		}
		drift.NativeID, drift.State = item.ID, string(item.State)
		found = append(found, drift)
	}
	for _, item := range volumes.Items {
		drift, ok := r.classify(item.Labels, "volume", item.Name, "synod-volume-")
		if !ok {
			continue
		}
		drift.NativeID, drift.State = item.Name, "present"
		found = append(found, drift)
	}
	for _, item := range networks.Items {
		// The daemon's own networks are not drift. They exist on every host and
		// nobody put them there.
		if item.Name == "bridge" || item.Name == "host" || item.Name == "none" {
			continue
		}
		drift, ok := r.classify(item.Labels, "network", item.Name, "synod-network-")
		if !ok {
			continue
		}
		drift.NativeID, drift.State = item.ID, "present"
		found = append(found, drift)
	}
	// The whole scan comes back; what a report carries is the agent's bound, not the
	// driver's. The order puts what an operator needs most first, so a truncated
	// report keeps the objects that were ours, and it is total, so two scans of an
	// unchanged host report the same thing in the same order.
	slices.SortFunc(found, func(a, b runtime.Drift) int {
		if rank := driftRank(a.Class) - driftRank(b.Class); rank != 0 {
			return rank
		}
		return strings.Compare(a.Kind+"\x00"+a.NativeID, b.Kind+"\x00"+b.NativeID)
	})
	return found, nil
}

// driftRank orders a report by how much it concerns this node. An object this node
// created and no longer recognises comes before one that was never ours.
func driftRank(class runtime.DriftClass) int {
	switch class {
	case runtime.DriftChanged:
		return 0
	case runtime.DriftMislabelled:
		return 1
	case runtime.DriftForeign:
		return 2
	}
	return 3
}

// classify decides what one native object is to this node. It returns false for an
// object this node owns cleanly, because that is inventory and not drift.
func (r *Runtime) classify(labels map[string]string, kind, name, prefix string) (runtime.Drift, bool) {
	drift := runtime.Drift{Kind: kind, Name: name}
	installation, node := labels[labelInstallation], labels[labelNode]
	switch {
	case installation == "" && node == "":
		// Nothing here says Synod. It was never ours, and it is reported so that an
		// operator can see it, not so that anything can act on it.
		drift.Class = runtime.DriftUnowned
		return drift, true
	case installation != r.options.InstallationID.String() || node != r.options.NodeID.String():
		// Another installation's, or another node's. Reused hosts and restored
		// backups both produce these, and neither is ours to touch.
		drift.Class = runtime.DriftForeign
		return drift, true
	}
	// From here the object carries this node's own labels. It was ours, so its
	// identity is recovered from whichever source is still intact rather than the
	// whole object being written off as foreign.
	drift.Class = runtime.DriftMislabelled
	if clusterID, err := uuid.Parse(labels[labelCluster]); err == nil && clusterID != uuid.Nil {
		drift.ClusterID = clusterID
	} else {
		drift.Detail = driftCluster
	}
	fromLabel, labelled := readIdentity(labels[labelResource])
	fromName, named := readIdentity(strings.TrimPrefix(name, prefix))
	if !strings.HasPrefix(name, prefix) {
		named, fromName = false, uuid.Nil
	}
	switch {
	case labelled && named && fromLabel == fromName:
		drift.ResourceID = fromLabel
	case labelled && named:
		// Two identities, and no way to tell which object this is.
		drift.ResourceID, drift.Detail = uuid.Nil, driftName
	case labelled:
		drift.ResourceID, drift.Detail = fromLabel, driftName
	case named:
		drift.ResourceID, drift.Detail = fromName, driftResource
	default:
		drift.Detail = driftIdentityUnknown
	}
	if labels[labelKind] != kind {
		drift.Detail = driftKind
	}
	// A clean object is inventory, not drift.
	return drift, drift.Detail != ""
}

// readIdentity parses one resource identity in its canonical form. A value that does
// not round-trip is not an identity, whatever it parses to.
func readIdentity(value string) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil || parsed.String() != value {
		return uuid.Nil, false
	}
	return parsed, true
}

// changedContainer reports an owned container that is no longer the container this
// node created. It returns an empty code for one that is unchanged.
//
// It reports and never corrects. Specification 33 requires reporting an unexpected
// change without deleting or silently fixing it, and forbids changing desired state
// from an observation, so nothing here writes anything.
//
// A stopped container is not a change. Stopping is an ordinary thing to observe, the
// state travels in the inventory page, and desired state is not the agent's to move.
func (r *Runtime) changedContainer(ctx context.Context, id string) (string, error) {
	found, err := r.api.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		if errdefs.IsNotFound(err) {
			// Removed between the list and the inspection. Nothing to report.
			return "", nil
		}
		return "", &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	item := found.Container
	if item.Config == nil || item.HostConfig == nil {
		return driftConfigUnreadable, nil
	}
	// The container carries the configuration it was created with, and its hash. A
	// configuration that no longer hashes to its own label has been edited.
	encoded, err := base64.RawStdEncoding.DecodeString(item.Config.Labels[labelConfig])
	if err != nil {
		return driftConfigUnreadable, nil
	}
	hash := sha256.Sum256(encoded)
	if item.Config.Labels[labelHash] != hex.EncodeToString(hash[:]) {
		return driftConfigHash, nil
	}
	var stored agentv1.EnsureContainer
	if proto.Unmarshal(encoded, &stored) != nil {
		return driftConfigUnreadable, nil
	}
	memory, cpu := containerLimits(&stored)
	host := item.HostConfig
	switch {
	case host.Privileged || len(host.Binds) != 0 || len(host.Devices) != 0 || len(host.DeviceRequests) != 0 ||
		len(host.CapAdd) != 0 || len(host.SecurityOpt) != 0 || host.AutoRemove || host.PublishAllPorts || host.ReadonlyRootfs:
		// Each of these grants the container more of the host than this node ever
		// gave it.
		return driftHostConfig, nil
	case host.RestartPolicy.Name != container.RestartPolicyDisabled && host.RestartPolicy.Name != container.RestartPolicyUnlessStopped:
		// This node uses exactly two policies: disabled while stopped, and
		// unless-stopped while running. Anything else undoes a stopped intent.
		return driftRestartPolicy, nil
	case host.Memory != memory || host.NanoCPUs != cpu*1000000:
		return driftResources, nil
	case len(host.PortBindings) != len(stored.Ports):
		return driftPorts, nil
	}
	return "", nil
}
