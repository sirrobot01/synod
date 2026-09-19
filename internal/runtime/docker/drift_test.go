package docker

import (
	"testing"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// drifted returns the one reported object with this native id, or fails.
func drifted(t *testing.T, r *Runtime, nativeID string) runtime.Drift {
	t.Helper()
	found, err := r.Drift(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range found {
		if item.NativeID == nativeID {
			return item
		}
	}
	t.Fatalf("%s was not reported as drift", nativeID)
	return runtime.Drift{}
}

// absent fails if the object is reported at all.
func absent(t *testing.T, r *Runtime, nativeID string) {
	t.Helper()
	found, err := r.Drift(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range found {
		if item.NativeID == nativeID {
			t.Fatalf("%s was reported as drift: %+v", nativeID, item)
		}
	}
}

// A volume nothing labelled is reported and left exactly where it is.
func TestDriftReportsAnUnownedObjectWithoutRemovingIt(t *testing.T) {
	r, api, _ := runtimeFixture(t)
	name := foreignVolume(t, api, client.VolumeCreateOptions{Name: "unrelated-" + uuid.NewString(), Driver: "local"})
	found := drifted(t, r, name)
	if found.Class != runtime.DriftUnowned || found.Kind != "volume" {
		t.Fatalf("drift=%+v, want an unowned volume", found)
	}
	// Nothing that was never ours carries an identity we could act on.
	if found.ClusterID != uuid.Nil || found.ResourceID != uuid.Nil || found.Detail != "" {
		t.Fatalf("drift=%+v, want no identity claimed for a foreign object", found)
	}
	if !exists(t, api, name) {
		t.Fatal("the drift scan removed an object it does not own")
	}
}

// An object carrying another installation's labels is somebody else's. Reused hosts
// and restored backups both produce these, and neither is ours to touch.
func TestDriftSeparatesAnotherInstallationFromOurOwn(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	otherID := uuid.NewString()
	name := foreignVolume(t, api, client.VolumeCreateOptions{
		Name: "synod-volume-" + otherID, Driver: "local",
		Labels: map[string]string{labelInstallation: uuid.NewString(), labelNode: uuid.NewString(), labelCluster: clusterID.String(), labelResource: otherID, labelKind: "volume"},
	})
	found := drifted(t, r, name)
	if found.Class != runtime.DriftForeign {
		t.Fatalf("drift=%+v, want a foreign object", found)
	}
	if !exists(t, api, name) {
		t.Fatal("the drift scan removed another installation's volume")
	}
}

// An object this node created and still owns is inventory, not drift.
func TestDriftLeavesAnOwnedObjectAlone(t *testing.T) {
	r, _, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	if _, err := r.Execute(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	absent(t, r, "synod-volume-"+id)
}

// A previously labelled Synod object is recovered rather than written off as foreign.
// Its identity is read back from whichever source is still intact, which is what
// separates "this was ours and we lost track of it" from "this was never ours".
func TestDriftRecoversAPreviouslyLabelledObject(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	ours := map[string]string{
		labelInstallation: r.options.InstallationID.String(), labelNode: r.options.NodeID.String(),
		labelCluster: clusterID.String(), labelResource: id, labelKind: "volume",
	}
	// The name is ours and readable; the resource label has been lost.
	lost := map[string]string{}
	for key, value := range ours {
		lost[key] = value
	}
	delete(lost, labelResource)
	name := foreignVolume(t, api, client.VolumeCreateOptions{Name: "synod-volume-" + id, Driver: "local", Labels: lost})
	found := drifted(t, r, name)
	if found.Class != runtime.DriftMislabelled {
		t.Fatalf("drift=%+v, want it recovered as ours rather than treated as foreign", found)
	}
	if found.ResourceID.String() != id || found.ClusterID != clusterID {
		t.Fatalf("drift=%+v, want the identity recovered from the name", found)
	}
	if found.Detail == "" {
		t.Fatal("a recovered object reported no disagreement")
	}
	if !exists(t, api, name) {
		t.Fatal("the drift scan removed a mislabelled object")
	}
}

// Two identities and no way to tell which object this is. Neither is claimed.
func TestDriftClaimsNoIdentityWhenTheNameAndLabelDisagree(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	named, labelled := uuid.NewString(), uuid.NewString()
	name := foreignVolume(t, api, client.VolumeCreateOptions{
		Name: "synod-volume-" + named, Driver: "local",
		Labels: map[string]string{
			labelInstallation: r.options.InstallationID.String(), labelNode: r.options.NodeID.String(),
			labelCluster: clusterID.String(), labelResource: labelled, labelKind: "volume",
		},
	})
	found := drifted(t, r, name)
	if found.Class != runtime.DriftMislabelled || found.ResourceID != uuid.Nil {
		t.Fatalf("drift=%+v, want no identity claimed when two disagree", found)
	}
}

// A manual change to an owned container is reported and never corrected. A manual
// stop is not a change: it is an ordinary observation, and desired state is not the
// agent's to move.
func TestDriftReportsAManualChangeAndNotAManualStop(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_HOST)
	name := "synod-instance-" + ensure.Input.ResourceId
	item, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	absent(t, r, item.Container.ID)

	// A container that was started and then stopped is not drift. Its state travels
	// in the inventory page, and nothing about it is unexpected.
	start := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{ResourceId: ensure.Input.ResourceId,
		Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}})
	if _, err := r.Execute(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	absent(t, r, item.Container.ID)
	if _, err := api.ContainerStop(t.Context(), item.Container.ID, client.ContainerStopOptions{}); err != nil {
		t.Fatal(err)
	}
	absent(t, r, item.Container.ID)

	// Somebody reaching past Synod to change what the container may do is drift.
	always := container.RestartPolicy{Name: container.RestartPolicyAlways}
	if _, err := api.ContainerUpdate(t.Context(), item.Container.ID, client.ContainerUpdateOptions{RestartPolicy: &always}); err != nil {
		t.Fatal(err)
	}
	found := drifted(t, r, item.Container.ID)
	if found.Class != runtime.DriftChanged || found.Detail != driftRestartPolicy {
		t.Fatalf("drift=%+v, want a reported restart policy change", found)
	}
	// It is still there. Reporting a change is not correcting it.
	if _, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{}); err != nil {
		t.Fatalf("the drift scan changed or removed the container: %v", err)
	}
	after, err := api.ContainerInspect(t.Context(), name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if after.Container.HostConfig.RestartPolicy.Name != container.RestartPolicyAlways {
		t.Fatal("the drift scan silently corrected the change it found")
	}
}

// Changing what a container may consume is a change to an owned object.
func TestDriftReportsChangedResourceLimits(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	ensure := createFixtureContainer(t, r, clusterID, agentv1.NetworkMode_NETWORK_MODE_HOST)
	item, err := api.ContainerInspect(t.Context(), "synod-instance-"+ensure.Input.ResourceId, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.ContainerUpdate(t.Context(), item.Container.ID, client.ContainerUpdateOptions{
		Resources: &container.Resources{Memory: 128 << 20},
	}); err != nil {
		t.Fatal(err)
	}
	if found := drifted(t, r, item.Container.ID); found.Detail != driftResources {
		t.Fatalf("drift=%+v, want the changed limit reported", found)
	}
}
