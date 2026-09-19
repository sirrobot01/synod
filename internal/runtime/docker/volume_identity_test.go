package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/volume"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// A volume this daemon manages sits under its own volume root.
func TestCheckVolumePathAcceptsADaemonManagedVolume(t *testing.T) {
	item := volume.Volume{
		Name: "synod-volume-abc", Driver: "local",
		Mountpoint: "/var/lib/docker/volumes/synod-volume-abc/_data",
	}
	if err := checkVolumePath(item); err != nil {
		t.Fatal(err)
	}
}

// A volume that redirects storage elsewhere is how a name like ours ends up holding,
// or destroying, something else.
func TestCheckVolumePathRefusesAnEscape(t *testing.T) {
	sound := volume.Volume{
		Name: "synod-volume-abc", Driver: "local",
		Mountpoint: "/var/lib/docker/volumes/synod-volume-abc/_data",
	}
	for name, spoil := range map[string]func(*volume.Volume){
		// A bind-style local volume points wherever its options say.
		"device option":  func(v *volume.Volume) { v.Options = map[string]string{"device": "/etc"} },
		"another driver": func(v *volume.Volume) { v.Driver = "nfs" },
		// A mountpoint outside any volume root is not this daemon's.
		"outside the root": func(v *volume.Volume) { v.Mountpoint = "/etc/passwd" },
		// Traversal inside the root climbs back out of it.
		"traversal": func(v *volume.Volume) {
			v.Mountpoint = "/var/lib/docker/volumes/synod-volume-abc/../../../../etc"
		},
		// A path that names a different volume is not this volume.
		"another volume": func(v *volume.Volume) {
			v.Mountpoint = "/var/lib/docker/volumes/other-volume/_data"
		},
		// A relative path names something other than it appears to.
		"relative":  func(v *volume.Volume) { v.Mountpoint = "var/lib/docker/volumes/synod-volume-abc/_data" },
		"unclean":   func(v *volume.Volume) { v.Mountpoint = "/var/lib/docker/volumes/synod-volume-abc/_data/" },
		"no mount":  func(v *volume.Volume) { v.Mountpoint = "" },
		"root only": func(v *volume.Volume) { v.Mountpoint = "/var/lib/docker/volumes" },
	} {
		t.Run(name, func(t *testing.T) {
			item := sound
			spoil(&item)
			if err := checkVolumePath(item); !errors.Is(err, runtime.ErrConfigurationConflict) {
				t.Fatalf("err=%v, want a refusal for %s", err, name)
			}
		})
	}
}

// A missing volume is a known configuration problem. Reporting it as an unknown
// outcome would hold the mutation slot for an operator to inspect something that is
// simply not there.
func TestVerifyVolumeRefusesAMissingVolume(t *testing.T) {
	r, _, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	err := r.verifyVolume(t.Context(), command, id)
	if !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("err=%v, want ErrConfigurationConflict", err)
	}
	if errors.Is(err, runtime.ErrOutcomeUnknown) {
		t.Fatal("an absent volume was reported as an unknown outcome")
	}
}

// A volume another installation owns is not this one's to read.
func TestVerifyVolumeRefusesAnotherOwner(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	name := "synod-volume-" + id
	// A volume with the right name and the wrong owner, as an adopted or stale
	// object on a reused host would be.
	if _, err := api.VolumeCreate(t.Context(), client.VolumeCreateOptions{
		Name: name, Driver: "local",
		Labels: map[string]string{labelInstallation: uuid.NewString(), labelNode: uuid.NewString(), labelCluster: clusterID.String(), labelResource: id, labelKind: "volume"},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = api.VolumeRemove(t.Context(), name, client.VolumeRemoveOptions{Force: true}) })
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	if err := r.verifyVolume(t.Context(), command, id); err == nil {
		t.Fatal("a volume owned by another installation was accepted")
	}
}

// A volume this node created and owns is accepted, so the check refuses foreign
// storage rather than everything.
func TestVerifyVolumeAcceptsItsOwn(t *testing.T) {
	r, _, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	if _, err := r.Execute(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if err := r.verifyVolume(t.Context(), command, id); err != nil {
		t.Fatalf("a volume this node owns was refused: %v", err)
	}
}

// exists reports whether one native volume is still on the host.
func exists(t *testing.T, api *client.Client, name string) bool {
	t.Helper()
	_, err := api.VolumeInspect(t.Context(), name, client.VolumeInspectOptions{})
	if err == nil {
		return true
	}
	if !errdefs.IsNotFound(err) {
		t.Fatal(err)
	}
	return false
}

// foreignVolume puts a volume on the host that this installation must never touch,
// and removes it afterwards itself, because the fixture only cleans up its own.
func foreignVolume(t *testing.T, api *client.Client, options client.VolumeCreateOptions) string {
	t.Helper()
	if _, err := api.VolumeCreate(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = api.VolumeRemove(context.WithoutCancel(t.Context()), options.Name, client.VolumeRemoveOptions{Force: true})
	})
	return options.Name
}

// An explicit purge removes the one volume it proved, and a repeat of it is ordinary.
// Storage that belongs to anything else on the host stays where it is.
func TestRemoveVolumeRemovesOnlyWhatItProved(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	command := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	if _, err := r.Execute(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	// Two things this purge must not reach: something with nothing to do with Synod,
	// and something named like ours that another installation owns.
	unrelated := foreignVolume(t, api, client.VolumeCreateOptions{Name: "unrelated-" + uuid.NewString(), Driver: "local"})
	otherID := uuid.NewString()
	other := foreignVolume(t, api, client.VolumeCreateOptions{
		Name: "synod-volume-" + otherID, Driver: "local",
		Labels: map[string]string{labelInstallation: uuid.NewString(), labelNode: uuid.NewString(), labelCluster: clusterID.String(), labelResource: otherID, labelKind: "volume"},
	})
	if err := r.RemoveVolume(t.Context(), command, id); err != nil {
		t.Fatal(err)
	}
	if exists(t, api, "synod-volume-"+id) {
		t.Fatal("the purged volume is still on the host")
	}
	// A purge request is retried like anything else, and the volume it named is
	// already gone, so the repeat must succeed rather than fail the operation.
	if err := r.RemoveVolume(t.Context(), command, id); err != nil {
		t.Fatalf("a repeated purge failed: %v", err)
	}
	for _, name := range []string{unrelated, other} {
		if !exists(t, api, name) {
			t.Fatalf("a purge removed %s, which it does not own", name)
		}
	}
}

// A purge proves identity immediately before it removes anything, so a volume it
// cannot prove is left alone rather than removed and explained afterwards.
func TestRemoveVolumeRefusesWhatItCannotProve(t *testing.T) {
	r, api, clusterID := runtimeFixture(t)
	// A volume named like ours, owned by somebody else.
	otherID := uuid.NewString()
	other := foreignVolume(t, api, client.VolumeCreateOptions{
		Name: "synod-volume-" + otherID, Driver: "local",
		Labels: map[string]string{labelInstallation: uuid.NewString(), labelNode: uuid.NewString(), labelCluster: clusterID.String(), labelResource: otherID, labelKind: "volume"},
	})
	otherCommand := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: otherID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: otherID}},
	})
	if err := r.RemoveVolume(t.Context(), otherCommand, otherID); !errors.Is(err, runtime.ErrOwnershipConflict) {
		t.Fatalf("err=%v, want another installation's volume refused", err)
	}
	if !exists(t, api, other) {
		t.Fatal("a purge removed a volume another installation owns")
	}
	// A volume with our own labels that redirects its storage to a host path. The
	// name is ours; the data is not, and removing it would take the host path with it.
	bindID := uuid.NewString()
	bindCommand := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: bindID, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: bindID}},
	})
	bind := foreignVolume(t, api, client.VolumeCreateOptions{
		Name: "synod-volume-" + bindID, Driver: "local",
		DriverOpts: map[string]string{"type": "none", "o": "bind", "device": "/tmp"},
		Labels:     r.labels(bindCommand, "volume", bindID),
	})
	if err := r.RemoveVolume(t.Context(), bindCommand, bindID); !errors.Is(err, runtime.ErrConfigurationConflict) {
		t.Fatalf("err=%v, want a redirected volume refused", err)
	}
	if !exists(t, api, bind) {
		t.Fatal("a purge removed a volume that redirects storage to a host path")
	}
}

// A volume a container still holds has not finished being released. Forcing it out
// is how a stop becomes a corruption, so the purge reports busy and waits.
func TestRemoveVolumeRefusesAVolumeStillInUse(t *testing.T) {
	r, _, clusterID := runtimeFixture(t)
	id := uuid.NewString()
	volume := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: id, Payload: &agentv1.CommandInput_EnsureVolume{EnsureVolume: &agentv1.EnsureVolume{VolumeId: id}},
	})
	if _, err := r.Execute(t.Context(), volume); err != nil {
		t.Fatal(err)
	}
	container := runtimeCommand(t, r, clusterID, &agentv1.CommandInput{
		ResourceId: uuid.NewString(),
		Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: &agentv1.EnsureContainer{
			ImageDigest: redisFixture, Engine: "redis", VolumeId: id,
			NetworkMode: agentv1.NetworkMode_NETWORK_MODE_HOST, MemoryBytes: 64 << 20, CpuMillis: 250,
		}},
	})
	if _, err := r.Execute(t.Context(), container); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveVolume(t.Context(), volume, id); !errors.Is(err, runtime.ErrBusy) {
		t.Fatalf("err=%v, want ErrBusy", err)
	}
	if !exists(t, r.api, "synod-volume-"+id) {
		t.Fatal("a volume a container still holds was removed")
	}
}
