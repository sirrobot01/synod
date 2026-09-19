package postgres

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

// declared builds one sound identity for a node the fixture already enrolled.
func declared(host *placementHost, clusterID uuid.UUID) VolumeIdentity {
	return VolumeIdentity{
		InstallationID: host.id, NodeID: host.node, ClusterID: clusterID, ResourceID: uuid.New(),
		Engine: "postgres", EngineVersion: "17.11", DataPath: "/var/lib/postgresql/data",
		CapacityRequestBytes: 100 << 30, Retention: Retained,
	}
}

func TestVolumeIdentityIsPersistedAndReadBack(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	identity := declared(host, claim.ClusterID)
	if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, identity.ResourceID)
	if err != nil {
		t.Fatal(err)
	}
	// Every field specification 33 names must survive the round trip.
	if stored != identity {
		t.Fatalf("stored=%+v, want %+v", stored, identity)
	}
}

// An exact repeat is an ordinary replay. A different claim about the same volume is
// a conflict, because a volume whose declared identity moved is one nobody can prove
// anything about.
func TestVolumeIdentityNeverChangesUnderneath(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	identity := declared(host, claim.ClusterID)
	if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
		t.Fatalf("an exact replay was refused: %v", err)
	}
	for name, change := range map[string]func(*VolumeIdentity){
		"another path":     func(v *VolumeIdentity) { v.DataPath = "/var/lib/postgresql/other" },
		"another engine":   func(v *VolumeIdentity) { v.Engine = "redis" },
		"another version":  func(v *VolumeIdentity) { v.EngineVersion = "18.6" },
		"another capacity": func(v *VolumeIdentity) { v.CapacityRequestBytes = 1 << 30 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := identity
			change(&changed)
			if err := host.store.RecordVolumeIdentity(t.Context(), changed); !errors.Is(err, ErrVolumeIdentityConflict) {
				t.Fatalf("err=%v, want ErrVolumeIdentityConflict", err)
			}
		})
	}
}

// A path an engine cannot read, or one that names something other than it appears
// to, is refused rather than normalised.
func TestVolumeIdentityRefusesAnUnsoundDeclaration(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	sound := declared(host, claim.ClusterID)
	for name, spoil := range map[string]func(*VolumeIdentity){
		"relative path":     func(v *VolumeIdentity) { v.DataPath = "var/lib/postgresql/data" },
		"traversal":         func(v *VolumeIdentity) { v.DataPath = "/var/lib/../../etc/passwd" },
		"empty path":        func(v *VolumeIdentity) { v.DataPath = "" },
		"unknown engine":    func(v *VolumeIdentity) { v.Engine = "mysql" },
		"no version":        func(v *VolumeIdentity) { v.EngineVersion = "" },
		"no capacity":       func(v *VolumeIdentity) { v.CapacityRequestBytes = 0 },
		"unknown retention": func(v *VolumeIdentity) { v.Retention = Retention("kept") },
		"no node":           func(v *VolumeIdentity) { v.NodeID = uuid.Nil },
	} {
		t.Run(name, func(t *testing.T) {
			bad := sound
			bad.ResourceID = uuid.New()
			spoil(&bad)
			if err := host.store.RecordVolumeIdentity(t.Context(), bad); err == nil {
				t.Fatalf("an unsound declaration was accepted for %s", name)
			}
		})
	}
}

// Retained storage is what stops a node being declared free, and it is not visible
// from the cluster alone.
func TestNodeVolumeIdentitiesAnswerForOneNode(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	first, second := declared(host, claim.ClusterID), declared(host, claim.ClusterID)
	for _, identity := range []VolumeIdentity{first, second} {
		if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
			t.Fatal(err)
		}
	}
	found, err := host.store.ReadNodeVolumeIdentities(t.Context(), host.id, host.node)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 {
		t.Fatalf("identities=%d, want both volumes on this node", len(found))
	}
	other, err := host.store.ReadNodeVolumeIdentities(t.Context(), host.id, uuid.New())
	if err != nil || len(other) != 0 {
		t.Fatalf("identities=%+v err=%v, want another node to hold nothing", other, err)
	}
}

// Retention is intent. Moving it removes nothing.
func TestVolumeRetentionMovesIntentOnly(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	identity := declared(host, claim.ClusterID)
	if err := host.store.RecordVolumeIdentity(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if err := host.store.SetVolumeRetention(t.Context(), host.id, identity.ResourceID, PurgeRequested); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadVolumeIdentity(t.Context(), host.id, identity.ResourceID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Retention != PurgeRequested {
		t.Fatalf("retention=%q, want purge_requested", stored.Retention)
	}
	// The row is still there. Intent moved; nothing was removed.
	if total := host.count(t, `SELECT count(*) FROM volume_identities WHERE resource_id=$1`, identity.ResourceID); total != 1 {
		t.Fatalf("rows=%d, want the identity kept", total)
	}
	if err := host.store.SetVolumeRetention(t.Context(), host.id, uuid.New(), Purged); err == nil {
		t.Fatal("retention moved for a volume that was never declared")
	}
	if err := host.store.SetVolumeRetention(t.Context(), host.id, identity.ResourceID, Retention("gone")); err == nil {
		t.Fatal("an unknown retention state was accepted")
	}
}
