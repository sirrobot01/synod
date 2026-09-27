package provision

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

const armDigest = "redis@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

// onArchitecture declares a node of one architecture.
func onArchitecture(architecture string) func(*placement.NodePolicy) {
	return func(policy *placement.NodePolicy) { policy.Architecture = architecture }
}

// mixedPlanner rebinds the fixture planner to prefer amd64, then arm64, with a Redis image for each.
func mixedPlanner(t *testing.T, f *fixture) {
	t.Helper()
	config := f.planner.config
	config.Profile.Architectures = []string{"amd64", "arm64"}
	config.Images = append(config.Images, Image{Engine: cluster.Redis, Version: "8.2.7", Architecture: "arm64", Digest: armDigest})
	planner, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	f.planner = planner
}

// addNode approves one more node in the fixture installation.
func addNode(t *testing.T, f *fixture, customize func(*placement.NodePolicy)) uuid.UUID {
	t.Helper()
	node := uuid.New()
	if _, err := f.pool.Exec(t.Context(), `INSERT INTO nodes (id,installation_id,public_key_hash,enrolled_at)
 VALUES ($1,$2,$3,clock_timestamp())`, node, f.claim.InstallationID, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	readyNodeWith(t, f.store, f.claim.InstallationID, node, customize)
	return node
}

// A create falls through to the next architecture when the preferred one has no node, and
// uses that architecture's image. A replica then stays on the primary's architecture, even
// when a node of the preferred architecture is free.
func TestMixedArchitecturesKeepOneArchitecturePerCluster(t *testing.T) {
	f := newFixtureWith(t, replicatedSpec("aof"), onArchitecture("arm64"))
	mixedPlanner(t, f)
	if _, err := f.planner.provision(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	if f.decider.request.Resources[0].Architecture != "arm64" {
		t.Fatalf("create placed on %s, want the only architecture with a node", f.decider.request.Resources[0].Architecture)
	}
	if container := installedPlan(t, f).Steps[1].Command.Input.GetEnsureContainer(); container.ImageDigest != armDigest {
		t.Fatalf("create image %s, want the arm64 image", container.ImageDigest)
	}
	addNode(t, f, onArchitecture("amd64"))
	claim := f.act(t, postgres.ScaleClusterAPIOperation)
	if err := f.planner.Plan(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if f.decider.request.Resources[0].Architecture != "arm64" || f.decider.decision.Assignments[0].NodeID != f.node {
		t.Fatalf("replica placed on %s node %s, want the primary's arm64 node", f.decider.request.Resources[0].Architecture, f.decider.decision.Assignments[0].NodeID)
	}
	if container := installedPlanFor(t, f, claim.OperationID).Steps[1].Command.Input.GetEnsureContainer(); container.ImageDigest != armDigest {
		t.Fatalf("replica image %s, want the arm64 image", container.ImageDigest)
	}
}

// Without a node for any listed architecture, a create reports the unsatisfied placement.
func TestMixedArchitecturesReportUnsatisfiedPlacement(t *testing.T) {
	f := newFixtureWith(t, replicatedSpec("aof"), onArchitecture("arm64"))
	config := f.planner.config
	// amd64 has a Redis image. arm64 has none, so it is never tried.
	config.Profile.Architectures = []string{"arm64", "amd64"}
	planner, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planner.provision(t.Context(), f.claim); !errors.Is(err, placement.ErrUnsatisfied) {
		t.Fatalf("create without an amd64 node: %v", err)
	}
	if f.decider.request.Resources[0].Architecture != "amd64" {
		t.Fatalf("create tried %s, want only the architecture with an image", f.decider.request.Resources[0].Architecture)
	}
}
