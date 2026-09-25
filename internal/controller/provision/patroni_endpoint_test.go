package provision

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/netip"
	"slices"
	"testing"

	"github.com/google/uuid"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// memberLeafNames opens a member TLS credential version and returns the IP addresses its leaf names.
func memberLeafNames(t *testing.T, f *patroniFixture, reference *agentv1.SecretReference) []netip.Addr {
	t.Helper()
	credential, err := f.store.ReadCredentialVersion(t.Context(), f.claim.InstallationID, f.claim.ClusterID, uuid.MustParse(reference.CredentialId), reference.Version, reference.Purpose)
	if err != nil {
		t.Fatal(err)
	}
	value, err := f.planner.config.Keyring.Open(secret.Binding{InstallationID: f.claim.InstallationID, OwnerType: "cluster", OwnerID: f.claim.ClusterID, CredentialID: credential.CredentialID, Version: credential.Version}, credential.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	var material enginepostgres.PatroniTLSMaterial
	if err := json.Unmarshal(value, &material); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(material.Certificate)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	var names []netip.Addr
	for _, ip := range leaf.IPAddresses {
		address, _ := netip.AddrFromSlice(ip)
		names = append(names, address.Unmap())
	}
	slices.SortFunc(names, func(a, b netip.Addr) int { return a.Compare(b) })
	return names
}

// memberLeaf returns the member TLS reference of one configuration.
func memberLeaf(container *agentv1.EnsureContainer) *agentv1.SecretReference {
	for _, reference := range container.Postgres.Patroni.Credentials {
		if reference.Purpose == postgres.PatroniMemberTLSCredential {
			return reference
		}
	}
	return nil
}

// Each member reserves pooler, writer, and reader ports. Each member leaf names every member
// and the load balancer, in create, rebuild, and renewal.
func TestPatroniRouteNamesAndPorts(t *testing.T) {
	f := newPatroniFixture(t, [3][2]string{{"host-a", "192.0.2.10"}, {"host-b", "192.0.2.11"}, {"host-c", "192.0.2.12"}})
	if _, err := f.pool.Exec(t.Context(), `UPDATE clusters SET spec=jsonb_set(spec,'{postgresql,endpoint}','{"address":"198.51.100.7","writer_port":5432,"reader_port":5433}') WHERE installation_id=$1 AND id=$2`, f.claim.InstallationID, f.claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	if err := f.planner.Plan(t.Context(), f.claim); err != nil {
		t.Fatal(err)
	}
	want := []netip.Addr{netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("192.0.2.11"), netip.MustParseAddr("192.0.2.12"), netip.MustParseAddr("198.51.100.7")}
	for _, name := range routePorts {
		var ports int
		if err := f.pool.QueryRow(t.Context(), `SELECT count(DISTINCT allocation_id) FROM allocation_ports WHERE installation_id=$1 AND cluster_id=$2 AND name=$3 AND released_at IS NULL`, f.claim.InstallationID, f.claim.ClusterID, name).Scan(&ports); err != nil || ports != 3 {
			t.Fatalf("%s ports reserved for %d members: %v", name, ports, err)
		}
	}
	plan := installedPlan(t, f.fixture)
	for _, step := range plan.Steps {
		if container := step.Command.Input.GetEnsureContainer(); container.GetPostgres().GetPatroni() != nil {
			if names := memberLeafNames(t, f, memberLeaf(container)); !slices.Equal(names, want) {
				t.Fatalf("created leaf names %v", names)
			}
		}
	}
	recordPatroniReceipts(t, f, plan.Steps)
	if err := f.store.UpdateProgress(t.Context(), f.claim, postgres.ProgressUpdate{Sequence: 1, Status: "succeeded", Reason: "Fixture terminal boundary."}); err != nil {
		t.Fatal(err)
	}
	// With a load balancer, the URLs use it. The routes are still listed for its backends.
	endpoints, err := f.store.InspectClusterEndpoints(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil || len(endpoints.Endpoints) != 6 || endpoints.WriterURL != "postgresql://198.51.100.7:5432/app?sslmode=verify-full" ||
		endpoints.ReaderURL != "postgresql://198.51.100.7:5433/app?sslmode=verify-full" || endpoints.Endpoints[0].ClientMode != "routed" {
		t.Fatalf("load balancer endpoints: %+v %v", endpoints, err)
	}
	renewal := renewPatroni(t, f)
	for block := range 3 {
		if names := memberLeafNames(t, f, memberLeaf(renewal.Steps[6*block+3].Command.Input.GetRecreatePatroniMember().Next)); !slices.Equal(names, want) {
			t.Fatalf("renewed leaf names %v", names)
		}
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE operations SET status='failed',finished_at=clock_timestamp(),claim_active=false WHERE id=$1`, f.claim.OperationID); err != nil {
		t.Fatal(err)
	}
	members, err := f.store.ReadClusterMembers(t.Context(), f.claim.InstallationID, f.claim.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	rebuild := rebuildPatroniMember(t, f, members[0])
	if names := memberLeafNames(t, f, memberLeaf(rebuild.Steps[7].Command.Input.GetEnsureContainer())); !slices.Equal(names, want) {
		t.Fatalf("rebuilt leaf names %v", names)
	}
}
