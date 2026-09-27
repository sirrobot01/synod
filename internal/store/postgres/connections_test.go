package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/secret"
)

func TestInspectClusterEndpointsUsesLiveReservedListener(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "connection")
	empty, err := host.store.InspectClusterEndpoints(t.Context(), host.id, claim.ClusterID)
	if err != nil || empty.Endpoints == nil || len(empty.Endpoints) != 0 || empty.UnavailableReason != "not_allocated" {
		t.Fatalf("unallocated inspection=%+v: %v", empty, err)
	}
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{{Address: "0.0.0.0", Transport: "tcp", Number: 16432}})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, client, bind, transport, reason string
		portReleased, allocationReleased      bool
	}{
		{name: "missing client address", bind: "0.0.0.0", transport: "tcp", reason: "client_address_required"},
		{name: "wildcard ipv4", client: "192.0.2.40", bind: "0.0.0.0", transport: "tcp"},
		{name: "exact ipv4", client: "192.0.2.40", bind: "192.0.2.40", transport: "tcp"},
		{name: "wildcard ipv6", client: "2001:db8::40", bind: "::", transport: "tcp"},
		{name: "exact ipv6", client: "2001:db8::40", bind: "2001:db8::40", transport: "tcp"},
		{name: "wrong family", client: "192.0.2.40", bind: "::", transport: "tcp", reason: "address_not_bound"},
		{name: "other interface", client: "192.0.2.40", bind: "192.0.2.41", transport: "tcp", reason: "address_not_bound"},
		{name: "loopback bind", client: "192.0.2.40", bind: "127.0.0.1", transport: "tcp", reason: "address_not_bound"},
		{name: "released port", client: "192.0.2.40", bind: "0.0.0.0", transport: "tcp", portReleased: true, reason: "not_allocated"},
		{name: "released allocation", client: "192.0.2.40", bind: "0.0.0.0", transport: "tcp", allocationReleased: true, reason: "not_allocated"},
		{name: "udp only", client: "192.0.2.40", bind: "0.0.0.0", transport: "udp", reason: "not_allocated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := host.policy
			policy.ClientAddress = test.client
			encoded, err := json.Marshal(policy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := host.store.pool.Exec(t.Context(), `UPDATE node_placement_policies SET policy_bytes=$1 WHERE installation_id=$2`, encoded, host.id); err != nil {
				t.Fatal(err)
			}
			if _, err := host.store.pool.Exec(t.Context(), `UPDATE allocation_ports SET address=$1,transport=$2,
 released_at=CASE WHEN $3 THEN clock_timestamp() ELSE NULL END WHERE cluster_id=$4`, test.bind, test.transport, test.portReleased, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			if _, err := host.store.pool.Exec(t.Context(), `UPDATE resource_allocations SET
 released_at=CASE WHEN $1 THEN clock_timestamp() ELSE NULL END WHERE cluster_id=$2`, test.allocationReleased, claim.ClusterID); err != nil {
				t.Fatal(err)
			}
			found, err := host.store.InspectClusterEndpoints(t.Context(), host.id, claim.ClusterID)
			if err != nil || found.Endpoints == nil || found.UnavailableReason != test.reason {
				t.Fatalf("inspection=%+v: %v", found, err)
			}
			if test.reason != "" {
				if len(found.Endpoints) != 0 {
					t.Fatal("unavailable metadata published an endpoint")
				}
				return
			}
			want := cluster.Endpoint{ID: request.Resources[0].ID, NodeID: host.node, Host: test.client, Port: 16432,
				Transport: "tcp", Database: "app", ClientMode: "direct", AvailabilityProfile: "single_node", TLSMode: "disabled"}
			if len(found.Endpoints) != 1 || found.Endpoints[0] != want || found.ClusterID != claim.ClusterID || found.Engine != cluster.PostgreSQL {
				t.Fatalf("inspection=%+v, want endpoint=%+v", found, want)
			}
		})
	}
	// A stopped Redis cluster still exposes its reserved metadata and observations.
	spec := cluster.CreateSpec{SchemaVersion: 1, Name: "connection", Engine: cluster.Redis,
		Redis: &cluster.RedisSpec{Version: "8.2.1", Topology: "standalone", Persistence: "aof"}}
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE clusters SET engine='redis',spec=$1,
 desired_lifecycle='stopped',observed_lifecycle='running',desired_revision=2,observed_revision=1 WHERE id=$2`, spec, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE allocation_ports SET transport='tcp' WHERE cluster_id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	found, err := host.store.InspectClusterEndpoints(t.Context(), host.id, claim.ClusterID)
	if err != nil || len(found.Endpoints) != 1 || found.Endpoints[0].Database != "0" || found.DesiredRevision != 2 || found.ObservedRevision != 1 ||
		found.DesiredLifecycle != cluster.DesiredStopped || found.ObservedLifecycle != cluster.ObservedRunning {
		t.Fatalf("stopped Redis inspection=%+v: %v", found, err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO allocation_ports
 (installation_id,cluster_id,node_id,allocation_id,address,transport,port,name)
 SELECT installation_id,cluster_id,node_id,allocation_id,address,transport,16433,name FROM allocation_ports WHERE cluster_id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	found, err = host.store.InspectClusterEndpoints(t.Context(), host.id, claim.ClusterID)
	if err != nil || found.UnavailableReason != "ambiguous_allocation" || len(found.Endpoints) != 0 {
		t.Fatalf("multiple listeners inspection=%+v: %v", found, err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO resource_allocations
 (id,installation_id,node_id,cluster_id,resource_id,created_operation_id,created_revision,allocation_bytes)
 SELECT $1,installation_id,node_id,cluster_id,$2,created_operation_id,created_revision,allocation_bytes
 FROM resource_allocations WHERE cluster_id=$3`, uuid.New(), uuid.New(), claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	found, err = host.store.InspectClusterEndpoints(t.Context(), host.id, claim.ClusterID)
	if err != nil || found.UnavailableReason != "ambiguous_allocation" || len(found.Endpoints) != 0 {
		t.Fatalf("multiple allocations inspection=%+v: %v", found, err)
	}
}

func TestEndpointInspectionKeepsOneSnapshotAcrossConcurrentChanges(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "snapshot")
	request, decision, plan := host.place(t, claim, smallBudget, []placement.Port{{Address: "0.0.0.0", Transport: "tcp", Number: 16432}})
	if _, err := host.store.InstallPlacementPlan(t.Context(), claim, request, decision, plan, nil); err != nil {
		t.Fatal(err)
	}
	host.policy.ClientAddress = "192.0.2.40"
	encoded, err := json.Marshal(host.policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `UPDATE node_placement_policies SET policy_bytes=$1`, encoded); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	writer, err := host.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(ctx)
	if _, err := writer.Exec(ctx, `LOCK TABLE allocation_ports IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	results := make(chan cluster.EndpointInspection, 1)
	errors := make(chan error, 1)
	go func() {
		found, err := host.store.InspectClusterEndpoints(ctx, host.id, claim.ClusterID)
		results <- found
		errors <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := host.store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
 WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT host(address)%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("endpoint inspection did not reach the blocked port read")
		case <-ticker.C:
		}
	}
	if _, err := writer.Exec(ctx, `UPDATE clusters SET desired_revision=2,desired_lifecycle='stopped' WHERE id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	host.policy.ClientAddress = "192.0.2.41"
	encoded, err = json.Marshal(host.policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(ctx, `UPDATE node_placement_policies SET policy_bytes=$1`, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(ctx, `UPDATE allocation_ports SET port=16433`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	found := <-results
	if err := <-errors; err != nil || found.DesiredRevision != 1 || found.DesiredLifecycle != cluster.DesiredRunning ||
		len(found.Endpoints) != 1 || found.Endpoints[0].Host != "192.0.2.40" || found.Endpoints[0].Port != 16432 {
		t.Fatalf("snapshot inspection=%+v: %v", found, err)
	}
	found, err = host.store.InspectClusterEndpoints(ctx, host.id, claim.ClusterID)
	if err != nil || found.DesiredRevision != 2 || len(found.Endpoints) != 1 || found.Endpoints[0].Host != "192.0.2.41" || found.Endpoints[0].Port != 16433 {
		t.Fatalf("next snapshot inspection=%+v: %v", found, err)
	}
}

func TestCredentialInspectionAndAuditedReveal(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "credentials")
	empty, err := host.store.InspectClusterCredentials(t.Context(), host.id, claim.ClusterID)
	if err != nil || empty.ClusterID != claim.ClusterID || empty.Credentials == nil || len(empty.Credentials) != 0 {
		t.Fatalf("empty metadata=%+v: %v", empty, err)
	}
	ring := keyring(t)
	password := "protected-" + uuid.NewString()
	intent := ClusterIntent{InstallationID: host.id, ClusterID: claim.ClusterID,
		Credentials: []Credential{sealed(t, ring, host, claim.ClusterID, SuperuserCredential, "synod_admin", password)}}
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	metadata, err := host.store.InspectClusterCredentials(t.Context(), host.id, claim.ClusterID)
	if err != nil || len(metadata.Credentials) != 1 || metadata.Credentials[0].Purpose != SuperuserCredential {
		t.Fatalf("metadata=%+v: %v", metadata, err)
	}
	_, err = host.store.AuthorizeCredentialReveal(t.Context(), host.id, claim.ClusterID, ApplicationCredential, "operator", "missing")
	requireCode(t, err, NotFound)
	newest := intent.Credentials[0]
	newest.Version = 2
	newest.Envelope, err = ring.Seal(secret.Binding{InstallationID: host.id, OwnerType: "cluster", OwnerID: claim.ClusterID,
		CredentialID: newest.CredentialID, Version: newest.Version}, []byte(password+"-rotated"))
	if err != nil {
		t.Fatal(err)
	}
	intent.Credentials = []Credential{newest, sealed(t, ring, host, claim.ClusterID, ApplicationCredential, "app", password+"-app")}
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	metadata, err = host.store.InspectClusterCredentials(t.Context(), host.id, claim.ClusterID)
	if err != nil || len(metadata.Credentials) != 2 || metadata.Credentials[0].Purpose != ApplicationCredential ||
		metadata.Credentials[1] != (cluster.CredentialMetadata{CredentialID: newest.CredentialID, ClusterID: claim.ClusterID, Version: 2, Purpose: SuperuserCredential, Username: "synod_admin"}) {
		t.Fatalf("latest metadata=%+v: %v", metadata, err)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil || bytes.Contains(encoded, []byte(password)) || bytes.Contains(encoded, []byte("sealed")) || bytes.Contains(encoded, []byte("password")) {
		t.Fatalf("metadata was not secret-free: %v", err)
	}
	beforeOperations := host.count(t, `SELECT count(*) FROM operations`)
	beforeOutbox := host.count(t, `SELECT count(*) FROM outbox`)
	owner := New(host.store.pool)
	owner.commitTx = func(ctx context.Context, tx pgx.Tx) error {
		if total := host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`); total != 0 {
			t.Error("an uncommitted authorization became visible")
		}
		return tx.Commit(ctx)
	}
	found, err := owner.AuthorizeCredentialReveal(t.Context(), host.id, claim.ClusterID, SuperuserCredential, "operator", "reveal-1")
	if err != nil || found.CredentialID != newest.CredentialID || found.Version != 2 || !bytes.Equal(found.Envelope.Sealed, newest.Envelope.Sealed) {
		t.Fatalf("authorized credential differs from newest stored version: %v", err)
	}
	var auditID, credentialID, clusterID uuid.UUID
	var version int64
	var actor, requestID, auditJSON string
	var created time.Time
	err = host.store.pool.QueryRow(t.Context(), `SELECT id,cluster_id,credential_id,credential_version,actor_id,request_id,created_at,to_jsonb(a)::text
 FROM audit_events a WHERE action='credential.reveal.accepted' AND installation_id=$1`, host.id).
		Scan(&auditID, &clusterID, &credentialID, &version, &actor, &requestID, &created, &auditJSON)
	if err != nil || auditID == uuid.Nil || clusterID != claim.ClusterID || credentialID != newest.CredentialID || version != 2 ||
		actor != "operator" || requestID != "reveal-1" || created.IsZero() || strings.Contains(auditJSON, password) || strings.Contains(auditJSON, "sealed") {
		t.Fatalf("audit identity or secret boundary failed: %v", err)
	}
	if host.count(t, `SELECT count(*) FROM operations`) != beforeOperations || host.count(t, `SELECT count(*) FROM outbox`) != beforeOutbox {
		t.Fatal("credential inspection created asynchronous work")
	}
	var revision int64
	if err := host.store.pool.QueryRow(t.Context(), `SELECT desired_revision FROM clusters WHERE id=$1`, claim.ClusterID).Scan(&revision); err != nil || revision != 1 {
		t.Fatalf("inspection changed desired state: revision=%d err=%v", revision, err)
	}
}

func TestCredentialRevealWithholdsCiphertextOnAuditFailure(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "audit-failure")
	if err := host.store.RecordClusterIntent(t.Context(), intentFor(t, keyring(t), host, claim.ClusterID, "must-not-be-returned")); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		committed bool
		code      ErrorCode
	}{
		{name: "rollback", code: DependencyUnavailable},
		{name: "unknown commit", committed: true, code: OutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner := New(host.store.pool)
			owner.commitTx = func(ctx context.Context, tx pgx.Tx) error {
				if test.committed {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
					return io.ErrUnexpectedEOF
				}
				if err := tx.Rollback(ctx); err != nil {
					return err
				}
				return pgx.ErrTxCommitRollback
			}
			found, err := owner.AuthorizeCredentialReveal(t.Context(), host.id, claim.ClusterID, SuperuserCredential, "operator", "failed-commit")
			requireCode(t, err, test.code)
			if found.CredentialID != uuid.Nil || len(found.Envelope.Sealed) != 0 {
				t.Fatal("a failed authorization returned a credential")
			}
			want := 0
			if test.committed {
				want = 1
			}
			if count := host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`); count != want {
				t.Fatalf("audits=%d, want %d", count, want)
			}
		})
	}
	if _, err := host.store.pool.Exec(t.Context(), `CREATE FUNCTION refuse_reveal_audit() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.action='credential.reveal.accepted' THEN RAISE EXCEPTION 'test audit failure'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `CREATE TRIGGER refuse_reveal BEFORE INSERT ON audit_events
 FOR EACH ROW EXECUTE FUNCTION refuse_reveal_audit()`); err != nil {
		t.Fatal(err)
	}
	found, err := host.store.AuthorizeCredentialReveal(t.Context(), host.id, claim.ClusterID, SuperuserCredential, "operator", "failed-insert")
	requireCode(t, err, DependencyUnavailable)
	if found.CredentialID != uuid.Nil || len(found.Envelope.Sealed) != 0 || host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`) != 1 {
		t.Fatal("audit insert failure returned a credential or changed accepted audits")
	}
}

func TestCredentialRevealSerializesWithDeletion(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "deletion")
	if err := host.store.RecordClusterIntent(t.Context(), intentFor(t, keyring(t), host, claim.ClusterID, "private")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	deleter, err := host.store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer deleter.Rollback(ctx)
	if _, err := deleter.Exec(ctx, `UPDATE clusters SET desired_lifecycle='deleted' WHERE id=$1`, claim.ClusterID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := host.store.AuthorizeCredentialReveal(ctx, host.id, claim.ClusterID, SuperuserCredential, "operator", "racing-delete")
		result <- err
	}()
	waitForKeyWait(t, host.store)
	if err := deleter.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	requireCode(t, <-result, NotFound)
	if count := host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`); count != 0 {
		t.Fatal("access behind a committed deletion was authorized")
	}
}

func TestCredentialRevealRejectsInvalidIdentityAndCrossClusterAudit(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "validation")
	intent := intentFor(t, keyring(t), host, claim.ClusterID, "private")
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		installation, cluster   uuid.UUID
		purpose, actor, request string
	}{
		{uuid.Nil, claim.ClusterID, SuperuserCredential, "operator", "request"},
		{host.id, uuid.Nil, SuperuserCredential, "operator", "request"},
		{host.id, claim.ClusterID, "unknown", "operator", "request"},
		{host.id, claim.ClusterID, SuperuserCredential, "", "request"},
		{host.id, claim.ClusterID, SuperuserCredential, strings.Repeat("a", 257), "request"},
		{host.id, claim.ClusterID, SuperuserCredential, "actor\x00", "request"},
		{host.id, claim.ClusterID, SuperuserCredential, "operator", ""},
		{host.id, claim.ClusterID, SuperuserCredential, "operator", "bad request"},
	} {
		found, err := host.store.AuthorizeCredentialReveal(t.Context(), test.installation, test.cluster, test.purpose, test.actor, test.request)
		requireCode(t, err, InvalidRequest)
		if len(found.Envelope.Sealed) != 0 {
			t.Fatal("invalid input disclosed ciphertext")
		}
	}
	other := host.claimCluster(t, "other")
	_, err := host.store.pool.Exec(t.Context(), `INSERT INTO audit_events
 (id,installation_id,cluster_id,credential_id,credential_version,actor_id,request_id,action,outcome,created_at)
 VALUES ($1,$2,$3,$4,1,'operator','invalid-binding','credential.reveal.accepted','accepted',clock_timestamp())`,
		uuid.New(), host.id, other.ClusterID, intent.Credentials[0].CredentialID)
	if err == nil {
		t.Fatal("the audit schema accepted another cluster's credential")
	}
	if count := host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`); count != 0 {
		t.Fatal("invalid access created an accepted audit")
	}
}

func TestConnectionInspectionEnforcesTenantAndDeletedBoundaries(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "boundaries")
	if err := host.store.RecordClusterIntent(t.Context(), intentFor(t, keyring(t), host, claim.ClusterID, "private")); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		installation uuid.UUID
		cluster      uuid.UUID
		change       string
	}{
		{name: "other installation", installation: uuid.New(), cluster: claim.ClusterID},
		{name: "unknown cluster", installation: host.id, cluster: uuid.New()},
		{name: "deletion requested", installation: host.id, cluster: claim.ClusterID, change: `UPDATE clusters SET desired_lifecycle='deleted'`},
		{name: "deleted timestamp", installation: host.id, cluster: claim.ClusterID, change: `UPDATE clusters SET desired_lifecycle='running',deleted_at=clock_timestamp()`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.change != "" {
				if _, err := host.store.pool.Exec(t.Context(), test.change); err != nil {
					t.Fatal(err)
				}
			}
			_, err := host.store.InspectClusterEndpoints(t.Context(), test.installation, test.cluster)
			requireCode(t, err, NotFound)
			_, err = host.store.InspectClusterCredentials(t.Context(), test.installation, test.cluster)
			requireCode(t, err, NotFound)
			credential, err := host.store.AuthorizeCredentialReveal(t.Context(), test.installation, test.cluster, SuperuserCredential, "operator", "boundary")
			requireCode(t, err, NotFound)
			if credential.CredentialID != uuid.Nil || len(credential.Envelope.Sealed) != 0 {
				t.Fatal("an inaccessible credential was returned")
			}
		})
	}
	if count := host.count(t, `SELECT count(*) FROM audit_events WHERE action='credential.reveal.accepted'`); count != 0 {
		t.Fatal("rejected access created an accepted audit")
	}
}
