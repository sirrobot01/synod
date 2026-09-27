package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
)

// keyring builds one wrapping key for a test. It is test input and qualifies nothing
// about key management, which lives with deployment.
func keyring(t *testing.T) *secret.Keyring {
	t.Helper()
	active := uuid.New()
	ring, err := secret.NewKeyring(active, map[uuid.UUID][]byte{active: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

// sealed builds one credential, sealed the way a caller must seal it.
func sealed(t *testing.T, ring *secret.Keyring, host *placementHost, clusterID uuid.UUID, purpose, role, password string) Credential {
	t.Helper()
	credential := Credential{CredentialID: uuid.New(), Version: 1, Purpose: purpose, RoleName: role}
	binding := secret.Binding{
		InstallationID: host.id, OwnerType: "cluster", OwnerID: clusterID,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}
	envelope, err := ring.Seal(binding, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	credential.Envelope = envelope
	return credential
}

// intent builds one sound provisioning intent for a cluster.
func intentFor(t *testing.T, ring *secret.Keyring, host *placementHost, clusterID uuid.UUID, password string) ClusterIntent {
	t.Helper()
	return ClusterIntent{
		InstallationID: host.id, ClusterID: clusterID,
		Credentials: []Credential{
			sealed(t, ring, host, clusterID, SuperuserCredential, "synod_admin", password),
			sealed(t, ring, host, clusterID, ApplicationCredential, "app", password+"-app"),
		},
		Databases: []Database{{Name: "app", OwnerRole: "app"}},
	}
}

// What was intended must survive the round trip sealed, and open again only with the
// binding it was sealed under.
func TestClusterIntentIsPersistedSealed(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	ring := keyring(t)
	password := "a-generated-password"
	intent := intentFor(t, ring, host, claim.ClusterID, password)
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadClusterCredential(t.Context(), host.id, claim.ClusterID, SuperuserCredential)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RoleName != "synod_admin" || stored.Version != 1 || stored.Purpose != SuperuserCredential {
		t.Fatalf("credential=%+v, want the superuser as recorded", stored)
	}
	binding := secret.Binding{
		InstallationID: host.id, OwnerType: "cluster", OwnerID: claim.ClusterID,
		CredentialID: stored.CredentialID, Version: stored.Version,
	}
	opened, err := ring.Open(binding, stored.Envelope)
	if err != nil || string(opened) != password {
		t.Fatalf("opened=%q err=%v, want the sealed password back", opened, err)
	}
	// The binding is authenticated data, so an envelope cannot be read as another
	// cluster's credential even by someone holding the key.
	moved := binding
	moved.OwnerID = uuid.New()
	if _, err := ring.Open(moved, stored.Envelope); err == nil {
		t.Fatal("an envelope opened under another cluster's binding")
	}
	databases, err := host.store.ReadClusterDatabases(t.Context(), host.id, claim.ClusterID)
	if err != nil || len(databases) != 1 || databases[0] != (Database{Name: "app", OwnerRole: "app"}) {
		t.Fatalf("databases=%+v err=%v, want the intended database", databases, err)
	}
}

// No plaintext may reach a column, whatever the caller does.
func TestNoPlaintextIsStored(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	password := "plaintext-nobody-should-find-" + uuid.NewString()
	if err := host.store.RecordClusterIntent(t.Context(), intentFor(t, keyring(t), host, claim.ClusterID, password)); err != nil {
		t.Fatal(err)
	}
	// Read every column of the table as text and look for the secret in any of them.
	rows, err := host.store.pool.Query(t.Context(), `SELECT to_jsonb(c)::text FROM cluster_credentials c
 WHERE installation_id=$1`, host.id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := 0
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(row, password) {
			t.Fatal("a credential row contains the plaintext password")
		}
		found++
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if found != 2 {
		t.Fatalf("rows=%d, want both credentials", found)
	}
}

// Specification 07 requires the intent to be persisted before dispatch. Recording it
// afterwards is refused, because it cannot then be what the dispatch was based on.
func TestClusterIntentIsRefusedAfterDispatch(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	// One dispatched command for this cluster, inserted the way the dispatch path
	// records one: the identity first, then the dispatch that references it.
	command, hash := uuid.New(), make([]byte, 32)
	if _, err := host.store.pool.Exec(t.Context(),
		`INSERT INTO command_identities (command_id,input_hash) VALUES ($1,$2)`, command, hash); err != nil {
		t.Fatal(err)
	}
	if _, err := host.store.pool.Exec(t.Context(), `INSERT INTO command_dispatches
 (command_id,execution_epoch,installation_id,cluster_id,operation_id,recovery_generation,node_id,step_id,input_hash,command_bytes)
 VALUES ($1,1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		command, host.id, claim.ClusterID, claim.OperationID, host.generation, host.node, uuid.New(),
		hash, []byte("command")); err != nil {
		t.Fatal(err)
	}
	err := host.store.RecordClusterIntent(t.Context(), intentFor(t, keyring(t), host, claim.ClusterID, "late"))
	if !errors.Is(err, ErrDispatchAlreadyHappened) {
		t.Fatalf("err=%v, want ErrDispatchAlreadyHappened", err)
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 0 {
		t.Fatalf("credentials=%d, want none written", total)
	}
}

// A repeat is an ordinary replay. A repeat that disagrees is a credential nobody can
// match to what a host actually holds.
func TestClusterIntentRepeatsAndDisagreement(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	ring := keyring(t)
	intent := intentFor(t, ring, host, claim.ClusterID, "secret")
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatalf("a replay was refused: %v", err)
	}
	changed := intent
	changed.Credentials = append([]Credential(nil), intent.Credentials...)
	changed.Credentials[0].RoleName = "someone_else"
	if err := host.store.RecordClusterIntent(t.Context(), changed); !errors.Is(err, ErrCredentialConflict) {
		t.Fatalf("err=%v, want ErrCredentialConflict", err)
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 2 {
		t.Fatalf("credentials=%d, want the original pair", total)
	}
}

// A later version is a rotation. It is added, never written over the one hosts may
// still be using, and the newest is what a reader gets.
func TestCredentialVersionsAccumulate(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	ring := keyring(t)
	intent := intentFor(t, ring, host, claim.ClusterID, "first")
	if err := host.store.RecordClusterIntent(t.Context(), intent); err != nil {
		t.Fatal(err)
	}
	rotated := sealed(t, ring, host, claim.ClusterID, SuperuserCredential, "synod_admin", "second")
	rotated.CredentialID = intent.Credentials[0].CredentialID
	rotated.Version = 2
	if err := host.store.RecordClusterIntent(t.Context(), ClusterIntent{
		InstallationID: host.id, ClusterID: claim.ClusterID, Credentials: []Credential{rotated},
	}); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadClusterCredential(t.Context(), host.id, claim.ClusterID, SuperuserCredential)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version != 2 {
		t.Fatalf("version=%d, want the newest", stored.Version)
	}
	// The older version is still readable, because a rotation may not have reached
	// every host yet.
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials
 WHERE installation_id=$1 AND credential_id=$2`, host.id, rotated.CredentialID); total != 2 {
		t.Fatalf("versions=%d, want both kept", total)
	}
}

func TestClusterIntentRefusesAnUnsoundRequest(t *testing.T) {
	host := placementFixture(t)
	claim := host.claimCluster(t, "db")
	ring := keyring(t)
	for name, spoil := range map[string]func(*ClusterIntent){
		"no credentials": func(i *ClusterIntent) { i.Credentials = nil },
		// A database owned by a role nothing creates is a database nobody can log in to.
		"a database owned by an uncreated role": func(i *ClusterIntent) {
			i.Databases = []Database{{Name: "app", OwnerRole: "nobody"}}
		},
		"a role name an engine would need quoted": func(i *ClusterIntent) {
			i.Credentials[0].RoleName = "Robert'); DROP TABLE"
		},
		"a database name an engine would need quoted": func(i *ClusterIntent) {
			i.Databases = []Database{{Name: "app-1", OwnerRole: "app"}}
		},
		"two credentials for one purpose": func(i *ClusterIntent) {
			i.Credentials[1].Purpose = SuperuserCredential
		},
		"an unknown purpose": func(i *ClusterIntent) { i.Credentials[0].Purpose = "root" },
		"no version":         func(i *ClusterIntent) { i.Credentials[0].Version = 0 },
		"an unsealed credential": func(i *ClusterIntent) {
			i.Credentials[0].Envelope = secret.Envelope{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := intentFor(t, ring, host, claim.ClusterID, "secret")
			bad.Credentials = append([]Credential(nil), bad.Credentials...)
			spoil(&bad)
			if err := host.store.RecordClusterIntent(t.Context(), bad); err == nil {
				t.Fatalf("an unsound intent was accepted for %s", name)
			}
		})
	}
	if total := host.count(t, `SELECT count(*) FROM cluster_credentials WHERE installation_id=$1`, host.id); total != 0 {
		t.Fatalf("credentials=%d, want none", total)
	}
}
