package postgres

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
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

// admitDatabase admits one tier 1 app and returns its ID.
func admitDatabase(t *testing.T, host *placementHost, name string) uuid.UUID {
	t.Helper()
	replication := app.DefaultDatabaseReplication()
	admitted, err := host.store.AdmitApp(t.Context(), CreateAppRequest{
		Identity:  RequestIdentity{InstallationID: host.id, ActorID: "actor-1", APIOperation: CreateAppAPIOperation, IdempotencyKey: uuid.NewString()},
		RequestID: "request-1",
		Spec: app.AppSpec{SchemaVersion: 1, Name: name, Tier: app.TierDatabase, Copies: 1, Replication: &replication,
			Database: &app.DatabaseSpec{Engine: app.EnginePostgres, Version: "17", Databases: []string{"app"}},
			Router:   app.RouterSpec{Protocol: "tcp", Port: 5432}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.AppID
}

// sealed builds one credential, sealed the way a caller must seal it.
func sealed(t *testing.T, ring *secret.Keyring, host *placementHost, appID uuid.UUID, purpose, role, password string) Credential {
	t.Helper()
	credential := Credential{CredentialID: uuid.New(), Version: 1, Purpose: purpose, RoleName: role}
	binding := secret.Binding{
		InstallationID: host.id, OwnerType: "app", OwnerID: appID,
		CredentialID: credential.CredentialID, Version: credential.Version,
	}
	envelope, err := ring.Seal(binding, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	credential.Envelope = envelope
	return credential
}

// credentialsFor builds the superuser and application credentials of one database.
func credentialsFor(t *testing.T, ring *secret.Keyring, host *placementHost, appID uuid.UUID, password string) []Credential {
	t.Helper()
	return []Credential{
		sealed(t, ring, host, appID, SuperuserCredential, "synod_admin", password),
		sealed(t, ring, host, appID, ApplicationCredential, "app", password+"-app"),
	}
}

// A credential must survive the round trip sealed, and open again only with the
// binding it was sealed under.
func TestAppCredentialsArePersistedSealed(t *testing.T) {
	host := placementFixture(t)
	database := admitDatabase(t, host, "db")
	ring := keyring(t)
	password := "a-generated-password"
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, credentialsFor(t, ring, host, database, password)); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadAppCredential(t.Context(), host.id, database, SuperuserCredential)
	if err != nil || stored == nil {
		t.Fatal(stored, err)
	}
	if stored.RoleName != "synod_admin" || stored.Version != 1 || stored.Purpose != SuperuserCredential {
		t.Fatalf("credential=%+v, want the superuser as recorded", stored)
	}
	binding := secret.Binding{
		InstallationID: host.id, OwnerType: "app", OwnerID: database,
		CredentialID: stored.CredentialID, Version: stored.Version,
	}
	opened, err := ring.Open(binding, stored.Envelope)
	if err != nil || string(opened) != password {
		t.Fatalf("opened=%q err=%v, want the sealed password back", opened, err)
	}
	// The binding is authenticated data, so an envelope cannot be read as another
	// app's credential even by someone holding the key.
	moved := binding
	moved.OwnerID = uuid.New()
	if _, err := ring.Open(moved, stored.Envelope); err == nil {
		t.Fatal("an envelope opened under another app's binding")
	}
	missing, err := host.store.ReadAppCredential(t.Context(), host.id, database, ReplicationCredential)
	if err != nil || missing != nil {
		t.Fatalf("an absent purpose read as %+v %v", missing, err)
	}
}

// No plaintext may reach a column, whatever the caller does.
func TestNoPlaintextIsStored(t *testing.T) {
	host := placementFixture(t)
	database := admitDatabase(t, host, "db")
	password := "plaintext-nobody-should-find-" + uuid.NewString()
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, credentialsFor(t, keyring(t), host, database, password)); err != nil {
		t.Fatal(err)
	}
	// Read every column of the table as text and look for the secret in any of them.
	rows, err := host.store.pool.Query(t.Context(), `SELECT to_jsonb(c)::text FROM app_credentials c
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

// A repeat is an ordinary replay. A repeat that disagrees is a credential nobody can
// match to what a host actually holds.
func TestAppCredentialsRepeatsAndDisagreement(t *testing.T) {
	host := placementFixture(t)
	database := admitDatabase(t, host, "db")
	credentials := credentialsFor(t, keyring(t), host, database, "secret")
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, credentials); err != nil {
		t.Fatal(err)
	}
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, credentials); err != nil {
		t.Fatalf("a replay was refused: %v", err)
	}
	changed := append([]Credential(nil), credentials...)
	changed[0].RoleName = "someone_else"
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, changed); !errors.Is(err, ErrCredentialConflict) {
		t.Fatalf("err=%v, want ErrCredentialConflict", err)
	}
	if total := host.count(t, `SELECT count(*) FROM app_credentials WHERE installation_id=$1`, host.id); total != 2 {
		t.Fatalf("credentials=%d, want the original pair", total)
	}
}

// A later version is a rotation. It is added, never written over the one hosts may
// still be using, and the newest is what a reader gets.
func TestCredentialVersionsAccumulate(t *testing.T) {
	host := placementFixture(t)
	database := admitDatabase(t, host, "db")
	ring := keyring(t)
	credentials := credentialsFor(t, ring, host, database, "first")
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, credentials); err != nil {
		t.Fatal(err)
	}
	rotated := sealed(t, ring, host, database, SuperuserCredential, "synod_admin", "second")
	rotated.CredentialID = credentials[0].CredentialID
	rotated.Version = 2
	if err := host.store.RecordAppCredentials(t.Context(), host.id, database, []Credential{rotated}); err != nil {
		t.Fatal(err)
	}
	stored, err := host.store.ReadAppCredential(t.Context(), host.id, database, SuperuserCredential)
	if err != nil || stored == nil {
		t.Fatal(stored, err)
	}
	if stored.Version != 2 {
		t.Fatalf("version=%d, want the newest", stored.Version)
	}
	// The older version is still readable, because a rotation may not have reached
	// every host yet.
	if total := host.count(t, `SELECT count(*) FROM app_credentials
 WHERE installation_id=$1 AND credential_id=$2`, host.id, rotated.CredentialID); total != 2 {
		t.Fatalf("versions=%d, want both kept", total)
	}
}

func TestAppCredentialsRefuseAnUnsoundRequest(t *testing.T) {
	host := placementFixture(t)
	database := admitDatabase(t, host, "db")
	ring := keyring(t)
	for name, spoil := range map[string]func([]Credential) []Credential{
		"no credentials": func([]Credential) []Credential { return nil },
		"a role name an engine would need quoted": func(c []Credential) []Credential {
			c[0].RoleName = "Robert'); DROP TABLE"
			return c
		},
		"two credentials for one purpose": func(c []Credential) []Credential {
			c[1].Purpose = SuperuserCredential
			return c
		},
		"an unknown purpose": func(c []Credential) []Credential {
			c[0].Purpose = "root"
			return c
		},
		"no version": func(c []Credential) []Credential {
			c[0].Version = 0
			return c
		},
		"an unsealed credential": func(c []Credential) []Credential {
			c[0].Envelope = secret.Envelope{}
			return c
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := spoil(credentialsFor(t, ring, host, database, "secret"))
			if err := host.store.RecordAppCredentials(t.Context(), host.id, database, bad); err == nil {
				t.Fatalf("unsound credentials were accepted for %s", name)
			}
		})
	}
	// Only a tier 1 app has engine credentials.
	web := host.claimApp(t, "web")
	if err := host.store.RecordAppCredentials(t.Context(), host.id, web.AppID, credentialsFor(t, ring, host, web.AppID, "secret")); err == nil {
		t.Fatal("credentials were recorded for a tier 3 app")
	}
	if total := host.count(t, `SELECT count(*) FROM app_credentials WHERE installation_id=$1`, host.id); total != 0 {
		t.Fatalf("credentials=%d, want none", total)
	}
}
