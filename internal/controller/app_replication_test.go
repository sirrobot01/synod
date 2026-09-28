package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestDatabaseCredentialsReachEachSessionOnce(t *testing.T) {
	dsn := os.Getenv("SYNOD_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh")
	}
	admin, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	name := "credentials_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	}()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := postgres.New(pool)
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	installation, node, keyID := uuid.New(), uuid.New(), uuid.New()
	ring, err := secret.NewKeyring(keyID, map[uuid.UUID][]byte{keyID: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.AdmitApp(t.Context(), postgres.CreateAppRequest{
		Identity: postgres.RequestIdentity{InstallationID: installation, ActorID: "test", APIOperation: postgres.CreateAppAPIOperation, IdempotencyKey: uuid.NewString()}, RequestID: uuid.NewString(),
		Spec: app.AppSpec{SchemaVersion: 1, Name: "cache", Tier: app.TierDatabase, Copies: 1, Database: &app.DatabaseSpec{Engine: app.EngineRedis, Version: "8"}, Replication: new(app.DefaultDatabaseReplication()), Router: app.RouterSpec{Protocol: "tcp", Port: 16379}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]*agentv1.SecretMaterial{}
	for purpose, suffix := range map[string]string{postgres.SuperuserCredential: "admin", postgres.ReplicationCredential: "replication"} {
		credential := postgres.Credential{CredentialID: uuid.New(), Version: 1, Purpose: purpose, RoleName: "synod_" + suffix}
		material := []byte("private-" + uuid.NewString())
		credential.Envelope, err = ring.Seal(secret.Binding{InstallationID: installation, OwnerType: "app", OwnerID: admitted.AppID, CredentialID: credential.CredentialID, Version: credential.Version}, material)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.RecordAppCredentials(t.Context(), installation, admitted.AppID, []postgres.Credential{credential}); err != nil {
			t.Fatal(err)
		}
		expected["db:"+admitted.AppID.String()+":"+suffix] = &agentv1.SecretMaterial{CredentialId: credential.CredentialID.String(), Version: credential.Version, Material: material}
	}
	caPEM, caKey, err := pki.CreateCA(installation, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid CA")
	}
	pairs := map[pki.Role]tls.Certificate{}
	for _, role := range []pki.Role{pki.Agent, pki.Controller} {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		if role == pki.Controller {
			names = []string{"127.0.0.1"}
		}
		cert, err := ca.Issue(csr, pki.Identity{InstallationID: installation, SubjectID: node, Role: role}, names, time.Now(), 30*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		pairs[role], err = tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		if err != nil {
			t.Fatal(err)
		}
	}
	server, err := controlstream.New(controlstream.Config{InstallationID: installation,
		TLS:       &tls.Config{MinVersion: tls.VersionTLS12, ClientCAs: roots, Certificates: []tls.Certificate{pairs[pki.Controller]}},
		Authorize: func(context.Context, uuid.UUID, uuid.UUID, *x509.Certificate) error { return nil },
		Handle:    func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); <-done }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pairs[pki.Agent]}})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	pushed := map[uuid.UUID]*sessionSecrets{}
	stale := uuid.NewString()
	if err := pushDatabaseCredentials(t.Context(), store, ring, server, installation, node, stale, admitted.AppID, pushed); !errors.Is(err, controlstream.ErrDisconnected) || len(pushed[node].sent) != 0 {
		t.Fatalf("failed delivery was cached: %v", err)
	}
	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		stream, err := agentv1.NewAgentServiceClient(connection).Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		hello := &agentv1.Hello{InstallationId: installation.String(), NodeId: node.String(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), Protocol: &agentv1.VersionRange{Minimum: 1, Maximum: 1}, ActivePayloadVersions: []uint32{1}}
		if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: hello}}); err != nil {
			t.Fatal(err)
		}
		first, err := stream.Recv()
		if err != nil || first.GetSession() == nil {
			t.Fatalf("session: %v", err)
		}
		session := first.GetSession().SessionId
		// A second call must not send duplicates. A new session must receive every value again.
		for range 2 {
			if err := pushDatabaseCredentials(ctx, store, ring, server, installation, node, session, admitted.AppID, pushed); err != nil {
				t.Fatal(err)
			}
		}
		marker := &agentv1.SecretMaterial{CredentialId: uuid.NewString(), Version: 1, Purpose: "test-marker", Material: []byte("end")}
		if err := server.Send(node, session, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: marker}}); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for {
			message, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			material := message.GetSecret()
			if material == nil {
				t.Fatal("unexpected message")
			}
			if material.Purpose == marker.Purpose {
				break
			}
			want := expected[material.Purpose]
			if want == nil || seen[material.Purpose] || material.CredentialId != want.CredentialId || material.Version != want.Version || !bytes.Equal(material.Material, want.Material) {
				t.Fatal("credential identity, value, or delivery count differs")
			}
			seen[material.Purpose] = true
		}
		if len(seen) != len(expected) {
			t.Fatalf("received %d credentials, want %d", len(seen), len(expected))
		}
		cancel()
	}
}
