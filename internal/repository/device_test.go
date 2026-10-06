package repository_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/sirrobot01/synod/internal/agent/blobserver"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/inventory"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/repository"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDeviceRepositorySurvivesRestartAndKeepsLocalRecovery(t *testing.T) {
	installation, owner, source, intruder := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	now := time.Now()
	caPEM, caKey, err := pki.CreateCA(installation, now, time.Hour*48)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(installation, caPEM, caKey)
	if err != nil {
		t.Fatal(err)
	}
	identities := map[uuid.UUID]*repository.DeviceIdentity{}
	certificates := map[uuid.UUID]tls.Certificate{}
	hashes := map[uuid.UUID]string{}
	for _, id := range []uuid.UUID{owner, source, intruder} {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := ca.Issue(csr, pki.Identity{InstallationID: installation, Role: pki.Agent, SubjectID: id}, nil, now, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
		identities[id] = &repository.DeviceIdentity{InstallationID: installation, CertificatePEM: string(cert), PrivateKeyPEM: string(privatePEM), CAPEM: string(caPEM)}
		pair, err := tls.X509KeyPair(cert, privatePEM)
		if err != nil {
			t.Fatal(err)
		}
		certificates[id] = pair
		hash := sha256.Sum256(pair.Certificate[0])
		hashes[id] = hex.EncodeToString(hash[:])
	}
	directory := filepath.Join(t.TempDir(), "repository")
	id := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: id, NodeID: owner, Name: "Peer", Location: repository.Location{Kind: "device", Path: directory, Device: &repository.DeviceLocation{NodeID: owner, DestinationID: id}}}, Credentials: repository.Credentials{Password: "private-device-repository-password"}}
	plan := destination.Plan{InstallationID: installation, Revision: 1, Destinations: []destination.Entry{entry}, Peers: []destination.Peer{{NodeID: owner, CertificateSHA256: hashes[owner]}, {NodeID: source, CertificateSHA256: hashes[source]}}}
	cachePath := filepath.Join(t.TempDir(), "agent.db")
	cache, err := journal.Open(t.Context(), cachePath, installation, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	ring, err := journal.DestinationKeyring(installation, owner, certificates[owner].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	handler, err := blobserver.New(installation, owner, certificates[owner].Leaf, func(ctx context.Context) (destination.Plan, error) { return cache.DestinationPlan(ctx, ring) })
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificates[owner]}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = tlsConfig
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(func() { server.Close() })
	address := server.Listener.Addr().String()
	plan.Revision++
	plan.Peers[0].Address = address
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	location, credentials, err := plan.Connection(id, identities[source])
	if err != nil {
		t.Fatal(err)
	}
	r, err := repository.Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	catalog := repository.Catalog{Version: 1, InstallationID: installation, Generation: 1, WrittenAt: now.UTC().Truncate(time.Millisecond), Devices: []repository.Device{{ID: source, Name: "private-device-catalog-marker", Architecture: "arm64"}}}
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatalf("write catalog: %v; cause: %+v", err, errors.Unwrap(err))
	}
	if got, err := r.ReadCatalog(t.Context()); err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("remote read: %v", err)
	}

	snapshotSource := t.TempDir()
	content := bytes.Repeat([]byte("private-device-snapshot-data\n"), 4096)
	if err := os.WriteFile(filepath.Join(snapshotSource, "state"), content, 0600); err != nil {
		t.Fatal(err)
	}
	fileRoot, err := localfs.NewEntry(snapshotSource)
	if err != nil {
		t.Fatal(err)
	}
	defer fileRoot.Close()
	copied, err := r.UploadFiles(t.Context(), fileRoot, snapshot.SourceInfo{Host: source.String(), UserName: "test", Path: "test-app"}, false)
	if err != nil {
		t.Fatal(err)
	}
	savedSnapshot, err := r.PublishSnapshot(t.Context(), copied, backup.Snapshot{ID: uuid.New(), InstallationID: installation, DestinationID: id, App: inventory.App{ID: uuid.New(), NodeID: source, Definition: &agentv1.DiscoveredApp{Key: "compose:test", Name: "test", Kind: "compose"}}, Method: backup.Live, Architecture: "arm64", StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(credentials.Password)) || bytes.Contains(data, []byte("private-device-catalog-marker")) || bytes.Contains(data, []byte("private-device-snapshot-data")) {
			t.Error("destination contains plaintext backup content")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The same directory opens through the ordinary local provider with only its password.
	local, err := repository.Open(t.Context(), repository.Location{Kind: "local", Path: directory}, repository.Credentials{Password: credentials.Password})
	if err != nil {
		t.Fatal(err)
	}
	got, err := local.ReadCatalog(t.Context())
	if err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("local recovery: %v", err)
	}

	targetDirectory := filepath.Join(t.TempDir(), "restored")
	if err := local.RestoreFiles(t.Context(), savedSnapshot.ID, targetDirectory); err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(targetDirectory, "state"))
	if err != nil || !bytes.Equal(restored, content) {
		t.Fatalf("device snapshot local recovery differs: %v", err)
	}
	if err := local.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	wrong := credentials
	wrong.DeviceIdentity = identities[intruder]
	if other, err := repository.Open(t.Context(), location, wrong); err == nil {
		other.Close(t.Context())
		t.Fatal("unapproved peer opened repository")
	}
	wrongLocation := location
	target := *location.Device
	target.NodeID = intruder
	wrongLocation.Device = &target
	if other, err := repository.Open(t.Context(), wrongLocation, credentials); err == nil {
		other.Close(t.Context())
		t.Fatal("wrong server identity accepted")
	}
	target = *location.Device
	target.CertificateSHA256 = []string{hashes[intruder]}
	wrongLocation.Device = &target
	if other, err := repository.Open(t.Context(), wrongLocation, credentials); err == nil {
		other.Close(t.Context())
		t.Fatal("wrong server certificate accepted")
	}
	server.Close()
	disconnected, cancel := context.WithTimeout(t.Context(), time.Second)
	if other, err := repository.Open(disconnected, location, credentials); err == nil {
		other.Close(t.Context())
		t.Fatal("disconnected destination was accepted")
	}
	cancel()
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache, err = journal.Open(t.Context(), cachePath, installation, owner)
	if err != nil {
		t.Fatal(err)
	}
	server = httptest.NewUnstartedServer(handler)
	server.Listener.Close()
	server.Listener, err = net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server.TLS = tlsConfig
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	saved, err := cache.DestinationPlan(t.Context(), ring)
	if err != nil {
		t.Fatal(err)
	}
	location, credentials, err = saved.Connection(id, identities[source])
	if err != nil {
		t.Fatal(err)
	}
	r, err = repository.Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	got, err = r.ReadCatalog(t.Context())
	if err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("restart without controller: %v", err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Pin the fixture server for raw protocol failure tests. It has no DNS certificate name.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificates[source]}, InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, certificates[owner].Certificate[0]) {
			return pki.ErrIdentity
		}
		return nil
	}}}}
	defer client.CloseIdleConnections()
	outside := filepath.Join(t.TempDir(), "private.f")
	if err := os.WriteFile(outside, []byte("outside-data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(directory, "escape")); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"../private.f", "escape/private.f", "/etc/passwd", ".synod-repository"} {
		response, err := client.Get(server.URL + "/v1/repositories/" + id.String() + "?path=" + url.QueryEscape(file))
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode < 400 || bytes.Contains(data, []byte("outside-data")) {
			t.Fatal("path escaped destination")
		}
	}
	modified := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	put, err := http.NewRequestWithContext(t.Context(), http.MethodPut, server.URL+"/v1/repositories/"+id.String()+"?path=timed.f&modified="+url.QueryEscape(modified.Format(time.RFC3339Nano)), strings.NewReader("test-object"))
	if err != nil {
		t.Fatal(err)
	}
	timestamped, err := client.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	timestamped.Body.Close()
	if timestamped.StatusCode != 204 || timestamped.Header.Get("X-Synod-Modified") != modified.Format(time.RFC3339Nano) {
		t.Fatal("saved blob timestamp differs")
	}
	if err := os.Remove(filepath.Join(directory, "timed.f")); err != nil {
		t.Fatal(err)
	}
	// An incomplete body must preserve the previous complete object.
	partial := filepath.Join(directory, "partial.f")
	if err := os.WriteFile(partial, []byte("old-complete-object"), 0600); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "https://fixture/v1/repositories/"+id.String()+"?path=partial.f", strings.NewReader("partial"))
	request.ContentLength = 100
	request.TLS = &tls.ConnectionState{HandshakeComplete: true, VerifiedChains: [][]*x509.Certificate{{certificates[source].Leaf}}, PeerCertificates: []*x509.Certificate{certificates[source].Leaf}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	data, err := os.ReadFile(partial)
	if err != nil || response.Code != 400 || string(data) != "old-complete-object" {
		t.Fatal("interrupted upload replaced complete object")
	}
	files, err := filepath.Glob(filepath.Join(directory, ".synod-*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatal("partial upload left temporary files")
	}
	if err := os.Remove(partial); err != nil {
		t.Fatal(err)
	}
	// A new cached peer list must deny an existing TLS connection too.
	live, err := client.Get(server.URL + "/v1/repositories/" + id.String())
	if err != nil {
		t.Fatal(err)
	}
	live.Body.Close()
	if live.StatusCode != 204 {
		t.Fatalf("approved raw request: %d", live.StatusCode)
	}
	plan.Revision++
	plan.Peers = plan.Peers[:1]
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	denied, err := client.Get(server.URL + "/v1/repositories/" + id.String())
	if err != nil {
		t.Fatal(err)
	}
	denied.Body.Close()
	if denied.StatusCode != 403 {
		t.Fatalf("revoked peer kept access: %d", denied.StatusCode)
	}
	if other, err := repository.Open(t.Context(), location, credentials); err == nil {
		other.Close(t.Context())
		t.Fatal("revoked peer reopened repository")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("wrong revocation error: %v", err)
	}
	// A destination cannot expose an existing directory of unrelated files.
	unsafe := t.TempDir()
	if err := os.WriteFile(filepath.Join(unsafe, "keep.f"), []byte("unrelated-file"), 0600); err != nil {
		t.Fatal(err)
	}
	plan.Revision++
	plan.Peers = append(plan.Peers, destination.Peer{NodeID: source, CertificateSHA256: hashes[source]})
	plan.Destinations[0].Destination.Location.Path = unsafe
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	refused, err := client.Get(server.URL + "/v1/repositories/" + id.String())
	if err != nil {
		t.Fatal(err)
	}
	refused.Body.Close()
	if refused.StatusCode != 503 {
		t.Fatal("non-repository directory accepted")
	}
	if _, err := os.Stat(filepath.Join(unsafe, ".synod-repository")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unrelated directory was modified")
	}
	plan.Revision++
	plan.Destinations = nil
	if err := cache.SaveDestinationPlan(t.Context(), ring, plan); err != nil {
		t.Fatal(err)
	}
	removed, err := client.Get(server.URL + "/v1/repositories/" + id.String())
	if err != nil {
		t.Fatal(err)
	}
	removed.Body.Close()
	if removed.StatusCode != 404 {
		t.Fatal("removed destination stayed available")
	}

}
