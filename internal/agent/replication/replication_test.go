package replication

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noFreeze struct{}

func (noFreeze) Freeze(context.Context) error { return nil }
func (noFreeze) Thaw(context.Context) error   { return nil }

type installation struct {
	id    uuid.UUID
	ca    *pki.CA
	roots *x509.CertPool
}

func newInstallation(t *testing.T) *installation {
	id := uuid.New()
	certPEM, keyPEM, err := pki.CreateCA(id, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := pki.LoadCA(id, certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return &installation{id: id, ca: ca, roots: roots}
}

func (i *installation) agent(t *testing.T, node uuid.UUID) tls.Certificate {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := i.ca.Issue(csr, pki.Identity{InstallationID: i.id, Role: pki.Agent, SubjectID: node}, nil, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return pair
}

type standby struct {
	node    uuid.UUID
	address string
	server  *Server
}

func startStandby(t *testing.T, inst *installation) *standby {
	node := uuid.New()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{InstallationID: inst.id, Root: t.TempDir(), Table: &Table{}, Status: &Status{}}
	certificate := inst.agent(t, node)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, certificate, inst.roots) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return &standby{node: node, address: listener.Addr().String(), server: server}
}

type active struct {
	node        uuid.UUID
	client      Client
	snapshotter *replica.Snapshotter
	live        string
}

func newActive(t *testing.T, inst *installation) *active {
	node := uuid.New()
	dir := t.TempDir()
	live, snapshot := filepath.Join(dir, "live"), filepath.Join(dir, "snapshot")
	for _, d := range []string{filepath.Join(live, "config"), snapshot} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return &active{node: node, live: live,
		client:      Client{InstallationID: inst.id, Certificate: inst.agent(t, node), Roots: inst.roots},
		snapshotter: &replica.Snapshotter{Live: live, Snapshot: snapshot, Freezer: noFreeze{}}}
}

func (a *active) push(t *testing.T, s *standby, app string, epoch, sequence uint64) (*agentv1.PushResult, error) {
	t.Helper()
	m, err := a.snapshotter.Take(t.Context(), epoch, sequence)
	if err != nil {
		t.Fatal(err)
	}
	return a.client.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: s.node.String(), Address: s.address}, app, m, a.snapshotter.Snapshot)
}

func assign(s *standby, app string, epoch uint64, from uuid.UUID) {
	s.server.Table.Set([]*agentv1.AppReplication{{AppId: app, Epoch: epoch, Role: RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: from.String(), Address: "127.0.0.1:1"}}}})
}

func TestPushReplicatesSnapshots(t *testing.T) {
	inst := newInstallation(t)
	s := startStandby(t, inst)
	a := newActive(t, inst)
	app := uuid.NewString()
	assign(s, app, 3, a.node)
	data := make([]byte, 3*replica.BlockSize)
	rand.Read(data)
	path := filepath.Join(a.live, "config", "app.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := a.push(t, s, app, 3, 1)
	if err != nil || result.FailureCode != "" || result.Sequence != 1 {
		t.Fatalf("first push: %+v %v", result, err)
	}
	copy(data[replica.BlockSize:], "changed")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err = a.push(t, s, app, 3, 2); err != nil || result.FailureCode != "" {
		t.Fatalf("second push: %+v %v", result, err)
	}
	got, err := os.ReadFile(filepath.Join(s.server.Root, app, "replica", "config", "app.db"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("replica content differs: %v", err)
	}
	statuses := s.server.Status.List()
	if len(statuses) != 1 || statuses[0].Sequence != 2 || statuses[0].Epoch != 3 || statuses[0].FailureCode != "" {
		t.Fatalf("status: %v", statuses)
	}
	if _, err := os.Lstat(filepath.Join(s.server.Root, app, incomingDir)); !os.IsNotExist(err) {
		t.Fatal("incoming blocks remained")
	}
	// A lost response can replay the exact complete manifest.
	stored := replica.Replica{Root: filepath.Join(s.server.Root, app)}
	manifest, err := stored.Current()
	if err != nil {
		t.Fatal(err)
	}
	result, err = a.client.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: s.node.String(), Address: s.address}, app, manifest, a.snapshotter.Snapshot)
	if err != nil || result.FailureCode != "" {
		t.Fatalf("exact replay: %+v %v", result, err)
	}
	restarted := &Server{Root: s.server.Root, Table: &Table{}, Status: &Status{}}
	if err := restarted.restoreStatus(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored := restarted.Status.List()
	if len(restored) != 1 || restored[0].Sequence != manifest.Sequence || restored[0].Epoch != manifest.Epoch || restored[0].FailureCode != "" {
		t.Fatalf("restart lost complete replica: %v", restored)
	}
	if err := os.WriteFile(filepath.Join(stored.Root, "replica", "config", "app.db"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err = a.client.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: s.node.String(), Address: s.address}, app, manifest, a.snapshotter.Snapshot)
	if err != nil || result.FailureCode != FailureCorrupt {
		t.Fatalf("corrupt replay accepted: %+v %v", result, err)
	}
	restarted.Status = &Status{}
	if err := restarted.restoreStatus(t.Context()); err != nil {
		t.Fatal(err)
	}
	restored = restarted.Status.List()
	if len(restored) != 1 || restored[0].Sequence != 0 || restored[0].FailureCode != FailureCorrupt {
		t.Fatalf("restart accepted corrupt replica: %v", restored)
	}
	// A different manifest with the same sequence is stale.
	result, err = a.client.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: s.node.String(), Address: s.address}, app, &replica.Manifest{Epoch: 3, Sequence: 2}, a.snapshotter.Snapshot)
	if err != nil || result.FailureCode != FailureStale {
		t.Fatalf("stale push: %+v %v", result, err)
	}
}

func TestPushRefusesWrongPeersAndEpochs(t *testing.T) {
	inst := newInstallation(t)
	s := startStandby(t, inst)
	a := newActive(t, inst)
	intruder := newActive(t, inst)
	app := uuid.NewString()
	if err := os.WriteFile(filepath.Join(a.live, "config", "app.db"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.push(t, s, app, 1, 1); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unassigned app: %v", err)
	}
	assign(s, app, 2, a.node)
	if _, err := intruder.push(t, s, app, 2, 1); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("other node: %v", err)
	}
	if result, err := a.push(t, s, app, 1, 2); err != nil || result.FailureCode != FailureEpochMismatch {
		t.Fatalf("old epoch: %+v %v", result, err)
	}
	// A certificate from another installation cannot connect.
	stranger := newInstallation(t)
	outsider := newActive(t, stranger)
	outsider.client.Roots = inst.roots
	if _, err := outsider.push(t, s, app, 2, 3); err == nil {
		t.Fatal("foreign certificate accepted")
	}
}

func TestPushVerifiesTheStandbyIdentity(t *testing.T) {
	inst := newInstallation(t)
	s := startStandby(t, inst)
	a := newActive(t, inst)
	app := uuid.NewString()
	assign(s, app, 1, a.node)
	if err := os.WriteFile(filepath.Join(a.live, "config", "app.db"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := a.snapshotter.Take(t.Context(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// The address serves node s, but the table names a different node.
	if _, err := a.client.Push(t.Context(), &agentv1.ReplicaPeer{NodeId: uuid.NewString(), Address: s.address}, app, m, a.snapshotter.Snapshot); err == nil {
		t.Fatal("pushed to a standby with the wrong identity")
	}
}

func TestValidateTable(t *testing.T) {
	node, app := uuid.NewString(), uuid.NewString()
	peer := []*agentv1.ReplicaPeer{{NodeId: node, Address: "192.168.0.10:8445"}}
	valid := []*agentv1.AppReplication{{AppId: app, Epoch: 1, Role: RoleActive, Peers: peer, IntervalSeconds: 60}}
	if err := Validate(valid); err != nil {
		t.Fatal(err)
	}
	for name, apps := range map[string][]*agentv1.AppReplication{
		"zero epoch":        {{AppId: app, Role: RoleStandby, Peers: peer}},
		"unknown role":      {{AppId: app, Epoch: 1, Role: "writer", Peers: peer}},
		"standby two peers": {{AppId: app, Epoch: 1, Role: RoleStandby, Peers: []*agentv1.ReplicaPeer{peer[0], {NodeId: uuid.NewString(), Address: "192.168.0.11:8445"}}}},
		"active no peers":   {{AppId: app, Epoch: 1, Role: RoleActive, IntervalSeconds: 60}},
		"short interval":    {{AppId: app, Epoch: 1, Role: RoleActive, Peers: peer, IntervalSeconds: 5}},
		"duplicate app":     {valid[0], valid[0]},
		"bad app id":        {{AppId: "APP", Epoch: 1, Role: RoleStandby, Peers: peer}},
		"host name":         {{AppId: app, Epoch: 1, Role: RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: node, Address: "odin:8445"}}}},
		"zero port":         {{AppId: app, Epoch: 1, Role: RoleStandby, Peers: []*agentv1.ReplicaPeer{{NodeId: node, Address: "192.168.0.10:0"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(apps); err == nil {
				t.Fatal("invalid table accepted")
			}
		})
	}
}

func TestCorruptReplicaRecoversWithAFullPush(t *testing.T) {
	inst := newInstallation(t)
	s := startStandby(t, inst)
	a := newActive(t, inst)
	app := uuid.NewString()
	assign(s, app, 1, a.node)
	data := make([]byte, 2*replica.BlockSize)
	rand.Read(data)
	path := filepath.Join(a.live, "config", "app.db")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := a.push(t, s, app, 1, 1); err != nil || result.FailureCode != "" {
		t.Fatalf("first push: %+v %v", result, err)
	}
	// Damage block 0 on the standby. The next snapshot changes only block 1.
	stored := filepath.Join(s.server.Root, app, "replica", "config", "app.db")
	damaged, err := os.ReadFile(stored)
	if err != nil {
		t.Fatal(err)
	}
	damaged[0] ^= 1
	if err := os.WriteFile(stored, damaged, 0o600); err != nil {
		t.Fatal(err)
	}
	copy(data[replica.BlockSize:], "changed")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := a.push(t, s, app, 1, 2); err != nil || result.FailureCode != FailureCorrupt {
		t.Fatalf("push onto a corrupt replica: %+v %v", result, err)
	}
	if statuses := s.server.Status.List(); statuses[0].FailureCode != FailureCorrupt || statuses[0].Sequence != 1 {
		t.Fatalf("status after corruption: %v", statuses)
	}
	if result, err := a.push(t, s, app, 1, 3); err != nil || result.FailureCode != "" {
		t.Fatalf("full push: %+v %v", result, err)
	}
	if got, err := os.ReadFile(stored); err != nil || string(got) != string(data) {
		t.Fatalf("replica after the full push differs: %v", err)
	}
}
