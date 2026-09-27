package pki

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCertificateIssuanceAndMutualTLS(t *testing.T) {
	installation := uuid.New()
	now := time.Now()
	certPEM, keyPEM, err := CreateCA(installation, now, 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadCA(installation, certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("invalid CA PEM")
	}
	agent := Identity{installation, Agent, uuid.New()}
	controller := Identity{installation, Controller, uuid.New()}
	// Each caller generates its own private key. The CA receives only its CSR.
	agentPublic, agentKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	untrustedURI, _ := url.Parse("spiffe://attacker.example/controller/other")
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{DNSNames: []string{"attacker.example"}, URIs: []*url.URL{untrustedURI}, ExtraExtensions: []pkix.Extension{{Id: []int{2, 5, 29, 19}, Critical: true, Value: []byte{0x30, 0x03, 0x01, 0x01, 0xff}}}}, agentKey)
	if err != nil {
		t.Fatal(err)
	}
	agentCert, err := ca.Issue(request, agent, nil, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(agentCert)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if !agentPublic.Equal(leaf.PublicKey) || leaf.IsCA || len(leaf.DNSNames) != 0 || !slices.Equal(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}) {
		t.Fatal("CSR overrode assigned identity or certificate policy")
	}
	expectedURI, _ := agent.URI()
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expectedURI.String() {
		t.Fatal("assigned URI missing")
	}
	agentKeyDER, err := x509.MarshalPKCS8PrivateKey(agentKey)
	if err != nil {
		t.Fatal(err)
	}
	agentPair, err := tls.X509KeyPair(agentCert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: agentKeyDER}))
	if err != nil {
		t.Fatal(err)
	}
	_, controllerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	controllerRequest, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, controllerKey)
	if err != nil {
		t.Fatal(err)
	}
	controllerCert, err := ca.Issue(controllerRequest, controller, []string{"localhost", "127.0.0.1"}, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	controllerKeyDER, err := x509.MarshalPKCS8PrivateKey(controllerKey)
	if err != nil {
		t.Fatal(err)
	}
	controllerPair, err := tls.X509KeyPair(controllerCert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: controllerKeyDER}))
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan tls.ConnectionState, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := PeerIdentity(*r.TLS, installation, Agent)
		if err != nil || identity != agent {
			http.Error(w, "identity rejected", 403)
			return
		}
		observed <- *r.TLS
		w.WriteHeader(204)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{controllerPair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.Config.ErrorLog = nil
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{agentPair}}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatalf("mutual TLS status=%d", response.StatusCode)
	}
	peer, err := PeerIdentity(*response.TLS, installation, Controller)
	if err != nil || peer != controller {
		t.Fatalf("controller identity: %v", err)
	}
	state := <-observed
	if _, err := PeerIdentity(state, uuid.New(), Agent); !errors.Is(err, ErrIdentity) {
		t.Fatalf("wrong installation: %v", err)
	}
	if _, err := PeerIdentity(state, installation, Controller); !errors.Is(err, ErrIdentity) {
		t.Fatalf("wrong role: %v", err)
	}
	unverified := state
	unverified.VerifiedChains = nil
	if _, err := PeerIdentity(unverified, installation, Agent); !errors.Is(err, ErrIdentity) {
		t.Fatalf("unverified chain: %v", err)
	}
	unverified = state
	unverified.HandshakeComplete = false
	if _, err := PeerIdentity(unverified, installation, Agent); !errors.Is(err, ErrIdentity) {
		t.Fatalf("incomplete handshake: %v", err)
	}
	for _, test := range []struct {
		name      string
		configure func(*tls.Config)
	}{
		{"no client certificate", func(c *tls.Config) { c.Certificates = nil }},
		{"untrusted client certificate", func(c *tls.Config) {
			otherInstallation := uuid.New()
			otherCert, otherKey, err := CreateCA(otherInstallation, now, 24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			otherCA, err := LoadCA(otherInstallation, otherCert, otherKey)
			if err != nil {
				t.Fatal(err)
			}
			issued, err := otherCA.Issue(request, Identity{otherInstallation, Agent, uuid.New()}, nil, now, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			pair, err := tls.X509KeyPair(issued, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: agentKeyDER}))
			if err != nil {
				t.Fatal(err)
			}
			c.Certificates = []tls.Certificate{pair}
		}},
		{"wrong trusted CA", func(c *tls.Config) {
			another, _, err := CreateCA(uuid.New(), now, 24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			c.RootCAs = x509.NewCertPool()
			c.RootCAs.AppendCertsFromPEM(another)
		}},
		{"wrong server name", func(c *tls.Config) { c.ServerName = "other.example" }},
		{"expired server", func(c *tls.Config) { c.Time = func() time.Time { return now.Add(2 * time.Hour) } }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := transport.TLSClientConfig.Clone()
			test.configure(config)
			badTransport := &http.Transport{TLSClientConfig: config}
			defer badTransport.CloseIdleConnections()
			bad := &http.Client{Transport: badTransport, Timeout: 5 * time.Second}
			response, err := bad.Get(server.URL)
			if err == nil {
				response.Body.Close()
				t.Fatal("invalid TLS peer accepted")
			}
		})
	}
}

func TestRejectInvalidIssuance(t *testing.T) {
	installation := uuid.New()
	now := time.Now()
	cert, key, err := CreateCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadCA(installation, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	request, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		t.Fatal(err)
	}
	identity := Identity{installation, Agent, uuid.New()}
	for _, test := range []struct {
		name     string
		identity Identity
		names    []string
		now      time.Time
		life     time.Duration
		csr      []byte
	}{
		{"wrong installation", Identity{uuid.New(), Agent, uuid.New()}, nil, now, time.Hour, request},
		{"empty subject", Identity{installation, Agent, uuid.Nil}, nil, now, time.Hour, request},
		{"unknown role", Identity{installation, Role("admin"), uuid.New()}, nil, now, time.Hour, request},
		{"agent server name", identity, []string{"example.com"}, now, time.Hour, request},
		{"controller without name", Identity{installation, Controller, uuid.New()}, nil, now, time.Hour, request},
		{"wildcard name", Identity{installation, Controller, uuid.New()}, []string{"*.example.com"}, now, time.Hour, request},
		{"invalid name", Identity{installation, Controller, uuid.New()}, []string{"example.com:443"}, now, time.Hour, request},
		{"short lifetime", identity, nil, now, time.Minute, request},
		{"beyond CA", identity, nil, now, 3 * time.Hour, request},
		{"before CA", identity, nil, now.Add(-time.Hour), time.Hour, request},
		{"long lifetime", identity, nil, now, 31 * 24 * time.Hour, request},
		{"invalid CSR", identity, nil, now, time.Hour, []byte("not a CSR")},
		{"oversized CSR", identity, nil, now, time.Hour, make([]byte, (64<<10)+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if cert, err := ca.Issue(test.csr, test.identity, test.names, test.now, test.life); err == nil || cert != nil {
				t.Fatal("accepted invalid issuance")
			}
		})
	}
	damaged := append([]byte(nil), request...)
	damaged[len(damaged)-1] ^= 1
	if _, err := ca.Issue(damaged, identity, nil, now, time.Hour); !errors.Is(err, ErrCSR) {
		t.Fatalf("invalid proof of possession: %v", err)
	}
	issued, err := ca.Issue(request, identity, nil, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(issued)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(cert)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now.Add(2 * time.Hour), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
		t.Fatal("accepted expired agent")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("agent certificate refused for replication serving: %v", err)
	}
}

func TestCARecoveryAndValidation(t *testing.T) {
	if _, err := new(CA).Issue(nil, Identity{}, nil, time.Now(), time.Hour); err == nil {
		t.Fatal("accepted uninitialized CA")
	}
	installation := uuid.New()
	now := time.Now()
	cert, key, err := CreateCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(installation, cert, key); err != nil {
		t.Fatal(err)
	}
	otherCert, otherKey, err := CreateCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		id        uuid.UUID
		cert, key []byte
	}{
		{uuid.New(), cert, key},
		{installation, cert, otherKey},
		{installation, otherCert, key},
		{installation, cert, nil},
		{installation, nil, key},
		{installation, append(append([]byte(nil), cert...), cert...), key},
		{installation, cert, []byte(strings.Repeat("x", (64<<10)+1))},
	} {
		if _, err := LoadCA(test.id, test.cert, test.key); !errors.Is(err, ErrCertificate) {
			t.Fatalf("invalid CA recovery: %v", err)
		}
	}
	if _, _, err := CreateCA(uuid.Nil, now, time.Hour); err == nil {
		t.Fatal("accepted empty installation")
	}
	// The public certificate cannot serve as private recovery material.
	if _, err := LoadCA(installation, cert, cert); err == nil {
		t.Fatal("accepted public-only recovery")
	}
}
