package pki

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPatroniCertificateProfiles(t *testing.T) {
	now := time.Now()
	installation, cluster, resource := uuid.New(), uuid.New(), uuid.New()
	root, rootKey, err := CreatePatroniCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadPatroniCA(installation, root, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(root)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := (Identity{installation, Controller, uuid.New()}).URI()
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "root"}, DNSNames: []string{"attacker.example"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, EmailAddresses: []string{"root@example.com"}, URIs: []*url.URL{uri},
		ExtraExtensions: []pkix.Extension{{Id: []int{2, 5, 29, 19}, Critical: true, Value: []byte{0x30, 0x03, 0x01, 0x01, 0xff}}},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var member, dcs tls.Certificate
	for _, test := range []struct {
		name     string
		resource uuid.UUID
		address  netip.Addr
		path     string
	}{
		{"member", resource, netip.MustParseAddr("10.31.0.2"), "/member/" + resource.String()},
		{"ipv6", resource, netip.MustParseAddr("fd00::2"), "/member/" + resource.String()},
		{"dcs", uuid.Nil, netip.Addr{}, "/dcs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cert, err := ca.IssuePatroni(csr, PatroniIdentity{installation, cluster, test.resource}, test.address, now, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			block, rest := pem.Decode(cert)
			if block == nil || len(rest) != 0 {
				t.Fatal("not one certificate")
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if leaf.SignatureAlgorithm != x509.ECDSAWithSHA256 || leaf.IsCA || !leaf.BasicConstraintsValid || leaf.KeyUsage != x509.KeyUsageDigitalSignature || leaf.Subject.String() != "" || len(leaf.DNSNames) != 0 || len(leaf.EmailAddresses) != 0 || len(leaf.UnhandledCriticalExtensions) != 0 {
				t.Fatal("CSR overrode leaf policy")
			}
			expected := "spiffe://" + installation.String() + ".synod/patroni/" + cluster.String() + test.path
			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected {
				t.Fatal("wrong native identity")
			}
			usages := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
			if test.resource != uuid.Nil {
				usages = append(usages, x509.ExtKeyUsageServerAuth)
				if len(leaf.IPAddresses) != 1 || !leaf.IPAddresses[0].Equal(test.address.AsSlice()) {
					t.Fatal("wrong IP identity")
				}
			} else if len(leaf.IPAddresses) != 0 {
				t.Fatal("DCS client has a server address")
			}
			if !slices.Equal(leaf.ExtKeyUsage, usages) {
				t.Fatal("wrong usages")
			}
			chains, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
			if err != nil {
				t.Fatal(err)
			}
			state := tls.ConnectionState{HandshakeComplete: true, VerifiedChains: chains, PeerCertificates: []*x509.Certificate{leaf}}
			for _, role := range []Role{Agent, Controller} {
				if _, err := PeerIdentity(state, installation, role); !errors.Is(err, ErrIdentity) {
					t.Fatal("native leaf accepted as control-plane peer")
				}
			}
			_, err = leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, DNSName: test.address.String(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
			if (err == nil) != (test.resource != uuid.Nil) {
				t.Fatalf("server profile check: %v", err)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now.Add(2 * time.Hour), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err == nil {
				t.Fatal("expired leaf accepted")
			}
			pair, err := tls.X509KeyPair(cert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
			if err != nil {
				t.Fatal(err)
			}
			if test.name == "member" {
				member = pair
			}
			if test.name == "dcs" {
				dcs = pair
			}
		})
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := PeerIdentity(*r.TLS, installation, Agent); !errors.Is(err, ErrIdentity) {
			t.Error("native client accepted as agent")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{member}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "10.31.0.2", Certificates: []tls.Certificate{dcs}}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal(response.Status)
	}
}

func TestRejectInvalidPatroniIssuance(t *testing.T) {
	now := time.Now()
	installation := uuid.New()
	root, rootKey, err := CreatePatroniCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadPatroniCA(installation, root, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	damaged := bytes.Clone(csr)
	damaged[len(damaged)-1] ^= 1
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	wrongAlgorithm, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, ecdsaKey)
	if err != nil {
		t.Fatal(err)
	}
	identity := PatroniIdentity{installation, uuid.New(), uuid.New()}
	address := netip.MustParseAddr("10.31.0.2")
	for _, test := range []struct {
		name     string
		issuer   *CA
		identity PatroniIdentity
		address  netip.Addr
		csr      []byte
		now      time.Time
		life     time.Duration
	}{
		{"nil CA", nil, identity, address, csr, now, time.Hour},
		{"empty CA", new(CA), identity, address, csr, now, time.Hour},
		{"foreign installation", ca, PatroniIdentity{uuid.New(), identity.ClusterID, identity.ResourceID}, address, csr, now, time.Hour},
		{"empty installation", ca, PatroniIdentity{uuid.Nil, identity.ClusterID, identity.ResourceID}, address, csr, now, time.Hour},
		{"empty cluster", ca, PatroniIdentity{installation, uuid.Nil, identity.ResourceID}, address, csr, now, time.Hour},
		{"DCS address", ca, PatroniIdentity{installation, identity.ClusterID, uuid.Nil}, address, csr, now, time.Hour},
		{"member without address", ca, identity, netip.Addr{}, csr, now, time.Hour},
		{"bad CSR", ca, identity, address, []byte("private-invalid-csr"), now, time.Hour},
		{"oversize CSR", ca, identity, address, make([]byte, (64<<10)+1), now, time.Hour},
		{"bad signature", ca, identity, address, damaged, now, time.Hour},
		{"wrong algorithm", ca, identity, address, wrongAlgorithm, now, time.Hour},
		{"short lifetime", ca, identity, address, csr, now, time.Minute},
		{"long lifetime", ca, identity, address, csr, now, 31 * 24 * time.Hour},
		{"past CA", ca, identity, address, csr, now.Add(-time.Hour), time.Hour},
		{"beyond CA", ca, identity, address, csr, now, 3 * time.Hour},
		{"expired CA", ca, identity, address, csr, now.Add(3 * time.Hour), time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			cert, err := test.issuer.IssuePatroni(test.csr, test.identity, test.address, test.now, test.life)
			if err == nil || cert != nil {
				t.Fatal("invalid issuance succeeded")
			}
			if bytes.Contains([]byte(err.Error()), []byte("private-invalid-csr")) {
				t.Fatal("error contains CSR")
			}
		})
	}
	for _, ip := range []string{"127.0.0.1", "::1", "0.0.0.0", "::", "224.0.0.1", "ff02::1", "::ffff:10.31.0.2", "fe80::1", "fe80::1%en0", "fd00::2%en0"} {
		t.Run(ip, func(t *testing.T) {
			if cert, err := ca.IssuePatroni(csr, identity, netip.MustParseAddr(ip), now, time.Hour); err == nil || cert != nil {
				t.Fatal("invalid member IP accepted")
			}
		})
	}
}

func TestPatroniCAProfileAndRecovery(t *testing.T) {
	installation, now := uuid.New(), time.Now()
	nativeCert, nativeKey, err := CreatePatroniCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	controlCert, controlKey, err := CreateCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	native, err := LoadPatroniCA(installation, nativeCert, nativeKey)
	if err != nil {
		t.Fatal(err)
	}
	control, err := LoadCA(installation, controlCert, controlKey)
	if err != nil {
		t.Fatal(err)
	}
	_, otherNativeKey, err := CreatePatroniCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		cert, key []byte
		id        uuid.UUID
	}{
		{controlCert, controlKey, installation},
		{nativeCert, controlKey, installation},
		{nativeCert, otherNativeKey, installation},
		{nativeCert, nativeKey, uuid.New()},
		{nativeCert, nativeCert, installation},
		{nativeCert, nil, installation},
	} {
		if result, err := LoadPatroniCA(test.id, test.cert, test.key); err == nil || result != nil {
			t.Fatal("invalid native CA accepted")
		}
	}
	if result, err := LoadCA(installation, nativeCert, nativeKey); err == nil || result != nil {
		t.Fatal("native root loaded as control-plane CA")
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	if cert, err := native.Issue(csr, Identity{installation, Agent, uuid.New()}, nil, now, time.Hour); !errors.Is(err, ErrIdentity) || cert != nil {
		t.Fatal("native CA issued control-plane leaf")
	}
	for _, unavailable := range []*CA{nil, new(CA)} {
		if cert, err := unavailable.Issue(csr, Identity{installation, Agent, uuid.New()}, nil, now, time.Hour); !errors.Is(err, ErrCertificate) || cert != nil {
			t.Fatal("unavailable control-plane CA did not retain its service error")
		}
	}
	identity := PatroniIdentity{installation, uuid.New(), uuid.New()}
	address := netip.MustParseAddr("10.31.0.2")
	if cert, err := control.IssuePatroni(csr, identity, address, now, time.Hour); !errors.Is(err, ErrIdentity) || cert != nil {
		t.Fatal("control-plane CA issued native leaf")
	}
	first, err := native.IssuePatroni(csr, identity, address, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := LoadPatroniCA(installation, nativeCert, nativeKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := recovered.IssuePatroni(csr, identity, address, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	firstBlock, _ := pem.Decode(first)
	secondBlock, _ := pem.Decode(second)
	firstLeaf, err := x509.ParseCertificate(firstBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	secondLeaf, err := x509.ParseCertificate(secondBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if firstLeaf.SerialNumber.Cmp(secondLeaf.SerialNumber) == 0 || firstLeaf.SerialNumber.Sign() <= 0 || secondLeaf.SerialNumber.Sign() <= 0 {
		t.Fatal("reused or invalid serial")
	}
	if !bytes.Equal(recovered.CertificatePEM(), nativeCert) {
		t.Fatal("recovery changed public trust")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(controlCert)
	if _, err := secondLeaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, DNSName: address.String(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err == nil {
		t.Fatal("native leaf trusted by control-plane root")
	}
}

// A member leaf names its routed endpoints. A DCS leaf names none, and bad endpoints are refused.
func TestPatroniEndpointNames(t *testing.T) {
	now := time.Now()
	installation, cluster, resource := uuid.New(), uuid.New(), uuid.New()
	root, rootKey, err := CreatePatroniCA(installation, now, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := LoadPatroniCA(installation, root, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}
	member, router := netip.MustParseAddr("10.31.0.2"), netip.MustParseAddr("10.31.0.9")
	cert, err := ca.IssuePatroni(csr, PatroniIdentity{installation, cluster, resource}, member, now, time.Hour, router)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(cert)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(leaf.IPAddresses) != 2 || leaf.VerifyHostname(member.String()) != nil || leaf.VerifyHostname(router.String()) != nil || leaf.VerifyHostname("10.31.0.3") == nil {
		t.Fatalf("member leaf names %v", leaf.IPAddresses)
	}
	many := make([]netip.Addr, maxPatroniEndpoints+1)
	for index := range many {
		many[index] = netip.AddrFrom4([4]byte{10, 31, 1, byte(index + 1)})
	}
	for name, test := range map[string]struct {
		resource  uuid.UUID
		address   netip.Addr
		endpoints []netip.Addr
	}{
		"DCS with endpoint":  {uuid.Nil, netip.Addr{}, []netip.Addr{router}},
		"loopback endpoint":  {resource, member, []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
		"member as endpoint": {resource, member, []netip.Addr{member}},
		"repeated endpoint":  {resource, member, []netip.Addr{router, router}},
		"too many endpoints": {resource, member, many},
	} {
		if _, err := ca.IssuePatroni(csr, PatroniIdentity{installation, cluster, test.resource}, test.address, now, time.Hour, test.endpoints...); !errors.Is(err, ErrIdentity) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
