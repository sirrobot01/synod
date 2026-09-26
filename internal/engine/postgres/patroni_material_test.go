package postgres

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

func TestPatroniTLSMaterial(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "material-test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(change func(*x509.Certificate)) PatroniTLSMaterial {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Minute), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("192.0.2.10")}}
		if change != nil {
			change(leaf)
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		private, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return PatroniTLSMaterial{CA: bytes.Clone(caPEM), Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), PrivateKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
	}
	member, dcs := issue(nil), issue(nil)
	settings := PatroniSettings{RequiredFreeBytes: 1 << 30, InstallationID: uuid.New(), ClusterID: uuid.New(), ResourceID: uuid.New(), VolumeID: uuid.New(), ProvisionID: uuid.New(), ApplicationCredentialID: uuid.New(), ApplicationCredentialVersion: 1, Address: netip.MustParseAddr("192.0.2.10"), DatabasePort: 5432, RESTPort: 8008, DCSEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}, ClientNetworks: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}
	secrets := PatroniSecrets{Superuser: uuid.NewString(), Replication: uuid.NewString(), Rewind: uuid.NewString(), REST: uuid.NewString(), ApplicationVerifier: "SCRAM-SHA-256$4096:AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
	files, err := PatroniFiles(settings, secrets, member, dcs, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 7 || !bytes.Contains(files["config.json"], []byte(secrets.Superuser)) {
		t.Fatal("missing protected native material")
	}
	for name, expected := range map[string][]byte{"ca.crt": member.CA, "member.crt": member.Certificate, "member.key": member.PrivateKey, "dcs-ca.crt": dcs.CA, "dcs-client.crt": dcs.Certificate, "dcs-client.key": dcs.PrivateKey} {
		if !bytes.Equal(files[name], expected) {
			t.Fatal("wrong protected file", name)
		}
		files[name][0] ^= 1
		if bytes.Equal(files[name], expected) {
			t.Fatal("prepared files share caller storage", name)
		}
	}
	chain := member
	chain.Certificate = append(bytes.Clone(member.Certificate), member.CA...)
	clientOnly := issue(func(c *x509.Certificate) {
		c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		c.IPAddresses = nil
	})
	if _, err := PatroniFiles(settings, secrets, chain, clientOnly, now); err != nil {
		t.Fatal("valid chain or dedicated DCS client refused", err)
	}
	foreignKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	foreign := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "foreign"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	foreignDER, err := x509.CreateCertificate(rand.Reader, foreign, foreign, &foreignKey.PublicKey, foreignKey)
	if err != nil {
		t.Fatal(err)
	}
	foreignCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: foreignDER})
	sec1, err := x509.MarshalECPrivateKey(foreignKey)
	if err != nil {
		t.Fatal(err)
	}
	badKey := issue(nil).PrivateKey
	for _, test := range []struct {
		name   string
		dcs    bool
		change func(*PatroniTLSMaterial)
	}{
		{"wrong_CA", false, func(m *PatroniTLSMaterial) { m.CA = foreignCA }},
		{"wrong_DCS_CA", true, func(m *PatroniTLSMaterial) { m.CA = foreignCA }},
		{"mislabelled_key_format", false, func(m *PatroniTLSMaterial) {
			m.PrivateKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: sec1})
		}},
		{"wrong_key", false, func(m *PatroniTLSMaterial) { m.PrivateKey = badKey }},
		{"wrong_address", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.IPAddresses = []net.IP{net.ParseIP("192.0.2.11")} })
		}},
		{"server_only", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} })
		}},
		{"client_only", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} })
		}},
		{"dcs_server_only", true, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} })
		}},
		{"dcs_common_name", true, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.Subject.CommonName = "wrong-native-identity" })
		}},
		{"expired", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.NotBefore = now.Add(-2 * time.Hour); c.NotAfter = now.Add(-time.Hour) })
		}},
		{"future", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) { c.NotBefore = now.Add(time.Minute); c.NotAfter = now.Add(2 * time.Minute) })
		}},
		{"authority_as_leaf", false, func(m *PatroniTLSMaterial) {
			*m = issue(func(c *x509.Certificate) {
				c.IsCA = true
				c.BasicConstraintsValid = true
				c.KeyUsage |= x509.KeyUsageCertSign
			})
		}},
		{"non_CA_trust", false, func(m *PatroniTLSMaterial) { m.CA = member.Certificate }},
		{"missing_CA", false, func(m *PatroniTLSMaterial) { m.CA = nil }},
		{"non_CA_intermediate", false, func(m *PatroniTLSMaterial) { m.Certificate = append(bytes.Clone(m.Certificate), dcs.Certificate...) }},
		{"leading_text", false, func(m *PatroniTLSMaterial) { m.CA = append([]byte("private-marker\n"), m.CA...) }},
		{"trailing_text", false, func(m *PatroniTLSMaterial) {
			m.Certificate = append(bytes.Clone(m.Certificate), []byte("private-marker")...)
		}},
		{"malformed_first_block", false, func(m *PatroniTLSMaterial) {
			m.Certificate = append([]byte("-----BEGIN CERTIFICATE-----\nbad\n"), m.Certificate...)
		}},
		{"key_in_chain", false, func(m *PatroniTLSMaterial) { m.Certificate = append(bytes.Clone(m.Certificate), m.PrivateKey...) }},
		{"repeated_key", false, func(m *PatroniTLSMaterial) { m.PrivateKey = append(bytes.Clone(m.PrivateKey), m.PrivateKey...) }},
		{"malformed_key_prefix", false, func(m *PatroniTLSMaterial) {
			m.PrivateKey = append([]byte("-----BEGIN PRIVATE KEY-----\nbad\n"), m.PrivateKey...)
		}},
		{"too_large", false, func(m *PatroniTLSMaterial) { m.CA = bytes.Repeat([]byte("x"), 128<<10+1) }},
		{"too_many_certificates", false, func(m *PatroniTLSMaterial) { m.CA = bytes.Repeat(m.CA, 9) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := member
			if test.dcs {
				changed = dcs
			}
			test.change(&changed)
			m, d := changed, dcs
			if test.dcs {
				m, d = member, changed
			}
			files, err := PatroniFiles(settings, secrets, m, d, now)
			if err == nil || files != nil {
				t.Fatal("invalid TLS material accepted")
			}
			for _, value := range []string{secrets.Superuser, secrets.ApplicationVerifier, string(changed.PrivateKey), "private-marker"} {
				if value != "" && strings.Contains(err.Error(), value) {
					t.Fatal("TLS refusal disclosed material")
				}
			}
		})
	}
	for _, instant := range []time.Time{{}, now.Add(2 * time.Hour)} {
		if files, err := PatroniFiles(settings, secrets, member, dcs, instant); err == nil || files != nil {
			t.Fatal("invalid verification time accepted")
		}
	}
	settings.ResourceID = uuid.Nil
	if files, err := PatroniFiles(settings, secrets, member, dcs, now); err == nil || files != nil {
		t.Fatal("invalid member settings accepted")
	}
}

func TestIssuePatroniMaterial(t *testing.T) {
	now := time.Now()
	installation, cluster, resource := uuid.New(), uuid.New(), uuid.New()
	nativeCert, nativeKey, err := pki.CreatePatroniCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	native, err := pki.LoadPatroniCA(installation, nativeCert, nativeKey)
	if err != nil {
		t.Fatal(err)
	}
	etcdTrust, _, err := pki.CreateCA(uuid.New(), now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	address := netip.MustParseAddr("192.0.2.10")
	settings := PatroniSettings{RequiredFreeBytes: 1 << 30, InstallationID: installation, ClusterID: cluster, ResourceID: resource, VolumeID: uuid.New(), ProvisionID: uuid.New(), ApplicationCredentialID: uuid.New(), ApplicationCredentialVersion: 1, Address: address, DatabasePort: 5432, RESTPort: 8008, DCSEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}, ClientNetworks: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}
	secrets := PatroniSecrets{Superuser: uuid.NewString(), Replication: uuid.NewString(), Rewind: uuid.NewString(), REST: uuid.NewString(), ApplicationVerifier: "SCRAM-SHA-256$4096:AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
	member, err := IssuePatroniMemberMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster, ResourceID: resource}, address, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dcs, err := IssuePatroniDCSMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster}, etcdTrust, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	files, err := PatroniFiles(settings, secrets, member, dcs, now)
	if err != nil {
		t.Fatal("issued material refused", err)
	}
	if !bytes.Contains(files["dcs-ca.crt"], bytes.TrimSpace(etcdTrust)) || !bytes.Contains(files["dcs-ca.crt"], native.CertificatePEM()) {
		t.Fatal("DCS trust does not carry the etcd server trust and the native CA")
	}
	if bytes.Equal(member.PrivateKey, dcs.PrivateKey) {
		t.Fatal("member and DCS material share a key")
	}
	moved := settings
	moved.Address = netip.MustParseAddr("192.0.2.11")
	if _, err := PatroniFiles(moved, secrets, member, dcs, now); err == nil {
		t.Fatal("member material accepted another member address")
	}
	controlCert, controlKey, err := pki.CreateCA(installation, now, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	control, err := pki.LoadCA(installation, controlCert, controlKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, issue := range map[string]func() error{
		"control CA": func() error {
			_, err := IssuePatroniMemberMaterial(control, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster, ResourceID: resource}, address, now, time.Hour)
			return err
		},
		"member without resource": func() error {
			_, err := IssuePatroniMemberMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster}, address, now, time.Hour)
			return err
		},
		"DCS with resource": func() error {
			_, err := IssuePatroniDCSMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster, ResourceID: resource}, etcdTrust, now, time.Hour)
			return err
		},
		"DCS without trust": func() error {
			_, err := IssuePatroniDCSMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster}, []byte(" \n"), now, time.Hour)
			return err
		},
		"lifetime beyond CA": func() error {
			_, err := IssuePatroniMemberMaterial(native, pki.PatroniIdentity{InstallationID: installation, ClusterID: cluster, ResourceID: resource}, address, now, 48*time.Hour)
			return err
		},
	} {
		if err := issue(); err == nil {
			t.Fatalf("%s: material issued", name)
		}
	}
}
