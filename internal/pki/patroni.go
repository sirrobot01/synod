package pki

import (
	"crypto/x509"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
)

// CreatePatroniCA creates a separate P-256 root for native TLS and SCRAM channel binding.
func CreatePatroniCA(installationID uuid.UUID, now time.Time, lifetime time.Duration) ([]byte, []byte, error) {
	return createCA(installationID, now, lifetime, true)
}

// LoadPatroniCA requires a matching P-256 key and the installation's native CA identity.
func LoadPatroniCA(installationID uuid.UUID, certificatePEM, privateKeyPEM []byte) (*CA, error) {
	return loadCA(installationID, certificatePEM, privateKeyPEM, true)
}

// PatroniIdentity binds a native leaf to its installation, cluster, and optional member resource.
type PatroniIdentity struct {
	InstallationID uuid.UUID
	ClusterID      uuid.UUID
	ResourceID     uuid.UUID
}

// maxPatroniEndpoints bounds the routed endpoint addresses one member leaf names.
const maxPatroniEndpoints = 8

// IssuePatroni signs a member CSR with its IP, or a cluster DCS client CSR with no IP or resource.
// A member leaf also names each routed endpoint address, because the member's pooler presents
// it to clients that connect through a router.
func (ca *CA) IssuePatroni(csrDER []byte, identity PatroniIdentity, address netip.Addr, now time.Time, lifetime time.Duration, endpoints ...netip.Addr) ([]byte, error) {
	if ca == nil || !ca.native || identity.InstallationID == uuid.Nil || identity.InstallationID != ca.installationID || identity.ClusterID == uuid.Nil {
		return nil, ErrIdentity
	}
	uri := &url.URL{Scheme: "spiffe", Host: identity.InstallationID.String() + ".synod", Path: "/patroni/" + identity.ClusterID.String() + "/dcs"}
	template := &x509.Certificate{
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{uri},
	}
	if identity.ResourceID == uuid.Nil {
		if address.IsValid() || len(endpoints) != 0 {
			return nil, ErrIdentity
		}
	} else {
		if !address.IsGlobalUnicast() || address.Is4In6() || address.Zone() != "" || len(endpoints) > maxPatroniEndpoints {
			return nil, ErrIdentity
		}
		uri.Path = "/patroni/" + identity.ClusterID.String() + "/member/" + identity.ResourceID.String()
		template.IPAddresses = []net.IP{address.AsSlice()}
		named := []netip.Addr{address}
		for _, endpoint := range endpoints {
			if !endpoint.IsGlobalUnicast() || endpoint.Is4In6() || endpoint.Zone() != "" || slices.Contains(named, endpoint) {
				return nil, ErrIdentity
			}
			named = append(named, endpoint)
			template.IPAddresses = append(template.IPAddresses, endpoint.AsSlice())
		}
		template.ExtKeyUsage = append(template.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	}
	return ca.issueLeaf(csrDER, template, now, lifetime)
}
