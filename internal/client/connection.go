package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

// GetEndpoints reads reserved connection metadata without probing the database.
func (c *Client) GetEndpoints(ctx context.Context, id uuid.UUID) (cluster.EndpointInspection, error) {
	var value cluster.EndpointInspection
	if id == uuid.Nil {
		return value, &Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
	}
	if err := c.request(ctx, http.MethodGet, "/v1/clusters/"+id.String()+"/endpoints", nil, "", http.StatusOK, &value); err != nil {
		return cluster.EndpointInspection{}, err
	}
	invalid := &Error{Code: "invalid_response", Message: "The API returned invalid endpoint metadata."}
	if value.ClusterID != id || (value.Engine != cluster.PostgreSQL && value.Engine != cluster.Redis) || value.DesiredRevision < 1 || value.ObservedRevision < 0 ||
		!slices.Contains([]cluster.DesiredLifecycle{cluster.DesiredRunning, cluster.DesiredStopped}, value.DesiredLifecycle) ||
		!slices.Contains([]cluster.ObservedLifecycle{cluster.ObservedProvisioning, cluster.ObservedRunning, cluster.ObservedStopped, cluster.ObservedDeleting, cluster.ObservedDeleted, cluster.ObservedUnknown}, value.ObservedLifecycle) ||
		value.Endpoints == nil || len(value.Endpoints) > 6 {
		return cluster.EndpointInspection{}, invalid
	}
	if len(value.Endpoints) == 0 {
		if value.WriterURL != "" || value.ReaderURL != "" || value.CACertificatePEM != "" || !slices.Contains([]string{"not_allocated", "client_address_required", "address_not_bound", "ambiguous_allocation", "members_incomplete"}, value.UnavailableReason) {
			return cluster.EndpointInspection{}, invalid
		}
		return value, nil
	}
	database := "app"
	if value.Engine == cluster.Redis {
		database = "0"
	}
	// One standalone address, three Patroni members with a writer URL, or a writer and a
	// reader route on each of three routers. Patroni endpoints carry their CA.
	profile, tls, mode := "single_node", "disabled", "direct"
	switch len(value.Endpoints) {
	case 1:
		if value.WriterURL != "" || value.ReaderURL != "" || value.CACertificatePEM != "" {
			return cluster.EndpointInspection{}, invalid
		}
	case 3:
		profile, tls = "patroni_direct", "verify-full"
		if value.ReaderURL != "" {
			return cluster.EndpointInspection{}, invalid
		}
	case 6:
		profile, tls, mode = "patroni_routed", "verify-full", "routed"
	default:
		return cluster.EndpointInspection{}, invalid
	}
	if profile != "single_node" && (value.Engine != cluster.PostgreSQL || !strings.HasPrefix(value.CACertificatePEM, "-----BEGIN CERTIFICATE-----")) {
		return cluster.EndpointInspection{}, invalid
	}
	hosts := make([]string, 0, len(value.Endpoints))
	routes := map[string][]string{}
	for index, endpoint := range value.Endpoints {
		address, err := netip.ParseAddr(endpoint.Host)
		if value.UnavailableReason != "" || endpoint.ID == uuid.Nil || endpoint.NodeID == uuid.Nil || endpoint.Port == 0 ||
			err != nil || !address.IsGlobalUnicast() || address.IsLoopback() || address.Zone() != "" || address.Unmap().String() != endpoint.Host ||
			endpoint.Transport != "tcp" || endpoint.Database != database || endpoint.ClientMode != mode || endpoint.AvailabilityProfile != profile || endpoint.TLSMode != tls {
			return cluster.EndpointInspection{}, invalid
		}
		host := net.JoinHostPort(endpoint.Host, strconv.Itoa(int(endpoint.Port)))
		if mode == "routed" {
			// Each router lists its writer route, then its reader route.
			if endpoint.Role != []string{"writer", "reader"}[index%2] {
				return cluster.EndpointInspection{}, invalid
			}
			routes[endpoint.Role] = append(routes[endpoint.Role], host)
			continue
		}
		if endpoint.Role != "" {
			return cluster.EndpointInspection{}, invalid
		}
		hosts = append(hosts, host)
	}
	switch profile {
	case "patroni_direct":
		if value.WriterURL != "postgresql://"+strings.Join(hosts, ",")+"/app?target_session_attrs=read-write&sslmode=verify-full" {
			return cluster.EndpointInspection{}, invalid
		}
	case "patroni_routed":
		routedURLs := value.WriterURL == "postgresql://"+strings.Join(routes["writer"], ",")+"/app?target_session_attrs=read-write&sslmode=verify-full" &&
			value.ReaderURL == "postgresql://"+strings.Join(routes["reader"], ",")+"/app?load_balance_hosts=random&sslmode=verify-full"
		if !routedURLs && !balancerURLs(value.WriterURL, value.ReaderURL) {
			return cluster.EndpointInspection{}, invalid
		}
	}
	return value, nil
}

// ListCredentials reads the latest metadata for each stored purpose without passwords.
func (c *Client) ListCredentials(ctx context.Context, id uuid.UUID) (cluster.CredentialInspection, error) {
	var value cluster.CredentialInspection
	if id == uuid.Nil {
		return value, &Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
	}
	if err := c.request(ctx, http.MethodGet, "/v1/clusters/"+id.String()+"/credentials", nil, "", http.StatusOK, &value); err != nil {
		return cluster.CredentialInspection{}, err
	}
	invalid := &Error{Code: "invalid_response", Message: "The API returned invalid credential metadata."}
	if value.ClusterID != id || value.Credentials == nil || len(value.Credentials) > 3 {
		return cluster.CredentialInspection{}, invalid
	}
	previous := ""
	for _, credential := range value.Credentials {
		if !validCredentialMetadata(credential, id) || credential.Purpose <= previous {
			return cluster.CredentialInspection{}, invalid
		}
		previous = credential.Purpose
	}
	return value, nil
}

// RevealCredential requests one audited disclosure and never retries the POST.
func (c *Client) RevealCredential(ctx context.Context, id uuid.UUID, purpose string) (cluster.RevealedCredential, error) {
	var value cluster.RevealedCredential
	if id == uuid.Nil || !slices.Contains([]string{"application", "superuser", "replication"}, purpose) {
		return value, &Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID and purpose application, superuser, or replication."}
	}
	if err := c.request(ctx, http.MethodPost, "/v1/clusters/"+id.String()+"/credentials/"+purpose+"/reveal", struct{}{}, "", http.StatusOK, &value); err != nil {
		if failure, ok := errors.AsType[*Error](err); ok && failure.Code == "outcome_unknown" {
			failure.Message = "Credential access authorization is uncertain. No password was returned."
		}
		return cluster.RevealedCredential{}, err
	}
	if !validCredentialMetadata(value.Credential, id) || value.Credential.Purpose != purpose || value.Password == "" {
		return cluster.RevealedCredential{}, &Error{Code: "invalid_response", Message: "The API returned an invalid credential disclosure."}
	}
	return value, nil
}

var credentialUsername = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func validCredentialMetadata(value cluster.CredentialMetadata, id uuid.UUID) bool {
	return value.ClusterID == id && value.CredentialID != uuid.Nil && value.Version > 0 &&
		slices.Contains([]string{"application", "superuser", "replication"}, value.Purpose) && credentialUsername.MatchString(value.Username)
}

// ClusterMember is one instance a cluster runs, the role it was named with, and where it runs.
type ClusterMember struct {
	ID      uuid.UUID `json:"id"`
	Role    string    `json:"role"`
	State   string    `json:"state"`
	NodeID  uuid.UUID `json:"node_id,omitzero"`
	Address string    `json:"address,omitempty"`
}

// ClusterMembers reads the members a cluster runs.
func (c *Client) ClusterMembers(ctx context.Context, id uuid.UUID) ([]ClusterMember, error) {
	var value struct {
		Members []ClusterMember `json:"members"`
	}
	if id == uuid.Nil {
		return nil, &Error{Code: "invalid_request", Message: "Name a cluster."}
	}
	err := c.request(ctx, http.MethodGet, "/v1/clusters/"+id.String()+"/members", nil, "", http.StatusOK, &value)
	return value.Members, err
}

// balancerURLs reports two load balancer URLs on one routable address with different ports.
func balancerURLs(writer, reader string) bool {
	host := func(url string) (netip.AddrPort, bool) {
		rest, ok := strings.CutPrefix(url, "postgresql://")
		if !ok {
			return netip.AddrPort{}, false
		}
		hostPort, ok := strings.CutSuffix(rest, "/app?sslmode=verify-full")
		if !ok {
			return netip.AddrPort{}, false
		}
		parsed, err := netip.ParseAddrPort(hostPort)
		if err != nil || !parsed.Addr().IsGlobalUnicast() || parsed.Port() == 0 || parsed.String() != hostPort {
			return netip.AddrPort{}, false
		}
		return parsed, true
	}
	w, ok := host(writer)
	r, fine := host(reader)
	return ok && fine && w.Addr() == r.Addr() && w.Port() != r.Port()
}
