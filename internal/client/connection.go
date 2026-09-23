package client

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"regexp"
	"slices"

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
		value.Endpoints == nil || len(value.Endpoints) > 1 {
		return cluster.EndpointInspection{}, invalid
	}
	if len(value.Endpoints) == 0 {
		if !slices.Contains([]string{"not_allocated", "client_address_required", "address_not_bound", "ambiguous_allocation"}, value.UnavailableReason) {
			return cluster.EndpointInspection{}, invalid
		}
		return value, nil
	}
	endpoint := value.Endpoints[0]
	address, err := netip.ParseAddr(endpoint.Host)
	database := "app"
	if value.Engine == cluster.Redis {
		database = "0"
	}
	if value.UnavailableReason != "" || endpoint.ID == uuid.Nil || endpoint.NodeID == uuid.Nil || endpoint.Port == 0 ||
		err != nil || !address.IsGlobalUnicast() || address.IsLoopback() || address.Zone() != "" || address.Unmap().String() != endpoint.Host ||
		endpoint.Transport != "tcp" || endpoint.Database != database || endpoint.ClientMode != "direct" || endpoint.AvailabilityProfile != "single_node" || endpoint.TLSMode != "disabled" {
		return cluster.EndpointInspection{}, invalid
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

// ClusterMember is one instance a cluster runs and the role it was named with.
type ClusterMember struct {
	ID    uuid.UUID `json:"id"`
	Role  string    `json:"role"`
	State string    `json:"state"`
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
