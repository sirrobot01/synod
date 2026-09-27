package cluster

import "github.com/google/uuid"

// EndpointInspection reports reserved addresses and stored lifecycle observations.
type EndpointInspection struct {
	ClusterID         uuid.UUID         `json:"cluster_id"`
	Engine            Engine            `json:"engine"`
	DesiredRevision   int64             `json:"desired_revision"`
	ObservedRevision  int64             `json:"observed_revision"`
	DesiredLifecycle  DesiredLifecycle  `json:"desired_lifecycle"`
	ObservedLifecycle ObservedLifecycle `json:"observed_lifecycle"`
	Endpoints         []Endpoint        `json:"endpoints"`
	UnavailableReason string            `json:"unavailable_reason,omitempty"`
}

// Endpoint contains connection metadata without credentials.
type Endpoint struct {
	ID                  uuid.UUID `json:"id"`
	NodeID              uuid.UUID `json:"node_id"`
	Host                string    `json:"host"`
	Port                uint16    `json:"port"`
	Transport           string    `json:"transport"`
	Database            string    `json:"database"`
	ClientMode          string    `json:"client_mode"`
	AvailabilityProfile string    `json:"availability_profile"`
	TLSMode             string    `json:"tls_mode"`
}

type CredentialInspection struct {
	ClusterID   uuid.UUID            `json:"cluster_id"`
	Credentials []CredentialMetadata `json:"credentials"`
}

// CredentialMetadata identifies a stored version without exposing its secret.
type CredentialMetadata struct {
	CredentialID uuid.UUID `json:"credential_id"`
	ClusterID    uuid.UUID `json:"cluster_id"`
	Version      int64     `json:"version"`
	Purpose      string    `json:"purpose"`
	Username     string    `json:"username"`
}

// RevealedCredential is returned only by explicit, audited credential access.
type RevealedCredential struct {
	Credential CredentialMetadata `json:"credential"`
	Password   string             `json:"password"`
}
