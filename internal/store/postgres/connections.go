package postgres

import (
	"context"
	"encoding/json"
	"net/netip"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/cluster"
	"github.com/sirrobot01/synod/internal/placement"
)

// InspectClusterEndpoints reads connection metadata from one committed snapshot.
func (s *Store) InspectClusterEndpoints(ctx context.Context, installationID, clusterID uuid.UUID) (cluster.EndpointInspection, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return cluster.EndpointInspection{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	found := cluster.EndpointInspection{ClusterID: clusterID, Endpoints: []cluster.Endpoint{}}
	var spec cluster.CreateSpec
	err = tx.QueryRow(ctx, `SELECT engine,spec,desired_revision,observed_revision,desired_lifecycle,observed_lifecycle
 FROM clusters WHERE installation_id=$1 AND id=$2 AND deleted_at IS NULL AND desired_lifecycle<>'deleted'`,
		installationID, clusterID).Scan(&found.Engine, &spec, &found.DesiredRevision, &found.ObservedRevision, &found.DesiredLifecycle, &found.ObservedLifecycle)
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	database := ""
	switch {
	case found.Engine == cluster.PostgreSQL && spec.PostgreSQL != nil && spec.PostgreSQL.Topology == "standalone":
		database = "app"
	case found.Engine == cluster.Redis && spec.Redis != nil && spec.Redis.Topology == "standalone":
		database = "0"
	default:
		found.UnavailableReason = "ambiguous_allocation"
		return found, nil
	}
	// The writer is the primary member, named in the membership rather than inferred
	// from how many reservations the cluster happens to hold. A replicated cluster
	// holds one reservation for each member, and reporting a replica would send writes
	// to a read-only engine.
	rows, err := tx.Query(ctx, `SELECT a.id,a.resource_id,a.node_id,p.policy_bytes
 FROM resource_allocations a
 JOIN cluster_members m ON m.installation_id=a.installation_id AND m.cluster_id=a.cluster_id
 AND m.resource_id=a.resource_id AND m.role='primary' AND m.removed_at IS NULL
 LEFT JOIN node_placement_policies p
 ON p.installation_id=a.installation_id AND p.node_id=a.node_id
 WHERE a.installation_id=$1 AND a.cluster_id=$2 AND a.released_at IS NULL ORDER BY a.id LIMIT 2`, installationID, clusterID)
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	type allocation struct {
		ID       uuid.UUID
		Resource uuid.UUID
		Node     uuid.UUID
		Policy   []byte
	}
	allocations, err := pgx.CollectRows(rows, pgx.RowToStructByPos[allocation])
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	if len(allocations) == 0 {
		// A cluster that holds nothing has not been placed. One that holds a
		// reservation without a live primary has a writer nothing can name, which is
		// not the same thing and must not read as an address that is merely pending.
		var held bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM resource_allocations
 WHERE installation_id=$1 AND cluster_id=$2 AND released_at IS NULL)`, installationID, clusterID).Scan(&held); err != nil {
			return cluster.EndpointInspection{}, admissionError(err)
		}
		found.UnavailableReason = "not_allocated"
		if held {
			found.UnavailableReason = "ambiguous_allocation"
		}
		return found, nil
	}
	// The membership index allows one live primary, so a second row here is a database
	// that disagrees with its own constraint.
	if len(allocations) != 1 {
		found.UnavailableReason = "ambiguous_allocation"
		return found, nil
	}
	allocated := allocations[0]
	rows, err = tx.Query(ctx, `SELECT host(address),port FROM allocation_ports
 WHERE installation_id=$1 AND cluster_id=$2 AND allocation_id=$3 AND node_id=$4
 AND released_at IS NULL AND transport='tcp' ORDER BY address,port LIMIT 2`, installationID, clusterID, allocated.ID, allocated.Node)
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	type listener struct {
		Address string
		Port    uint16
	}
	listeners, err := pgx.CollectRows(rows, pgx.RowToStructByPos[listener])
	if err != nil {
		return cluster.EndpointInspection{}, admissionError(err)
	}
	if len(listeners) == 0 {
		found.UnavailableReason = "not_allocated"
		return found, nil
	}
	if len(listeners) != 1 {
		found.UnavailableReason = "ambiguous_allocation"
		return found, nil
	}
	address, reason, err := clientAddress(allocated.Policy, installationID, allocated.Node, listeners[0].Address, false)
	if err != nil {
		return cluster.EndpointInspection{}, err
	}
	if reason != "" {
		found.UnavailableReason = reason
		return found, nil
	}
	found.Endpoints = append(found.Endpoints, cluster.Endpoint{
		ID: allocated.Resource, NodeID: allocated.Node, Host: address.String(), Port: listeners[0].Port,
		Transport: "tcp", Database: database, ClientMode: "direct", AvailabilityProfile: "single_node", TLSMode: "disabled",
	})
	return found, nil
}

// clientAddress answers where clients reach a node, or why a reserved binding has no such address.
//
// A published port bound to a wildcard serves one address family, so the family must match the
// client address. A host-network member binds the node's client address itself, and its wildcard
// reservation only holds the port number, so any wildcard serves it.
func clientAddress(policyBytes []byte, installationID, nodeID uuid.UUID, bind string, hostNetwork bool) (netip.Addr, string, error) {
	var policy placement.NodePolicy
	if len(policyBytes) > 0 {
		if err := json.Unmarshal(policyBytes, &policy); err != nil {
			return netip.Addr{}, "", admissionError(err)
		}
	}
	if policy.ClientAddress == "" {
		return netip.Addr{}, "client_address_required", nil
	}
	if err := policy.Validate(); err != nil {
		return netip.Addr{}, "", admissionError(err)
	}
	if policy.InstallationID != installationID || policy.NodeID != nodeID {
		return netip.Addr{}, "", &AdmissionError{Code: DependencyUnavailable}
	}
	client, err := netip.ParseAddr(policy.ClientAddress)
	if err != nil {
		return netip.Addr{}, "", admissionError(err)
	}
	bound, err := netip.ParseAddr(bind)
	if err != nil {
		return netip.Addr{}, "", admissionError(err)
	}
	if bound != client && (!bound.IsUnspecified() || (!hostNetwork && bound.Is4() != client.Is4())) {
		return netip.Addr{}, "address_not_bound", nil
	}
	return client, "", nil
}

// InspectClusterCredentials lists stored versions without loading their ciphertext.
func (s *Store) InspectClusterCredentials(ctx context.Context, installationID, clusterID uuid.UUID) (cluster.CredentialInspection, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil {
		return cluster.CredentialInspection{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return cluster.CredentialInspection{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	found := cluster.CredentialInspection{ClusterID: clusterID, Credentials: []cluster.CredentialMetadata{}}
	err = tx.QueryRow(ctx, `SELECT id FROM clusters WHERE installation_id=$1 AND id=$2
 AND deleted_at IS NULL AND desired_lifecycle<>'deleted'`, installationID, clusterID).Scan(&found.ClusterID)
	if err != nil {
		return cluster.CredentialInspection{}, admissionError(err)
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (purpose) credential_id,cluster_id,version,purpose,role_name
 FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND purpose IN ('superuser','application','replication') ORDER BY purpose,version DESC`, installationID, clusterID)
	if err != nil {
		return cluster.CredentialInspection{}, admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var metadata cluster.CredentialMetadata
		if err := rows.Scan(&metadata.CredentialID, &metadata.ClusterID, &metadata.Version, &metadata.Purpose, &metadata.Username); err != nil {
			return cluster.CredentialInspection{}, admissionError(err)
		}
		found.Credentials = append(found.Credentials, metadata)
	}
	if err := rows.Err(); err != nil {
		return cluster.CredentialInspection{}, admissionError(err)
	}
	return found, nil
}

// AuthorizeCredentialReveal commits an access audit before returning a sealed credential.
func (s *Store) AuthorizeCredentialReveal(ctx context.Context, installationID, clusterID uuid.UUID, purpose, actorID, requestID string) (Credential, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || !validSQLPurpose(purpose) ||
		len(actorID) < 1 || len(actorID) > 256 || !utf8.ValidString(actorID) || strings.ContainsRune(actorID, 0) || !printableWithin(requestID, 128) {
		return Credential{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Keep deletion and credential intent writes behind the access decision.
	err = tx.QueryRow(ctx, `SELECT id FROM clusters WHERE installation_id=$1 AND id=$2
 AND deleted_at IS NULL AND desired_lifecycle<>'deleted' FOR SHARE`, installationID, clusterID).Scan(&clusterID)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	var found Credential
	err = tx.QueryRow(ctx, `SELECT credential_id,version,purpose,role_name,key_id,format_version,sealed
 FROM cluster_credentials WHERE installation_id=$1 AND cluster_id=$2 AND purpose=$3 ORDER BY version DESC LIMIT 1`, installationID, clusterID, purpose).
		Scan(&found.CredentialID, &found.Version, &found.Purpose, &found.RoleName, &found.Envelope.KeyID, &found.Envelope.FormatVersion, &found.Envelope.Sealed)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events
 (id,installation_id,cluster_id,credential_id,credential_version,actor_id,request_id,action,outcome,created_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,'credential.reveal.accepted','accepted',clock_timestamp())`,
		uuid.New(), installationID, clusterID, found.CredentialID, found.Version, actorID, requestID)
	if err != nil {
		return Credential{}, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return Credential{}, err
	}
	return found, nil
}
