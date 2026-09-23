package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// DriftClass says how one object on a host relates to this installation.
type DriftClass string

const (
	// DriftUnowned was never ours; DriftForeign is another installation's or node's.
	// DriftMislabelled and DriftChanged were ours: the first lost its identity, the
	// second is no longer the object the node created.
	DriftUnowned     DriftClass = "unowned"
	DriftForeign     DriftClass = "foreign"
	DriftMislabelled DriftClass = "mislabelled"
	DriftChanged     DriftClass = "changed"
)

// DriftItem is one object a node did not expect to find as it is.
type DriftItem struct {
	Kind     string
	NativeID string
	Name     string
	State    string
	Class    DriftClass
	// An object that was ours keeps whatever identity survived, which separates
	// losing track of our own from finding somebody else's.
	ClusterID  uuid.UUID
	ResourceID uuid.UUID
	Detail     string
}

// DriftReport is one node's whole drift observation.
type DriftReport struct {
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	SessionID      uuid.UUID
	ObservedAt     time.Time
	Truncated      bool
	Items          []DriftItem
}

// maxDriftItems bounds one report. A host with more drift than this needs an
// operator on it.
const maxDriftItems = 256

// RecordNodeDrift replaces what one node last reported. A report is a snapshot, not a
// log: an object an operator has removed must stop being reported.
//
// Specification 33 forbids changing desired state from an observation, and this writes
// only into the two drift tables.
func (s *Store) RecordNodeDrift(ctx context.Context, report DriftReport) error {
	if report.InstallationID == uuid.Nil || report.NodeID == uuid.Nil || report.SessionID == uuid.Nil || len(report.Items) > maxDriftItems {
		return &AdmissionError{Code: InvalidRequest}
	}
	seen := make(map[string]bool, len(report.Items))
	for _, item := range report.Items {
		if err := item.validate(); err != nil {
			return err
		}
		key := item.Kind + "\x00" + item.NativeID
		if seen[key] {
			return &AdmissionError{Code: InvalidRequest}
		}
		seen[key] = true
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO node_drift (installation_id,node_id,session_id,observed_at,truncated)
 VALUES ($1,$2,$3,clock_timestamp(),$4)
 ON CONFLICT (installation_id,node_id) DO UPDATE SET session_id=EXCLUDED.session_id,
 observed_at=EXCLUDED.observed_at,truncated=EXCLUDED.truncated`,
		report.InstallationID, report.NodeID, report.SessionID, report.Truncated); err != nil {
		return admissionError(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM node_drift_items WHERE installation_id=$1 AND node_id=$2`,
		report.InstallationID, report.NodeID); err != nil {
		return admissionError(err)
	}
	for _, item := range report.Items {
		if _, err := tx.Exec(ctx, `INSERT INTO node_drift_items
 (installation_id,node_id,kind,native_id,name,state,class,cluster_id,resource_id,detail)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			report.InstallationID, report.NodeID, item.Kind, item.NativeID, item.Name, item.State,
			string(item.Class), nullUUID(item.ClusterID), nullUUID(item.ResourceID), item.Detail); err != nil {
			return admissionError(err)
		}
	}
	return s.commitMetadata(ctx, tx)
}

// ReadNodeDrift answers what one node last reported.
func (s *Store) ReadNodeDrift(ctx context.Context, installationID, nodeID uuid.UUID) (DriftReport, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return DriftReport{}, &AdmissionError{Code: InvalidRequest}
	}
	report := DriftReport{InstallationID: installationID, NodeID: nodeID}
	err := s.pool.QueryRow(ctx, `SELECT session_id,observed_at,truncated FROM node_drift
 WHERE installation_id=$1 AND node_id=$2`, installationID, nodeID).
		Scan(&report.SessionID, &report.ObservedAt, &report.Truncated)
	if err != nil {
		return DriftReport{}, admissionError(err)
	}
	rows, err := s.pool.Query(ctx, `SELECT kind,native_id,name,state,class,cluster_id,resource_id,detail
 FROM node_drift_items WHERE installation_id=$1 AND node_id=$2 ORDER BY kind,native_id`, installationID, nodeID)
	if err != nil {
		return DriftReport{}, admissionError(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item DriftItem
		var class string
		var clusterID, resourceID *uuid.UUID
		if err := rows.Scan(&item.Kind, &item.NativeID, &item.Name, &item.State, &class,
			&clusterID, &resourceID, &item.Detail); err != nil {
			return DriftReport{}, admissionError(err)
		}
		item.Class = DriftClass(class)
		if clusterID != nil {
			item.ClusterID = *clusterID
		}
		if resourceID != nil {
			item.ResourceID = *resourceID
		}
		report.Items = append(report.Items, item)
	}
	if rows.Err() != nil {
		return DriftReport{}, admissionError(rows.Err())
	}
	return report, nil
}

func nullUUID(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

func (d DriftItem) validate() error {
	switch d.Kind {
	case "container", "volume", "network":
	default:
		return &AdmissionError{Code: InvalidRequest}
	}
	switch d.Class {
	case DriftUnowned, DriftForeign, DriftMislabelled, DriftChanged:
	default:
		return &AdmissionError{Code: InvalidRequest}
	}
	if !boundedIdentityText(d.NativeID, 256) || len(d.Name) > 256 || len(d.State) > 64 || len(d.Detail) > 64 {
		return &AdmissionError{Code: InvalidRequest}
	}
	// An object that was never ours carries no Synod identity.
	if d.Class == DriftUnowned && (d.ClusterID != uuid.Nil || d.ResourceID != uuid.Nil) {
		return &AdmissionError{Code: InvalidRequest}
	}
	return nil
}
