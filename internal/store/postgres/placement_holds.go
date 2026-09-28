package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
)

// Hold is one reservation that is still held, and the operation holding it.
//
// Specification 33 requires exposing reservations held by blocked operations. A held
// reservation is the reason a later placement has nowhere to go, so an operator who
// cannot see the holder sees a refusal with no cause.
type Hold struct {
	ReservationID  uuid.UUID
	InstallationID uuid.UUID
	AppID          uuid.UUID
	NodeID         uuid.UUID
	ResourceID     uuid.UUID
	// OperationID created the reservation.
	OperationID uuid.UUID
	Kind        string
	Status      operation.Status
	// Bound names every operation bound to this reservation that has not stopped
	// without success. Release consults exactly this set, so an operator reading a
	// refused release sees the same reason the Store used.
	Bound []Holder
	// Blocked reports a holder that cannot free this reservation by itself. A
	// pending or running operation is working; anything else is holding capacity
	// that will not come back without a decision.
	Blocked bool
	Budget  placement.Vector
	Ports   []placement.Port
}

// ReadPlacementHolds lists every reservation this installation still holds.
//
// It reads only. Nothing here releases a reservation or moves an allocation:
// specification 33 states that a failed reservation never justifies relocating
// existing data, so this answers what is held and leaves the decision to an operator.
func (s *Store) ReadPlacementHolds(ctx context.Context, installationID uuid.UUID) ([]Hold, error) {
	if installationID == uuid.Nil {
		return nil, &AdmissionError{Code: InvalidRequest}
	}
	bound, err := s.readHolders(ctx, installationID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT a.id, a.app_id, a.node_id, a.resource_id,
 a.allocation_bytes, a.created_operation_id, o.kind, o.status
 FROM resource_allocations a
 JOIN operations o ON o.installation_id=a.installation_id AND o.id=a.created_operation_id
 WHERE a.installation_id=$1 AND a.released_at IS NULL
 ORDER BY a.id LIMIT $2`, installationID, placement.MaxAllocations+1)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	var holds []Hold
	for rows.Next() {
		var hold Hold
		var encoded []byte
		var status string
		if err := rows.Scan(&hold.ReservationID, &hold.AppID, &hold.NodeID, &hold.ResourceID,
			&encoded, &hold.OperationID, &hold.Kind, &status); err != nil {
			return nil, admissionError(err)
		}
		var allocation placement.Allocation
		if json.Unmarshal(encoded, &allocation) != nil {
			return nil, &AdmissionError{Code: DependencyUnavailable}
		}
		hold.InstallationID = installationID
		hold.Budget, hold.Ports = allocation.Budget, allocation.Ports
		hold.Status = operation.Status(status)
		hold.Blocked = blockedHolder(hold.Status)
		hold.Bound = bound[hold.ReservationID]
		holds = append(holds, hold)
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	if len(holds) > placement.MaxAllocations {
		return nil, ErrSnapshotTooLarge
	}
	return holds, nil
}

// Holder is one operation bound to a reservation.
type Holder struct {
	OperationID uuid.UUID
	Kind        string
	Status      operation.Status
}

// readHolders groups the operations bound to each reservation that still stand in
// the way of a release. An operation that ended failed or cancelled has stopped
// without success and does not block, which is the rule release itself applies.
func (s *Store) readHolders(ctx context.Context, installationID uuid.UUID) (map[uuid.UUID][]Holder, error) {
	rows, err := s.pool.Query(ctx, `SELECT b.allocation_id, o.id, o.kind, o.status
 FROM operation_allocation_bindings b
 JOIN operations o ON o.installation_id=b.installation_id AND o.id=b.operation_id
 WHERE b.installation_id=$1 AND o.status NOT IN ('failed','cancelled')
 ORDER BY b.allocation_id, o.id LIMIT $2`, installationID, placement.MaxAllocations*8+1)
	if err != nil {
		return nil, admissionError(err)
	}
	defer rows.Close()
	bound := make(map[uuid.UUID][]Holder)
	total := 0
	for rows.Next() {
		var allocationID uuid.UUID
		var holder Holder
		var status string
		if err := rows.Scan(&allocationID, &holder.OperationID, &holder.Kind, &status); err != nil {
			return nil, admissionError(err)
		}
		holder.Status = operation.Status(status)
		bound[allocationID] = append(bound[allocationID], holder)
		total++
	}
	if rows.Err() != nil {
		return nil, admissionError(rows.Err())
	}
	if total > placement.MaxAllocations*8 {
		return nil, ErrSnapshotTooLarge
	}
	return bound, nil
}

// blockedHolder reports an operation that will not free its reservation on its own.
//
// Pending and running work is progressing and will release or keep its reservation
// as it finishes. Everything else is either waiting on a decision, stopping, or
// already stopped while still holding capacity.
func blockedHolder(status operation.Status) bool {
	switch status {
	case operation.Pending, operation.Running:
		return false
	default:
		return true
	}
}
