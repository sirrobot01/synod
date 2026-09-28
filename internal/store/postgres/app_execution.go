package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// recordAppCopies keeps copy identity in the transaction that installs its commands.
func recordAppCopies(ctx context.Context, tx pgx.Tx, claim operation.Claim, request placement.Request, decision placement.Decision, plan *agentv1.ExecutionPlan) error {
	var name string
	var revision int64
	var copies int
	var mode string
	var tier, standbys int
	var worker bool
	if err := tx.QueryRow(ctx, `SELECT name,desired_revision,(spec->>'copies')::integer,COALESCE(spec->>'copies_mode',''),tier,COALESCE((spec->'replication'->>'standbys')::integer,0),tier=3 AND COALESCE((spec->'router'->>'port')::integer,0)=0 FROM apps WHERE installation_id=$1 AND id=$2 FOR UPDATE`, claim.InstallationID, claim.AppID).Scan(&name, &revision, &copies, &mode, &tier, &standbys, &worker); err != nil {
		return admissionError(err)
	}
	if revision != request.DesiredRevision {
		return operation.ErrCommandConflict
	}
	count := 0
	indexes := map[uint32]bool{}
	resources := map[string]bool{}
	for _, step := range plan.Steps {
		command := step.Command
		input := command.Input.GetEnsureContainer()
		if input == nil || input.Generic == nil {
			continue
		}
		if input.Generic.AppName != name || (!worker && (len(input.Ports) != 1 || input.Ports[0].HostPort == 0)) || (worker && len(input.Ports) != 0) {
			return operation.ErrCommandConflict
		}
		if indexes[input.Generic.CopyIndex] || resources[command.Input.ResourceId] {
			return operation.ErrCommandConflict
		}
		indexes[input.Generic.CopyIndex] = true
		resources[command.Input.ResourceId] = true
		count++
		index := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool {
			return a.ResourceID.String() == command.Input.ResourceId && a.NodeID.String() == command.Epoch.NodeId
		})
		if index < 0 {
			return operation.ErrCommandConflict
		}
		assignment := decision.Assignments[index]
		var hostPort uint32
		if worker {
			if len(assignment.Ports) != 0 {
				return operation.ErrCommandConflict
			}
		} else {
			if !slices.ContainsFunc(assignment.Ports, func(p placement.Port) bool {
				return uint32(p.Number) == input.Ports[0].HostPort && p.Transport == "tcp" && p.Name == "backend" && p.Address == input.Ports[0].HostAddress
			}) {
				return operation.ErrCommandConflict
			}
			hostPort = input.Ports[0].HostPort
		}
		tag, err := tx.Exec(ctx, `INSERT INTO app_copies (id,installation_id,app_id,node_id,copy_index,host_port,desired_revision,observed_state,created_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',clock_timestamp(),clock_timestamp())
 ON CONFLICT (id) DO UPDATE SET updated_at=app_copies.updated_at
 WHERE app_copies.installation_id=EXCLUDED.installation_id AND app_copies.app_id=EXCLUDED.app_id
 AND app_copies.node_id=EXCLUDED.node_id AND app_copies.copy_index=EXCLUDED.copy_index
 AND app_copies.host_port=EXCLUDED.host_port AND app_copies.desired_revision=EXCLUDED.desired_revision AND app_copies.desired`, assignment.ResourceID, claim.InstallationID, claim.AppID, assignment.NodeID, input.Generic.CopyIndex, hostPort, revision)
		if err != nil {
			return admissionError(err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("app copy identity differs: %w", operation.ErrCommandConflict)
		}
	}
	// Every tier 1 copy runs its engine, so the primary and each standby have a container.
	if tier == 1 {
		copies = 1 + standbys
	}
	if count == 0 || (mode != "all_capable" && count != copies) {
		return operation.ErrCommandConflict
	}
	for index := range count {
		if !indexes[uint32(index)] {
			return operation.ErrCommandConflict
		}
	}
	if tier == 1 {
		if standbys < 0 || standbys > 3 {
			return operation.ErrCommandConflict
		}
		primaries := 0
		for _, resource := range request.Resources {
			if resource.Role != "active" && resource.Role != "standby" {
				continue
			}
			i := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool { return a.ResourceID == resource.ID })
			if i < 0 || !resources[resource.ID.String()] {
				return operation.ErrCommandConflict
			}
			if _, err := tx.Exec(ctx, `UPDATE app_copies SET role=$2 WHERE id=$1`, resource.ID, resource.Role); err != nil {
				return admissionError(err)
			}
			if resource.Role != "active" {
				continue
			}
			primaries++
			tag, err := tx.Exec(ctx, `UPDATE apps SET writer_node_id=$3,writer_epoch=1 WHERE installation_id=$1 AND id=$2 AND writer_epoch=0`, claim.InstallationID, claim.AppID, decision.Assignments[i].NodeID)
			if err != nil {
				return admissionError(err)
			}
			if tag.RowsAffected() != 1 {
				return operation.ErrCommandConflict
			}
		}
		if primaries != 1 {
			return operation.ErrCommandConflict
		}
	}
	if tier == 2 {
		if count != 1 || standbys < 1 || standbys > 3 {
			return operation.ErrCommandConflict
		}
		standbyCount := 0
		for _, resource := range request.Resources {
			if resource.Role != "active" && resource.Role != "standby" {
				continue
			}
			i := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool { return a.ResourceID == resource.ID })
			if i < 0 {
				return operation.ErrCommandConflict
			}
			a := decision.Assignments[i]
			if resource.Role == "active" {
				if !resources[resource.ID.String()] {
					return operation.ErrCommandConflict
				}
				if _, err := tx.Exec(ctx, `UPDATE app_copies SET role='active' WHERE id=$1`, resource.ID); err != nil {
					return admissionError(err)
				}
				tag, err := tx.Exec(ctx, `UPDATE apps SET writer_node_id=$3,writer_epoch=1 WHERE installation_id=$1 AND id=$2 AND writer_epoch=0`, claim.InstallationID, claim.AppID, a.NodeID)
				if err != nil {
					return admissionError(err)
				}
				if tag.RowsAffected() != 1 {
					return operation.ErrCommandConflict
				}
			} else {
				if len(a.Ports) != 1 || a.Ports[0].Name != "backend" {
					return operation.ErrCommandConflict
				}
				standbyCount++
				if _, err := tx.Exec(ctx, `INSERT INTO app_copies(id,installation_id,app_id,node_id,copy_index,host_port,desired_revision,role,observed_state,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,'standby','pending',clock_timestamp(),clock_timestamp())`, a.ResourceID, claim.InstallationID, claim.AppID, a.NodeID, standbyCount, a.Ports[0].Number, revision); err != nil {
					return admissionError(err)
				}
			}
		}
		if standbyCount != standbys {
			return operation.ErrCommandConflict
		}
	}
	return nil
}

// RecordAppPlanningFailure reports a blocked plan without terminating retries.
func (s *Store) RecordAppPlanningFailure(ctx context.Context, claim operation.Claim, code string) error {
	if code == "" || len(code) > 128 || strings.ContainsFunc(code, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	_, _, plan, err := lockClaim(ctx, tx, claim)
	if err != nil {
		return err
	}
	if plan != nil {
		return operation.ErrCommandConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE operations SET failure_code=$1,updated_at=clock_timestamp() WHERE installation_id=$2 AND id=$3 AND kind LIKE 'app.%'`, code, claim.InstallationID, claim.OperationID); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}
