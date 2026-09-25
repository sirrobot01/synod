package postgres

import (
	"context"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// PatroniStartHistory is what the starts of one Patroni member did.
type PatroniStartHistory struct {
	// Executed reports that some start may have reached the runtime.
	Executed bool
	// LatestRunning reports that the latest executed start reported a running member.
	LatestRunning bool
	// EverRunning reports that some start reported a running member, so its storage completed once.
	EverRunning bool
}

// ReadPatroniStartHistory reads the starts dispatched for one member of a cluster.
func (s *Store) ReadPatroniStartHistory(ctx context.Context, installationID, clusterID, resourceID uuid.UUID) (PatroniStartHistory, error) {
	if installationID == uuid.Nil || clusterID == uuid.Nil || resourceID == uuid.Nil {
		return PatroniStartHistory{}, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return PatroniStartHistory{}, admissionError(err)
	}
	defer tx.Rollback(ctx)
	return patroniStartHistory(ctx, tx, installationID, clusterID, resourceID)
}

// patroniStartHistory classifies each dispatched start in dispatch order. A dispatch without
// a receipt may have run. Only a failure with no effects from an agent guard proves that
// the runtime never started.
func patroniStartHistory(ctx context.Context, tx pgx.Tx, installationID, clusterID, resourceID uuid.UUID) (PatroniStartHistory, error) {
	rows, err := tx.Query(ctx, `SELECT d.command_bytes,r.result_bytes FROM command_dispatches d
 LEFT JOIN command_results r USING (command_id,execution_epoch,recovery_generation)
 WHERE d.installation_id=$1 AND d.cluster_id=$2 ORDER BY d.created_at,d.command_id`, installationID, clusterID)
	if err != nil {
		return PatroniStartHistory{}, admissionError(err)
	}
	defer rows.Close()
	var history PatroniStartHistory
	for rows.Next() {
		var commandBytes, resultBytes []byte
		if err := rows.Scan(&commandBytes, &resultBytes); err != nil {
			return PatroniStartHistory{}, admissionError(err)
		}
		var command agentv1.Command
		if proto.Unmarshal(commandBytes, &command) != nil {
			return PatroniStartHistory{}, &AdmissionError{Code: DependencyUnavailable}
		}
		if command.GetInput().GetStartPatroniMember() == nil || command.Input.ResourceId != resourceID.String() {
			continue
		}
		var result *agentv1.TerminalResult
		if resultBytes != nil {
			result = new(agentv1.TerminalResult)
			if proto.Unmarshal(resultBytes, result) != nil {
				return PatroniStartHistory{}, &AdmissionError{Code: DependencyUnavailable}
			}
		}
		if result != nil && result.State == "failed" && len(result.Effects) == 0 && (result.FailureCode == "capacity_unavailable" || result.FailureCode == "secret_unavailable") {
			continue
		}
		running := result != nil && result.State == "succeeded" && slices.ContainsFunc(result.Effects, func(effect *agentv1.Effect) bool { return effect.State == "running" })
		history.Executed, history.LatestRunning = true, running
		history.EverRunning = history.EverRunning || running
	}
	if err := rows.Err(); err != nil {
		return PatroniStartHistory{}, admissionError(err)
	}
	return history, nil
}

// Resume reports that the member can start again with its own storage.
func (h PatroniStartHistory) Resume() bool { return !h.Executed || h.LatestRunning }
