package postgres

import (
	"context"
	"slices"

	"github.com/sirrobot01/synod/internal/app"
)

func (s *Store) appWriterStatus(ctx context.Context, item *app.App) error {
	writer, err := s.GetAppWriter(ctx, item.InstallationID, item.ID)
	if err != nil {
		return err
	}
	status := item.Status
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(last_pause_nanos) FILTER(WHERE node_id=$3),0)::double precision/1e9,COALESCE(MAX(max_pause_nanos),0)::double precision/1e9,LEAST(COALESCE(SUM(skipped_snapshots),0),9223372036854775807) FROM app_replicas WHERE installation_id=$1 AND app_id=$2`, item.InstallationID, item.ID, writer.NodeID).Scan(&status.LastPauseSeconds, &status.MaxPauseSeconds, &status.SkippedSnapshots); err != nil {
		return admissionError(err)
	}
	status.ActiveNodeID, status.WriterEpoch, status.Placement = writer.NodeID, writer.Epoch, "down"
	candidates, err := s.AppFailoverCandidates(ctx, item.InstallationID, item.ID)
	if err != nil {
		return err
	}
	rows, err := s.pool.Query(ctx, `SELECT c.node_id,COALESCE(r.writer_epoch,0),COALESCE(r.sequence,0),r.completed_at,
 CASE WHEN r.completed_at IS NOT NULL THEN GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-r.completed_at)) END,COALESCE(r.failure_code,'')
 FROM app_copies c LEFT JOIN app_replicas r USING(installation_id,app_id,node_id)
 WHERE c.installation_id=$1 AND c.app_id=$2 AND c.desired AND c.role='standby' ORDER BY c.copy_index`, item.InstallationID, item.ID)
	if err != nil {
		return admissionError(err)
	}
	for rows.Next() {
		var replica app.StandbyStatus
		if err := rows.Scan(&replica.NodeID, &replica.Epoch, &replica.Sequence, &replica.CompletedAt, &replica.LagSeconds, &replica.FailureCode); err != nil {
			rows.Close()
			return admissionError(err)
		}
		replica.Usable = slices.ContainsFunc(candidates, func(c AppFailoverCandidate) bool { return c.NodeID == replica.NodeID })
		if replica.Usable && replica.LagSeconds != nil {
			window := *replica.LagSeconds + float64(item.Spec.Replication.IntervalSeconds)
			if status.LossWindowSeconds == nil || window > *status.LossWindowSeconds {
				status.LossWindowSeconds = new(window)
			}
		}
		status.Standbys = append(status.Standbys, replica)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return admissionError(err)
	}
	if item.DesiredLifecycle != "running" || writer.Epoch == 0 {
		return nil
	}
	if status.State == "running" || status.State == "degraded" {
		status.Placement = "fallback"
		policy, err := s.ReadNodePlacementPolicy(ctx, item.InstallationID, writer.NodeID)
		if err != nil {
			return err
		}
		if len(item.Spec.DevicePreference.Preferred) == 0 || slices.Contains(item.Spec.DevicePreference.Preferred, writer.NodeID.String()) || slices.Contains(item.Spec.DevicePreference.Preferred, policy.Labels["synod.name"]) {
			status.Placement = "preferred"
		}
		return nil
	}
	status.State = "down"
	if writer.LastGrantAt != nil {
		var due bool
		if err := s.pool.QueryRow(ctx, `SELECT $1::timestamptz+make_interval(secs=>$2)<=clock_timestamp()`, writer.LastGrantAt, float64(item.Spec.Replication.FailoverAfterSeconds)).Scan(&due); err != nil {
			return admissionError(err)
		}
		if due && len(candidates) == 0 {
			status.FailureCode = "no_replica"
		}
	}
	return nil
}
