package postgres

import (
	"context"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// recordAppObservations runs with the authenticated presence row locked.
func recordAppObservations(ctx context.Context, tx pgx.Tx, installation, node, session uuid.UUID, observations []*agentv1.Observation) error {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return admissionError(err)
	}
	for _, observed := range observations {
		at := observed.ObservedAt.AsTime()
		if at.After(now.Add(5*time.Second)) || !at.Add(20*time.Second).After(now) {
			continue
		}
		if at.After(now) {
			at = now
		}
		health := observed.Health
		if !slices.Contains([]string{"unknown", "starting", "healthy", "unhealthy"}, health) {
			health = "unknown"
		}
		state := "pending"
		switch observed.Lifecycle {
		case "running":
			state = "running"
		case "exited", "dead":
			state = "failed"
		case "paused", "created":
			state = "stopped"
		}
		var ports []int32
		for _, port := range observed.Ports {
			if port != nil && port.Transport == "tcp" && port.HostAddress == "0.0.0.0" && port.HostPort > 0 && port.HostPort <= 65535 {
				ports = append(ports, int32(port.HostPort))
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE app_copies SET observed_state=CASE WHEN $1='failed' AND EXISTS(SELECT 1 FROM apps a WHERE a.installation_id=app_copies.installation_id AND a.id=app_copies.app_id AND a.desired_lifecycle='stopped') THEN 'stopped' ELSE $1 END,health=CASE WHEN host_port=ANY($2::integer[]) OR (host_port=0 AND $10) THEN $3 ELSE 'unknown' END,
 observed_at=$4,observed_session_id=$5,updated_at=clock_timestamp()
 WHERE installation_id=$6 AND node_id=$7 AND id=$8 AND desired
 AND EXISTS(SELECT 1 FROM apps a WHERE a.installation_id=app_copies.installation_id AND a.id=app_copies.app_id AND a.updated_at<=$4 AND (app_copies.role<>'standby' OR a.tier=1))
 AND ($9::bigint=0 OR desired_revision=$9) AND (observed_at IS NULL OR observed_session_id IS DISTINCT FROM $5 OR observed_at<$4)`, state, ports, health, at, session, installation, node, observed.ResourceId, observed.ObservedRevision, len(observed.Ports) == 0); err != nil {
			return admissionError(err)
		}
	}
	return nil
}
