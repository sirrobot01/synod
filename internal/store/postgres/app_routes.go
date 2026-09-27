package postgres

import (
	"context"
	"errors"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/router"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

type AppRouteTarget struct {
	Address string
	Table   *agentv1.RouteTable
}

// AppRouteTargets derives full route snapshots from reservations and fresh copy evidence.
func (s *Store) AppRouteTargets(ctx context.Context, installation uuid.UUID) (map[uuid.UUID]AppRouteTarget, error) {
	snapshot, err := s.ReadPlacementSnapshot(ctx, installation)
	if err != nil {
		return nil, err
	}
	nodes := map[uuid.UUID]placement.Node{}
	targets := map[uuid.UUID]AppRouteTarget{}
	for _, node := range snapshot.Nodes {
		if node.SessionID == uuid.Nil {
			continue
		}
		presence, err := s.GetAgentPresence(ctx, installation, node.ID)
		if err != nil {
			return nil, err
		}
		if presence.SessionID != node.SessionID {
			continue
		}
		var address string
		if node.Policy != nil {
			address = node.Policy.ClientAddress
		}
		// Ineligible nodes receive an empty snapshot to remove their old listeners.
		if slices.Contains(presence.Hello.Capabilities, controlstream.RouterCapability) {
			targets[node.ID] = AppRouteTarget{Address: address, Table: &agentv1.RouteTable{SessionId: node.SessionID.String()}}
		}
		if !node.Revoked && !node.RecoveryRequired && node.Policy != nil && !node.Policy.Maintenance && address != "" && node.LastSeenAt.Add(placement.PresenceAge).After(snapshot.ObservedAt) {
			nodes[node.ID] = node
		}
	}

	apps, err := s.ListApps(ctx, installation)
	if err != nil {
		return nil, err
	}
	for _, item := range apps {
		if item.DesiredLifecycle != "running" {
			continue
		}
		copies, err := s.GetAppCopies(ctx, installation, item.ID)
		if err != nil {
			return nil, err
		}
		var backends []string
		for _, copy := range copies {
			node, ok := nodes[copy.NodeID]
			if ok && copy.ObservedState == "running" && copy.Health == "healthy" {
				backends = append(backends, net.JoinHostPort(node.Policy.ClientAddress, strconv.Itoa(int(copy.HostPort))))
			}
		}
		slices.Sort(backends)
		for _, allocation := range snapshot.Allocations {
			if _, eligible := nodes[allocation.NodeID]; !eligible {
				continue
			}
			if allocation.ClusterID != item.ID || allocation.Released {
				continue
			}
			target, ok := targets[allocation.NodeID]
			if !ok {
				continue
			}
			for _, port := range allocation.Ports {
				if port.Name != "ingress" || port.Number != item.Spec.Router.Port {
					continue
				}
				route := &agentv1.AppRoute{AppId: item.ID.String(), Protocol: item.Spec.Router.Protocol, IngressPort: uint32(port.Number), Backends: backends}
				if health := item.Spec.Service.HealthCheck; health != nil && health.Type == "http" {
					route.HealthPath = health.Path
				}
				target.Table.Routes = append(target.Table.Routes, route)
			}
		}
	}
	for _, target := range targets {
		slices.SortFunc(target.Table.Routes, func(a, b *agentv1.AppRoute) int { return strings.Compare(a.AppId, b.AppId) })
		if err := router.Validate(target.Table.Routes); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// PrepareAppRoutes preserves the revision until the desired snapshot or session changes.
func (s *Store) PrepareAppRoutes(ctx context.Context, installation, node uuid.UUID, desired *agentv1.RouteTable) (*agentv1.RouteTable, bool, error) {
	if desired == nil || router.Validate(desired.Routes) != nil {
		return nil, false, &AdmissionError{Code: InvalidRequest}
	}
	session, err := uuid.Parse(desired.SessionId)
	if err != nil || session == uuid.Nil {
		return nil, false, &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, admissionError(err)
	}
	defer tx.Rollback(ctx)
	// Lock the referenced apps before presence. A snapshot read before stop or delete
	// must not restore an old listener after withdrawal has been proved.
	ids := make([]string, 0, len(desired.Routes))
	for _, route := range desired.Routes {
		ids = append(ids, route.AppId)
	}
	slices.Sort(ids)
	for _, id := range ids {
		var lifecycle string
		err := tx.QueryRow(ctx, `SELECT desired_lifecycle FROM apps WHERE installation_id=$1 AND id=$2 FOR SHARE`, installation, id).Scan(&lifecycle)
		if err != nil {
			return nil, false, admissionError(err)
		}
		if lifecycle != "running" {
			return nil, false, operation.ErrCommandConflict
		}
	}
	var current uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&current); err != nil {
		return nil, false, admissionError(err)
	}
	if current != session {
		return nil, false, ErrStaleSession
	}
	var stored []byte
	var acknowledged bool
	err = tx.QueryRow(ctx, `SELECT table_bytes,acknowledged_at IS NOT NULL AND failure_code='' FROM agent_routes WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&stored, &acknowledged)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, admissionError(err)
	}
	table := proto.Clone(desired).(*agentv1.RouteTable)
	table.Revision = 1
	if len(stored) > 0 {
		var previous agentv1.RouteTable
		if proto.Unmarshal(stored, &previous) != nil {
			return nil, false, &AdmissionError{Code: DependencyUnavailable}
		}
		table.Revision = previous.Revision
		if proto.Equal(table, &previous) {
			return table, acknowledged, nil
		}
		if previous.Revision >= math.MaxInt64 {
			return nil, false, operation.ErrCommandConflict
		}
		table.Revision++
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(table)
	if err != nil {
		return nil, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO agent_routes(installation_id,node_id,session_id,revision,table_bytes) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT(installation_id,node_id) DO UPDATE SET session_id=EXCLUDED.session_id,revision=EXCLUDED.revision,table_bytes=EXCLUDED.table_bytes,acknowledged_at=NULL,failure_code=''`, installation, node, session, table.Revision, encoded); err != nil {
		return nil, false, admissionError(err)
	}
	if err := s.commitMetadata(ctx, tx); err != nil {
		return nil, false, err
	}
	return table, false, nil
}

// RecordAppRouteAck accepts only the current authenticated session and published revision.
func (s *Store) RecordAppRouteAck(ctx context.Context, installation, node, session uuid.UUID, ack *agentv1.RouteAck) error {
	if ack == nil || ack.SessionId != session.String() || ack.Revision == 0 || ack.Revision > math.MaxInt64 || len(ack.FailureCode) > 128 {
		return &AdmissionError{Code: InvalidRequest}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return admissionError(err)
	}
	defer tx.Rollback(ctx)
	var current uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT session_id FROM agent_presence WHERE installation_id=$1 AND node_id=$2 FOR UPDATE`, installation, node).Scan(&current); err != nil {
		return admissionError(err)
	}
	if current != session {
		return ErrStaleSession
	}
	// An acknowledgement for a replaced snapshot is harmless. It cannot mark the new one ready.
	if _, err := tx.Exec(ctx, `UPDATE agent_routes SET acknowledged_at=clock_timestamp(),failure_code=$1
 WHERE installation_id=$2 AND node_id=$3 AND session_id=$4 AND revision=$5`, ack.FailureCode, installation, node, session, ack.Revision); err != nil {
		return admissionError(err)
	}
	return s.commitMetadata(ctx, tx)
}

// GetAppStatus combines command outcome, current copy health, and acknowledged routes.
func (s *Store) GetAppStatus(ctx context.Context, installation, id uuid.UUID) (app.App, error) {
	item, err := s.GetApp(ctx, installation, id)
	if err != nil {
		return app.App{}, err
	}
	status := &app.Status{State: "pending", Copies: []app.Copy{}, Endpoints: []app.Endpoint{}}
	item.Status = status
	err = s.pool.QueryRow(ctx, `SELECT id,status,COALESCE(failure_code,'') FROM operations WHERE installation_id=$1 AND app_id=$2 ORDER BY desired_revision DESC LIMIT 1`, installation, id).Scan(&status.OperationID, &status.OperationState, &status.FailureCode)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return app.App{}, admissionError(err)
	}
	copies, err := s.GetAppCopies(ctx, installation, id)
	if err != nil {
		return app.App{}, err
	}
	if copies != nil {
		status.Copies = copies
	}
	if item.DesiredLifecycle != "running" {
		if status.OperationState == "failed" || status.OperationState == "cancelled" {
			status.State = "failed"
		} else if status.OperationState == "succeeded" && item.ObservedRevision == item.DesiredRevision {
			status.State = item.ObservedLifecycle
		} else if item.DesiredLifecycle == "deleted" {
			status.State = "deleting"
		} else {
			status.State = "stopping"
		}
		return item, nil
	}
	targets, err := s.AppRouteTargets(ctx, installation)
	if err != nil {
		return app.App{}, err
	}
	healthy := 0
	failed := 0
	for _, copy := range copies {
		if copy.ObservedState == "failed" || copy.Health == "unhealthy" {
			failed++
		}
		if copy.ObservedState == "running" && copy.Health == "healthy" {
			healthy++
		}
	}
	ready := false
	routedHealthy := 0
	for node, target := range targets {
		index := slices.IndexFunc(target.Table.Routes, func(r *agentv1.AppRoute) bool { return r.AppId == id.String() })
		if index < 0 {
			continue
		}
		routedHealthy = max(routedHealthy, len(target.Table.Routes[index].Backends))
		endpoint := app.Endpoint{NodeID: node, Address: net.JoinHostPort(target.Address, strconv.Itoa(int(item.Spec.Router.Port)))}
		var encoded []byte
		var acknowledged bool
		err := s.pool.QueryRow(ctx, `SELECT table_bytes,acknowledged_at IS NOT NULL,failure_code FROM agent_routes WHERE installation_id=$1 AND node_id=$2`, installation, node).Scan(&encoded, &acknowledged, &endpoint.FailureCode)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return app.App{}, admissionError(err)
		}
		var table agentv1.RouteTable
		if err == nil && proto.Unmarshal(encoded, &table) == nil && acknowledged && endpoint.FailureCode == "" {
			target.Table.Revision = table.Revision
			endpoint.Ready = proto.Equal(target.Table, &table) && len(target.Table.Routes[index].Backends) > 0
		}
		ready = ready || endpoint.Ready
		status.Endpoints = append(status.Endpoints, endpoint)
	}
	slices.SortFunc(status.Endpoints, func(a, b app.Endpoint) int { return strings.Compare(a.NodeID.String(), b.NodeID.String()) })
	switch {
	case status.OperationState == "failed" || status.OperationState == "cancelled":
		status.State = "failed"
	case status.OperationState == "succeeded" && healthy == len(copies) && routedHealthy == len(copies) && healthy > 0 && ready:
		status.State = "running"
	case status.OperationState == "succeeded" && healthy == 0 && failed > 0:
		status.State = "failed"
		status.FailureCode = "app_copies_unhealthy"
	case healthy > 0 && ready:
		status.State = "degraded"
	}
	if status.State == "running" {
		item.ObservedLifecycle = "running"
	} else if item.ObservedLifecycle == "running" {
		item.ObservedLifecycle = "unknown"
	}
	return item, nil
}
