package postgres

import (
	"cmp"
	"context"
	"errors"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

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
		if item.DesiredLifecycle != "running" || item.Spec.Router.Port == 0 {
			continue
		}
		copies, err := s.GetAppCopies(ctx, installation, item.ID)
		if err != nil {
			return nil, err
		}
		var writer AppWriter
		if item.Tier == app.TierSingleWriter || item.Tier == app.TierDatabase {
			writer, err = s.GetAppWriter(ctx, installation, item.ID)
			if err != nil {
				return nil, err
			}
		}
		// A tier 1 reader route also reaches standbys whose engine streams from the primary.
		streaming := map[uuid.UUID]bool{}
		if item.Tier == app.TierDatabase && item.Spec.Router.ReaderPort != 0 {
			rows, err := s.pool.Query(ctx, `SELECT r.node_id FROM app_replicas r JOIN agent_presence p USING(installation_id,node_id) WHERE r.installation_id=$1 AND r.app_id=$2 AND r.role='standby' AND r.state='streaming' AND r.failure_code='' AND r.sequence>0 AND r.session_id=p.session_id AND r.writer_epoch=$3 AND r.observed_at>clock_timestamp()-interval '20 seconds'`, installation, item.ID, int64(writer.Epoch))
			if err != nil {
				return nil, admissionError(err)
			}
			nodes, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
			if err != nil {
				return nil, admissionError(err)
			}
			for _, node := range nodes {
				streaming[node] = true
			}
		}
		var backends, readers []string
		for _, copy := range copies {
			node, ok := nodes[copy.NodeID]
			if !ok || copy.ObservedState != "running" || copy.Health != "healthy" {
				continue
			}
			address := net.JoinHostPort(node.Policy.ClientAddress, strconv.Itoa(int(copy.HostPort)))
			leased := copy.NodeID == writer.NodeID && writer.LastGrantAt != nil && writer.LastGrantAt.Add(app.LeaseSeconds*time.Second).After(snapshot.ObservedAt)
			if (copy.Role == "active" && leased) || (copy.Role == "standby" && streaming[copy.NodeID]) {
				readers = append(readers, address)
			}
			if (item.Tier == app.TierSingleWriter || item.Tier == app.TierDatabase) && !leased {
				continue
			}
			if copy.Role != "standby" {
				backends = append(backends, address)
			}
		}
		slices.Sort(backends)
		slices.Sort(readers)
		for _, allocation := range snapshot.Allocations {
			if _, eligible := nodes[allocation.NodeID]; !eligible {
				continue
			}
			if allocation.AppID != item.ID || allocation.Released {
				continue
			}
			target, ok := targets[allocation.NodeID]
			if !ok {
				continue
			}
			for _, port := range allocation.Ports {
				routed := backends
				if item.Tier == app.TierDatabase && port.Name == "reader" && port.Number == item.Spec.Router.ReaderPort {
					routed = readers
				} else if port.Name != "ingress" || port.Number != item.Spec.Router.Port {
					continue
				}
				route := &agentv1.AppRoute{AppId: item.ID.String(), Protocol: item.Spec.Router.Protocol, IngressPort: uint32(port.Number), Backends: routed}
				if health := item.Spec.Service.HealthCheck; health != nil && health.Type == "http" {
					route.HealthPath = health.Path
				}
				target.Table.Routes = append(target.Table.Routes, route)
			}
		}
	}
	for _, target := range targets {
		slices.SortFunc(target.Table.Routes, func(a, b *agentv1.AppRoute) int {
			return cmp.Or(strings.Compare(a.AppId, b.AppId), cmp.Compare(a.IngressPort, b.IngressPort))
		})
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
	for _, id := range slices.Compact(ids) {
		var lifecycle string
		var tier int
		var writerPort, readerPort uint16
		err := tx.QueryRow(ctx, `SELECT desired_lifecycle,tier,(spec->'router'->>'port')::integer,COALESCE((spec->'router'->>'reader_port')::integer,0) FROM apps WHERE installation_id=$1 AND id=$2 FOR SHARE`, installation, id).Scan(&lifecycle, &tier, &writerPort, &readerPort)
		if err != nil {
			return nil, false, admissionError(err)
		}
		if lifecycle != "running" {
			return nil, false, operation.ErrCommandConflict
		}
		if tier != 1 && tier != 2 {
			continue
		}
		for _, route := range desired.Routes {
			if route.AppId != id {
				continue
			}
			reader := tier == 1 && readerPort != 0 && route.IngressPort == uint32(readerPort)
			if !reader && (route.IngressPort != uint32(writerPort) || len(route.Backends) > 1) {
				return nil, false, operation.ErrCommandConflict
			}
			rows, err := tx.Query(ctx, `SELECT (convert_from(policy.policy_bytes,'UTF8')::jsonb)->>'client_address',c.host_port
 FROM apps a JOIN app_copies c ON c.installation_id=a.installation_id AND c.app_id=a.id
 JOIN node_placement_policies policy ON policy.installation_id=c.installation_id AND policy.node_id=c.node_id
 JOIN agent_presence p ON p.installation_id=c.installation_id AND p.node_id=c.node_id
 LEFT JOIN app_replicas r ON r.installation_id=c.installation_id AND r.app_id=c.app_id AND r.node_id=c.node_id
 WHERE a.installation_id=$1 AND a.id=$2 AND c.desired AND
 ((c.role='active' AND c.node_id=a.writer_node_id AND a.writer_last_grant_at>clock_timestamp()-interval '30 seconds') OR
 ($3 AND c.role='standby' AND r.role='standby' AND r.state='streaming' AND r.failure_code='' AND r.sequence>0
 AND r.writer_epoch=a.writer_epoch AND r.session_id=p.session_id AND r.observed_at>clock_timestamp()-interval '20 seconds'))`, installation, id, reader)
			if err != nil {
				return nil, false, admissionError(err)
			}
			var allowed []string
			for rows.Next() {
				var address string
				var port uint16
				if err := rows.Scan(&address, &port); err != nil {
					rows.Close()
					return nil, false, admissionError(err)
				}
				allowed = append(allowed, net.JoinHostPort(address, strconv.Itoa(int(port))))
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, false, admissionError(err)
			}
			for _, backend := range route.Backends {
				if !slices.Contains(allowed, backend) {
					return nil, false, operation.ErrCommandConflict
				}
			}
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
		if item.Tier == app.TierSingleWriter || item.Tier == app.TierDatabase {
			if err := s.appWriterStatus(ctx, &item); err != nil {
				return app.App{}, err
			}
		}
		return item, nil
	}
	targets, err := s.AppRouteTargets(ctx, installation)
	if err != nil {
		return app.App{}, err
	}
	healthy := 0
	failed := 0
	expected := 0
	for _, copy := range copies {
		if copy.Role == "standby" {
			continue
		}
		expected++
		if copy.ObservedState == "failed" || copy.Health == "unhealthy" {
			failed++
		}
		if copy.ObservedState == "running" && copy.Health == "healthy" {
			healthy++
		}
	}
	worker := item.Tier == app.TierParallel && item.Spec.Router.Port == 0
	ready := worker
	routedHealthy := 0
	if worker {
		routedHealthy = healthy
	}
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
	case status.OperationState == "succeeded" && healthy == expected && routedHealthy == expected && healthy > 0 && ready:
		status.State = "running"
	case status.OperationState == "succeeded" && healthy == 0 && failed > 0:
		status.State = "failed"
		status.FailureCode = "app_copies_unhealthy"
	case healthy > 0 && ready:
		status.State = "degraded"
	}
	if item.Tier == app.TierSingleWriter || item.Tier == app.TierDatabase {
		if err := s.appWriterStatus(ctx, &item); err != nil {
			return app.App{}, err
		}
	}
	if status.State == "running" {
		item.ObservedLifecycle = "running"
	} else if item.ObservedLifecycle == "running" {
		item.ObservedLifecycle = "unknown"
	}
	return item, nil
}
