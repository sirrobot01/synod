package postgres

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/placement"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// AppReplicationTargets includes empty tables to withdraw old assignments.
func (s *Store) AppReplicationTargets(ctx context.Context, installation uuid.UUID) (map[uuid.UUID]*agentv1.ReplicationTable, error) {
	snapshot, err := s.ReadPlacementSnapshot(ctx, installation)
	if err != nil {
		return nil, err
	}
	targets := map[uuid.UUID]*agentv1.ReplicationTable{}
	addresses := map[uuid.UUID]string{}
	for _, node := range snapshot.Nodes {
		if node.Policy != nil && node.Policy.ClientAddress != "" {
			port := node.Policy.ReplicationPort
			if port == 0 {
				port = 8445
			}
			addresses[node.ID] = net.JoinHostPort(node.Policy.ClientAddress, strconv.Itoa(int(port)))
		}
		if node.SessionID == uuid.Nil || node.Revoked {
			continue
		}
		presence, err := s.GetAgentPresence(ctx, installation, node.ID)
		if err != nil {
			return nil, err
		}
		if presence.SessionID == node.SessionID && slices.Contains(presence.Hello.Capabilities, replication.Capability) {
			targets[node.ID] = &agentv1.ReplicationTable{SessionId: node.SessionID.String()}
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
		if item.Tier == app.TierDatabase {
			if err := s.addDatabaseTargets(ctx, installation, item, snapshot, targets); err != nil {
				return nil, err
			}
			continue
		}
		if item.Tier != app.TierSingleWriter {
			continue
		}
		writer, err := s.GetAppWriter(ctx, installation, item.ID)
		if err != nil {
			return nil, err
		}
		if writer.Epoch == 0 || addresses[writer.NodeID] == "" {
			continue
		}
		copies, err := s.GetAppCopies(ctx, installation, item.ID)
		if err != nil {
			return nil, err
		}
		var peers []*agentv1.ReplicaPeer
		for _, copy := range copies {
			if copy.Role == replication.RoleStandby && addresses[copy.NodeID] != "" {
				peers = append(peers, &agentv1.ReplicaPeer{NodeId: copy.NodeID.String(), Address: addresses[copy.NodeID]})
			}
		}
		if len(peers) == 0 {
			continue
		}
		for _, copy := range copies {
			table := targets[copy.NodeID]
			if table == nil {
				continue
			}
			assignment := &agentv1.AppReplication{AppId: item.ID.String(), Epoch: writer.Epoch, Role: copy.Role, IntervalSeconds: item.Spec.Replication.IntervalSeconds}
			if copy.Role == replication.RoleActive && copy.NodeID == writer.NodeID {
				assignment.Peers = peers
			} else if copy.Role == replication.RoleStandby {
				assignment.Peers = []*agentv1.ReplicaPeer{{NodeId: writer.NodeID.String(), Address: addresses[writer.NodeID]}}
			} else {
				continue
			}
			table.Apps = append(table.Apps, assignment)
		}
	}
	for _, table := range targets {
		slices.SortFunc(table.Apps, func(a, b *agentv1.AppReplication) int { return strings.Compare(a.AppId, b.AppId) })
		if err := replication.Validate(table.Apps); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// addDatabaseTargets assigns each tier 1 copy its engine role. A standby follows the
// primary at the primary node's client address and the primary copy's published port.
func (s *Store) addDatabaseTargets(ctx context.Context, installation uuid.UUID, item app.App, snapshot placement.Snapshot, targets map[uuid.UUID]*agentv1.ReplicationTable) error {
	writer, err := s.GetAppWriter(ctx, installation, item.ID)
	if err != nil || writer.Epoch == 0 {
		return err
	}
	copies, err := s.GetAppCopies(ctx, installation, item.ID)
	if err != nil {
		return err
	}
	primary := slices.IndexFunc(copies, func(c app.Copy) bool { return c.Role == replication.RoleActive && c.NodeID == writer.NodeID })
	address := ""
	for _, node := range snapshot.Nodes {
		if node.ID == writer.NodeID && node.Policy != nil {
			address = node.Policy.ClientAddress
		}
	}
	if primary < 0 || address == "" || copies[primary].HostPort == 0 {
		return nil
	}
	var hold, target uuid.UUID
	err = s.pool.QueryRow(ctx, `SELECT m.operation_id,m.target_node_id FROM app_moves m JOIN operations o ON o.id=m.operation_id
 WHERE m.installation_id=$1 AND m.app_id=$2 AND m.writer_epoch=$3 AND m.source_node_id=$4 AND m.promoted_at IS NULL
 AND o.plan_version>0 AND o.status IN ('pending','running','waiting','cancelling')`, installation, item.ID, writer.Epoch, writer.NodeID).Scan(&hold, &target)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return admissionError(err)
	}
	rebuilding, err := s.DatabaseRebuildNodes(ctx, installation, item.ID)
	if err != nil {
		return err
	}
	for _, copy := range copies {
		if slices.Contains(rebuilding, copy.NodeID) {
			continue
		}
		table := targets[copy.NodeID]
		if table == nil || (copy.Role != replication.RoleActive && copy.Role != replication.RoleStandby) || (copy.Role == replication.RoleActive && copy.NodeID != writer.NodeID) {
			continue
		}
		database := &agentv1.DatabaseAssignment{Engine: item.Spec.Database.Engine, Databases: item.Spec.Database.Databases}
		if copy.Role == replication.RoleStandby {
			database.PrimaryHost, database.PrimaryPort = address, uint32(copies[primary].HostPort)
			if copy.NodeID == target {
				database.SwitchoverOperationId = hold.String()
			}
		}
		table.Apps = append(table.Apps, &agentv1.AppReplication{AppId: item.ID.String(), Epoch: writer.Epoch, Role: copy.Role, Database: database})
	}
	return nil
}
