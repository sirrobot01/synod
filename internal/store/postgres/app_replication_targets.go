package postgres

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
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
		if item.Tier != app.TierSingleWriter || item.DesiredLifecycle != "running" {
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
