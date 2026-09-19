package agentstream

import (
	"context"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// recordDrift commits what one node reported finding on its host.
//
// The installation, node, and session come from the authenticated peer and never
// from the frame, exactly as for a capacity report: a claimed identity must not
// outrank the certificate that proved one.
//
// Nothing here changes desired state. Specification 33 forbids a manual restart or
// stop moving desired state on its own, and this is an observation: it writes to the
// drift tables and to nothing else. The resource observations on the same page are
// left alone, because deciding that an observed state is wrong needs desired state,
// which is not this receiver's to read or to move.
//
// A page with no drift is not a report. Drift travels on the first page of a scan,
// so a later page carrying none would otherwise erase what the first page said.
func (r Receiver) recordDrift(ctx context.Context, peer controlstream.Peer, sessionID uuid.UUID, page *agentv1.InventoryPage) error {
	if page == nil {
		return ErrUnsupportedMessage
	}
	if len(page.Drift) == 0 && !page.DriftTruncated {
		return nil
	}
	report := postgres.DriftReport{
		InstallationID: peer.InstallationID, NodeID: peer.NodeID, SessionID: sessionID,
		Truncated: page.DriftTruncated,
	}
	for _, item := range page.Drift {
		found := postgres.DriftItem{
			Kind: item.Kind, NativeID: item.NativeId, Name: item.Name,
			State: item.State, Class: postgres.DriftClass(item.Class), Detail: item.Detail,
		}
		// An identity that does not parse is absent, not zero. An object that has
		// lost its labels has no identity to report, and inventing one would be
		// worse than saying so.
		if clusterID, err := uuid.Parse(item.ClusterId); err == nil && item.ClusterId != "" {
			found.ClusterID = clusterID
		} else if item.ClusterId != "" {
			return ErrUnsupportedMessage
		}
		if resourceID, err := uuid.Parse(item.ResourceId); err == nil && item.ResourceId != "" {
			found.ResourceID = resourceID
		} else if item.ResourceId != "" {
			return ErrUnsupportedMessage
		}
		report.Items = append(report.Items, found)
	}
	return r.Store.RecordNodeDrift(ctx, report)
}
