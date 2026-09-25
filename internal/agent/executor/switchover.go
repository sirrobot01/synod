package executor

import (
	"context"
	"errors"
	"net/netip"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// errSwitchoverInterrupted reports a switchover that the agent lost before it saw the result.
// A switchover leaves no native work behind, so the command fails. A new switchover reads
// the leader again and succeeds at once when the member already leads.
var errSwitchoverInterrupted = errors.New("patroni_switchover_interrupted")

// switchover makes the command's member the Patroni leader through its REST API. Like
// readiness, it is a network action and does not go to the container runtime.
func (e *Executor) switchover(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	p := command.Input.GetSwitchoverPatroni()
	material, err := postgres.DecodePatroniTLSMaterial(secrets["patroni_member_tls"])
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	// Validation accepted the address and the port before the command was admitted.
	address := netip.MustParseAddr(p.Address)
	client, err := postgres.PatroniRESTClient(material, address)
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	defer client.CloseIdleConnections()
	base := "https://" + netip.AddrPortFrom(address, uint16(p.RestPort)).String()
	if _, err := postgres.PatroniSwitchover(ctx, client, base, uuid.MustParse(command.Input.ResourceId), string(secrets["patroni_rest"])); err != nil {
		return runtime.Result{}, err
	}
	return runtime.Result{Effects: []*agentv1.Effect{{
		CommandId: command.CommandId, ResourceId: command.Input.ResourceId,
		NativeOperationId: "patroni-switchover:" + command.CommandId, State: "patroni_leader",
	}}}, nil
}
