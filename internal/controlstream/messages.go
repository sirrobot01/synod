package controlstream

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/router"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

const CommandInspectionCapability = "runtime.command-inspection.v1"
const RouterCapability = "runtime.router.v1"

func checkEpoch(epoch *agentv1.Epoch, installationID, nodeID string) error {
	if epoch == nil || epoch.InstallationId != installationID || epoch.NodeId != nodeID || epoch.ExecutionEpoch < 1 {
		return ErrMessage
	}
	for _, id := range []string{epoch.ClusterId, epoch.RecoveryGeneration} {
		value, err := uuid.Parse(id)
		if err != nil {
			return fmt.Errorf("parse %q in the epoch: %w: %w", id, ErrMessage, err)
		}
		if value == uuid.Nil || value.String() != id {
			return fmt.Errorf("%q in the epoch is not a canonical non-nil UUID: %w", id, ErrMessage)
		}
	}
	return nil
}

// CheckAgentMessage validates wire shape and authenticated target, not execution policy.
func CheckAgentMessage(message *agentv1.AgentMessage, installationID, nodeID string) error {
	if message == nil {
		return ErrMessage
	}
	var epoch *agentv1.Epoch
	switch value := message.Message.(type) {
	case *agentv1.AgentMessage_RouteAck:
		ack := value.RouteAck
		if ack == nil || ack.Revision == 0 {
			return ErrMessage
		}
		id, err := uuid.Parse(ack.SessionId)
		if err != nil || id == uuid.Nil || id.String() != ack.SessionId {
			return ErrMessage
		}
		switch ack.FailureCode {
		case "", "route_port_conflict", "invalid_route_configuration", "router_closed", "stale_route_revision", "route_revision_conflict", "router_unavailable":
			return nil
		default:
			return ErrMessage
		}
	case *agentv1.AgentMessage_CommandInspection:
		return CheckCommandInspectionResult(value.CommandInspection, installationID, nodeID)
	case *agentv1.AgentMessage_Heartbeat:
		if value.Heartbeat == nil {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_Inventory:
		if value.Inventory == nil {
			return ErrMessage
		}
		return nil
	case *agentv1.AgentMessage_CapacityReport:
		// Capacity belongs to a node, not a cluster, so this carries no epoch and
		// binds the probe challenge instead.
		return CheckCapacityReport(value.CapacityReport)
	case *agentv1.AgentMessage_BarrierResult:
		if value.BarrierResult == nil {
			return ErrMessage
		}
		epoch = value.BarrierResult.Epoch
	case *agentv1.AgentMessage_AdmissionAck:
		if value.AdmissionAck == nil {
			return ErrMessage
		}
		epoch = value.AdmissionAck.Epoch
	case *agentv1.AgentMessage_Progress:
		if value.Progress == nil {
			return ErrMessage
		}
		epoch = value.Progress.Epoch
	case *agentv1.AgentMessage_CancellationResult:
		if value.CancellationResult == nil {
			return ErrMessage
		}
		epoch = value.CancellationResult.Epoch
	case *agentv1.AgentMessage_TerminalResult:
		result := value.TerminalResult
		if result == nil || result.Sequence == 0 || len(result.InputHash) != 32 || (result.State != "succeeded" && result.State != "failed" && result.State != "cancelled") {
			return ErrMessage
		}
		id, err := uuid.Parse(result.CommandId)
		if err != nil {
			return fmt.Errorf("parse the command of a terminal result (%q): %w: %w", result.CommandId, ErrMessage, err)
		}
		if id == uuid.Nil || id.String() != result.CommandId {
			return fmt.Errorf("the terminal result names command %q, which is not a canonical non-nil UUID: %w", result.CommandId, ErrMessage)
		}
		epoch = result.Epoch
	default:
		return fmt.Errorf("an agent may not send %T: %w", value, ErrMessage)
	}
	return checkEpoch(epoch, installationID, nodeID)
}

// CheckControllerMessage checks target identity. The journal must still enforce epochs and hashes.
func CheckControllerMessage(message *agentv1.ControllerMessage, installationID, nodeID string) error {
	if message == nil {
		return ErrMessage
	}
	var epoch *agentv1.Epoch
	switch value := message.Message.(type) {
	case *agentv1.ControllerMessage_Routes:
		table := value.Routes
		if table == nil || table.Revision == 0 || router.Validate(table.Routes) != nil {
			return ErrMessage
		}
		id, err := uuid.Parse(table.SessionId)
		if err != nil || id == uuid.Nil || id.String() != table.SessionId {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_CommandInspection:
		return CheckCommandInspectionRequest(value.CommandInspection, installationID, nodeID)
	case *agentv1.ControllerMessage_Session:
		if value.Session == nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_Error:
		if value.Error == nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_Inventory:
		if value.Inventory == nil {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_Secret:
		// A credential is delivered to a session, not to an epoch: the agent holds
		// it in session memory and a command names it later.
		if value.Secret == nil || value.Secret.CredentialId == "" || value.Secret.Version < 1 ||
			value.Secret.Purpose == "" || len(value.Secret.Material) == 0 {
			return ErrMessage
		}
		return nil
	case *agentv1.ControllerMessage_PrepareEpoch:
		if value.PrepareEpoch == nil {
			return ErrMessage
		}
		epoch = value.PrepareEpoch.Epoch
	case *agentv1.ControllerMessage_ActivateEpoch:
		if value.ActivateEpoch == nil {
			return ErrMessage
		}
		epoch = value.ActivateEpoch.Epoch
	case *agentv1.ControllerMessage_Command:
		if value.Command == nil {
			return ErrMessage
		}
		epoch = value.Command.Epoch
	case *agentv1.ControllerMessage_ResultAck:
		if value.ResultAck == nil || value.ResultAck.Sequence == 0 {
			return ErrMessage
		}
		epoch = value.ResultAck.Epoch
	case *agentv1.ControllerMessage_Cancel:
		if value.Cancel == nil {
			return ErrMessage
		}
		epoch = value.Cancel.Epoch
	case *agentv1.ControllerMessage_CapacityRequest:
		return CheckCapacityRequest(value.CapacityRequest)
	default:
		return fmt.Errorf("a controller may not send %T: %w", value, ErrMessage)
	}
	return checkEpoch(epoch, installationID, nodeID)
}

// CheckCommandInspectionRequest validates bounded read-only input, not local authority.
func CheckCommandInspectionRequest(request *agentv1.CommandInspectionRequest, installationID, nodeID string) error {
	if request == nil || proto.Size(request) > 1<<20 {
		return fmt.Errorf("the inspection request is absent or exceeds 1 MiB: %w", ErrMessage)
	}
	if err := operation.ValidateCommand(request.Command); err != nil {
		return fmt.Errorf("validate the command of inspection request %s: %w: %w", request.RequestId, ErrMessage, err)
	}
	id, err := uuid.Parse(request.RequestId)
	if err != nil {
		return fmt.Errorf("parse the inspection request identity (%q): %w: %w", request.RequestId, ErrMessage, err)
	}
	if id == uuid.Nil || id.String() != request.RequestId {
		return fmt.Errorf("the inspection request identity %q is not a canonical non-nil UUID: %w", request.RequestId, ErrMessage)
	}
	if err := checkEpoch(request.Command.Epoch, installationID, nodeID); err != nil {
		return err
	}
	return checkInspectionEffects(request.PriorEffects, request.Command.CommandId, request.Command.Input.ResourceId)
}

// CheckCommandInspectionResult validates wire evidence. The Store must bind its request.
func CheckCommandInspectionResult(result *agentv1.CommandInspectionResult, installationID, nodeID string) error {
	if result == nil || proto.Size(result) > 1<<20 || len(result.InputHash) != 32 || len(result.FailureCode) > 128 || strings.ContainsFunc(result.FailureCode, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) {
		return ErrMessage
	}
	for _, value := range []string{result.RequestId, result.CommandId} {
		id, err := uuid.Parse(value)
		if err != nil {
			return fmt.Errorf("parse %q in the inspection result: %w: %w", value, ErrMessage, err)
		}
		if id == uuid.Nil || id.String() != value {
			return fmt.Errorf("%q in the inspection result is not a canonical non-nil UUID: %w", value, ErrMessage)
		}
	}
	if err := checkEpoch(result.Epoch, installationID, nodeID); err != nil {
		return err
	}
	return checkInspectionEffects(result.Effects, result.CommandId, "")
}

func checkInspectionEffects(effects []*agentv1.Effect, commandID, resourceID string) error {
	if len(effects) > 1024 {
		return ErrMessage
	}
	seen := make(map[string]bool, len(effects))
	for _, effect := range effects {
		if effect == nil || effect.CommandId != commandID || len(effect.NativeOperationId) > 256 || strings.ContainsFunc(effect.NativeOperationId, func(r rune) bool { return r < '!' || r > '~' }) || len(effect.State) == 0 || len(effect.State) > 64 || strings.ContainsFunc(effect.State, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') }) || seen[effect.NativeOperationId] {
			return ErrMessage
		}
		id, err := uuid.Parse(effect.ResourceId)
		if err != nil {
			return fmt.Errorf("parse the resource of an effect of command %s (%q): %w: %w", commandID, effect.ResourceId, ErrMessage, err)
		}
		if id == uuid.Nil || id.String() != effect.ResourceId {
			return fmt.Errorf("an effect of command %s names resource %q, which is not a canonical non-nil UUID: %w", commandID, effect.ResourceId, ErrMessage)
		}
		if resourceID == "" {
			resourceID = effect.ResourceId
		}
		if effect.ResourceId != resourceID {
			return ErrMessage
		}
		seen[effect.NativeOperationId] = true
	}
	return nil
}

// CheckCapacityRequest validates one outgoing capacity challenge.
// It carries no epoch, because capacity is a property of a node.
func CheckCapacityRequest(request *agentv1.CapacityRequest) error {
	if request == nil || request.ProbeRevision < 1 || request.ExpiresUnixMs <= 0 || len(request.Known) > maxReservationItems {
		return ErrMessage
	}
	for _, id := range []string{request.ProbeId, request.SessionId, request.BootId, request.JournalGeneration} {
		if !identifier(id) {
			return ErrMessage
		}
	}
	seen := make(map[string]bool, len(request.Known)*2)
	for _, item := range request.Known {
		if item == nil || !identifier(item.ReservationId) || !identifier(item.ResourceId) || seen[item.ReservationId] || seen[item.ResourceId] {
			return ErrMessage
		}
		seen[item.ReservationId], seen[item.ResourceId] = true, true
	}
	return nil
}

// CheckCapacityReport validates one measured report before any transaction sees it.
//
// A malformed frame is refused here so a bad report never reaches the Store. The
// rules mirror the placement report: positive capacity, a known architecture,
// bounded text, no duplicate identity, and itemized use never above the total.
func CheckCapacityReport(report *agentv1.CapacityReport) error {
	if report == nil || report.ProbeRevision < 1 || len(report.Reservations) > maxReservationItems {
		return ErrMessage
	}
	for _, id := range []string{report.ProbeId, report.SessionId, report.BootId, report.JournalGeneration} {
		if !identifier(id) {
			return ErrMessage
		}
	}
	if report.Architecture != "arm64" && report.Architecture != "amd64" {
		return ErrMessage
	}
	if !boundedText(report.RuntimeVersion) || !boundedText(report.StorageId) {
		return ErrMessage
	}
	// A capacity of zero would pass a nonnegative check and then place nothing.
	if !positiveVector(report.Capacity) || !nonNegativeVector(report.Used) {
		return ErrMessage
	}
	var sum agentv1.ResourceVector
	seen := make(map[string]bool, len(report.Reservations)*2)
	for _, item := range report.Reservations {
		if item == nil || !identifier(item.ReservationId) || !identifier(item.ResourceId) || seen[item.ReservationId] || seen[item.ResourceId] || !nonNegativeVector(item.Used) {
			return ErrMessage
		}
		seen[item.ReservationId], seen[item.ResourceId] = true, true
		if sum.CpuMillicores > maxAmount-item.Used.CpuMillicores || sum.MemoryBytes > maxAmount-item.Used.MemoryBytes || sum.DiskBytes > maxAmount-item.Used.DiskBytes {
			return ErrMessage
		}
		sum.CpuMillicores += item.Used.CpuMillicores
		sum.MemoryBytes += item.Used.MemoryBytes
		sum.DiskBytes += item.Used.DiskBytes
	}
	if sum.CpuMillicores > report.Used.CpuMillicores || sum.MemoryBytes > report.Used.MemoryBytes || sum.DiskBytes > report.Used.DiskBytes {
		return ErrMessage
	}
	return nil
}

// maxReservationItems matches the bound that the placement report validates.
const maxReservationItems = 512

// maxAmount bounds one measured figure well inside int64, so a sum cannot overflow.
const maxAmount = int64(1) << 60

// identifier reports a canonical, non-nil UUID string.
func identifier(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

// boundedText reports printable text within the length the Store accepts.
func boundedText(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < ' ' || r > '~' })
}

func positiveVector(v *agentv1.ResourceVector) bool {
	return nonNegativeVector(v) && v.CpuMillicores > 0 && v.MemoryBytes > 0 && v.DiskBytes > 0
}

func nonNegativeVector(v *agentv1.ResourceVector) bool {
	return v != nil && v.CpuMillicores >= 0 && v.MemoryBytes >= 0 && v.DiskBytes >= 0 &&
		v.CpuMillicores <= maxAmount && v.MemoryBytes <= maxAmount && v.DiskBytes <= maxAmount
}
