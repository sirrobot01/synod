package executor

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/capacity"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// capacity answers one capacity challenge by measuring this host.
//
// Nothing here is read from configuration and reported as an observation. The
// agent measures, or it refuses. Placement treats this as evidence, and a
// declared figure would place work on a host nobody looked at.
//
// The measurement is slow: a CPU rate needs an interval, and container stats need
// a sample pair. It runs outside the executor lock, like an inventory read, and
// the answer is checked against the session and the challenge again afterwards.
func (e *Executor) capacity(ctx context.Context, current *session, request *agentv1.CapacityRequest) (*agentv1.AgentMessage, error) {
	if request == nil || request.ProbeId == "" || request.ProbeRevision < 1 || request.ExpiresUnixMs <= 0 {
		return nil, controlstream.ErrMessage
	}
	identity := e.journal.Identity()
	// The challenge must name this session, this boot, and this journal. A
	// changed session invalidates old capacity even when the boot matches.
	if request.SessionId != current.id || request.BootId != e.options.BootID || request.JournalGeneration != identity.JournalGeneration {
		return nil, operation.ErrStaleOwner
	}
	if e.options.StorageID == "" || e.options.RuntimeVersion == "" || e.options.Host.StoragePath == "" {
		return nil, operation.ErrStaleOwner
	}
	if e.capacityBusy {
		return nil, journal.ErrBusy
	}
	deadline := time.UnixMilli(request.ExpiresUnixMs)
	// A late answer cannot publish capacity, so a window that has already closed
	// is refused here rather than measured and thrown away at the far end.
	if !time.Now().Before(deadline) {
		return nil, operation.ErrStaleOwner
	}
	resources, err := knownResources(request.Known)
	if err != nil {
		return nil, err
	}

	current.probe = request.ProbeId
	e.capacityBusy = true
	e.mu.Unlock()
	measured, usage, err := e.measure(ctx, deadline, resources)
	e.mu.Lock()
	e.capacityBusy = false
	if err != nil {
		return nil, err
	}
	// The session or the challenge can be replaced while the host is measured.
	// An answer for a challenge that no longer stands must never be sent.
	if e.session != current || current.ctx.Err() != nil || current.probe != request.ProbeId {
		return nil, operation.ErrStaleOwner
	}

	report := &agentv1.CapacityReport{
		ProbeId:           request.ProbeId,
		ProbeRevision:     request.ProbeRevision,
		SessionId:         current.id,
		BootId:            e.options.BootID,
		JournalGeneration: identity.JournalGeneration,
		Architecture:      measured.Architecture,
		RuntimeVersion:    e.options.RuntimeVersion,
		StorageId:         e.options.StorageID,
		Capacity:          &agentv1.ResourceVector{CpuMillicores: measured.Capacity.CPUMillis, MemoryBytes: measured.Capacity.MemoryBytes, DiskBytes: measured.Capacity.DiskBytes},
		Used:              &agentv1.ResourceVector{CpuMillicores: measured.Used.CPUMillis, MemoryBytes: measured.Used.MemoryBytes, DiskBytes: measured.Used.DiskBytes},
	}
	for _, item := range request.Known {
		found, ok := usage[item.ResourceId]
		if !ok {
			// A resource this node does not hold is left out. Reporting zero for
			// it would be a measurement of something never examined.
			continue
		}
		report.Reservations = append(report.Reservations, &agentv1.ReservationUsage{
			ReservationId: item.ReservationId,
			ResourceId:    item.ResourceId,
			Used:          &agentv1.ResourceVector{CpuMillicores: found.CPUMillis, MemoryBytes: found.MemoryBytes, DiskBytes: found.DiskBytes},
		})
	}
	return &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: report}}, nil
}

// measure observes the host and the named resources inside the probe window.
func (e *Executor) measure(ctx context.Context, deadline time.Time, resources []uuid.UUID) (capacity.Measurement, map[string]amount, error) {
	work, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	measured, err := e.options.Host.Measure(work)
	if err != nil {
		return measured, nil, err
	}
	items, err := e.runtime.Usage(work, resources)
	if err != nil {
		return measured, nil, err
	}
	usage := make(map[string]amount, len(items))
	// Itemized use can never exceed total observed use, which the far end checks.
	// A container sample and a host sample are taken at different instants, so
	// the total is raised to cover the parts rather than sending a report that
	// contradicts itself.
	var sum amount
	for _, item := range items {
		usage[item.ResourceID.String()] = amount{CPUMillis: item.Used.CPUMillis, MemoryBytes: item.Used.MemoryBytes, DiskBytes: item.Used.DiskBytes}
		sum.CPUMillis += item.Used.CPUMillis
		sum.MemoryBytes += item.Used.MemoryBytes
		sum.DiskBytes += item.Used.DiskBytes
	}
	measured.Used.CPUMillis = min(max(measured.Used.CPUMillis, sum.CPUMillis), measured.Capacity.CPUMillis)
	measured.Used.MemoryBytes = min(max(measured.Used.MemoryBytes, sum.MemoryBytes), measured.Capacity.MemoryBytes)
	measured.Used.DiskBytes = min(max(measured.Used.DiskBytes, sum.DiskBytes), measured.Capacity.DiskBytes)
	return measured, usage, nil
}

// amount mirrors one measured triple without importing a controller package.
type amount struct {
	CPUMillis   int64
	MemoryBytes int64
	DiskBytes   int64
}

// knownResources reads the reservation list the controller sent. A Docker label
// names a resource and never a reservation, so this mapping only ever comes from
// the controller.
func knownResources(known []*agentv1.KnownReservation) ([]uuid.UUID, error) {
	if len(known) > maxKnownReservations {
		return nil, controlstream.ErrMessage
	}
	resources := make([]uuid.UUID, 0, len(known))
	seen := make(map[string]bool, len(known))
	for _, item := range known {
		if item == nil || seen[item.ResourceId] || seen[item.ReservationId] {
			return nil, controlstream.ErrMessage
		}
		resource, err := uuid.Parse(item.ResourceId)
		if err != nil || resource == uuid.Nil {
			return nil, controlstream.ErrMessage
		}
		reservation, err := uuid.Parse(item.ReservationId)
		if err != nil || reservation == uuid.Nil {
			return nil, controlstream.ErrMessage
		}
		seen[item.ResourceId], seen[item.ReservationId] = true, true
		resources = append(resources, resource)
	}
	return resources, nil
}

// maxKnownReservations matches the bound that the placement report validates.
const maxKnownReservations = 512
