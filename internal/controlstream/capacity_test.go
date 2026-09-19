package controlstream

import (
	"testing"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func vector(cpu, memory, disk int64) *agentv1.ResourceVector {
	return &agentv1.ResourceVector{CpuMillicores: cpu, MemoryBytes: memory, DiskBytes: disk}
}

// soundReport is a report that every rule accepts, so a test can break one field.
func soundReport() *agentv1.CapacityReport {
	return &agentv1.CapacityReport{
		ProbeId: uuid.NewString(), ProbeRevision: 3,
		SessionId: uuid.NewString(), BootId: uuid.NewString(), JournalGeneration: uuid.NewString(),
		Architecture: "arm64", RuntimeVersion: "26.1.4", StorageId: "pool-a",
		Capacity: vector(16000, 64<<30, 900<<30),
		Used:     vector(4000, 24<<30, 340<<30),
		Reservations: []*agentv1.ReservationUsage{
			{ReservationId: uuid.NewString(), ResourceId: uuid.NewString(), Used: vector(1000, 8<<30, 100<<30)},
		},
	}
}

func TestCheckCapacityReportAcceptsAMeasuredReport(t *testing.T) {
	if err := CheckCapacityReport(soundReport()); err != nil {
		t.Fatal(err)
	}
}

func TestCheckCapacityReportRefusesABadFrame(t *testing.T) {
	for name, break_ := range map[string]func(*agentv1.CapacityReport){
		"no report":           func(r *agentv1.CapacityReport) { *r = agentv1.CapacityReport{} },
		"unknown arch":        func(r *agentv1.CapacityReport) { r.Architecture = "riscv64" },
		"empty storage":       func(r *agentv1.CapacityReport) { r.StorageId = "" },
		"unprintable storage": func(r *agentv1.CapacityReport) { r.StorageId = "pool\x00a" },
		"no probe revision":   func(r *agentv1.CapacityReport) { r.ProbeRevision = 0 },
		"non canonical id":    func(r *agentv1.CapacityReport) { r.ProbeId = "not-a-uuid" },
		// A zero capacity passes a nonnegative check and then places nothing.
		"zero cpu capacity":  func(r *agentv1.CapacityReport) { r.Capacity.CpuMillicores = 0 },
		"zero disk capacity": func(r *agentv1.CapacityReport) { r.Capacity.DiskBytes = 0 },
		"negative use":       func(r *agentv1.CapacityReport) { r.Used.MemoryBytes = -1 },
		"missing capacity":   func(r *agentv1.CapacityReport) { r.Capacity = nil },
		// Itemized use above the total is a report that contradicts itself.
		"item above total": func(r *agentv1.CapacityReport) {
			r.Reservations[0].Used = vector(1000, 8<<30, 999<<30)
		},
		"duplicate resource": func(r *agentv1.CapacityReport) {
			r.Reservations = append(r.Reservations, &agentv1.ReservationUsage{
				ReservationId: uuid.NewString(), ResourceId: r.Reservations[0].ResourceId, Used: vector(0, 0, 0),
			})
		},
		"duplicate reservation": func(r *agentv1.CapacityReport) {
			r.Reservations = append(r.Reservations, &agentv1.ReservationUsage{
				ReservationId: r.Reservations[0].ReservationId, ResourceId: uuid.NewString(), Used: vector(0, 0, 0),
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			report := soundReport()
			break_(report)
			if err := CheckCapacityReport(report); err == nil {
				t.Fatal("a bad frame was accepted before the Store could refuse it")
			}
		})
	}
}

func TestCheckCapacityReportRefusesTooManyItems(t *testing.T) {
	report := soundReport()
	report.Reservations = make([]*agentv1.ReservationUsage, maxReservationItems+1)
	if err := CheckCapacityReport(report); err == nil {
		t.Fatal("an unbounded item list was accepted")
	}
}

func TestCheckCapacityRequestAcceptsAChallenge(t *testing.T) {
	request := &agentv1.CapacityRequest{
		ProbeId: uuid.NewString(), ProbeRevision: 1, SessionId: uuid.NewString(),
		BootId: uuid.NewString(), JournalGeneration: uuid.NewString(), ExpiresUnixMs: 1,
		Known: []*agentv1.KnownReservation{{ReservationId: uuid.NewString(), ResourceId: uuid.NewString()}},
	}
	if err := CheckCapacityRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Known = append(request.Known, &agentv1.KnownReservation{ReservationId: uuid.NewString(), ResourceId: request.Known[0].ResourceId})
	if err := CheckCapacityRequest(request); err == nil {
		t.Fatal("a duplicate resource was accepted")
	}
}

// Capacity carries no epoch, so the envelope check must accept it on its own terms.
func TestAgentMessageAcceptsACapacityReport(t *testing.T) {
	report := soundReport()
	message := &agentv1.AgentMessage{Message: &agentv1.AgentMessage_CapacityReport{CapacityReport: report}}
	if err := CheckAgentMessage(message, uuid.NewString(), uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	report.Architecture = "riscv64"
	if err := CheckAgentMessage(message, uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("the envelope accepted a report the rules refuse")
	}
}
