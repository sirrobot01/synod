package executor

import (
	"fmt"
	"net"
	"strconv"

	"github.com/sirrobot01/synod/internal/agent/capacity"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// portFree reports whether a host port can still be bound. A test replaces it.
var portFree = bindable

// precheck rechecks that the host can still satisfy what one command needs.
//
// Specification 33 states that observed free space can change outside Synod and that
// the agent must recheck required space and ports before each relevant effect. A
// reservation is a claim on Synod's own books; it does not stop another process on
// the host from filling the disk or taking the port.
//
// Nothing here removes anything to make room. Specification 11 forbids deleting data,
// a backup dependency, or required WAL as emergency cleanup, so a host that cannot fit
// the work is refused and left exactly as it was.
//
// It runs only before a fresh start. A recovery is resolving effects that already
// exist, and refusing it would strand them.
func (e *Executor) precheck(command *agentv1.Command) error {
	if command == nil || command.Input == nil {
		return nil
	}
	switch payload := command.Input.Payload.(type) {
	case *agentv1.CommandInput_EnsureVolume:
		return e.checkSpace(0, 0)
	case *agentv1.CommandInput_ClonePostgresReplica:
		return e.checkSpace(0, payload.ClonePostgresReplica.GetRequiredFreeBytes())
	case *agentv1.CommandInput_StartPatroniMember:
		p := payload.StartPatroniMember
		if p == nil || p.Container == nil || p.Container.Postgres == nil || p.Container.Postgres.Patroni == nil {
			return fmt.Errorf("Patroni start configuration is absent")
		}
		var required int64
		if p.AllowInitialization {
			required = p.Container.Postgres.Patroni.RequiredFreeBytes
		}
		return e.checkSpace(p.Container.MemoryBytes, required)
	case *agentv1.CommandInput_EnsureContainer:
		container := payload.EnsureContainer
		if container == nil {
			return nil
		}
		if err := e.checkSpace(container.MemoryBytes, 0); err != nil {
			return err
		}
		return checkPorts(container.Ports)
	default:
		// Stopping, removing, inspecting, and ensuring a network claim no pool
		// space and no host port. A check would refuse work that frees capacity.
		return nil
	}
}

// checkSpace refuses work the host can no longer hold.
//
// Keep the declared disk request above the operator's floor. This measurement is
// a precondition for new work. It does not reserve space against external writes.
func (e *Executor) checkSpace(memoryBytes, requiredFreeBytes int64) error {
	if e.options.MinFreeBytes < 0 || requiredFreeBytes < 0 {
		return fmt.Errorf("%w: disk request and storage floor must not be negative", capacity.ErrCapacityUnavailable)
	}
	if e.options.Host.StoragePath == "" {
		// An agent with no declared pool cannot measure one. It reports that rather
		// than passing work through an unchecked path.
		return fmt.Errorf("%w: no local storage pool is declared", capacity.ErrCapacityUnavailable)
	}
	space, err := e.options.Host.Free()
	if err != nil {
		return fmt.Errorf("%w: the host could not be measured at %s: %w", capacity.ErrCapacityUnavailable, e.options.Host.StoragePath, err)
	}
	if space.DiskBytes <= e.options.MinFreeBytes {
		return fmt.Errorf("%w: the storage pool has %d bytes free and the floor is %d",
			capacity.ErrCapacityUnavailable, space.DiskBytes, e.options.MinFreeBytes)
	}
	if requiredFreeBytes > space.DiskBytes-e.options.MinFreeBytes {
		return fmt.Errorf("%w: the command needs %d disk bytes above the floor and %d are available",
			capacity.ErrCapacityUnavailable, requiredFreeBytes, space.DiskBytes-e.options.MinFreeBytes)
	}
	// An unmeasurable memory figure is not a refusal. The pool floor above is the
	// check that always applies; this one applies when the host can answer it.
	if memoryBytes > 0 && space.MemoryKnown && memoryBytes > space.MemoryBytes {
		return fmt.Errorf("%w: the command needs %d memory bytes and %d are available",
			capacity.ErrCapacityUnavailable, memoryBytes, space.MemoryBytes)
	}
	return nil
}

// checkPorts refuses a container whose declared host port is already taken.
//
// The bind is attempted on the host, not asked of the container runtime, because a
// port can be held by any process. A reservation only proves that Synod did not give
// it away twice.
func checkPorts(bindings []*agentv1.PortBinding) error {
	for _, binding := range bindings {
		if binding == nil || binding.HostPort == 0 {
			continue
		}
		address := binding.HostAddress
		if address == "" {
			address = "0.0.0.0"
		}
		transport := binding.Transport
		if transport != "udp" {
			transport = "tcp"
		}
		if !portFree(transport, net.JoinHostPort(address, strconv.Itoa(int(binding.HostPort)))) {
			return fmt.Errorf("%w: %s %s is already in use on this host",
				capacity.ErrPortOccupied, transport, net.JoinHostPort(address, strconv.Itoa(int(binding.HostPort))))
		}
	}
	return nil
}

// bindable reports a host port that nothing currently holds.
func bindable(transport, address string) bool {
	switch transport {
	case "udp":
		conn, err := net.ListenPacket("udp", address)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	default:
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return false
		}
		listener.Close()
		return true
	}
}
