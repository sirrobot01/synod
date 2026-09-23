// Package capacity measures the host that an agent runs on.
//
// Every number here is observed, not declared. The agent never reports a figure
// from configuration that it did not measure, because placement treats this as
// evidence and presence can never substitute for it.
package capacity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var ErrUnsupported = errors.New("capacity_unsupported_host")
var ErrMeasure = errors.New("capacity_measurement_failed")

// Vector is one triple of measured amounts. It mirrors placement.Vector, which
// this package cannot import: the agent does not depend on controller packages.
type Vector struct {
	CPUMillis   int64
	MemoryBytes int64
	DiskBytes   int64
}

// Measurement is one host observation.
type Measurement struct {
	Architecture string
	Capacity     Vector
	Used         Vector
}

// Host names what to measure. ProcRoot and the storage path are injected so a
// test can measure a fixture instead of the machine running the test.
// Serialize Measure calls. Free can run during a measurement.
type Host struct {
	ProcRoot string
	// StoragePath is the one explicit local pool. The contract forbids summing
	// unrelated filesystems, so exactly one path is measured.
	StoragePath string
	// Sample is how long the CPU rate is observed over. CPU use is a rate, so it
	// cannot be read from a single instant. It bounds only the first measurement:
	// afterwards the rate covers the whole interval since the previous one.
	Sample time.Duration
	// The previous counter reading. Placement treats used CPU as a hard constraint,
	// and a short burst is not evidence: consecutive 200ms windows on an idle
	// four-core host read between 50 and 1600 millis.
	lastTotal, lastIdle int64
}

// statfs reports the capacity and used bytes of one filesystem.
// It is separated so a test can measure without a real mount.
var statfs = diskUsage

// Measure observes the host once. It returns ErrUnsupported on a host with no
// procfs rather than guessing, because a guessed capacity would place work.
func (h *Host) Measure(ctx context.Context) (Measurement, error) {
	var found Measurement
	if err := ctx.Err(); err != nil {
		return found, err
	}
	if h.ProcRoot == "" || h.StoragePath == "" {
		return found, fmt.Errorf("measure the host: no proc root or storage path is declared: %w", ErrMeasure)
	}
	sample := h.Sample
	if sample <= 0 {
		sample = 200 * time.Millisecond
	}
	// Validate rejects any architecture outside this pair, so an unknown one is
	// refused here rather than sent and rejected at the far end.
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" {
		return found, fmt.Errorf("measure the host: architecture %s is not supported: %w", runtime.GOARCH, ErrUnsupported)
	}
	found.Architecture = runtime.GOARCH

	total, available, err := h.memory()
	if err != nil {
		return found, err
	}
	found.Capacity.MemoryBytes = total
	found.Used.MemoryBytes = total - available

	cores, used, err := h.processor(ctx, sample)
	if err != nil {
		return found, err
	}
	found.Capacity.CPUMillis = cores
	found.Used.CPUMillis = used

	capacityBytes, usedBytes, err := statfs(h.StoragePath)
	if err != nil {
		return found, err
	}
	found.Capacity.DiskBytes = capacityBytes
	found.Used.DiskBytes = usedBytes

	// A capacity of zero would pass a nonnegative check and then place nothing.
	// Refusing here keeps an unmeasurable dimension from reading as an empty host.
	if found.Capacity.CPUMillis <= 0 || found.Capacity.MemoryBytes <= 0 || found.Capacity.DiskBytes <= 0 {
		return Measurement{}, fmt.Errorf("measure the host: capacity is %d millicores, %d memory bytes, %d disk bytes on %s: %w",
			found.Capacity.CPUMillis, found.Capacity.MemoryBytes, found.Capacity.DiskBytes, h.StoragePath, ErrMeasure)
	}
	found.Used = clamp(found.Used, found.Capacity)
	return found, nil
}

// memory reads total and available bytes from meminfo.
// MemAvailable is the kernel's own estimate of what a new workload can use. It is
// not free memory, and using free memory instead would under-place every host.
func (h *Host) memory() (int64, int64, error) {
	name := filepath.Join(h.ProcRoot, "meminfo")
	content, err := os.ReadFile(name)
	if err != nil {
		return 0, 0, fmt.Errorf("read %s: %w: %w", name, ErrUnsupported, err)
	}
	var total, available int64
	for line := range strings.SplitSeq(string(content), "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch key {
		case "MemTotal":
			total = kilobytes(value)
		case "MemAvailable":
			available = kilobytes(value)
		}
	}
	if total <= 0 || available < 0 || available > total {
		return 0, 0, fmt.Errorf("read %s: MemTotal is %d bytes and MemAvailable is %d bytes: %w", name, total, available, ErrMeasure)
	}
	return total, available, nil
}

// kilobytes reads a meminfo value, which the kernel writes in kB.
func kilobytes(value string) int64 {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return 0
	}
	amount, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || amount < 0 || amount > (1<<63-1)/1024 {
		return 0
	}
	return amount * 1024
}

// processor reports capacity and used millicores. Used is a rate, so it is the
// busy share of one interval rather than a reading at one instant.
func (h *Host) processor(ctx context.Context, sample time.Duration) (int64, int64, error) {
	first, firstIdle := h.lastTotal, h.lastIdle
	// A zero total is the first measurement: the counter is monotonic from boot, so
	// only an unread Host has one. There is nothing to rate against, so sample.
	if first == 0 {
		var err error
		if first, firstIdle, err = h.jiffies(); err != nil {
			return 0, 0, err
		}
		timer := time.NewTimer(sample)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-timer.C:
		}
	}
	second, secondIdle, err := h.jiffies()
	if err != nil {
		return 0, 0, err
	}
	h.lastTotal, h.lastIdle = second, secondIdle
	cores := int64(runtime.NumCPU()) * 1000
	elapsed, idle := second-first, secondIdle-firstIdle
	if elapsed <= 0 || idle < 0 || idle > elapsed {
		// A counter that did not advance says nothing about use. Reporting zero
		// would claim an idle host, so the measurement fails instead.
		return 0, 0, fmt.Errorf("sample the processor over %s: the aggregate counter moved %d jiffies and the idle counter moved %d: %w",
			sample, elapsed, idle, ErrMeasure)
	}
	return cores, cores * (elapsed - idle) / elapsed, nil
}

// jiffies reads the aggregate and idle counters of the first cpu line.
func (h *Host) jiffies() (int64, int64, error) {
	name := filepath.Join(h.ProcRoot, "stat")
	content, err := os.ReadFile(name)
	if err != nil {
		return 0, 0, fmt.Errorf("read %s: %w: %w", name, ErrUnsupported, err)
	}
	for line := range strings.SplitSeq(string(content), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 || fields[0] != "cpu" {
			continue
		}
		var total, idle int64
		for i, field := range fields[1:] {
			amount, err := strconv.ParseInt(field, 10, 64)
			if err != nil {
				return 0, 0, fmt.Errorf("read %s: parse aggregate cpu field %d (%q): %w: %w", name, i+1, field, ErrMeasure, err)
			}
			if amount < 0 {
				return 0, 0, fmt.Errorf("read %s: aggregate cpu field %d is %d: %w", name, i+1, amount, ErrMeasure)
			}
			if total > (1<<62)-amount {
				return 0, 0, fmt.Errorf("read %s: the aggregate cpu counter overflows at field %d: %w", name, i+1, ErrMeasure)
			}
			total += amount
			// Fields 4 and 5 of a cpu line are idle and iowait. A host waiting on
			// disk is not running work, so both count as idle here.
			if i == 3 || i == 4 {
				idle += amount
			}
		}
		return total, idle, nil
	}
	return 0, 0, fmt.Errorf("read %s: it carries no aggregate cpu line: %w", name, ErrUnsupported)
}

// clamp keeps observed use inside capacity. A sampling race can read a use above
// capacity for an instant, and reporting that would fail validation at the far end.
func clamp(used, capacity Vector) Vector {
	used.CPUMillis = min(max(used.CPUMillis, 0), capacity.CPUMillis)
	used.MemoryBytes = min(max(used.MemoryBytes, 0), capacity.MemoryBytes)
	used.DiskBytes = min(max(used.DiskBytes, 0), capacity.DiskBytes)
	return used
}

var ErrCapacityUnavailable = errors.New("capacity_unavailable")
var ErrPortOccupied = errors.New("host_port_occupied")

// Space is what the host can still give to new work right now.
type Space struct {
	// MemoryBytes is meaningful only when MemoryKnown is true. A host with no
	// procfs cannot report it, and an unread figure must not read as zero free.
	MemoryBytes int64
	MemoryKnown bool
	DiskBytes   int64
}

// Free measures only what a precheck needs.
//
// A reservation is a claim on Synod's own books. It does not stop another process
// on the host from filling the disk, so the amounts are measured again immediately
// before an effect rather than trusted from planning time.
//
// It takes no CPU sample. A CPU rate needs an interval, and the rules this serves
// are about space, not about how busy the host is.
func (h *Host) Free() (Space, error) {
	var space Space
	if h.ProcRoot == "" || h.StoragePath == "" {
		return space, fmt.Errorf("measure free space: no proc root or storage path is declared: %w", ErrMeasure)
	}
	// The pool is the measurement this serves, and statfs answers on every host
	// this runs on. Without it there is no answer at all.
	capacityBytes, usedBytes, err := statfs(h.StoragePath)
	if err != nil {
		return space, err
	}
	if capacityBytes <= 0 {
		return space, fmt.Errorf("measure free space: %s reports a capacity of %d bytes: %w", h.StoragePath, capacityBytes, ErrMeasure)
	}
	space.DiskBytes = capacityBytes - usedBytes
	// Memory is best effort. A host with no procfs still has a measurable pool, and
	// refusing every command there would stop work for a dimension that may not
	// even be declared. An unknown figure is reported as unknown, never as zero.
	if total, available, err := h.memory(); err == nil && total > 0 {
		space.MemoryBytes, space.MemoryKnown = available, true
	}
	return space, nil
}
