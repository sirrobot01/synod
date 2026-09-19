package docker

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/runtime"
)

// usageWorkers bounds the concurrent stats reads. Each one asks the daemon for a
// sample pair, which costs about a second, so they run together to fit a probe
// window. The bound keeps a busy node from opening a stream for every container.
const usageWorkers = 8

// maxUsageResources bounds one request. Placement never itemizes more than this.
const maxUsageResources = 512

// Usage measures what the named owned resources consume on this node.
//
// A resource this node does not own is absent from the answer. Reporting zero for
// it would be a claim about a host that was never examined, and the controller
// would attribute free capacity that nobody measured.
func (r *Runtime) Usage(ctx context.Context, resources []uuid.UUID) ([]runtime.ResourceUsage, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	if len(resources) > maxUsageResources {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	wanted := make(map[uuid.UUID]bool, len(resources))
	for _, id := range resources {
		if id == uuid.Nil {
			return nil, &runtime.Error{Kind: runtime.ErrUnsupportedConfiguration}
		}
		wanted[id] = true
	}
	owned := client.Filters{}.Add("label", labelInstallation+"="+r.options.InstallationID.String(), labelNode+"="+r.options.NodeID.String())

	// One list call carries every writable-layer size, so no per-container round
	// trip is needed for disk.
	containers, err := r.api.ContainerList(ctx, client.ContainerListOptions{All: true, Size: true, Filters: owned})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	found := make(map[uuid.UUID]*runtime.ResourceUsage, len(resources))
	native := make(map[uuid.UUID]string, len(resources))
	for _, item := range containers.Items {
		id, ok := ownedResource(item.Labels, wanted)
		if !ok {
			continue
		}
		entry := usageFor(found, id)
		entry.Used.DiskBytes += max(item.SizeRw, 0)
		native[id] = item.ID
	}

	// A database keeps its data in a volume, so a writable layer alone would
	// under-report it. One system call carries every volume size.
	//
	// Verbose is what fills the per-volume items; without it the daemon answers
	// with totals only. The daemon sizes every volume on the host to do it, so
	// this is the expensive part of a probe. The caller's deadline bounds it, and
	// a probe that runs out of time fails rather than reporting a partial figure.
	usage, err := r.api.DiskUsage(ctx, client.DiskUsageOptions{Volumes: true, Verbose: true})
	if err != nil {
		return nil, &runtime.Error{Kind: runtime.ErrOutcomeUnknown, Cause: err}
	}
	for _, item := range usage.Volumes.Items {
		if item.UsageData == nil || item.UsageData.Size < 0 {
			continue
		}
		id, ok := ownedResource(item.Labels, wanted)
		if !ok {
			continue
		}
		usageFor(found, id).Used.DiskBytes += item.UsageData.Size
	}

	if err := r.sampleContainers(ctx, native, found); err != nil {
		return nil, err
	}
	answer := make([]runtime.ResourceUsage, 0, len(found))
	for _, entry := range found {
		answer = append(answer, *entry)
	}
	return answer, nil
}

// ownedResource reads the resource identity from a native object's labels and
// reports whether the caller asked about it.
func ownedResource(labels map[string]string, wanted map[uuid.UUID]bool) (uuid.UUID, bool) {
	id, err := uuid.Parse(labels[labelResource])
	if err != nil || id == uuid.Nil || !wanted[id] {
		return uuid.Nil, false
	}
	return id, true
}

// usageFor returns the entry for one resource, creating it on first sight.
func usageFor(found map[uuid.UUID]*runtime.ResourceUsage, id uuid.UUID) *runtime.ResourceUsage {
	if entry, ok := found[id]; ok {
		return entry
	}
	entry := &runtime.ResourceUsage{ResourceID: id}
	found[id] = entry
	return entry
}

// sampleContainers reads one CPU and memory sample for each owned container.
//
// CPU is a rate, so the daemon is asked for a previous sample and the delta is
// taken against it. A container that cannot be sampled keeps the disk figure
// already measured rather than failing the whole probe: a stopped container is
// ordinary, and one unreadable container must not blind placement to the node.
func (r *Runtime) sampleContainers(ctx context.Context, native map[uuid.UUID]string, found map[uuid.UUID]*runtime.ResourceUsage) error {
	type job struct {
		id        uuid.UUID
		container string
	}
	jobs := make(chan job)
	var mu sync.Mutex
	var workers sync.WaitGroup
	for range min(usageWorkers, max(len(native), 1)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range jobs {
				amount, ok := r.sample(ctx, task.container)
				if !ok {
					continue
				}
				mu.Lock()
				entry := found[task.id]
				entry.Used.CPUMillis += amount.CPUMillis
				entry.Used.MemoryBytes += amount.MemoryBytes
				mu.Unlock()
			}
		}()
	}
	for id, containerID := range native {
		select {
		case jobs <- job{id: id, container: containerID}:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	return ctx.Err()
}

// sample reads one stats record and converts it to millicores and bytes.
func (r *Runtime) sample(ctx context.Context, containerID string) (runtime.Amount, bool) {
	var amount runtime.Amount
	result, err := r.api.ContainerStats(ctx, containerID, client.ContainerStatsOptions{IncludePreviousSample: true})
	if err != nil {
		return amount, false
	}
	defer result.Body.Close()
	var stats container.StatsResponse
	if err := json.NewDecoder(result.Body).Decode(&stats); err != nil {
		return amount, false
	}
	// A container with no prior sample yields no rate. Reporting the cumulative
	// counter as a rate would claim an impossible amount of CPU.
	elapsed := int64(stats.CPUStats.SystemUsage) - int64(stats.PreCPUStats.SystemUsage)
	busy := int64(stats.CPUStats.CPUUsage.TotalUsage) - int64(stats.PreCPUStats.CPUUsage.TotalUsage)
	cores := int64(stats.CPUStats.OnlineCPUs)
	if elapsed > 0 && busy >= 0 && cores > 0 {
		amount.CPUMillis = busy * cores * 1000 / elapsed
	}
	// Cache is reclaimable, so counting it would hold a node back from work it
	// could actually take.
	used := int64(stats.MemoryStats.Usage)
	if cache, ok := stats.MemoryStats.Stats["inactive_file"]; ok && int64(cache) <= used {
		used -= int64(cache)
	}
	amount.MemoryBytes = max(used, 0)
	return amount, true
}
