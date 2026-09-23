package capacity

import (
	"fmt"
	"syscall"
)

// diskUsage reports the capacity and used bytes of the filesystem holding one path.
//
// Capacity is the total size of the pool, and used is what is not free to a
// non-privileged writer. Blocks reserved for root count as used, because a
// workload cannot write into them.
//
// The agent config already uses syscall directly, and this needs one call that
// the portable os package does not expose.
func diskUsage(path string) (int64, int64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w: %w", path, ErrUnsupported, err)
	}
	size := int64(fs.Bsize)
	blocks, available := int64(fs.Blocks), int64(fs.Bavail)
	if size <= 0 || blocks <= 0 || available < 0 || available > blocks || blocks > (1<<62)/size {
		return 0, 0, fmt.Errorf("statfs %s: block size %d, %d blocks, %d available: %w", path, size, blocks, available, ErrMeasure)
	}
	return blocks * size, (blocks - available) * size, nil
}
