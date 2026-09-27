// Package replica keeps crash-consistent copies of a tier 2 app's volumes.
//
// The active device freezes the app briefly, copies its volumes, and describes
// the copy in a Manifest. A standby device rebuilds that copy from its previous
// replica and the blocks it lacks. Specification 38 defines the method.
package replica

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrFreezeTimeout reports a freeze that did not finish in time. The app is thawed again.
var ErrFreezeTimeout = errors.New("replica: freeze did not finish in time")

// thawLimit is long because a thaw that fails leaves the app stopped.
const thawLimit = 10 * time.Second

// Freezer stops and resumes every process of one app container.
type Freezer interface {
	Freeze(ctx context.Context) error
	Thaw(ctx context.Context) error
}

// CgroupFreezer freezes one cgroup v2 directory. It does not use docker pause,
// which can report a timeout and still leave the cgroup frozen.
type CgroupFreezer struct {
	Dir   string
	Limit time.Duration
}

// Freeze waits until every task in the cgroup is frozen.
// On any failure it thaws the cgroup before it returns.
func (f CgroupFreezer) Freeze(ctx context.Context) error {
	err := f.set(ctx, true, f.Limit)
	if err == nil {
		return nil
	}
	if thawErr := f.Thaw(ctx); thawErr != nil {
		return errors.Join(err, thawErr)
	}
	return err
}

// Thaw resumes the cgroup and confirms it. A cancelled ctx does not stop a thaw.
func (f CgroupFreezer) Thaw(ctx context.Context) error {
	return f.set(context.WithoutCancel(ctx), false, thawLimit)
}

func (f CgroupFreezer) set(ctx context.Context, frozen bool, limit time.Duration) error {
	value := "0"
	if frozen {
		value = "1"
	}
	if err := os.WriteFile(filepath.Join(f.Dir, "cgroup.freeze"), []byte(value), 0); err != nil {
		return fmt.Errorf("replica: write cgroup.freeze: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		events, err := os.ReadFile(filepath.Join(f.Dir, "cgroup.events"))
		if err != nil {
			return fmt.Errorf("replica: read cgroup.events: %w", err)
		}
		if strings.Contains(string(events), "frozen "+value+"\n") {
			return nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrFreezeTimeout
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// CgroupDir returns the cgroup v2 directory of a process, such as a container's main process.
func CgroupDir(pid int) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(data)) {
		path, ok := strings.CutPrefix(strings.TrimSpace(line), "0::")
		if ok && strings.HasPrefix(path, "/") {
			return filepath.Join("/sys/fs/cgroup", filepath.Clean(path)), nil
		}
	}
	return "", fmt.Errorf("replica: process %d has no cgroup v2 entry", pid)
}
