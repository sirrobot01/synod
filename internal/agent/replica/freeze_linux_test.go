package replica

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCgroupFreezer freezes a real process. It needs Linux, root, and a writable cgroup v2 tree.
func TestCgroupFreezer(t *testing.T) {
	own, err := CgroupDir(os.Getpid())
	if err != nil || os.Geteuid() != 0 {
		t.Skip("needs root and cgroup v2")
	}
	dir := filepath.Join(own, fmt.Sprintf("synod-replica-test-%d", os.Getpid()))
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Skipf("cgroup tree is not writable: %v", err)
	}
	t.Cleanup(func() { os.Remove(dir) })
	sleep := exec.Command("sleep", "60")
	if err := sleep.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sleep.Process.Kill(); sleep.Wait() })
	if err := os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(fmt.Sprint(sleep.Process.Pid)), 0); err != nil {
		t.Skipf("cannot move a process into the test cgroup: %v", err)
	}
	if got, err := CgroupDir(sleep.Process.Pid); err != nil || got != dir {
		t.Fatalf("cgroup of the child: %q %v, want %q", got, err, dir)
	}
	freezer := CgroupFreezer{Dir: dir, Limit: 2 * time.Second}
	if err := freezer.Freeze(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := procState(t, sleep.Process.Pid); state == "" {
		t.Fatal("no process state")
	}
	events, _ := os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if !strings.Contains(string(events), "frozen 1\n") {
		t.Fatalf("events after freeze: %s", events)
	}
	if err := freezer.Thaw(t.Context()); err != nil {
		t.Fatal(err)
	}
	events, _ = os.ReadFile(filepath.Join(dir, "cgroup.events"))
	if !strings.Contains(string(events), "frozen 0\n") {
		t.Fatalf("events after thaw: %s", events)
	}
}

func procState(t *testing.T, pid int) string {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return ""
	}
	return fields[2]
}
