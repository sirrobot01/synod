package capacity

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// procfs writes a fixture proc tree so a test measures a known host rather than
// the machine running the test.
func procfs(t *testing.T, meminfo, first, second string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meminfo"), []byte(meminfo), 0o600); err != nil {
		t.Fatal(err)
	}
	// The CPU rate needs two readings, so the fixture rewrites stat after the first.
	original := filepath.Join(dir, "stat")
	if err := os.WriteFile(original, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	// The rewrite must not outlive the test. A write into a directory the testing
	// package is removing fails that removal, which fails the test that owns it.
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		select {
		case <-stop:
			return
		case <-time.After(20 * time.Millisecond):
		}
		_ = os.WriteFile(original, []byte(second), 0o600)
	}()
	return dir
}

const meminfo = "MemTotal:       16384000 kB\nMemAvailable:    4096000 kB\nCached: 100 kB\n"

// fakeDisk replaces the filesystem call for one test.
func fakeDisk(t *testing.T, capacityBytes, usedBytes int64, err error) {
	t.Helper()
	previous := statfs
	statfs = func(string) (int64, int64, error) { return capacityBytes, usedBytes, err }
	t.Cleanup(func() { statfs = previous })
}

func TestMeasureReadsCapacityAndUse(t *testing.T) {
	fakeDisk(t, 900<<30, 340<<30, nil)
	// 1000 jiffies pass. Idle plus iowait rises by 250, so the host is 75% busy.
	root := procfs(t, meminfo,
		"cpu  100 0 100 700 100 0 0 0 0 0\n",
		"cpu  600 0 350 950 100 0 0 0 0 0\n")
	found, err := Host{ProcRoot: root, StoragePath: "/pool", Sample: 60 * time.Millisecond}.Measure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if found.Architecture != runtime.GOARCH {
		t.Fatalf("architecture=%q, want the measured one", found.Architecture)
	}
	if found.Capacity.MemoryBytes != 16384000*1024 {
		t.Fatalf("memory capacity=%d", found.Capacity.MemoryBytes)
	}
	// Available memory is what a new workload can take, so use is total less that.
	if found.Used.MemoryBytes != (16384000-4096000)*1024 {
		t.Fatalf("memory used=%d", found.Used.MemoryBytes)
	}
	if found.Capacity.DiskBytes != 900<<30 || found.Used.DiskBytes != 340<<30 {
		t.Fatalf("disk=%+v", found.Capacity)
	}
	cores := int64(runtime.NumCPU()) * 1000
	if found.Capacity.CPUMillis != cores {
		t.Fatalf("cpu capacity=%d, want %d", found.Capacity.CPUMillis, cores)
	}
	// 750 of 1000 jiffies were busy.
	if want := cores * 750 / 1000; found.Used.CPUMillis != want {
		t.Fatalf("cpu used=%d, want %d", found.Used.CPUMillis, want)
	}
}

func TestMeasureCountsIowaitAsIdle(t *testing.T) {
	fakeDisk(t, 100<<30, 0, nil)
	// Every added jiffy lands in iowait. A host waiting on disk runs no work.
	root := procfs(t, meminfo,
		"cpu  100 0 100 700 100 0 0 0 0 0\n",
		"cpu  100 0 100 700 1100 0 0 0 0 0\n")
	found, err := Host{ProcRoot: root, StoragePath: "/pool", Sample: 60 * time.Millisecond}.Measure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if found.Used.CPUMillis != 0 {
		t.Fatalf("cpu used=%d, want an idle host", found.Used.CPUMillis)
	}
}

func TestMeasureRefusesAnUnmeasurableHost(t *testing.T) {
	fakeDisk(t, 900<<30, 0, nil)
	for name, procs := range map[string][3]string{
		// A counter that did not move says nothing about use.
		"still cpu counter": {meminfo, "cpu  1 0 1 1 1\n", "cpu  1 0 1 1 1\n"},
		// Available above total is a broken read, not an empty host.
		"impossible memory": {"MemTotal: 100 kB\nMemAvailable: 900 kB\n", "cpu  1 0 1 1 1\n", "cpu  9 0 9 9 9\n"},
		"missing memory":    {"Cached: 10 kB\n", "cpu  1 0 1 1 1\n", "cpu  9 0 9 9 9\n"},
	} {
		t.Run(name, func(t *testing.T) {
			root := procfs(t, procs[0], procs[1], procs[2])
			if _, err := (Host{ProcRoot: root, StoragePath: "/pool", Sample: 40 * time.Millisecond}).Measure(t.Context()); err == nil {
				t.Fatal("an unmeasurable host reported a measurement")
			}
		})
	}
}

func TestMeasureRefusesAHostWithoutProcfs(t *testing.T) {
	fakeDisk(t, 900<<30, 0, nil)
	_, err := Host{ProcRoot: filepath.Join(t.TempDir(), "absent"), StoragePath: "/pool"}.Measure(t.Context())
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("err=%v, want ErrUnsupported", err)
	}
}

// A zero capacity would pass a nonnegative check and then place nothing.
func TestMeasureRefusesAnEmptyPool(t *testing.T) {
	fakeDisk(t, 0, 0, nil)
	root := procfs(t, meminfo, "cpu  1 0 1 1 1\n", "cpu  9 0 9 9 9\n")
	if _, err := (Host{ProcRoot: root, StoragePath: "/pool", Sample: 40 * time.Millisecond}).Measure(t.Context()); !errors.Is(err, ErrMeasure) {
		t.Fatalf("err=%v, want ErrMeasure", err)
	}
}

// A sampling race can read a use above capacity for an instant.
func TestMeasureKeepsUseInsideCapacity(t *testing.T) {
	fakeDisk(t, 100, 900, nil)
	root := procfs(t, meminfo, "cpu  1 0 1 1 1\n", "cpu  9 0 9 9 9\n")
	found, err := Host{ProcRoot: root, StoragePath: "/pool", Sample: 40 * time.Millisecond}.Measure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if found.Used.DiskBytes != 100 {
		t.Fatalf("disk used=%d, want it clamped to capacity", found.Used.DiskBytes)
	}
}

// The real filesystem call must answer for a path that exists.
func TestDiskUsageMeasuresARealFilesystem(t *testing.T) {
	capacityBytes, usedBytes, err := diskUsage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if capacityBytes <= 0 || usedBytes < 0 || usedBytes > capacityBytes {
		t.Fatalf("capacity=%d used=%d", capacityBytes, usedBytes)
	}
}

// A host with no procfs still has a measurable pool. Refusing every command there
// would stop work for a dimension that may not even be declared.
func TestFreeMeasuresThePoolWithoutProcfs(t *testing.T) {
	fakeDisk(t, 900<<30, 340<<30, nil)
	space, err := Host{ProcRoot: filepath.Join(t.TempDir(), "absent"), StoragePath: "/pool"}.Free()
	if err != nil {
		t.Fatal(err)
	}
	if space.DiskBytes != (900<<30)-(340<<30) {
		t.Fatalf("disk free=%d", space.DiskBytes)
	}
	// An unknown figure must never read as zero free.
	if space.MemoryKnown || space.MemoryBytes != 0 {
		t.Fatalf("memory=%+v, want it reported as unknown", space)
	}
}

func TestFreeReportsMemoryWhenTheHostCanAnswer(t *testing.T) {
	fakeDisk(t, 100<<30, 0, nil)
	root := procfs(t, meminfo, "cpu  1 0 1 1 1\n", "cpu  9 0 9 9 9\n")
	space, err := Host{ProcRoot: root, StoragePath: "/pool"}.Free()
	if err != nil {
		t.Fatal(err)
	}
	if !space.MemoryKnown || space.MemoryBytes != 4096000*1024 {
		t.Fatalf("memory=%+v, want the measured amount", space)
	}
}

// Without a pool there is no answer at all.
func TestFreeRefusesWithoutAPool(t *testing.T) {
	fakeDisk(t, 0, 0, ErrUnsupported)
	if _, err := (Host{ProcRoot: t.TempDir(), StoragePath: "/pool"}).Free(); err == nil {
		t.Fatal("an unmeasurable pool reported free space")
	}
	if _, err := (Host{ProcRoot: t.TempDir()}).Free(); err == nil {
		t.Fatal("a host with no declared pool reported free space")
	}
}
