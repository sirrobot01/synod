package executor

import (
	"testing"
	"time"
)

func TestBootTimeAdvances(t *testing.T) {
	before, err := bootTime()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	after, err := bootTime()
	if err != nil {
		t.Fatal(err)
	}
	if before <= 0 || after-before < 10*time.Millisecond {
		t.Fatalf("invalid boot clock: %v -> %v", before, after)
	}
}
