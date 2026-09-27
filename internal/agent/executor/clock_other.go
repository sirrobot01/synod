//go:build !linux

package executor

import (
	"errors"
	"time"
)

func bootTime() (time.Duration, error) {
	return 0, errors.New("writer leases require Linux CLOCK_BOOTTIME")
}
