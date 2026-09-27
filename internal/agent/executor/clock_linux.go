package executor

import (
	"time"

	"golang.org/x/sys/unix"
)

func bootTime() (time.Duration, error) {
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
		return 0, err
	}
	return time.Duration(now.Nano()), nil
}
