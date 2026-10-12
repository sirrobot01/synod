package restore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

// errNamespace means that the agent cannot enter the network namespace. This needs root.
var errNamespace = errors.New("network namespace unavailable")

// dialIn connects from inside the network namespace of process pid. The drill network
// publishes no ports, so the host cannot reach the app in another way.
func dialIn(ctx context.Context, pid int, address string) (net.Conn, error) {
	if pid <= 0 {
		return nil, errNamespace
	}
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		// A thread that cannot return to its namespace must not run other goroutines.
		// Without UnlockOSThread, the runtime ends the thread with this goroutine.
		runtime.LockOSThread()
		original, err := unix.Open("/proc/thread-self/ns/net", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			done <- result{err: errors.Join(errNamespace, err)}
			return
		}
		defer unix.Close(original)
		target, err := unix.Open(fmt.Sprintf("/proc/%d/ns/net", pid), unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			runtime.UnlockOSThread()
			done <- result{err: errors.Join(errNamespace, err)}
			return
		}
		defer unix.Close(target)
		if err := unix.Setns(target, unix.CLONE_NEWNET); err != nil {
			runtime.UnlockOSThread()
			done <- result{err: errors.Join(errNamespace, err)}
			return
		}
		// The socket belongs to the namespace of the thread that creates it.
		conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", address)
		if unix.Setns(original, unix.CLONE_NEWNET) == nil {
			runtime.UnlockOSThread()
		}
		done <- result{conn, err}
	}()
	r := <-done
	return r.conn, r.err
}
