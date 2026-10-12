//go:build !linux

package restore

import (
	"context"
	"errors"
	"net"
)

var errNamespace = errors.New("network namespace unavailable")

// dialIn needs Linux network namespaces.
func dialIn(context.Context, int, string) (net.Conn, error) { return nil, errNamespace }
