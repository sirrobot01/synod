//go:build !linux

package host

import (
	"context"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

func Sockets() ([]Socket, error) { return nil, ErrUnverified }

func ContainerSockets(context.Context, *client.Client, container.InspectResponse) ([]Socket, error) {
	return nil, ErrUnverified
}

func PublishedSockets(container.InspectResponse) ([]Socket, error) { return nil, ErrUnverified }
