// Package router serves app ingress on each participating node.
package router

import (
	"errors"
	"net/netip"
	"net/url"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

var ErrConfiguration = errors.New("invalid_route_configuration")
var ErrPortConflict = errors.New("route_port_conflict")
var ErrClosed = errors.New("router_closed")

// Validate checks a full snapshot before any listener changes.
func Validate(routes []*agentv1.AppRoute) error {
	if len(routes) > 256 {
		return ErrConfiguration
	}
	ports := map[uint32]bool{}
	total := 0
	for _, route := range routes {
		if route == nil {
			return ErrConfiguration
		}
		id, err := uuid.Parse(route.AppId)
		if err != nil || id == uuid.Nil || id.String() != route.AppId || ports[route.IngressPort] || route.IngressPort == 0 || route.IngressPort > 65535 || (route.Protocol != "http" && route.Protocol != "tcp") {
			return ErrConfiguration
		}
		ports[route.IngressPort] = true
		if route.HealthPath != "" {
			path, err := url.ParseRequestURI(route.HealthPath)
			if err != nil || path.IsAbs() || path.Host != "" || len(route.HealthPath) > 2048 || route.HealthPath[0] != '/' {
				return ErrConfiguration
			}
		}
		seen := map[string]bool{}
		total += len(route.Backends)
		if total > 4096 {
			return ErrConfiguration
		}
		for _, address := range route.Backends {
			endpoint, err := netip.ParseAddrPort(address)
			if err != nil || endpoint.Port() == 0 || endpoint.Addr().IsUnspecified() || endpoint.Addr().IsMulticast() || endpoint.Addr().Zone() != "" || seen[address] {
				return ErrConfiguration
			}
			seen[address] = true
		}
	}
	return nil
}
