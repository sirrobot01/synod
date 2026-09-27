package docker

import (
	"cmp"
	"context"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/moby/moby/api/types/container"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

type genericHealth struct {
	started  string
	checked  time.Time
	failures uint32
	state    string
}

func (r *Runtime) genericProbe(ctx context.Context, item container.InspectResponse, probe *agentv1.HealthCheckConfig, ports []*agentv1.PortBinding) string {
	if item.State == nil || !item.State.Running {
		return "unhealthy"
	}
	if probe == nil || probe.Type == "exec" {
		if item.State.Health == nil {
			if probe != nil {
				return "starting"
			}
			return "healthy"
		}
		return string(item.State.Health.Status)
	}
	r.healthMu.Lock()
	prior := r.health[item.ID]
	if prior.started != item.State.StartedAt {
		prior = genericHealth{started: item.State.StartedAt, state: "starting"}
	}
	if time.Since(prior.checked) < time.Duration(cmp.Or(probe.IntervalSeconds, uint32(10)))*time.Second {
		r.healthMu.Unlock()
		return prior.state
	}
	prior.checked = time.Now()
	r.health[item.ID] = prior
	r.healthMu.Unlock()
	address := ""
	for _, binding := range ports {
		if binding.ContainerPort != probe.Port || binding.Transport != "tcp" {
			continue
		}
		ip, err := netip.ParseAddr(binding.HostAddress)
		if err != nil {
			continue
		}
		if ip.IsUnspecified() {
			if ip.Is4() {
				ip = netip.MustParseAddr("127.0.0.1")
			} else {
				ip = netip.IPv6Loopback()
			}
		}
		address = net.JoinHostPort(ip.String(), strconv.FormatUint(uint64(binding.HostPort), 10))
		break
	}
	healthy := false
	if address != "" {
		check, cancel := context.WithTimeout(ctx, time.Duration(cmp.Or(probe.TimeoutSeconds, uint32(2)))*time.Second)
		defer cancel()
		if probe.Type == "tcp" {
			connection, err := (&net.Dialer{}).DialContext(check, "tcp", address)
			if err == nil {
				healthy = true
				connection.Close()
			}
		} else {
			request, err := http.NewRequestWithContext(check, http.MethodGet, "http://"+address+probe.Path, nil)
			if err == nil {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				transport.Proxy = nil
				transport.DisableKeepAlives = true
				client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
				response, err := client.Do(request)
				if err == nil {
					healthy = response.StatusCode >= 200 && response.StatusCode < 400
					response.Body.Close()
				}
				transport.CloseIdleConnections()
			}
		}
	}
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	current := r.health[item.ID]
	if current.started != prior.started || !current.checked.Equal(prior.checked) {
		return current.state
	}
	if ctx.Err() != nil {
		current.state = "unknown"
	} else if healthy {
		current.state = "healthy"
		current.failures = 0
	} else {
		current.failures++
		if current.failures >= cmp.Or(probe.UnhealthyThreshold, uint32(3)) {
			current.state = "unhealthy"
		}
	}
	r.health[item.ID] = current
	return current.state
}
