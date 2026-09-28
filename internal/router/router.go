package router

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Options sets local listener and probe bounds.
type Options struct {
	ListenAddress  string
	ProbeInterval  time.Duration
	ProbeTimeout   time.Duration
	DialTimeout    time.Duration
	MaxConnections int
}

// Manager owns ingress ports and updates backends without replacing listeners.
type Manager struct {
	mu            sync.Mutex
	routes        map[string]*route
	ctx           context.Context
	cancel        context.CancelFunc
	options       Options
	wake          chan struct{}
	workers       sync.WaitGroup
	transport     *http.Transport
	connectionsMu sync.Mutex
	connections   map[*trackedConn]struct{}
	closed        bool
}

type route struct {
	manager  *Manager
	protocol string
	port     uint32
	listener net.Listener
	server   *http.Server
	mu       sync.RWMutex
	backends []*backend
	next     uint64
}

type backend struct {
	address    string
	healthPath string
	healthy    atomic.Bool
}

// New starts health checks. Close releases its listeners and connections.
func New(ctx context.Context, options Options) (*Manager, error) {
	if options.ListenAddress == "" {
		options.ListenAddress = "0.0.0.0"
	}
	if options.ProbeInterval == 0 {
		options.ProbeInterval = 2 * time.Second
	}
	if options.ProbeTimeout == 0 {
		options.ProbeTimeout = time.Second
	}
	if options.DialTimeout == 0 {
		options.DialTimeout = 2 * time.Second
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = 4096
	}
	if _, err := netip.ParseAddr(options.ListenAddress); err != nil || options.ProbeInterval < 10*time.Millisecond || options.ProbeTimeout <= 0 || options.ProbeTimeout > time.Minute || options.DialTimeout <= 0 || options.DialTimeout > time.Minute || options.MaxConnections < 1 {
		return nil, ErrConfiguration
	}
	ctx, cancel := context.WithCancel(ctx)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: options.DialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 16
	m := &Manager{routes: map[string]*route{}, ctx: ctx, cancel: cancel, options: options, wake: make(chan struct{}, 1), transport: transport, connections: map[*trackedConn]struct{}{}}
	m.workers.Go(m.probeLoop)
	return m, nil
}

// Apply reserves every new port before it publishes the snapshot.
func (m *Manager) Apply(ctx context.Context, routes []*agentv1.AppRoute) error {
	if err := Validate(routes); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	staged := map[string]*route{}
	rollback := true
	defer func() {
		if rollback {
			for _, r := range staged {
				r.listener.Close()
			}
		}
	}()
	// A route is one app port. A tier 1 app owns a writer port and a reader port.
	key := func(config *agentv1.AppRoute) string {
		return config.AppId + "/" + strconv.FormatUint(uint64(config.IngressPort), 10)
	}
	for _, config := range routes {
		if old := m.routes[key(config)]; old != nil && old.protocol == config.Protocol {
			continue
		}
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", net.JoinHostPort(m.options.ListenAddress, strconv.FormatUint(uint64(config.IngressPort), 10)))
		if err != nil {
			return errors.Join(ErrPortConflict, err)
		}
		staged[key(config)] = &route{manager: m, protocol: config.Protocol, port: config.IngressPort, listener: &trackedListener{Listener: listener, manager: m}}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	next := make(map[string]*route, len(routes))
	for _, config := range routes {
		r := staged[key(config)]
		if r == nil {
			r = m.routes[key(config)]
		}
		r.mu.Lock()
		existing := map[string]*backend{}
		for _, b := range r.backends {
			existing[b.address+"\x00"+b.healthPath] = b
		}
		backends := make([]*backend, 0, len(config.Backends))
		for _, address := range config.Backends {
			b := existing[address+"\x00"+config.HealthPath]
			if b == nil {
				b = &backend{address: address, healthPath: config.HealthPath}
			}
			backends = append(backends, b)
		}
		r.backends = backends
		r.mu.Unlock()
		next[key(config)] = r
	}
	for id, old := range m.routes {
		if next[id] != old {
			old.listener.Close()
			if old.server != nil {
				old.server.SetKeepAlivesEnabled(false)
			}
		}
	}
	m.routes = next
	for _, r := range staged {
		if r.protocol == "http" {
			r.server = &http.Server{Handler: http.HandlerFunc(r.serveHTTP), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20, BaseContext: func(net.Listener) context.Context { return m.ctx }}
			m.workers.Go(func() { _ = r.server.Serve(r.listener) })
		} else {
			m.workers.Go(r.serveTCP)
		}
	}
	rollback = false
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}

// Close stops listeners, probes, and open connections.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.cancel()
	for _, r := range m.routes {
		r.listener.Close()
	}
	m.connectionsMu.Lock()
	m.closed = true
	connections := make([]*trackedConn, 0, len(m.connections))
	for conn := range m.connections {
		connections = append(connections, conn)
	}
	m.connectionsMu.Unlock()
	m.mu.Unlock()
	for _, conn := range connections {
		conn.Close()
	}
	m.transport.CloseIdleConnections()
	m.workers.Wait()
	return nil
}

func (r *route) pick() *backend {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.backends) == 0 {
		return nil
	}
	for range r.backends {
		b := r.backends[r.next%uint64(len(r.backends))]
		r.next++
		if b.healthy.Load() {
			return b
		}
	}
	return nil
}
