package router

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// trackedConn lets shutdown close TCP tunnels and upgraded HTTP connections too.
type trackedConn struct {
	net.Conn
	manager *Manager
	once    sync.Once
	err     error
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.manager.connectionsMu.Lock()
		delete(c.manager.connections, c)
		c.manager.connectionsMu.Unlock()
	})
	return c.err
}

func (c *trackedConn) CloseWrite() error { return c.Conn.(*net.TCPConn).CloseWrite() }

type trackedListener struct {
	net.Listener
	manager *Manager
}

func (l *trackedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.manager.connectionsMu.Lock()
		if l.manager.closed || len(l.manager.connections) >= l.manager.options.MaxConnections {
			l.manager.connectionsMu.Unlock()
			conn.Close()
			if l.manager.ctx.Err() != nil {
				return nil, net.ErrClosed
			}
			continue
		}
		tracked := &trackedConn{Conn: conn, manager: l.manager}
		l.manager.connections[tracked] = struct{}{}
		l.manager.connectionsMu.Unlock()
		return tracked, nil
	}
}

func (r *route) serveHTTP(w http.ResponseWriter, request *http.Request) {
	b := r.pick()
	if b == nil {
		http.Error(w, "no healthy backend", http.StatusServiceUnavailable)
		return
	}
	proxy := &httputil.ReverseProxy{
		Rewrite:   func(p *httputil.ProxyRequest) { p.SetURL(&url.URL{Scheme: "http", Host: b.address}); p.SetXForwarded() },
		Transport: r.manager.transport,
		ErrorHandler: func(w http.ResponseWriter, request *http.Request, _ error) {
			if request.Context().Err() == nil {
				b.healthy.Store(false)
			}
			http.Error(w, "backend unavailable", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, request)
}

func (r *route) serveTCP() {
	for {
		downstream, err := r.listener.Accept()
		if err != nil {
			return
		}
		r.manager.workers.Go(func() {
			defer downstream.Close()
			var upstream net.Conn
			// A failed dial sends no application data. Try the remaining healthy copies.
			r.mu.RLock()
			attempts := len(r.backends)
			r.mu.RUnlock()
			for range attempts {
				b := r.pick()
				if b == nil {
					return
				}
				candidate, err := (&net.Dialer{Timeout: r.manager.options.DialTimeout, KeepAlive: 30 * time.Second}).DialContext(r.manager.ctx, "tcp", b.address)
				if err != nil {
					b.healthy.Store(false)
					continue
				}
				upstream = candidate
				break
			}
			if upstream == nil {
				return
			}
			defer upstream.Close()
			stop := context.AfterFunc(r.manager.ctx, func() { upstream.Close() })
			defer stop()
			copied := make(chan struct{})
			go func() {
				_, _ = io.Copy(upstream, downstream)
				_ = upstream.(*net.TCPConn).CloseWrite()
				close(copied)
			}()
			_, _ = io.Copy(downstream, upstream)
			_ = downstream.(*trackedConn).CloseWrite()
			<-copied
		})
	}
}
