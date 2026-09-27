package router

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

func (m *Manager) probeLoop() {
	tick := time.NewTicker(m.options.ProbeInterval)
	defer tick.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-tick.C:
		case <-m.wake:
		}
		m.mu.Lock()
		var backends []*backend
		for _, r := range m.routes {
			r.mu.RLock()
			backends = append(backends, r.backends...)
			r.mu.RUnlock()
		}
		m.mu.Unlock()
		jobs := make(chan *backend, len(backends))
		for _, b := range backends {
			jobs <- b
		}
		close(jobs)
		var workers sync.WaitGroup
		for range min(16, len(backends)) {
			workers.Go(func() {
				for b := range jobs {
					if m.ctx.Err() != nil {
						return
					}
					ctx, cancel := context.WithTimeout(m.ctx, m.options.ProbeTimeout)
					healthy := false
					if b.healthPath == "" {
						conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", b.address)
						if err == nil {
							healthy = true
							conn.Close()
						}
					} else {
						request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+b.address+b.healthPath, nil)
						if err == nil {
							client := &http.Client{Transport: m.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
							response, err := client.Do(request)
							if err == nil {
								healthy = response.StatusCode >= 200 && response.StatusCode < 400
								response.Body.Close()
							}
						}
					}
					cancel()
					b.healthy.Store(healthy)
				}
			})
		}
		workers.Wait()
	}
}
