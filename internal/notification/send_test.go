package notification

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendHTTP(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || !strings.Contains(string(body), "Synod test") {
			t.Error("missing notification body")
		}
		if fail.Load() {
			http.Error(w, "secret-url-token", http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	settings := DefaultSettings()
	settings.URL = "generic+" + server.URL + "/secret-url-token"
	if err := ValidateService(settings); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("validation sent a request")
	}
	if err := Send(t.Context(), settings, "Synod test"); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := Send(t.Context(), settings, "Synod test"); !errors.Is(err, ErrDelivery) || strings.Contains(err.Error(), "secret-url-token") {
		t.Fatalf("unsafe failure: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatal("wrong send count")
	}
}

func TestSendRefusesCrossOriginRedirect(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer origin.Close()
	settings := DefaultSettings()
	settings.URL = "generic+" + origin.URL + "/secret"
	if err := Send(t.Context(), settings, "test"); !errors.Is(err, ErrDelivery) {
		t.Fatalf("redirect error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("redirect leaked notification")
	}
}

func TestSendCancellation(t *testing.T) {
	t.Run("HTTP", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
		defer server.Close()
		settings := DefaultSettings()
		settings.URL = "generic+" + server.URL
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := Send(ctx, settings, "test"); !errors.Is(err, ErrDelivery) {
			t.Fatal(err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("HTTP cancellation exceeded one second")
		}
	})
	t.Run("SMTP greeting", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			conn, err := listener.Accept()
			if err == nil {
				defer conn.Close()
				io.Copy(io.Discard, conn)
			}
		}()
		settings := DefaultSettings()
		settings.URL = "smtp://" + listener.Addr().String() + "/?fromaddress=alerts@example.com&toaddresses=ops@example.com&auth=None&encryption=None&usestarttls=false"
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := Send(ctx, settings, "test"); !errors.Is(err, ErrDelivery) {
			t.Fatal(err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("SMTP cancellation exceeded one second")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("SMTP socket remained open")
		}
	})
}

func TestSendReportsPermanentHTTPRejection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "private-token", http.StatusUnauthorized) }))
	defer server.Close()
	settings := DefaultSettings()
	settings.URL = "generic+" + server.URL
	if err := Send(t.Context(), settings, "test"); !errors.Is(err, ErrRejected) {
		t.Fatalf("permanent rejection = %v", err)
	}
}

func TestSendReportsPermanentSMTPRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			defer conn.Close()
			io.WriteString(conn, "550 private-server-error\r\n")
		}
	}()
	settings := DefaultSettings()
	settings.URL = "smtp://" + listener.Addr().String() + "/?fromaddress=alerts@example.com&toaddresses=ops@example.com&auth=None&encryption=None&usestarttls=false"
	if err := Send(t.Context(), settings, "test"); !errors.Is(err, ErrRejected) {
		t.Fatalf("SMTP rejection = %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SMTP session did not close")
	}
}
