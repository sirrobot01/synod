package postgres

import (
	"net/netip"
	"strings"
	"testing"
)

var routeMaterial = PatroniTLSMaterial{CA: []byte("ca"), Certificate: []byte("certificate\n"), PrivateKey: []byte("key\n")}

// routePassword holds a double quote, which the auth file must double.
const routePassword = `application-password-with-a-"-quote-0123`

func TestPoolerFiles(t *testing.T) {
	settings := PoolerSettings{Address: netip.MustParseAddr("10.0.0.2"), Port: 6432, DatabasePort: 5432, RESTPort: 8008}
	files, err := PoolerFiles(settings, routePassword, routeMaterial)
	if err != nil {
		t.Fatal(err)
	}
	config := string(files["pgbouncer.ini"])
	for _, want := range []string{
		"app = host=10.0.0.2 port=5432 dbname=app", "listen_addr = 10.0.0.2, 127.0.0.2", "listen_port = 6432", "auth_type = scram-sha-256",
		"pool_mode = session", "track_extra_parameters = IntervalStyle, search_path", "client_tls_sslmode = require", "server_tls_sslmode = verify-full", "admin_users =\n",
	} {
		if !strings.Contains(config, want) {
			t.Errorf("pgbouncer.ini lacks %q", want)
		}
	}
	if string(files["role.json"]) != `{"address": "10.0.0.2", "rest_port": 8008}`+"\n" {
		t.Errorf("role.json: %q", files["role.json"])
	}
	if string(files["userlist.txt"]) != `"synod_app" "application-password-with-a-""-quote-0123"`+"\n" || len(files) != 6 {
		t.Fatalf("pooler files: %q", files["userlist.txt"])
	}
	for name, change := range map[string]func(*PoolerSettings, *string){
		"loopback":        func(s *PoolerSettings, _ *string) { s.Address = netip.MustParseAddr("127.0.0.1") },
		"same ports":      func(s *PoolerSettings, _ *string) { s.Port = s.DatabasePort },
		"no REST port":    func(s *PoolerSettings, _ *string) { s.RESTPort = 0 },
		"REST on pooler":  func(s *PoolerSettings, _ *string) { s.RESTPort = s.Port },
		"short password":  func(_ *PoolerSettings, v *string) { *v = "password" },
		"spaced password": func(_ *PoolerSettings, v *string) { *v = routePassword + " x" },
	} {
		bad, verifier := settings, routePassword
		change(&bad, &verifier)
		if _, err := PoolerFiles(bad, verifier, routeMaterial); err == nil {
			t.Errorf("%s: pooler accepted", name)
		}
	}
}

func TestRouterFiles(t *testing.T) {
	backends := []RouterBackend{
		{Address: netip.MustParseAddr("10.0.0.2"), PoolerPort: 6432, RESTPort: 8008},
		{Address: netip.MustParseAddr("10.0.0.3"), PoolerPort: 6432, RESTPort: 8008},
		{Address: netip.MustParseAddr("fd00::4"), PoolerPort: 6432, RESTPort: 8008},
	}
	settings := RouterSettings{Address: netip.MustParseAddr("10.0.0.2"), WriterPort: 5000, ReaderPort: 5001, Backends: backends}
	files, err := RouterFiles(settings, routeMaterial)
	if err != nil {
		t.Fatal(err)
	}
	config := string(files["haproxy.cfg"])
	for _, want := range []string{
		"bind 10.0.0.2:5000", "bind 127.0.0.2:5000", "bind 10.0.0.2:5001", "bind 127.0.0.2:5001", "uri /primary\n", "uri /replica?lag=16777216\n",
		"server member1 10.0.0.2:6432 check port 8008 check-ssl verify required", "server member3 [fd00::4]:6432",
		"on-marked-down shutdown-sessions", "mode tcp", "init-state down",
	} {
		if !strings.Contains(config, want) {
			t.Errorf("haproxy.cfg lacks %q", want)
		}
	}
	if string(files["check.pem"]) != "certificate\nkey\n" || len(files) != 3 {
		t.Fatalf("router files: %q", files["check.pem"])
	}
	settings.MaxLagBytes = 1024
	if files, err := RouterFiles(settings, routeMaterial); err != nil || !strings.Contains(string(files["haproxy.cfg"]), "uri /replica?lag=1024\n") {
		t.Fatal("configured lag limit was not rendered", err)
	}
	for name, change := range map[string]func(*RouterSettings){
		"same ports":         func(s *RouterSettings) { s.ReaderPort = s.WriterPort },
		"no backends":        func(s *RouterSettings) { s.Backends = nil },
		"repeated backend":   func(s *RouterSettings) { s.Backends = []RouterBackend{backends[0], backends[0]} },
		"negative lag":       func(s *RouterSettings) { s.MaxLagBytes = -1 },
		"local port overlap": func(s *RouterSettings) { s.WriterPort = 6432 },
		"zero REST port": func(s *RouterSettings) {
			s.Backends = []RouterBackend{{Address: backends[1].Address, PoolerPort: 6432}}
		},
	} {
		bad := settings
		change(&bad)
		if _, err := RouterFiles(bad, routeMaterial); err == nil {
			t.Errorf("%s: router accepted", name)
		}
	}
}
