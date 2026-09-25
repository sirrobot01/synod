package postgres

import (
	_ "embed"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// Protected source and private runtime directories of the pooler and the router.
const (
	PoolerSourceDirectory = "/etc/synod-pooler"
	PoolerDirectory       = "/run/synod-pooler"
	RouterSourceDirectory = "/etc/synod-router"
	RouterDirectory       = "/run/synod-router"
)

// DefaultReaderLagBytes is the replay lag above which a replica leaves the reader route.
const DefaultReaderLagBytes = 16 << 20

// maxRouterBackends bounds one router. A Patroni cluster has three members.
const maxRouterBackends = 8

//go:embed route_start.py
var routeStartup string

// PoolerSettings places one PgBouncer beside its member.
type PoolerSettings struct {
	// Address is the member address. The pooler listens on it and reaches PostgreSQL on it,
	// so the server certificate check names the member.
	Address      netip.Addr
	Port         uint16
	DatabasePort uint16
	// RESTPort is the member's Patroni REST port. The pooler reads the member role there.
	RESTPort uint16
}

// RouterBackend is one member as the router sees it.
type RouterBackend struct {
	Address    netip.Addr
	PoolerPort uint16
	RESTPort   uint16
}

// RouterSettings places one HAProxy on a member node.
type RouterSettings struct {
	Address     netip.Addr
	WriterPort  uint16
	ReaderPort  uint16
	MaxLagBytes int64
	Backends    []RouterBackend
}

// PoolerFiles renders the PgBouncer files. Clients authenticate to PgBouncer with SCRAM, and PgBouncer
// logs in to PostgreSQL with the application password. SCRAM pass-through from a verifier is not used:
// in PgBouncer 1.25, one failed client login in a new pool leaves it without keys, and every later
// server login fails until PgBouncer restarts. The password stays in the protected private files.
// PgBouncer presents the member certificate to clients and verifies PostgreSQL against the native CA.
//
// PgBouncer sends each new client the startup parameters of the pool's first server login, and it
// never updates in_hot_standby. After a promotion, clients that ask for a writer would refuse the
// new primary. So role.json names the member's Patroni REST API, and the startup program restarts
// PgBouncer when the member's role changes.
func PoolerFiles(settings PoolerSettings, applicationPassword string, member PatroniTLSMaterial) (map[string][]byte, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	if len(applicationPassword) < 32 || len(applicationPassword) > 256 || strings.ContainsFunc(applicationPassword, func(r rune) bool { return r < '!' || r > '~' }) {
		return nil, errors.New("pooler application password must be 32 to 256 visible ASCII bytes")
	}
	if len(member.CA) == 0 || len(member.Certificate) == 0 || len(member.PrivateKey) == 0 {
		return nil, errors.New("pooler TLS material is incomplete")
	}
	address := settings.Address.String()
	var config strings.Builder
	fmt.Fprintf(&config, "[databases]\napp = host=%s port=%d dbname=app\n\n", address, settings.DatabasePort)
	// The pooler also listens on the loopback readiness address, so the local agent can check it.
	fmt.Fprintf(&config, "[pgbouncer]\nlisten_addr = %s, %s\nlisten_port = %d\nunix_socket_dir =\n", address, ReadinessLoopback(settings.Address).Addr(), settings.Port)
	config.WriteString("auth_type = scram-sha-256\nauth_file = " + PoolerDirectory + "/userlist.txt\n")
	config.WriteString("pool_mode = session\nmax_client_conn = 200\ndefault_pool_size = 20\n")
	// PgBouncer refuses startup parameters it does not track. It forwards tracked ones to the
	// server for each client, so a client's search_path is kept rather than dropped.
	config.WriteString("admin_users =\nstats_users =\nignore_startup_parameters = extra_float_digits\ntrack_extra_parameters = IntervalStyle, search_path\n")
	config.WriteString("client_tls_sslmode = require\nclient_tls_protocols = secure\n")
	config.WriteString("client_tls_cert_file = " + PoolerDirectory + "/member.crt\nclient_tls_key_file = " + PoolerDirectory + "/member.key\n")
	config.WriteString("server_tls_sslmode = verify-full\nserver_tls_protocols = secure\nserver_tls_ca_file = " + PoolerDirectory + "/ca.crt\n")
	config.WriteString("logfile =\npidfile =\nlog_connections = 0\nlog_disconnections = 0\n")
	return map[string][]byte{
		"pgbouncer.ini": []byte(config.String()),
		// The auth file escapes a double quote by doubling it.
		"userlist.txt": []byte(`"synod_app" "` + strings.ReplaceAll(applicationPassword, `"`, `""`) + "\"\n"),
		"ca.crt":       member.CA,
		"member.crt":   member.Certificate,
		"member.key":   member.PrivateKey,
		"role.json":    fmt.Appendf(nil, "{\"address\": %q, \"rest_port\": %d}\n", address, settings.RESTPort),
	}, nil
}

// RouterFiles renders the HAProxy files. The writer route checks Patroni /primary and cuts the
// sessions of a member that loses it. The reader route checks /replica with a lag limit.
// Checks use TLS verified against the native CA and present the member certificate, because the
// Patroni REST API requires a client certificate.
func RouterFiles(settings RouterSettings, member PatroniTLSMaterial) (map[string][]byte, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	lag := settings.MaxLagBytes
	if lag == 0 {
		lag = DefaultReaderLagBytes
	}
	if len(member.CA) == 0 || len(member.Certificate) == 0 || len(member.PrivateKey) == 0 {
		return nil, errors.New("router TLS material is incomplete")
	}
	var config strings.Builder
	config.WriteString("global\n    maxconn 1000\n    ssl-default-server-options ssl-min-ver TLSv1.2\n\n")
	config.WriteString("defaults\n    mode tcp\n    timeout connect 3s\n    timeout client 30m\n    timeout server 30m\n    timeout check 2s\n")
	// A member stays out of a route until its checks pass, so a new router never sends a writer
	// session to a replica before the first checks.
	config.WriteString("    default-server inter 2s fall 2 rise 2 init-state down\n\n")
	for _, route := range []struct {
		name, check string
		port        uint16
	}{{"writer", "/primary", settings.WriterPort}, {"reader", "/replica?lag=" + strconv.FormatInt(lag, 10), settings.ReaderPort}} {
		// HAProxy 3.3 and later refuse a backend that shares a frontend name.
		// Each route also listens on the loopback readiness address, so the local agent can check it.
		fmt.Fprintf(&config, "frontend %s\n    bind %s\n    bind %s\n    default_backend %s_members\n\n", route.name, joinHostPort(settings.Address, route.port), joinHostPort(ReadinessLoopback(settings.Address).Addr(), route.port), route.name)
		fmt.Fprintf(&config, "backend %s_members\n    option httpchk\n    http-check send meth GET uri %s\n    http-check expect status 200\n", route.name, route.check)
		for index, backend := range settings.Backends {
			fmt.Fprintf(&config, "    server member%d %s check port %d check-ssl verify required ca-file %s/ca.crt crt %s/check.pem on-marked-down shutdown-sessions\n",
				index+1, joinHostPort(backend.Address, backend.PoolerPort), backend.RESTPort, RouterDirectory, RouterDirectory)
		}
		config.WriteString("\n")
	}
	pair := append(append([]byte{}, member.Certificate...), member.PrivateKey...)
	return map[string][]byte{"haproxy.cfg": []byte(config.String()), "ca.crt": member.CA, "check.pem": pair}, nil
}

// joinHostPort writes an HAProxy address. IPv6 literals need brackets.
func joinHostPort(address netip.Addr, port uint16) string {
	return netip.AddrPortFrom(address, port).String()
}

// PoolerCommand restores the pooler files from protected configuration and runs PgBouncer.
func PoolerCommand() []string {
	return []string{"/usr/bin/python3", "-c", routeStartup, "synod-pooler"}
}

// RouterCommand restores the router files from protected configuration and runs HAProxy.
func RouterCommand() []string {
	return []string{"/usr/bin/python3", "-c", routeStartup, "synod-router"}
}

func (s PoolerSettings) validate() error {
	if !s.Address.IsGlobalUnicast() || s.Address.Zone() != "" || s.Port == 0 || s.DatabasePort == 0 || s.RESTPort == 0 ||
		s.Port == s.DatabasePort || s.RESTPort == s.Port || s.RESTPort == s.DatabasePort {
		return errors.New("pooler settings are invalid")
	}
	return nil
}

func (s RouterSettings) validate() error {
	if !s.Address.IsGlobalUnicast() || s.Address.Zone() != "" || s.WriterPort == 0 || s.ReaderPort == 0 || s.WriterPort == s.ReaderPort {
		return errors.New("router settings are invalid")
	}
	if s.MaxLagBytes < 0 || len(s.Backends) == 0 || len(s.Backends) > maxRouterBackends {
		return errors.New("router backends or lag limit are invalid")
	}
	var seen []netip.Addr
	for _, backend := range s.Backends {
		if !backend.Address.IsGlobalUnicast() || backend.Address.Zone() != "" || backend.PoolerPort == 0 || backend.RESTPort == 0 || backend.PoolerPort == backend.RESTPort || slices.Contains(seen, backend.Address) {
			return errors.New("router backend is invalid")
		}
		if backend.Address == s.Address && (backend.PoolerPort == s.WriterPort || backend.PoolerPort == s.ReaderPort || backend.RESTPort == s.WriterPort || backend.RESTPort == s.ReaderPort) {
			return errors.New("router port collides with a local backend port")
		}
		seen = append(seen, backend.Address)
	}
	return nil
}

// RouteSettings reads and checks a typed route container. It returns the pooler or the router
// settings and the credential purposes that the container needs.
func RouteSettings(route *agentv1.PostgresRouteConfig) (*PoolerSettings, *RouterSettings, []string, error) {
	if route == nil || (route.Pooler == nil) == (route.Router == nil) {
		return nil, nil, nil, errors.New("a route container holds exactly one of pooler or router")
	}
	port := func(value uint32) (uint16, error) {
		if value == 0 || value > 65535 {
			return 0, errors.New("route port is out of range")
		}
		return uint16(value), nil
	}
	address := func(value string) (netip.Addr, error) {
		parsed, err := netip.ParseAddr(value)
		if err != nil || parsed.String() != value {
			return netip.Addr{}, errors.New("route address is not a canonical IP")
		}
		return parsed, nil
	}
	if p := route.Pooler; p != nil {
		settings := PoolerSettings{}
		var err error
		if settings.Address, err = address(p.Address); err != nil {
			return nil, nil, nil, err
		}
		if settings.Port, err = port(p.Port); err != nil {
			return nil, nil, nil, err
		}
		if settings.DatabasePort, err = port(p.DatabasePort); err != nil {
			return nil, nil, nil, err
		}
		if settings.RESTPort, err = port(p.RestPort); err != nil {
			return nil, nil, nil, err
		}
		if err := settings.validate(); err != nil {
			return nil, nil, nil, err
		}
		return &settings, nil, []string{"application", "patroni_member_tls"}, nil
	}
	r := route.Router
	settings := RouterSettings{MaxLagBytes: r.MaxLagBytes}
	var err error
	if settings.Address, err = address(r.Address); err != nil {
		return nil, nil, nil, err
	}
	if settings.WriterPort, err = port(r.WriterPort); err != nil {
		return nil, nil, nil, err
	}
	if settings.ReaderPort, err = port(r.ReaderPort); err != nil {
		return nil, nil, nil, err
	}
	for _, backend := range r.Backends {
		if backend == nil {
			return nil, nil, nil, errors.New("router backend is absent")
		}
		var parsed RouterBackend
		if parsed.Address, err = address(backend.Address); err != nil {
			return nil, nil, nil, err
		}
		if parsed.PoolerPort, err = port(backend.PoolerPort); err != nil {
			return nil, nil, nil, err
		}
		if parsed.RESTPort, err = port(backend.RestPort); err != nil {
			return nil, nil, nil, err
		}
		settings.Backends = append(settings.Backends, parsed)
	}
	if err := settings.validate(); err != nil {
		return nil, nil, nil, err
	}
	return nil, &settings, []string{"patroni_member_tls"}, nil
}
