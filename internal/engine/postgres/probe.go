package postgres

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirrobot01/synod/internal/engine"
)

// Stable probe reasons. They name what failed and never carry a credential, a host
// path, or a driver message, because an observation travels to an operator's screen.
const (
	reasonTLSInvalid            = "postgres_tls_configuration_invalid"
	reasonTLSVerification       = "postgres_tls_verification_failed"
	reasonRoleInvalid           = "postgres_role_invalid"
	reasonRoleUnavailable       = "postgres_role_unavailable"
	reasonRoleMismatch          = "postgres_role_mismatch"
	reasonReadOnlyMismatch      = "postgres_read_only_mismatch"
	reasonNoEndpoint            = "endpoint_not_declared"
	reasonUnreachable           = "endpoint_refused_connection"
	reasonNoCredential          = "credential_unavailable"
	reasonAuthRejected          = "authentication_rejected"
	reasonNotReady              = "database_not_ready"
	reasonWrongDatabase         = "database_absent"
	reasonApplicationPrivileges = "application_privileges_invalid"
)

// dialTimeout bounds one connection attempt. A probe that hangs is an unavailable
// probe, not a slow pass.
const dialTimeout = 3 * time.Second

// Probe checks one live PostgreSQL through the endpoint it publishes.
//
// Specification 07 requires checking database access through the published endpoint
// before reporting success, and specification 06 requires the four checks to be
// reported apart. They are taken in order, and a check whose precondition did not pass
// stays unavailable rather than being guessed at.
func (Engine) Probe(ctx context.Context, endpoint engine.Endpoint, password []byte, revision int64) engine.Observation {
	observed := engine.Observation{ObservedRevision: revision, ObservedAt: time.Now().UTC()}
	var tlsConfig *tls.Config
	if settings := endpoint.PostgresTLS; settings != nil {
		var err error
		tlsConfig, err = ReadinessTLSConfig(settings.RootCAPEM, settings.ServerName)
		if err != nil {
			observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: reasonTLSInvalid}
			return observed
		}
	}
	if endpoint.PostgresRole != "primary" && endpoint.PostgresRole != "replica" {
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: reasonRoleInvalid}
		return observed
	}
	if !validReplicationCheck(endpoint) {
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: reasonReplicationInvalid}
		return observed
	}
	if endpoint.Address == "" || endpoint.Port == 0 || endpoint.Database == "" || endpoint.Role == "" {
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: reasonNoEndpoint}
		return observed
	}
	address := net.JoinHostPort(endpoint.Address, strconv.Itoa(int(endpoint.Port)))
	dial, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dial, "tcp", address)
	if err != nil {
		// Nothing is listening, so nothing further can be told. The remaining probes
		// stay unavailable rather than being reported as failures of their own.
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: reasonUnreachable}
		return observed
	}
	_ = conn.Close()
	observed.Reachable = engine.Probe{State: engine.ProbePassed}

	// A listening port means a process. It does not mean a database.
	observed.ProcessLive = engine.Probe{State: engine.ProbePassed}

	if len(password) == 0 {
		// Without a credential the database cannot be asked anything. That is an
		// unavailable probe, not a failing database.
		observed.Ready = engine.Probe{State: engine.ProbeUnavailable, Reason: reasonNoCredential}
		return observed
	}
	// pgx reads ambient certificate files during parsing, even with sslmode=disable.
	config, err := pgx.ParseConfig("host=localhost port=5432 sslmode=disable sslrootcert='' sslcert='' sslkey='' sslnegotiation=postgres target_session_attrs=any connect_timeout=3")
	if err != nil {
		observed.Ready = engine.Probe{State: engine.ProbeUnavailable, Reason: reasonNotReady}
		return observed
	}
	config.Host, config.Port = endpoint.Address, endpoint.Port
	config.User, config.Password, config.Database = endpoint.Role, string(password), endpoint.Database
	config.ConnectTimeout = dialTimeout
	config.DialFunc = (&net.Dialer{Timeout: dialTimeout}).DialContext
	config.TLSConfig, config.Fallbacks = tlsConfig, nil
	config.SSLNegotiation = "postgres"
	config.ValidateConnect = nil
	config.RuntimeParams = map[string]string{"search_path": "pg_catalog"}
	session, cancelSession := context.WithTimeout(ctx, dialTimeout)
	defer cancelSession()
	conn2, err := pgx.ConnectConfig(session, config)
	if err != nil {
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: readinessReason(err)}
		return observed
	}
	defer conn2.Close(context.WithoutCancel(session))
	observed.Ready = engine.Probe{State: engine.ProbePassed}

	// The endpoint is healthy when the database it serves answers a real query, not
	// when a handshake completed. A connection that cannot run a statement is a
	// connection an application cannot use.
	var answer int
	if err := conn2.QueryRow(session, "SELECT 1").Scan(&answer); err != nil || answer != 1 {
		observed.Endpoint = engine.Probe{State: engine.ProbeFailed, Reason: reasonNotReady}
		return observed
	}
	if endpoint.Role == "synod_app" {
		var permitted bool
		err := conn2.QueryRow(session, `SELECT
 current_user = 'synod_app' AND session_user = 'synod_app' AND current_database() = 'app'
 AND r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls
 AND d.datdba = r.oid AND d.datallowconn
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database AS owned WHERE owned.datdba=r.oid AND owned.datname<>'app')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members AS m WHERE m.member = r.oid)
 AND pg_catalog.has_database_privilege(r.oid, d.oid, 'CONNECT')
 AND pg_catalog.has_database_privilege(r.oid, d.oid, 'CREATE')
 AND pg_catalog.has_database_privilege(r.oid, d.oid, 'TEMP')
 AND pg_catalog.has_schema_privilege(r.oid, 'public', 'USAGE')
 AND pg_catalog.has_schema_privilege(r.oid, 'public', 'CREATE')
 AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database AS other
 WHERE other.datname IN ('postgres', 'template1')
 AND (pg_catalog.has_database_privilege(r.oid, other.oid, 'CONNECT')
 OR pg_catalog.has_database_privilege(r.oid, other.oid, 'TEMP')))
 FROM pg_catalog.pg_roles AS r
 JOIN pg_catalog.pg_database AS d ON d.datname = current_database()
 WHERE r.rolname = current_user`).Scan(&permitted)
		if err != nil || !permitted {
			observed.Endpoint = engine.Probe{State: engine.ProbeFailed, Reason: reasonApplicationPrivileges}
			return observed
		}
	}
	var recovery, readOnly bool
	if err := conn2.QueryRow(session, "SELECT pg_catalog.pg_is_in_recovery(), pg_catalog.current_setting('transaction_read_only')::boolean").Scan(&recovery, &readOnly); err != nil {
		observed.Ready = engine.Probe{State: engine.ProbeUnavailable, Reason: reasonRoleUnavailable}
		return observed
	}
	role := "primary"
	if recovery {
		role = "replica"
	}
	observed.Postgres = &engine.PostgresObservation{Role: role, ReadOnly: readOnly}
	if role != endpoint.PostgresRole {
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: reasonRoleMismatch}
		return observed
	}
	if readOnly != recovery {
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: reasonReadOnlyMismatch}
		return observed
	}
	if endpoint.PostgresReplication != nil {
		observed.Ready = probeReplication(session, conn2, endpoint, observed.Postgres)
		if observed.Ready.State != engine.ProbePassed {
			return observed
		}
	}
	observed.Endpoint = engine.Probe{State: engine.ProbePassed}
	return observed
}

// readinessReason maps a connection failure to a stable code without repeating the
// driver's message, which can carry a host, a port, or a role name.
func readinessReason(err error) string {
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return reasonTLSVerification
	}
	if pg, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pg.Code {
		case "28P01", "28000":
			return reasonAuthRejected
		case "3D000":
			return reasonWrongDatabase
		}
	}
	return reasonNotReady
}
