// Package database runs the engine steps of a tier 1 app inside its container.
// Every step is idempotent. The supervisor repeats a step until it succeeds.
package database

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Exec runs one program inside the engine container and returns its standard output.
// Env carries secrets, so they never appear in a process list.
type Exec func(ctx context.Context, env []string, stdin string, args ...string) ([]byte, error)

// ErrConfiguration reports a value that the engine steps refuse to place in SQL.
var ErrConfiguration = errors.New("database: invalid engine configuration")

// ErrUpstreamUnavailable means the primary could not supply sequence values.
var ErrUpstreamUnavailable = errors.New("database: upstream unavailable")

// Postgres holds what the PostgreSQL steps need. The passwords come from the credential store.
type Postgres struct {
	// Node names this copy. It forms the subscription name.
	Node                uuid.UUID
	Databases           []string
	AdminPassword       string
	ReplicationPassword string
	AppPassword         string
}

// Endpoint is how a standby reaches its primary.
type Endpoint struct {
	Host string
	Port uint16
	// CAFile is the installation CA inside the container.
	CAFile string
}

// Standby states reported in heartbeats.
const (
	StateDisconnected = "disconnected"
	StateCopying      = "copying"
	StateStreaming    = "streaming"
	StateDrifted      = "drifted"
	StateFailed       = "failed"
)

// PostgresStatus is what one standby database reports.
type PostgresStatus struct {
	State string
	// LagSeconds is the age of the last change the primary confirmed to this standby.
	LagSeconds float64
	// ApplyErrors is cumulative. The supervisor compares it with its last reading.
	ApplyErrors int64
}

// Sequences on a standby run this many increments ahead of the primary's last copied value.
const sequenceMargin = 1000

var (
	namePattern     = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	passwordPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
	hostPattern     = regexp.MustCompile(`^[A-Za-z0-9.:-]{1,253}$`)
	pathPattern     = regexp.MustCompile(`^/[A-Za-z0-9._/-]{1,255}$`)
)

func (p Postgres) validate() error {
	if p.Node == uuid.Nil || len(p.Databases) == 0 {
		return ErrConfiguration
	}
	for _, name := range p.Databases {
		if !namePattern.MatchString(name) {
			return ErrConfiguration
		}
	}
	for _, password := range []string{p.AdminPassword, p.ReplicationPassword, p.AppPassword} {
		if !passwordPattern.MatchString(password) {
			return ErrConfiguration
		}
	}
	return nil
}

func (e Endpoint) validate() error {
	if !hostPattern.MatchString(e.Host) || e.Port == 0 || !pathPattern.MatchString(e.CAFile) {
		return ErrConfiguration
	}
	return nil
}

func (p Postgres) subscription() string {
	return "synod_sub_" + strings.ReplaceAll(p.Node.String(), "-", "_")
}

// slot names this copy's replication slot for one database. Slots are shared by every
// database on the primary, so the name covers the node and the database.
func (p Postgres) slot(database string) string {
	digest := sha256.Sum256([]byte(p.Node.String() + "/" + database))
	return "synod_" + hex.EncodeToString(digest[:12])
}

// connection is a libpq connection string for one database on the primary.
func (e Endpoint) connection(database, user, password string) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=verify-ca sslrootcert=%s connect_timeout=5", e.Host, e.Port, database, user, password, e.CAFile)
}

// writable lets Synod's own sessions write on a standby whose databases default to read-only.
var writable = []string{"PGOPTIONS=-c default_transaction_read_only=off"}

// psql runs a script as the Synod superuser over the local socket.
func psql(ctx context.Context, x Exec, database, script string) ([]byte, error) {
	return x(ctx, writable, script, "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At", "-U", "synod_admin", "-d", database, "-f", "-")
}

// ensureRoles creates or repairs the Synod roles. A copied role carries no password,
// so every copy sets its own from the credential store.
func (p Postgres) ensureRoles(ctx context.Context, x Exec) error {
	script := fmt.Sprintf(`ALTER ROLE synod_admin PASSWORD '%s';
SELECT 'CREATE ROLE synod_replication' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'synod_replication') \gexec
ALTER ROLE synod_replication LOGIN REPLICATION PASSWORD '%s';
GRANT pg_read_all_data TO synod_replication;
SELECT 'CREATE ROLE app' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app') \gexec
ALTER ROLE app LOGIN PASSWORD '%s';
`, p.AdminPassword, p.ReplicationPassword, p.AppPassword)
	_, err := psql(ctx, x, "postgres", script)
	return err
}

// ddlTrigger marks each table without a primary key REPLICA IDENTITY FULL when it is
// created. Without it PostgreSQL refuses UPDATE and DELETE on the primary itself. It also
// counts every DDL command.
const ddlTrigger = `CREATE SCHEMA IF NOT EXISTS synod;
CREATE TABLE IF NOT EXISTS synod.schema_version (id integer PRIMARY KEY CHECK (id = 1), changes bigint NOT NULL, changed_at timestamptz NOT NULL);
INSERT INTO synod.schema_version VALUES (1, 0, now()) ON CONFLICT (id) DO NOTHING;
CREATE OR REPLACE FUNCTION synod.on_ddl() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE object_type = 'table' AND schema_name <> 'synod' LOOP
    IF NOT EXISTS (SELECT FROM pg_index WHERE indrelid = r.objid AND indisprimary)
       AND (SELECT relreplident FROM pg_class WHERE oid = r.objid) = 'd' THEN
      EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', r.object_identity);
    END IF;
  END LOOP;
  UPDATE synod.schema_version SET changes = changes + 1, changed_at = now() WHERE id = 1;
END $$;
SELECT 'CREATE EVENT TRIGGER synod_ddl ON ddl_command_end EXECUTE FUNCTION synod.on_ddl()' WHERE NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'synod_ddl') \gexec
SELECT format('ALTER TABLE %s REPLICA IDENTITY FULL', c.oid::regclass) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'synod') AND c.relreplident = 'd'
 AND NOT EXISTS (SELECT FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) \gexec
`

// EnsurePrimary prepares this copy to accept writes and publish every table.
// It also finishes a promotion, so it clears the standby read-only default.
func (p Postgres) EnsurePrimary(ctx context.Context, x Exec) error {
	if err := p.validate(); err != nil {
		return err
	}
	if err := p.ensureRoles(ctx, x); err != nil {
		return err
	}
	for _, name := range p.Databases {
		script := fmt.Sprintf(`SELECT 'CREATE DATABASE %[1]s OWNER app' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '%[1]s') \gexec
ALTER DATABASE %[1]s RESET default_transaction_read_only;
`, name)
		if _, err := psql(ctx, x, "postgres", script); err != nil {
			return err
		}
		script = ddlTrigger + `SELECT 'CREATE PUBLICATION synod_pub FOR ALL TABLES' WHERE NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'synod_pub') \gexec
`
		if _, err := psql(ctx, x, name, script); err != nil {
			return err
		}
	}
	return nil
}

// DropStaleSyncSlots removes table sync slots that a failed copy left on the primary.
// They retain WAL. A slot inactive for ten minutes belongs to no running sync.
func DropStaleSyncSlots(ctx context.Context, x Exec) error {
	_, err := psql(ctx, x, "postgres", `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
 WHERE NOT active AND slot_name ~ '^pg_[0-9]+_sync_' AND inactive_since < now() - interval '10 minutes';
`)
	return err
}

// SchemaHash digests the schema of one database. It leaves out publications,
// subscriptions, and the random token that pg_dump writes for each run.
func SchemaHash(ctx context.Context, x Exec, env []string, target string) (string, error) {
	dump, err := x(ctx, env, "", "pg_dump", "--schema-only", "--no-owner", "--no-privileges", "--no-comments", "--no-publications", "--no-subscriptions", "-d", target)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	scanner := bufio.NewScanner(bytes.NewReader(dump))
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, `\restrict `) || strings.HasPrefix(line, `\unrestrict `) {
			continue
		}
		digest.Write([]byte(line))
		digest.Write([]byte{'\n'})
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// LocalSchemaHash digests one database of this copy.
func (p Postgres) LocalSchemaHash(ctx context.Context, x Exec, database string) (string, error) {
	return SchemaHash(ctx, x, nil, "postgresql:///"+database+"?user=synod_admin")
}

// PrimarySchemaHash digests one database of the primary, read from this standby.
func (p Postgres) PrimarySchemaHash(ctx context.Context, x Exec, primary Endpoint, database string) (string, error) {
	if err := primary.validate(); err != nil {
		return "", err
	}
	return SchemaHash(ctx, x, nil, primary.connection(database, "synod_admin", p.AdminPassword))
}

// Copy rebuilds one standby database from the primary: a fresh schema and a
// subscription that copies every row. It is the first copy and the re-copy after drift.
func (p Postgres) Copy(ctx context.Context, x Exec, primary Endpoint, database string) error {
	if err := p.validate(); err != nil {
		return err
	}
	if err := primary.validate(); err != nil {
		return err
	}
	if err := p.ensureRoles(ctx, x); err != nil {
		return err
	}
	if err := p.dropSubscription(ctx, x, database); err != nil {
		return err
	}
	// A standby publishes nothing. Slots left from a time as primary only retain WAL.
	script := fmt.Sprintf(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active AND database = '%[1]s' AND slot_name LIKE 'synod\_%%';
DROP DATABASE IF EXISTS %[1]s WITH (FORCE);
CREATE DATABASE %[1]s OWNER app;
ALTER DATABASE %[1]s SET default_transaction_read_only = on;
`, database)
	if _, err := psql(ctx, x, "postgres", script); err != nil {
		return err
	}
	// A copied publication would block the apply worker, so the schema copy leaves it out.
	copySchema := `set -o pipefail; pg_dump --schema-only --no-publications --no-subscriptions -d "$SYNOD_PRIMARY" | psql -X -q -v ON_ERROR_STOP=1 -U synod_admin -d "$1" -f -`
	env := append([]string{"SYNOD_PRIMARY=" + primary.connection(database, "synod_admin", p.AdminPassword)}, writable...)
	if _, err := x(ctx, env, "", "bash", "-c", copySchema, "copy-schema", database); err != nil {
		return err
	}
	script = fmt.Sprintf(`CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION synod_pub WITH (copy_data = true, slot_name = '%s');
`, p.subscription(), primary.connection(database, "synod_replication", p.ReplicationPassword), p.slot(database))
	_, err := psql(ctx, x, database, script)
	return err
}

// dropSubscription removes this copy's subscription in one database. When the
// primary is unreachable a plain drop fails, so it detaches the remote slot first.
func (p Postgres) dropSubscription(ctx context.Context, x Exec, database string) error {
	exists, err := psql(ctx, x, "postgres", fmt.Sprintf(`SELECT count(*) FROM pg_subscription s JOIN pg_database d ON d.oid = s.subdbid WHERE s.subname = '%s' AND d.datname = '%s';
`, p.subscription(), database))
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(exists)) == "0" {
		return nil
	}
	if _, err := psql(ctx, x, database, fmt.Sprintf("DROP SUBSCRIPTION %s;\n", p.subscription())); err == nil {
		return nil
	}
	_, err = psql(ctx, x, database, fmt.Sprintf(`ALTER SUBSCRIPTION %[1]s DISABLE;
ALTER SUBSCRIPTION %[1]s SET (slot_name = NONE);
DROP SUBSCRIPTION %[1]s;
`, p.subscription()))
	return err
}

// Status reads the replication state of one standby database.
func (p Postgres) Status(ctx context.Context, x Exec, database string) (PostgresStatus, error) {
	if !namePattern.MatchString(database) {
		return PostgresStatus{}, ErrConfiguration
	}
	exists, err := psql(ctx, x, "postgres", fmt.Sprintf("SELECT count(*) FROM pg_database WHERE datname = '%s';", database))
	if err != nil {
		return PostgresStatus{}, err
	}
	if strings.TrimSpace(string(exists)) == "0" {
		return PostgresStatus{State: StateFailed, LagSeconds: -1}, nil
	}
	// The subscription catalogs and statistics are shared by every database, and this
	// copy uses one subscription name in each, so each read goes through this database's OID.
	out, err := psql(ctx, x, database, fmt.Sprintf(`WITH sub AS (SELECT oid, subenabled FROM pg_subscription WHERE subname = '%s' AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database()))
SELECT (SELECT count(*) FROM sub WHERE subenabled),
 (SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM sub)),
 (SELECT count(*) FROM pg_subscription_rel WHERE srsubid IN (SELECT oid FROM sub) AND srsubstate <> 'r'),
 coalesce((SELECT extract(epoch FROM now() - latest_end_time) FROM pg_stat_subscription WHERE subid IN (SELECT oid FROM sub) AND relid IS NULL), -1),
 coalesce((SELECT apply_error_count + sync_error_count FROM pg_stat_subscription_stats WHERE subid IN (SELECT oid FROM sub)), 0);
`, p.subscription()))
	if err != nil {
		return PostgresStatus{}, err
	}
	fields := strings.Split(strings.TrimSpace(string(out)), "|")
	if len(fields) != 5 {
		return PostgresStatus{}, fmt.Errorf("database: unexpected status %q", out)
	}
	enabled, _ := strconv.Atoi(fields[0])
	tables, _ := strconv.Atoi(fields[1])
	pending, _ := strconv.Atoi(fields[2])
	lag, _ := strconv.ParseFloat(fields[3], 64)
	errorsSeen, _ := strconv.ParseInt(fields[4], 10, 64)
	status := PostgresStatus{State: StateStreaming, LagSeconds: lag, ApplyErrors: errorsSeen}
	switch {
	case enabled == 0:
		status.State = StateFailed
	case pending > 0:
		status.State = StateCopying
	case lag < 0:
		status.State = StateDisconnected
	case tables == 0:
		// A database with no tables has nothing to copy and streams at once.
	}
	return status, nil
}

// SyncSequences sets each standby sequence ahead of the primary's value by a margin.
// The margin covers values that the primary issues before the next sync.
func (p Postgres) SyncSequences(ctx context.Context, x Exec, primary Endpoint, database string) error {
	if err := primary.validate(); err != nil {
		return err
	}
	script := fmt.Sprintf("SELECT format('SELECT setval(%%L, %%s, true);', quote_ident(schemaname) || '.' || quote_ident(sequencename), coalesce(last_value, 0) + %d * increment_by) FROM pg_sequences;", sequenceMargin)
	values, err := x(ctx, []string{"PGHOST=" + primary.Host, "PGPORT=" + strconv.Itoa(int(primary.Port)), "PGDATABASE=" + database, "PGUSER=synod_admin", "PGPASSWORD=" + p.AdminPassword, "PGSSLMODE=verify-ca", "PGSSLROOTCERT=" + primary.CAFile, "PGCONNECT_TIMEOUT=5"}, script, "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-f", "-")
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUpstreamUnavailable, err)
	}
	_, err = psql(ctx, x, database, string(values))
	return err
}

// Promote turns this standby into the primary. It needs no contact with the old primary.
// Each owned sequence moves past every value already in its column.
func (p Postgres) Promote(ctx context.Context, x Exec) error {
	if err := p.validate(); err != nil {
		return err
	}
	for _, name := range p.Databases {
		if err := p.dropSubscription(ctx, x, name); err != nil {
			return err
		}
		script := `SELECT format('SELECT setval(%L, greatest((SELECT last_value FROM %s), coalesce((SELECT max(%I) FROM %s), 0) + 1), true)',
   s.oid::regclass, s.oid::regclass, a.attname, t.oid::regclass)
 FROM pg_class s JOIN pg_depend d ON d.objid = s.oid AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
 JOIN pg_class t ON t.oid = d.refobjid JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
 WHERE s.relkind = 'S' \gexec
`
		if _, err := psql(ctx, x, name, script); err != nil {
			return err
		}
	}
	return p.EnsurePrimary(ctx, x)
}
