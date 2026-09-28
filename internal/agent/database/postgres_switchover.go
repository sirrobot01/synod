package database

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// PostgresPosition binds a final transaction to its database schema.
type PostgresPosition struct {
	LSN        string
	SchemaHash string
}

var lsnPattern = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}$`)

// PauseWrites ends client sessions and returns a final WAL position for each database.
// The caller must stop this primary or call ResumeWrites, including after an error.
func (p Postgres) PauseWrites(ctx context.Context, x Exec) (positions map[string]PostgresPosition, err error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	var script strings.Builder
	script.WriteString("BEGIN;\n")
	for _, name := range p.Databases {
		fmt.Fprintf(&script, "ALTER DATABASE %s SET default_transaction_read_only = on;\n", name)
	}
	script.WriteString("COMMIT;\n")
	if _, err := psql(ctx, x, "postgres", script.String()); err != nil {
		return nil, err
	}
	// New sessions use the read-only default. Wait for older clients to end before
	// taking the final position. Replication workers and Synod sessions stay open.
	script.Reset()
	fmt.Fprintf(&script, `SELECT coalesce(bool_and(pg_terminate_backend(pid, 5000)), true) FROM pg_stat_activity
 WHERE datname IN ('%s') AND backend_type = 'client backend' AND usename <> 'synod_admin';`, strings.Join(p.Databases, "','"))
	out, err := psql(ctx, x, "postgres", script.String())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(out)) != "t" {
		return nil, fmt.Errorf("database: client sessions did not end")
	}
	positions = make(map[string]PostgresPosition, len(p.Databases))
	for _, name := range p.Databases {
		// This published row forces a final applied transaction even in an idle
		// database. Its insert position precedes the transaction's commit position.
		out, err := psql(ctx, x, name, `BEGIN;
UPDATE synod.schema_version SET changed_at = clock_timestamp() WHERE id = 1;
SELECT pg_current_wal_insert_lsn();
COMMIT;
`)
		if err != nil {
			return nil, err
		}
		position := strings.TrimSpace(string(out))
		if !lsnPattern.MatchString(position) || position == "0/0" {
			return nil, fmt.Errorf("database: invalid final WAL position")
		}
		hash, err := p.LocalSchemaHash(ctx, x, name)
		if err != nil {
			return nil, err
		}
		positions[name] = PostgresPosition{LSN: position, SchemaHash: hash}
	}
	return positions, nil
}

// CaughtUp requires a complete subscription with a durable applied position in every database.
func (p Postgres) CaughtUp(ctx context.Context, x Exec, positions map[string]PostgresPosition) (bool, error) {
	if err := p.validate(); err != nil {
		return false, err
	}
	if len(positions) != len(p.Databases) {
		return false, ErrConfiguration
	}
	for _, name := range p.Databases {
		hash, err := hex.DecodeString(positions[name].SchemaHash)
		if !lsnPattern.MatchString(positions[name].LSN) || positions[name].LSN == "0/0" || err != nil || len(hash) != 32 {
			return false, ErrConfiguration
		}
	}
	for _, name := range p.Databases {
		hash, err := p.LocalSchemaHash(ctx, x, name)
		if err != nil {
			return false, err
		}
		if hash != positions[name].SchemaHash {
			return false, nil
		}
		out, err := psql(ctx, x, name, fmt.Sprintf(`SELECT coalesce(bool_and(pg_replication_origin_progress(o.roname, true) >= '%s'::pg_lsn), false)
 FROM pg_subscription s JOIN pg_replication_origin o ON o.roname = 'pg_' || s.oid::text
 WHERE s.subname = '%s' AND s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND s.subenabled
 AND NOT EXISTS (SELECT FROM pg_subscription_rel WHERE srsubid = s.oid AND srsubstate <> 'r');
`, positions[name].LSN, p.subscription()))
		if err != nil {
			return false, err
		}
		if strings.TrimSpace(string(out)) != "t" {
			return false, nil
		}
	}
	return true, nil
}

// ResumeWrites restores writes and ends sessions that kept the read-only default.
func (p Postgres) ResumeWrites(ctx context.Context, x Exec) error {
	if err := p.validate(); err != nil {
		return err
	}
	var script strings.Builder
	for _, name := range p.Databases {
		fmt.Fprintf(&script, "ALTER DATABASE %s RESET default_transaction_read_only;\n", name)
	}
	fmt.Fprintf(&script, `SELECT coalesce(bool_and(pg_terminate_backend(pid, 5000)), true) FROM pg_stat_activity
 WHERE datname IN ('%s') AND backend_type = 'client backend' AND usename <> 'synod_admin';`, strings.Join(p.Databases, "','"))
	out, err := psql(ctx, x, "postgres", script.String())
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(out)) != "t" {
		return fmt.Errorf("database: read-only client sessions did not end")
	}
	return nil
}
