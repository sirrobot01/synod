// Package journal records local work before execution and keeps results until delivery.
package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

var (
	ErrConflict           = errors.New("journal input or state conflicts with the saved record")
	ErrInspectionRequired = errors.New("journal work already started; inspect its effects before completion")
	ErrNotFound           = errors.New("journal record not found")
	ErrBusy               = errors.New("journal has 64 undelivered records; deliver results before admitting work")
)

type State string

const (
	Queued   State = "queued"
	Running  State = "running"
	Terminal State = "terminal"
)

type Record struct {
	ID     uuid.UUID
	State  State
	Input  []byte
	Result []byte
}

// Journal keeps at most 64 undelivered records. Each input and result is at most 1 MiB.
type Journal struct {
	db                     *sql.DB
	installationID, nodeID uuid.UUID
}

// Open binds a journal file to one installation and device.
func Open(ctx context.Context, path string, installationID, nodeID uuid.UUID) (*Journal, error) {
	if path == "" || installationID == uuid.Nil || nodeID == uuid.Nil {
		return nil, ErrConflict
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve journal path: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal %s: %w", path, err)
	}
	info, statErr := file.Stat()
	if err := errors.Join(statErr, file.Close()); err != nil {
		return nil, fmt.Errorf("check journal %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("journal %s must be a regular file owned by this user; set owner-only permissions", path)
	}
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{"_pragma": {"busy_timeout(10000)", "journal_mode(WAL)", "synchronous(FULL)"}, "_txlock": {"immediate"}}
	u.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open journal %s: %w", path, err)
	}
	ready := false
	defer func() {
		if !ready {
			db.Close()
		}
	}()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin journal initialization: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS identity (
 singleton INTEGER PRIMARY KEY CHECK (singleton=1), installation_id TEXT NOT NULL, node_id TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS destination_plan (
 singleton INTEGER PRIMARY KEY CHECK (singleton=1), revision INTEGER NOT NULL CHECK (revision>0), envelope BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS records (
 id TEXT PRIMARY KEY, input_hash BLOB NOT NULL CHECK (length(input_hash)=32),
 state TEXT NOT NULL CHECK (state IN ('queued','running','terminal')),
 input BLOB, result BLOB, acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0,1)),
 CHECK (length(input)<=1048576), CHECK (length(result)<=1048576),
 CHECK (acknowledged=0 OR (state='terminal' AND input IS NULL AND result IS NULL)));
 INSERT INTO identity (singleton,installation_id,node_id) VALUES (1,?,?) ON CONFLICT DO NOTHING`, installationID, nodeID); err != nil {
		return nil, fmt.Errorf("initialize journal: %w", err)
	}
	var savedInstallation, savedNode uuid.UUID
	if err := tx.QueryRowContext(ctx, `SELECT installation_id,node_id FROM identity WHERE singleton=1`).Scan(&savedInstallation, &savedNode); err != nil {
		return nil, fmt.Errorf("read journal identity: %w", err)
	}
	if savedInstallation != installationID || savedNode != nodeID {
		return nil, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit journal initialization: %w", err)
	}
	ready = true
	return &Journal{db: db, installationID: installationID, nodeID: nodeID}, nil
}

func (j *Journal) Close() error { return j.db.Close() }

// Admit saves immutable input. A repeated ID must name the same bytes, even after acknowledgement.
func (j *Journal) Admit(ctx context.Context, id uuid.UUID, input []byte) (Record, error) {
	if id == uuid.Nil || len(input) == 0 || len(input) > 1<<20 {
		return Record{}, ErrConflict
	}
	hash := sha256.Sum256(input)
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Record{}, fmt.Errorf("admit journal work %s: %w", id, err)
	}
	defer tx.Rollback()
	r := Record{ID: id}
	var savedHash []byte
	err = tx.QueryRowContext(ctx, `SELECT state,input,result,input_hash FROM records WHERE id=?`, id).Scan(&r.State, &r.Input, &r.Result, &savedHash)
	if err == nil {
		if !bytes.Equal(savedHash, hash[:]) {
			return Record{}, ErrConflict
		}
		return r, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Record{}, fmt.Errorf("read journal work %s: %w", id, err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM records WHERE acknowledged=0`).Scan(&count); err != nil {
		return Record{}, err
	}
	if count >= 64 {
		return Record{}, ErrBusy
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records (id,input_hash,state,input) VALUES (?,?,'queued',?)`, id, hash[:], input); err != nil {
		return Record{}, fmt.Errorf("save journal work %s: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return Record{}, fmt.Errorf("commit journal work %s: %w", id, err)
	}
	return Record{ID: id, State: Queued, Input: bytes.Clone(input)}, nil
}

// Start grants execution once. A running record must be inspected after a restart.
func (j *Journal) Start(ctx context.Context, id uuid.UUID) error {
	result, err := j.db.ExecContext(ctx, `UPDATE records SET state='running' WHERE id=? AND state='queued'`, id)
	if err != nil {
		return fmt.Errorf("start journal work %s: %w", id, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrInspectionRequired
	}
	return nil
}

// Complete stores an immutable result before it can be sent to the controller.
func (j *Journal) Complete(ctx context.Context, id uuid.UUID, result []byte) error {
	if len(result) == 0 || len(result) > 1<<20 {
		return ErrConflict
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("complete journal work %s: %w", id, err)
	}
	defer tx.Rollback()
	var state State
	var previous []byte
	if err := tx.QueryRowContext(ctx, `SELECT state,result FROM records WHERE id=?`, id).Scan(&state, &previous); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("read journal result %s: %w", id, err)
	}
	if state == Terminal && bytes.Equal(previous, result) {
		return nil
	}
	if state != Running {
		return ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `UPDATE records SET state='terminal',result=? WHERE id=?`, result, id); err != nil {
		return fmt.Errorf("save journal result %s: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit journal result %s: %w", id, err)
	}
	return nil
}

// Records returns undelivered work and results. A caller must inspect running work before it resumes.
func (j *Journal) Records(ctx context.Context) ([]Record, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT id,state,input,result FROM records WHERE acknowledged=0 ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("read journal records: %w", err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.State, &r.Input, &r.Result); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// Acknowledge releases payloads after durable delivery. The input hash stays to prevent replay.
func (j *Journal) Acknowledge(ctx context.Context, id uuid.UUID) error {
	result, err := j.db.ExecContext(ctx, `UPDATE records SET acknowledged=1,input=NULL,result=NULL WHERE id=? AND state='terminal'`, id)
	if err != nil {
		return fmt.Errorf("acknowledge journal result %s: %w", id, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrConflict
	}
	return nil
}
