// Package journal records local work before execution and keeps results until delivery.
package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/restore"
	_ "modernc.org/sqlite"
)

var (
	ErrConflict           = errors.New("journal input or state conflicts with the saved record")
	ErrInspectionRequired = errors.New("journal work already started; inspect its effects before completion")
	ErrNotFound           = errors.New("journal record not found")
	ErrBusy               = errors.New("journal has 64 pending jobs; finish work before admitting another job")
)

type State string

const (
	Queued   State = "queued"
	Running  State = "running"
	Terminal State = "terminal"
)

type Record struct {
	ID       uuid.UUID
	State    State
	Input    []byte
	Result   []byte
	Progress []byte
}

// Journal keeps at most 64 pending jobs. Undelivered results stay on disk until acknowledged.
// Each input and result is at most 1 MiB. Reads return at most 512 records.
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
 CREATE TABLE IF NOT EXISTS opened_destinations (id TEXT PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS backup_history (
 app_id TEXT NOT NULL, destination_id TEXT NOT NULL, last_success INTEGER NOT NULL,
 PRIMARY KEY (app_id,destination_id));
 CREATE TABLE IF NOT EXISTS proof_history (
 kind TEXT NOT NULL, app_id TEXT NOT NULL, destination_id TEXT NOT NULL, last_success INTEGER NOT NULL,
 PRIMARY KEY (kind,app_id,destination_id));
 CREATE TABLE IF NOT EXISTS records (
 id TEXT PRIMARY KEY, input_hash BLOB NOT NULL CHECK (length(input_hash)=32),
 state TEXT NOT NULL CHECK (state IN ('queued','running','terminal')),
 input BLOB, result BLOB, progress BLOB, acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0,1)),
 CHECK (length(input)<=1048576), CHECK (length(result)<=1048576), CHECK (length(progress)<=4194304),
 CHECK (acknowledged=0 OR (state='terminal' AND input IS NULL AND result IS NULL AND progress IS NULL)));
 CREATE INDEX IF NOT EXISTS records_pending ON records(state) WHERE state!='terminal';
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
	err = tx.QueryRowContext(ctx, `SELECT state,input,result,input_hash,progress FROM records WHERE id=?`, id).Scan(&r.State, &r.Input, &r.Result, &savedHash, &r.Progress)
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
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM records WHERE state!='terminal'`).Scan(&count); err != nil {
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
	var previous, input []byte
	if err := tx.QueryRowContext(ctx, `SELECT state,result,input FROM records WHERE id=?`, id).Scan(&state, &previous, &input); err != nil {
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
	var job backup.Request
	if json.Unmarshal(input, &job) == nil && job.Validate() == nil {
		var saved backup.Result
		if json.Unmarshal(result, &saved) != nil || job.ID != id || job.InstallationID != j.installationID || job.NodeID != j.nodeID {
			return ErrConflict
		}
		status := "succeeded"
		if saved.FailureCode != "" {
			status = "failed"
		}
		if (backup.Event{ID: id, Request: &job, Status: status, Result: saved}).Validate(job) != nil {
			return ErrConflict
		}
		if saved.RepositoryOpened {
			if _, err := tx.ExecContext(ctx, `INSERT INTO opened_destinations (id) VALUES (?) ON CONFLICT DO NOTHING`, job.DestinationID); err != nil {
				return fmt.Errorf("save opened destination %s: %w", job.DestinationID, err)
			}
		}
		if status == "succeeded" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO backup_history (app_id,destination_id,last_success) VALUES (?,?,?) ON CONFLICT(app_id,destination_id) DO UPDATE SET last_success=max(last_success,excluded.last_success)`, job.AppID, job.DestinationID, saved.StartedAt.UnixMilli()); err != nil {
				return fmt.Errorf("save backup history %s: %w", job.ID, err)
			}
		}
	}
	var restoreJob restore.Request
	if json.Unmarshal(input, &restoreJob) == nil && restoreJob.Kind == "restore" {
		var saved restore.Result
		if restoreJob.Validate() != nil || restoreJob.ID != id || restoreJob.InstallationID != j.installationID || restoreJob.NodeID != j.nodeID || json.Unmarshal(result, &saved) != nil {
			return ErrConflict
		}
		status := "succeeded"
		if saved.FailureCode != "" {
			status = "failed"
		}
		if (restore.Event{ID: id, Status: status, Result: saved}).Validate(restoreJob) != nil {
			return ErrConflict
		}
	}
	var check proof.Request
	if json.Unmarshal(input, &check) == nil && (check.Kind == proof.Integrity || check.Kind == proof.Drill) {
		var saved proof.Result
		if check.Validate() != nil || check.ID != id || check.InstallationID != j.installationID || check.NodeID != j.nodeID || json.Unmarshal(result, &saved) != nil {
			return ErrConflict
		}
		status := "succeeded"
		if saved.FailureCode != "" {
			status = "failed"
		}
		if (proof.Event{ID: id, Status: status, Result: saved, Request: &check}).Validate(check) != nil {
			return ErrConflict
		}
		if status == "succeeded" {
			destinationID := check.DestinationID
			if check.Kind == proof.Drill {
				destinationID = uuid.Nil
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO proof_history (kind,app_id,destination_id,last_success) VALUES (?,?,?,?) ON CONFLICT(kind,app_id,destination_id) DO UPDATE SET last_success=max(last_success,excluded.last_success)`, check.Kind, check.AppID, destinationID, saved.StartedAt.UnixMilli()); err != nil {
				return fmt.Errorf("save proof history %s: %w", check.ID, err)
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE records SET state='terminal',result=?,progress=NULL WHERE id=?`, result, id); err != nil {
		return fmt.Errorf("save journal result %s: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit journal result %s: %w", id, err)
	}
	return nil
}

// Records returns a bounded delivery batch. Terminal results go first to clear an outage backlog.
func (j *Journal) Records(ctx context.Context) ([]Record, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT id,state,input,result,progress FROM records WHERE acknowledged=0 ORDER BY state!='terminal',id LIMIT 512`)
	if err != nil {
		return nil, fmt.Errorf("read journal records: %w", err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.State, &r.Input, &r.Result, &r.Progress); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// Pending returns admitted work even when many results await a controller connection.
// A caller must recover running work before it starts queued work.
func (j *Journal) Pending(ctx context.Context) ([]Record, error) {
	rows, err := j.db.QueryContext(ctx, `SELECT id,state,input,result,progress FROM records WHERE state!='terminal' ORDER BY state!='running',id LIMIT 64`)
	if err != nil {
		return nil, fmt.Errorf("read pending journal work: %w", err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.ID, &record.State, &record.Input, &record.Result, &record.Progress); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// Acknowledge releases payloads after durable delivery. The input hash stays to prevent replay.
func (j *Journal) Acknowledge(ctx context.Context, id uuid.UUID) error {
	result, err := j.db.ExecContext(ctx, `UPDATE records SET acknowledged=1,input=NULL,result=NULL,progress=NULL WHERE id=? AND state='terminal'`, id)
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

// SaveProgress commits recovery state before the worker changes an external resource.
func (j *Journal) SaveProgress(ctx context.Context, id uuid.UUID, progress []byte) error {
	if len(progress) == 0 || len(progress) > 4<<20 {
		return ErrConflict
	}
	result, err := j.db.ExecContext(ctx, `UPDATE records SET progress=? WHERE id=? AND state='running'`, progress, id)
	if err != nil {
		return fmt.Errorf("save journal progress %s: %w", id, err)
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
