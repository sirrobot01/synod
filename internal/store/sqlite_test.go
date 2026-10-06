package store

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestMetadataPathAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata?#%.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.CreateUser(t.Context(), "alice", "hash", "admin"); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Stat(path + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("metadata%s permissions: %o", suffix, info.Mode().Perm())
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := s.CountUsers(t.Context()); err != nil || count != 1 {
		t.Fatalf("reopened wrong database: %d %v", count, err)
	}
	link := filepath.Join(t.TempDir(), "link.db")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(t.Context(), link); err == nil {
		other.Close()
		t.Fatal("metadata followed a symlink")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(t.Context(), path); err == nil {
		other.Close()
		t.Fatal("metadata accepted public permissions")
	}
}

func TestMetadataRefusesChangedMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(t.Context(), `UPDATE schema_migrations SET checksum=zeroblob(32) WHERE version=1`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(t.Context(), path); err == nil {
		other.Close()
		t.Fatal("changed migration accepted")
	} else if !strings.Contains(err.Error(), "checksum changed") {
		t.Fatalf("migration refusal: %v", err)
	}
}

func TestSQLiteSettingsApplyToEveryConnection(t *testing.T) {
	s := open(t)
	for range 3 {
		connection, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { connection.Close() })
		var journal string
		var foreignKeys, synchronous int
		if err := connection.QueryRowContext(t.Context(), `PRAGMA journal_mode`).Scan(&journal); err != nil {
			t.Fatal(err)
		}
		if err := connection.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
			t.Fatal(err)
		}
		if err := connection.QueryRowContext(t.Context(), `PRAGMA synchronous`).Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if journal != "wal" || foreignKeys != 1 || synchronous != 2 {
			t.Fatalf("settings: %s %d %d", journal, foreignKeys, synchronous)
		}
		if _, err := connection.ExecContext(t.Context(), `INSERT INTO sessions (id,user_id,verifier,created_at,last_used_at,absolute_expires_at) VALUES (?,?,?,0,0,1)`, uuid.New(), uuid.New(), make([]byte, 32)); err == nil {
			t.Fatal("missing user passed foreign key constraint")
		}
	}
}

func TestConcurrentEnrollmentConsumesTokenOnce(t *testing.T) {
	s, ca := open(t), authority(t)
	verifier := sha256.Sum256([]byte("one-use-token"))
	if _, err := s.RegisterEnrollmentToken(t.Context(), TokenRegistration{ActorID: "admin", RequestID: "r", IdempotencyKey: "key", TokenVerifier: verifier, TTLSeconds: 60}); err != nil {
		t.Fatal(err)
	}
	requests := make([]EnrollmentRequest, 8)
	for i := range requests {
		requests[i] = EnrollmentRequest{NodeID: uuid.New(), TokenVerifier: verifier, RequestID: uuid.New(), CSRDER: csr(t)}
	}
	results := make(chan error, len(requests))
	var workers sync.WaitGroup
	for _, request := range requests {
		workers.Go(func() { _, err := s.EnrollAgent(t.Context(), request, ca); results <- err })
	}
	workers.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if code(err) != EnrollmentUnauthorized {
			t.Fatalf("concurrent enrollment: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("enrolled %d devices with one token", successes)
	}
	for _, table := range []string{"nodes", "node_certificates"} {
		var count int
		if err := s.db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s contains %d records", table, count)
		}
	}
}
