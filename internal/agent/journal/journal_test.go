package journal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestJournalRecoveryAndResultDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	installation, node, id := uuid.New(), uuid.New(), uuid.New()
	j, err := Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	input, result := []byte(`{"app":"test"}`), []byte(`{"status":"succeeded"}`)
	if _, err := j.Admit(t.Context(), id, input); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(t.Context(), id, result); !errors.Is(err, ErrConflict) {
		t.Fatalf("unstarted completion: %v", err)
	}
	if err := j.Start(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Start(t.Context(), id); !errors.Is(err, ErrInspectionRequired) {
		t.Fatalf("restarted work: %v", err)
	}
	if _, err := j.Admit(t.Context(), id, []byte("different")); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed input: %v", err)
	}
	if err := j.Complete(t.Context(), id, result); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(t.Context(), id, result); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(t.Context(), id, []byte("different")); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed result: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	records, err := j.Records(t.Context())
	if err != nil || len(records) != 1 || records[0].State != Terminal || !bytes.Equal(records[0].Result, result) {
		t.Fatalf("saved result: %+v %v", records, err)
	}
	if err := j.Acknowledge(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	records, err = j.Records(t.Context())
	if err != nil || len(records) != 0 {
		t.Fatalf("acknowledged result returned: %+v %v", records, err)
	}
	replay, err := j.Admit(t.Context(), id, input)
	if err != nil || replay.State != Terminal {
		t.Fatalf("replayed work: %+v %v", replay, err)
	}
	if _, err := Open(t.Context(), path, installation, uuid.New()); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong device opened journal: %v", err)
	}
}

func TestJournalUsesPrivateLiteralPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal?#%.db")
	installation, node, id := uuid.New(), uuid.New(), uuid.New()
	j, err := Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(t.Context(), id, []byte("saved input")); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	records, err := j.Records(t.Context())
	if err != nil || len(records) != 1 || records[0].ID != id {
		t.Fatalf("reopened wrong journal: %+v %v", records, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(t.Context(), path, installation, node); err == nil {
		other.Close()
		t.Fatal("journal accepted public permissions")
	}
}

func TestJournalBoundsPendingWork(t *testing.T) {
	j, err := Open(t.Context(), filepath.Join(t.TempDir(), "journal.db"), uuid.New(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	first := uuid.New()
	if _, err := j.Admit(t.Context(), first, []byte("input")); err != nil {
		t.Fatal(err)
	}
	for range 63 {
		if _, err := j.Admit(t.Context(), uuid.New(), []byte("input")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.Admit(t.Context(), uuid.New(), []byte("input")); !errors.Is(err, ErrBusy) {
		t.Fatalf("over capacity: %v", err)
	}
	if _, err := j.Admit(t.Context(), first, []byte("input")); err != nil {
		t.Fatal(err)
	}
	if err := j.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(t.Context(), first, []byte("result")); err != nil {
		t.Fatal(err)
	}
	if err := j.Acknowledge(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Admit(t.Context(), uuid.New(), []byte("input")); err != nil {
		t.Fatal(err)
	}
}

func TestProgressSurvivesRestartUntilCompletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	installation, node, id := uuid.New(), uuid.New(), uuid.New()
	j, err := Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if j != nil {
			j.Close()
		}
	}()
	if _, err := j.Admit(t.Context(), id, []byte("immutable request")); err != nil {
		t.Fatal(err)
	}
	if err := j.SaveProgress(t.Context(), id, []byte("too early")); !errors.Is(err, ErrConflict) {
		t.Fatalf("queued progress accepted: %v", err)
	}
	if err := j.Start(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	progress := []byte(`{"restart":["container-id"]}`)
	if err := j.SaveProgress(t.Context(), id, progress); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = Open(t.Context(), path, installation, node)
	if err != nil {
		t.Fatal(err)
	}
	records, err := j.Records(t.Context())
	if err != nil || len(records) != 1 || !bytes.Equal(records[0].Progress, progress) {
		t.Fatalf("recovery intent lost: %v", err)
	}
	if err := j.Start(t.Context(), id); !errors.Is(err, ErrInspectionRequired) {
		t.Fatalf("running work restarted blindly: %v", err)
	}
	if err := j.Complete(t.Context(), id, []byte("recovered result")); err != nil {
		t.Fatal(err)
	}
	if err := j.SaveProgress(t.Context(), id, progress); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal progress changed: %v", err)
	}
	records, err = j.Records(t.Context())
	if err != nil || len(records) != 1 || len(records[0].Progress) != 0 {
		t.Fatalf("completed progress not released: %v", err)
	}
}
