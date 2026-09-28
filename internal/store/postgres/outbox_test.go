package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// waitFor reports whether a notification arrived inside the timeout.
func waitFor(t *testing.T, listener *OutboxListener, timeout time.Duration) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	notified, err := listener.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return notified
}

func TestOutboxListenerWakesOnCommitAndSurvivesTimeout(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	listener, err := s.ListenOutbox(t.Context(), request.Identity.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if waitFor(t, listener, 300*time.Millisecond) {
		t.Fatal("notified before any event committed")
	}
	admitted, err := s.AdmitApp(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	// The same listener must still work after the timeout above. A relay reuses it.
	if !waitFor(t, listener, 10*time.Second) {
		t.Fatal("commit did not wake the listener")
	}
	// The notification is only a hint. The scan remains the source of truth.
	events, err := s.ListPendingOutbox(t.Context(), request.Identity.InstallationID, 0, 100)
	if err != nil || len(events) != 1 || events[0].OperationID != admitted.OperationID {
		t.Fatalf("scan after notification: %+v: %v", events, err)
	}
}

func TestOutboxListenerIgnoresOtherInstallations(t *testing.T) {
	s := testStore(t)
	mine, other := testRequest(), testRequest()
	listener, err := s.ListenOutbox(t.Context(), mine.Identity.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if _, err := s.AdmitApp(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	if waitFor(t, listener, time.Second) {
		t.Fatal("woke for another installation")
	}
	if _, err := s.AdmitApp(t.Context(), mine); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, listener, 10*time.Second) {
		t.Fatal("own event did not wake the listener")
	}
}

func TestOutboxNotifyWaitsForCommit(t *testing.T) {
	s := testStore(t)
	request := testRequest()
	listener, err := s.ListenOutbox(t.Context(), request.Identity.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ready, release := make(chan struct{}), make(chan struct{})
	owner := New(s.pool)
	owner.commitTx = func(ctx context.Context, tx pgx.Tx) error { close(ready); <-release; return tx.Commit(ctx) }
	result := make(chan error, 1)
	go func() { _, err := owner.AdmitApp(t.Context(), request); result <- err }()
	<-ready
	// The row exists in an open transaction. NOTIFY must not reach the listener yet.
	if waitFor(t, listener, time.Second) {
		t.Fatal("notified before commit")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, listener, 10*time.Second) {
		t.Fatal("commit did not deliver the notification")
	}
}

func TestListenOutboxRejectsEmptyInstallation(t *testing.T) {
	s := testStore(t)
	_, err := s.ListenOutbox(t.Context(), uuid.Nil)
	requireCode(t, err, InvalidRequest)
}
