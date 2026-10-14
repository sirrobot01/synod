package agentstream

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/store"
)

func TestReceiverKeepsTheFirstTerminalResult(t *testing.T) {
	r := Receiver{}
	peer := controlstream.Peer{NodeID: uuid.New(), SessionID: uuid.NewString()}
	conflict := &store.Error{Code: store.OperationConflict}
	for _, test := range []struct {
		status string
		err    error
		want   bool
	}{
		// A second terminal result is acknowledged, so the agent drops it.
		{"succeeded", conflict, true},
		{"failed", conflict, true},
		// Other refusals keep the stream closed, so the agent sends the event again.
		{"running", conflict, false},
		{"succeeded", &store.Error{Code: store.InvalidRequest}, false},
		{"succeeded", store.ErrStaleSession, false},
		{"succeeded", errors.New("database is locked"), false},
	} {
		if got := r.keepFirst(peer, uuid.New(), test.status, test.err); got != test.want {
			t.Errorf("keepFirst(%s, %v) = %v, want %v", test.status, test.err, got, test.want)
		}
	}
}
