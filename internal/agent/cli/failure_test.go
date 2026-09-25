package cli

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/synod/internal/agent/daemon"
)

// A journal or inventory refusal names its cause. Other failures keep fixed text, because an
// argument error can echo raw input.
func TestFailureDetailOnlyForJournalAndInventory(t *testing.T) {
	cause := "validate the saved command 5f0c: pooler settings are invalid"
	for name, test := range map[string]struct {
		err    error
		code   string
		detail bool
	}{
		"journal":       {fmt.Errorf("%w: %s", daemon.ErrJournal, cause), "journal_recovery_required", true},
		"inventory":     {fmt.Errorf("%w: %s", daemon.ErrInventory, cause), "inventory_recovery_required", true},
		"configuration": {fmt.Errorf("%w: %s", daemon.ErrConfiguration, cause), "invalid_configuration", false},
		"argument":      {errors.New("unknown flag --token=" + cause), "invalid_configuration", false},
	} {
		failure := failureOf(test.err)
		if failure.Code != test.code || (failure.Detail == test.err.Error()) != test.detail || (!test.detail && failure.Detail != "") {
			t.Errorf("%s: %+v", name, failure)
		}
	}
}
