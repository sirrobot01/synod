package cli

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirrobot01/synod/internal/agent/daemon"
)

// A runtime refusal names its cause. Other failures keep fixed text, because an
// argument error can echo raw input.
func TestFailureDetailOnlyForRuntime(t *testing.T) {
	cause := "read the runtime version from /var/run/docker.sock: connection refused"
	for name, test := range map[string]struct {
		err    error
		code   string
		detail bool
	}{
		"runtime":       {fmt.Errorf("%w: %s", daemon.ErrRuntime, cause), "runtime_unavailable", true},
		"configuration": {fmt.Errorf("%w: %s", daemon.ErrConfiguration, cause), "invalid_configuration", false},
		"argument":      {errors.New("unknown flag --token=" + cause), "invalid_configuration", false},
	} {
		failure := failureOf(test.err)
		if failure.Code != test.code || (failure.Detail == test.err.Error()) != test.detail || (!test.detail && failure.Detail != "") {
			t.Errorf("%s: %+v", name, failure)
		}
	}
}
