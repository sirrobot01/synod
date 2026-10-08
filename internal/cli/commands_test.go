package cli_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/cli"
)

// The server and agent trees keep their own failure output inside the one binary.
func TestServerAndAgentCommandsReportTheirOwnFailures(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), []string{"agent", "run", "--unknown", "private-value"}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatalf("agent code=%d stdout=%q", code, stdout.String())
	}
	var failure struct{ Code string }
	if err := json.Unmarshal(stderr.Bytes(), &failure); err != nil || failure.Code != "invalid_configuration" || strings.Contains(stderr.String(), "private-value") {
		t.Fatalf("agent stderr = %q", stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := cli.Run(t.Context(), []string{"server", "init", "--dir", ""}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "--dir") {
		t.Fatalf("server init code=%d stderr=%q", code, stderr.String())
	}
}

func TestServerAndAgentHelpOmitClientFlags(t *testing.T) {
	for _, args := range [][]string{{"server", "--help"}, {"agent", "run", "--help"}} {
		var stdout, stderr bytes.Buffer
		if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 {
			t.Fatalf("%v code=%d stderr=%q", args, code, stderr.String())
		}
		if strings.Contains(stdout.String(), "--api-url") || strings.Contains(stdout.String(), "--poll-interval") {
			t.Fatalf("%v help lists client flags:\n%s", args, stdout.String())
		}
	}
	var stdout, stderr bytes.Buffer
	if code := cli.Run(t.Context(), []string{"--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "--api-url") {
		t.Fatalf("root help code=%d:\n%s", code, stdout.String())
	}
}
