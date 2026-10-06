package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestArgumentErrorsNeverEchoInputs(t *testing.T) {
	secret := "private-value-do-not-print"
	for _, args := range [][]string{
		{secret},
		{"join", "--unknown=" + secret, "token"},
		{"join", secret},
		{"run", "--config=" + secret},
		{"run", secret},
	} {
		var stdout, stderr bytes.Buffer
		if run(t, args, &stdout, &stderr) != 1 || stdout.Len() != 0 {
			t.Fatalf("unexpected output or status for %d args", len(args))
		}
		var failure Failure
		if json.Unmarshal(stderr.Bytes(), &failure) != nil || strings.Contains(stderr.String(), secret) {
			t.Fatalf("unsafe error: %s", stderr.String())
		}
	}
}

func run(t *testing.T, args []string, stdout, stderr io.Writer) int {
	t.Helper()
	command := Command()
	command.SetArgs(args)
	command.SetOut(stdout)
	command.SetErr(stderr)
	return Report(command.ExecuteContext(t.Context()), stderr)
}
