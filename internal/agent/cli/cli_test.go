package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNonLinuxAgentRefusesBeforeEnrollment(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("agents run on Linux")
	}
	dir := filepath.Join(t.TempDir(), "agent")
	for _, args := range [][]string{
		{"join", "private-join-token", "--dir", dir},
		{"renew", "private-join-token", "--dir", dir},
		{"run", "--config", filepath.Join(dir, "agent.json")},
	} {
		var stdout, stderr bytes.Buffer
		if run(t, args, &stdout, &stderr) != 1 || stdout.Len() != 0 {
			t.Fatalf("agent command was not refused: %s", stdout.String())
		}
		var failure Failure
		if json.Unmarshal(stderr.Bytes(), &failure) != nil || failure.Code != "unsupported_platform" || !strings.Contains(failure.Message, "require Linux") || strings.Contains(stderr.String(), "private-join-token") {
			t.Fatalf("unsafe or unclear failure: %s", stderr.String())
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("agent command created state: %v", err)
	}
}

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

func TestRunHelpNeverShowsTheJoinToken(t *testing.T) {
	t.Setenv("SYNOD_JOIN_TOKEN", "private-env-join-token")
	var stdout, stderr bytes.Buffer
	run(t, []string{"run", "--help"}, &stdout, &stderr)
	if !strings.Contains(stdout.String(), "--join-token") || strings.Contains(stdout.String()+stderr.String(), "private-env-join-token") {
		t.Fatalf("help output: %s %s", stdout.String(), stderr.String())
	}
}
