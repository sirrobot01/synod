package daemon

import (
	"bytes"
	"log/slog"
	"os"
	"os/exec"
	"testing"
)

func TestJournalDiagnosticsAreFixed(t *testing.T) {
	if os.Getenv("SYNOD_TEST_FATAL_JOURNAL_LOG") == "1" {
		journalLogger{log: slog.New(slog.NewJSONHandler(os.Stderr, nil))}.Fatalf("private/path %s", "secret-token")
		return
	}
	var output bytes.Buffer
	logger := journalLogger{log: slog.New(slog.NewJSONHandler(&output, nil))}
	logger.Infof("private/path %s", "secret-token")
	logger.Errorf("private/path %s", "secret-token")
	if bytes.Contains(output.Bytes(), []byte("private/path")) || bytes.Contains(output.Bytes(), []byte("secret-token")) {
		t.Fatal("formatted diagnostics escaped")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestJournalDiagnosticsAreFixed$")
	command.Env = append(os.Environ(), "SYNOD_TEST_FATAL_JOURNAL_LOG=1")
	data, err := command.CombinedOutput()
	if err == nil || !bytes.Contains(data, []byte("journal_fatal")) || bytes.Contains(data, []byte("secret-token")) || bytes.Contains(data, []byte("private/path")) {
		t.Fatalf("unsafe fatal behavior: %v %s", err, data)
	}
}
