package daemon

import (
	"log/slog"
	"os"
)

// journalLogger replaces formatted storage diagnostics before they can expose paths.
// Fatal storage failures keep Pebble's process-exit behavior with a fixed message.
type journalLogger struct{ log *slog.Logger }

func (l journalLogger) Infof(string, ...any) { l.log.Info("journal_event") }

func (l journalLogger) Errorf(string, ...any) { l.log.Error("journal_error") }

func (l journalLogger) Fatalf(string, ...any) { l.log.Error("journal_fatal"); os.Exit(1) }
