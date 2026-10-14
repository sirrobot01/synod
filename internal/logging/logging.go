// Package logging builds the logger of the controller and the agent.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"time"
)

// New returns a logger that writes text lines, or JSON lines when format is "json".
// An empty level is "info", and an empty format is "text".
// A nil error or other nil value is left out, so a line never shows error=<nil>.
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	var min slog.Level
	if level != "" {
		if err := min.UnmarshalText([]byte(level)); err != nil {
			return nil, fmt.Errorf("log level %q: use debug, info, warn, or error", level)
		}
	}
	options := &slog.HandlerOptions{Level: min, ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Value.Kind() == slog.KindAny && a.Value.Any() == nil {
			return slog.Attr{}
		}
		if len(groups) == 0 && a.Key == slog.TimeKey && format != "json" {
			return slog.String(slog.TimeKey, a.Value.Time().UTC().Format(time.DateTime))
		}
		return a
	}}
	switch format {
	case "", "text":
		return slog.New(slog.NewTextHandler(w, options)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, options)), nil
	}
	return nil, fmt.Errorf("log format %q: use text or json", format)
}
