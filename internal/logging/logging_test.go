package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
)

func TestNewWritesReadableTextAndJSON(t *testing.T) {
	var text bytes.Buffer
	logger, err := New(&text, "", "")
	if err != nil {
		t.Fatal(err)
	}
	var missing error
	logger.Info("backup finished", "app", "compose:immich", "error", missing, "resource", "")
	logger.Debug("hidden at info")
	line := text.String()
	if !regexp.MustCompile(`^time="\d{4}-\d\d-\d\d \d\d:\d\d:\d\d" level=INFO msg="backup finished" app=compose:immich\n$`).MatchString(line) {
		t.Fatalf("text line: %q", line)
	}
	var raw bytes.Buffer
	if logger, err = New(&raw, "debug", "json"); err != nil {
		t.Fatal(err)
	}
	logger.Warn("backup failed", "error", errors.New("disk full"))
	logger.Debug("shown at debug")
	lines := strings.Split(strings.TrimSpace(raw.String()), "\n")
	var first map[string]any
	if len(lines) != 2 || json.Unmarshal([]byte(lines[0]), &first) != nil || first["error"] != "disk full" || first["msg"] != "backup failed" {
		t.Fatalf("json lines: %q", raw.String())
	}
	for _, bad := range [][2]string{{"loud", ""}, {"", "xml"}} {
		if _, err := New(&raw, bad[0], bad[1]); err == nil {
			t.Errorf("accepted level %q format %q", bad[0], bad[1])
		}
	}
}
