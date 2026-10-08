package controller

import (
	"encoding/json/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/config"
)

func TestLaunchdServiceKeepsPathsAndPrivateLogs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "Mac & user's <controller>")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "controller.json")
	executable := filepath.Join(dir, "synod")
	content, err := controllerService(executable, configPath)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, serviceFileName)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(t.Context(), "/usr/bin/plutil", "-convert", "json", "-o", "-", path).Output()
	if err != nil {
		t.Fatal(err)
	}
	var plist struct {
		Label                              string
		ProgramArguments                   []string
		KeepAlive, RunAtLoad               bool
		Umask                              int
		StandardOutPath, StandardErrorPath string
	}
	if err := json.Unmarshal(output, &plist); err != nil {
		t.Fatal(err)
	}
	if plist.Label != "dev.biodun.synod.server" || !slices.Equal(plist.ProgramArguments, []string{executable, "server", "--config", configPath}) || !plist.KeepAlive || !plist.RunAtLoad || plist.Umask != 0o077 || plist.StandardOutPath != filepath.Join(dir, "controller.log") || plist.StandardErrorPath != plist.StandardOutPath {
		t.Fatalf("invalid launchd configuration: %+v", plist)
	}
	if config.DefaultControllerDir != "/Library/Application Support/Synod/controller" {
		t.Fatalf("unexpected macOS state directory: %s", config.DefaultControllerDir)
	}
}

func TestLaunchdStartAndFailures(t *testing.T) {
	for _, test := range []struct {
		name, loaded, fail, want string
	}{
		{name: "new", want: "enable system/dev.biodun.synod.server\nbootstrap system /test/controller.plist\n"},
		{name: "replace", loaded: "yes", want: "enable system/dev.biodun.synod.server\nbootout system/dev.biodun.synod.server\nbootstrap system /test/controller.plist\n"},
		{name: "enable failure", fail: "enable", want: "enable system/dev.biodun.synod.server\n"},
		{name: "bootout failure", loaded: "yes", fail: "bootout", want: "enable system/dev.biodun.synod.server\nbootout system/dev.biodun.synod.server\n"},
		{name: "bootstrap failure", fail: "bootstrap", want: "enable system/dev.biodun.synod.server\nbootstrap system /test/controller.plist\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			command := `#!/bin/sh
if [ "$1" = print ]; then [ "$SYNOD_TEST_LOADED" = yes ]; exit; fi
printf '%s\n' "$*" >> "$SYNOD_TEST_COMMANDS"
if [ "$1" = "$SYNOD_TEST_FAIL" ]; then echo 'service operation failed' >&2; exit 1; fi
`
			if err := os.WriteFile(filepath.Join(dir, "launchctl"), []byte(command), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir)
			t.Setenv("SYNOD_TEST_LOADED", test.loaded)
			t.Setenv("SYNOD_TEST_FAIL", test.fail)
			t.Setenv("SYNOD_TEST_COMMANDS", filepath.Join(dir, "commands"))
			err := startLaunchd(t.Context(), "/test/controller.plist")
			if test.fail == "" && err != nil || test.fail != "" && (err == nil || !strings.Contains(err.Error(), test.fail) || !strings.Contains(err.Error(), "service operation failed")) {
				t.Fatalf("start error: %v", err)
			}
			got, err := os.ReadFile(filepath.Join(dir, "commands"))
			if err != nil || string(got) != test.want {
				t.Fatalf("commands = %q, error = %v; want %q", got, err, test.want)
			}
		})
	}
}

func TestLaunchdCustomDirectoryDoesNotStartService(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	note := installControllerService(t.Context(), "plist", filepath.Join(dir, "local.plist"), filepath.Join(dir, "controller.json"), dir)
	if !strings.Contains(note, "does not start") {
		t.Fatal(note)
	}
	if content, err := os.ReadFile(filepath.Join(dir, serviceFileName)); err != nil || string(content) != "plist" {
		t.Fatalf("installed service = %q, %v", content, err)
	}
	note = installControllerService(t.Context(), "plist", "local.plist", "controller.json", filepath.Join(dir, "absent"))
	if !strings.Contains(note, "Could not install") || !strings.Contains(note, "synod server --config") {
		t.Fatal(note)
	}
}
