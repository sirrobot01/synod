// Package systemd starts Synod services on hosts that run systemd.
package systemd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// UnitDir is the system unit directory. Only units installed here are started.
const UnitDir = "/etc/systemd/system"

// Running reports whether this host booted with systemd.
func Running() bool {
	_, err := os.Stat("/run/systemd/system")
	return err == nil
}

// Start reloads the unit files, enables unit, and restarts it.
// A restart also replaces a running service that used an older unit file.
func Start(unit string) error {
	for _, arguments := range [][]string{{"daemon-reload"}, {"enable", unit}, {"restart", unit}} {
		if err := Run(arguments...); err != nil {
			return err
		}
	}
	return nil
}

// Run runs one systemctl command and returns its output in the error.
func Run(arguments ...string) error {
	if output, err := exec.Command("systemctl", arguments...).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl %s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Serves reports whether the installed unit runs with configPath and systemd is running it,
// or restarting it after a failure.
func Serves(unit, configPath string) bool {
	content, err := os.ReadFile(filepath.Join(UnitDir, unit+".service"))
	if err != nil || !strings.Contains(string(content), " --config "+configPath+"\n") || !Running() {
		return false
	}
	state, _ := exec.Command("systemctl", "is-active", unit).Output()
	return slices.Contains([]string{"active", "activating"}, strings.TrimSpace(string(state)))
}
