package synod_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallerVerifiesAndUpgradesEachPlatform(t *testing.T) {
	for _, platform := range []struct{ system, machine, asset string }{
		{"Linux", "x86_64", "synod-linux-amd64"},
		{"Linux", "aarch64", "synod-linux-arm64"},
		{"Darwin", "x86_64", "synod-darwin-amd64"},
		{"Darwin", "arm64", "synod-darwin-arm64"},
	} {
		t.Run(platform.asset, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			release := filepath.Join(dir, "release")
			installed := filepath.Join(dir, "installed with spaces")
			for _, path := range []string{bin, release} {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for name, script := range map[string]string{
				"uname": fmt.Sprintf("#!/bin/sh\ncase $1 in -s) echo %s;; -m) echo %s;; esac\n", platform.system, platform.machine),
				"launchctl": `#!/bin/sh
if [ "$1" = print ]; then [ "$SYNOD_TEST_LOADED" = yes ]; exit; fi
printf '%s\n' "$*" >> "$SYNOD_TEST_RESTARTS"
[ "$SYNOD_TEST_RESTART_FAIL" != yes ]
`,
				"systemctl": `#!/bin/sh
if [ "$1" = is-active ]; then [ "$SYNOD_TEST_LOADED" = yes ]; exit; fi
if [ "$1" = show ]; then printf '{ path=%s ; argv[]=%s server ; ignore_errors=no ; }\n' "$SYNOD_TEST_SERVICE_BINARY" "$SYNOD_TEST_SERVICE_BINARY"; exit; fi
printf '%s\n' "$*" >> "$SYNOD_TEST_RESTARTS"
[ "$SYNOD_TEST_RESTART_FAIL" != yes ]
`,
				"plutil": `#!/bin/sh
printf '%s\n' "$SYNOD_TEST_SERVICE_BINARY"
`,
			} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("SYNOD_TEST_RESTARTS", filepath.Join(dir, "restarts"))
			t.Setenv("SYNOD_TEST_LOADED", "yes")
			t.Setenv("SYNOD_TEST_RESTART_FAIL", "no")
			t.Setenv("SYNOD_TEST_SERVICE_BINARY", filepath.Join(installed, "synod"))
			// Release fixtures are executables so the installer verifies their version too.
			writeRelease := func(version string) string {
				t.Helper()
				content := fmt.Sprintf("#!/bin/sh\necho 'synod version %s (test; %s)'\n", version, platform.asset)
				if err := os.WriteFile(filepath.Join(release, platform.asset), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
				checksum := fmt.Sprintf("%x  %s\n", sha256.Sum256([]byte(content)), platform.asset)
				if err := os.WriteFile(filepath.Join(release, "SHA256SUMS"), []byte(checksum), 0o600); err != nil {
					t.Fatal(err)
				}
				return content
			}
			install := func(version string, wantSuccess bool) string {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "sh", "install.sh", "--version", version, "--from-dir", release, "--install-dir", installed)
				output, err := cmd.CombinedOutput()
				if (err == nil) != wantSuccess {
					t.Fatalf("install result: %v\n%s", err, output)
				}
				if _, err := os.Stat(filepath.Join(installed, ".synod-install.lock")); !os.IsNotExist(err) {
					t.Fatalf("installer left a lock: %v", err)
				}
				return string(output)
			}
			first := writeRelease("v0.1.0")
			install("v0.1.0", true)
			before, err := os.Stat(filepath.Join(installed, "synod"))
			if err != nil || before.Mode().Perm() != 0o755 {
				t.Fatalf("installed executable: %v", err)
			}
			if output := install("v0.1.0", true); !strings.Contains(output, "already installed") {
				t.Fatal(output)
			}
			after, err := os.Stat(filepath.Join(installed, "synod"))
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("repeat install replaced the executable")
			}
			if _, err := os.Stat(filepath.Join(dir, "restarts")); !os.IsNotExist(err) {
				t.Fatal("fresh or repeated install restarted a service")
			}
			if err := os.WriteFile(filepath.Join(release, platform.asset), []byte("corrupt"), 0o600); err != nil {
				t.Fatal(err)
			}
			if output := install("v0.1.0", false); !strings.Contains(output, "checksum differs") {
				t.Fatal(output)
			}
			second := writeRelease("v0.2.0")
			if output := install("v0.1.0", false); !strings.Contains(output, "version differs") {
				t.Fatal(output)
			}
			if data, err := os.ReadFile(filepath.Join(installed, "synod")); err != nil || string(data) != first {
				t.Fatal("invalid release changed the installed executable")
			}
			install("v0.2.0", true)
			for name, want := range map[string]string{"synod": second, "synod.previous": first} {
				if data, err := os.ReadFile(filepath.Join(installed, name)); err != nil || string(data) != want {
					t.Fatalf("upgrade did not preserve %s", name)
				}
			}
			wantRestarts := "restart synod-server\nrestart synod-agent\n"
			if platform.system == "Darwin" {
				wantRestarts = "kickstart -k system/dev.biodun.synod.server\n"
			}
			if data, err := os.ReadFile(filepath.Join(dir, "restarts")); err != nil || string(data) != wantRestarts {
				t.Fatalf("service restarts = %q, error = %v", data, err)
			}
			writeRelease("v0.3.0")
			t.Setenv("SYNOD_TEST_LOADED", "no")
			install("v0.3.0", true)
			if data, err := os.ReadFile(filepath.Join(dir, "restarts")); err != nil || string(data) != wantRestarts {
				t.Fatal("upgrade started an unloaded service")
			}
			writeRelease("v0.4.0")
			t.Setenv("SYNOD_TEST_LOADED", "yes")
			t.Setenv("SYNOD_TEST_SERVICE_BINARY", "/another/installation/synod")
			install("v0.4.0", true)
			if data, err := os.ReadFile(filepath.Join(dir, "restarts")); err != nil || string(data) != wantRestarts {
				t.Fatal("upgrade restarted a service that uses another binary")
			}
			writeRelease("v0.5.0")
			t.Setenv("SYNOD_TEST_SERVICE_BINARY", filepath.Join(installed, "synod"))
			t.Setenv("SYNOD_TEST_RESTART_FAIL", "yes")
			if output := install("v0.5.0", true); !strings.Contains(output, "to use the new version") {
				t.Fatalf("restart failure has no recovery instruction: %s", output)
			}
		})
	}
}
