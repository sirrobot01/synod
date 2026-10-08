package controller

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	serviceDirFlag    = "launchd-dir"
	defaultServiceDir = "/Library/LaunchDaemons"
	serviceFileName   = "dev.biodun.synod.server.plist"
	launchdTarget     = "system/dev.biodun.synod.server"
)

func controllerService(executable, configPath string) (string, error) {
	paths := []string{executable, configPath, filepath.Join(filepath.Dir(configPath), "controller.log")}
	for i, path := range paths {
		var escaped bytes.Buffer
		if err := xml.EscapeText(&escaped, []byte(path)); err != nil {
			return "", fmt.Errorf("encode launchd path: %w", err)
		}
		paths[i] = escaped.String()
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>dev.biodun.synod.server</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>server</string>
        <string>--config</string>
        <string>%s</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>ThrottleInterval</key><integer>5</integer>
    <key>Umask</key><integer>63</integer>
    <key>StandardOutPath</key><string>%s</string>
    <key>StandardErrorPath</key><string>%s</string>
</dict>
</plist>
`, paths[0], paths[1], paths[2], paths[2]), nil
}

func installControllerService(ctx context.Context, content, localPath, configPath, dir string) string {
	note := fmt.Sprintf("Wrote launchd service to %s.\nStart the controller with: synod server --config '%s'", localPath, strings.ReplaceAll(configPath, "'", "'\"'\"'"))
	installedPath := filepath.Join(dir, serviceFileName)
	if err := os.WriteFile(installedPath, []byte(content), 0o644); err != nil {
		return note + fmt.Sprintf("\nCould not install %s: %v", installedPath, err)
	}
	if dir != defaultServiceDir {
		return fmt.Sprintf("Installed launchd service to %s.\nA custom service directory does not start the controller.\n%s", installedPath, note)
	}
	if err := startLaunchd(ctx, installedPath); err != nil {
		return fmt.Sprintf("Installed launchd service to %s.\nThe service did not start: %v\nCheck the service with: sudo launchctl print %s\n%s", installedPath, err, launchdTarget, note)
	}
	return "Started the dev.biodun.synod.server service. It starts again after a reboot."
}

// startLaunchd replaces a loaded definition before it starts the saved service.
func startLaunchd(ctx context.Context, path string) error {
	commands := [][]string{{"enable", launchdTarget}}
	if exec.CommandContext(ctx, "launchctl", "print", launchdTarget).Run() == nil {
		commands = append(commands, []string{"bootout", launchdTarget})
	}
	commands = append(commands, []string{"bootstrap", "system", path})
	for _, args := range commands {
		if output, err := exec.CommandContext(ctx, "launchctl", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("launchctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}
