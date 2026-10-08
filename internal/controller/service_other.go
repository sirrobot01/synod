//go:build !darwin

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirrobot01/synod/internal/systemd"
)

const (
	serviceDirFlag    = "systemd-dir"
	defaultServiceDir = systemd.UnitDir
	serviceFileName   = "synod-server.service"
)

func controllerService(executable, configPath string) (string, error) {
	return fmt.Sprintf(`[Unit]
Description=Synod Controller
After=network.target docker.service
Wants=docker.service

[Service]
Type=simple
ExecStart=%s server --config %s
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
`, executable, configPath), nil
}

func installControllerService(_ context.Context, content, localPath, configPath, dir string) string {
	note := fmt.Sprintf("Wrote systemd service to %s.\nStart the controller with: synod server --config %s", localPath, configPath)
	installedPath := filepath.Join(dir, serviceFileName)
	if err := os.WriteFile(installedPath, []byte(content), 0o644); err != nil {
		return note + fmt.Sprintf("\nCould not install %s: %v", installedPath, err)
	}
	note = fmt.Sprintf("Installed systemd service to %s.\nStart it with: systemctl daemon-reload && systemctl enable --now synod-server", installedPath)
	if dir != systemd.UnitDir || !systemd.Running() {
		return note
	}
	if err := systemd.Start("synod-server"); err != nil {
		return note + fmt.Sprintf("\nThe service did not start: %v", err)
	}
	return "Started the synod-server service. It starts again after a reboot."
}
