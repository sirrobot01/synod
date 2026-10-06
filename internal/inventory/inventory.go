// Package inventory defines the app metadata exchanged by agents and the controller.
package inventory

import (
	"errors"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// App keeps its identity when a Compose container is replaced or an app disappears.
type App struct {
	ID         uuid.UUID              `json:"id"`
	NodeID     uuid.UUID              `json:"node_id"`
	NodeName   string                 `json:"node_name"`
	ObservedAt time.Time              `json:"observed_at"`
	Present    bool                   `json:"present"`
	Definition *agentv1.DiscoveredApp `json:"definition"`
}

// Device records the last scan attempt. A failure does not erase its saved apps.
type Device struct {
	NodeID      uuid.UUID  `json:"node_id"`
	NodeName    string     `json:"node_name"`
	ReceivedAt  *time.Time `json:"received_at"`
	ObservedAt  *time.Time `json:"observed_at"`
	FailureCode string     `json:"failure_code"`
}

type Catalog struct {
	Apps    []App    `json:"apps"`
	Devices []Device `json:"devices"`
}

var ErrInvalid = errors.New("invalid app inventory")

// Validate bounds the inventory and rejects ambiguous identities before storage.
func Validate(report *agentv1.AppInventory) error {
	if report == nil || report.ObservedAt.CheckValid() != nil || proto.Size(report) > 900<<10 || len(report.Apps) > 512 {
		return ErrInvalid
	}
	if report.FailureCode != "" {
		if len(report.Apps) != 0 || !slices.Contains([]string{"docker_scan_failed", "inventory_limit"}, report.FailureCode) {
			return ErrInvalid
		}
		return nil
	}
	keys := make(map[string]bool)
	for _, app := range report.Apps {
		if app == nil || keys[app.Key] || !text(app.Key, 512) || !text(app.Name, 256) || !slices.Contains([]string{"compose", "container"}, app.Kind) || !strings.HasPrefix(app.Key, app.Kind+":") || (app.ProjectDirectory != "" && !text(app.ProjectDirectory, 4096)) || len(app.Containers) == 0 || len(app.Containers) > 256 || len(app.Paths) > 2048 || len(app.Issues) > 512 {
			return ErrInvalid
		}
		keys[app.Key] = true
		containers := make(map[string]bool)
		for _, c := range app.Containers {
			if c == nil || containers[c.Id] || !text(c.Id, 128) || !text(c.Name, 256) || !text(c.Image, 2048) || (c.Service != "" && !text(c.Service, 256)) || (c.ImageId != "" && !text(c.ImageId, 256)) || len(c.RepoDigests) > 64 || (c.Platform != "" && !text(c.Platform, 128)) || (c.State != "" && !text(c.State, 64)) || !slices.Contains([]string{"", "postgres", "mysql", "mariadb", "redis"}, c.Database) {
				return ErrInvalid
			}
			containers[c.Id] = true
			for _, digest := range c.RepoDigests {
				if !text(digest, 2048) {
					return ErrInvalid
				}
			}
		}
		for _, p := range app.Paths {
			if p == nil || (p.Source != "" && !text(p.Source, 4096)) || (p.Target != "" && !text(p.Target, 4096)) || (p.Volume != "" && !text(p.Volume, 256)) || (p.ContainerId != "" && !containers[p.ContainerId]) || !slices.Contains([]string{"project", "compose", "env", "bind", "volume", "tmpfs", "npipe", "cluster", "image"}, p.Kind) || !slices.Contains([]string{"Included", "Large", "Dumped", "Never"}, p.Status) || !text(p.Reason, 512) || p.SizeBytes < 0 {
				return ErrInvalid
			}
		}
		for _, issue := range app.Issues {
			if !text(issue, 512) {
				return ErrInvalid
			}
		}
	}
	return nil
}

func text(value string, limit int) bool {
	return len(value) > 0 && len(value) <= limit && !strings.ContainsFunc(value, unicode.IsControl)
}
