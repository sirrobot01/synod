// Package discovery reads Docker app metadata and proposes paths for backup.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"github.com/sirrobot01/synod/internal/runtime/host"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ImageLabel holds the saved image name of a restored container, which runs from an image ID.
const ImageLabel = "io.synod.image"

// Scan returns a complete inventory. Filters can restrict a scan to test projects.
// A Docker failure returns no inventory, so a partial scan cannot remove saved apps.
func Scan(ctx context.Context, api *client.Client, filters client.Filters) (*agentv1.AppInventory, error) {
	listed, err := api.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil {
		return nil, fmt.Errorf("list Docker containers: %w", err)
	}
	inventory := &agentv1.AppInventory{ObservedAt: timestamppb.Now()}
	// Without a mount table, no path is classified as a network share or a nested mount.
	table, err := host.ReadMounts()
	if err != nil {
		table = nil
	}
	apps := make(map[string]*agentv1.DiscoveredApp)
	for _, summary := range listed.Items {
		// Synod's own containers are not apps: drills, restore helpers, and the agent image.
		if summary.Labels["io.synod.proof"] != "" || summary.Labels["io.synod.restore.role"] != "" || summary.Labels["io.synod.role"] == "agent" {
			continue
		}
		inspected, err := api.ContainerInspect(ctx, summary.ID, client.ContainerInspectOptions{})
		if err != nil {
			return nil, fmt.Errorf("inspect container %s: %w", summary.ID, err)
		}
		c := inspected.Container
		if c.Config == nil {
			return nil, fmt.Errorf("inspect container %s: configuration is absent", summary.ID)
		}
		labels := c.Config.Labels
		project := labels["com.docker.compose.project"]
		key, kind, name := "container:"+c.ID, "container", strings.TrimPrefix(c.Name, "/")
		if project != "" {
			key, kind, name = "compose:"+project, "compose", project
		}
		app := apps[key]
		if app == nil {
			app = &agentv1.DiscoveredApp{Key: key, Name: name, Kind: kind}
			apps[key] = app
			inventory.Apps = append(inventory.Apps, app)
		}
		reference := c.Config.Image
		// A restored container runs from an image ID. Restore saves the original image name.
		if strings.HasPrefix(reference, "sha256:") && labels[ImageLabel] != "" {
			reference = labels[ImageLabel]
		}
		item := &agentv1.DiscoveredContainer{Id: c.ID, Name: strings.TrimPrefix(c.Name, "/"), Service: labels["com.docker.compose.service"], Image: reference, ImageId: c.Image}
		if c.State != nil {
			item.State = string(c.State.Status)
		}
		image, err := api.ImageInspect(ctx, c.Image)
		if err != nil {
			app.Issues = append(app.Issues, "image_metadata_unavailable:"+c.ID)
		} else {
			item.RepoDigests = image.RepoDigests
			item.Platform = image.Os + "/" + image.Architecture
			if image.Variant != "" {
				item.Platform += "/" + image.Variant
			}
		}
		databasePath := ""
		imageName := strings.Split(strings.Split(filepath.Base(reference), "@")[0], ":")[0]
		switch filepath.Base(imageName) {
		case "postgres":
			item.Database, databasePath = "postgres", "/var/lib/postgresql/data"
			for _, variable := range c.Config.Env {
				if value, ok := strings.CutPrefix(variable, "PGDATA="); ok && filepath.IsAbs(value) {
					databasePath = filepath.Clean(value)
				}
			}
		case "mysql", "mariadb":
			item.Database, databasePath = filepath.Base(imageName), "/var/lib/mysql"
		case "redis":
			item.Database, databasePath = "redis", "/data"
		}
		if databasePath != "" {
			flag := map[string]string{"postgres": "-D", "mysql": "--datadir", "mariadb": "--datadir", "redis": "--dir"}[item.Database]
			for i, arg := range c.Config.Cmd {
				value, found := strings.CutPrefix(arg, flag+"=")
				if arg == flag && i+1 < len(c.Config.Cmd) {
					value, found = c.Config.Cmd[i+1], true
				}
				if !found {
					continue
				}
				if !filepath.IsAbs(value) {
					app.Issues = append(app.Issues, "database_path_unsupported:"+c.ID)
					continue
				}
				databasePath = filepath.Clean(value)
			}
		}
		app.Containers = append(app.Containers, item)
		if project != "" {
			working := labels["com.docker.compose.project.working_dir"]
			if working == "" || !filepath.IsAbs(working) {
				app.Issues = append(app.Issues, "project_directory_unavailable:"+c.ID)
			} else {
				if app.ProjectDirectory != "" && app.ProjectDirectory != working {
					app.Issues = append(app.Issues, "project_directories_differ")
				}
				app.ProjectDirectory = working
				app.Paths = append(app.Paths, &agentv1.DiscoveredPath{Source: working, Kind: "project"})
				// Compose reads .env only when it exists. Each backup scans again, so a new file is copied.
				if _, err := os.Lstat(filepath.Join(working, ".env")); !errors.Is(err, fs.ErrNotExist) {
					app.Paths = append(app.Paths, &agentv1.DiscoveredPath{Source: filepath.Join(working, ".env"), Kind: "env"})
				}
			}
			files := labels["com.docker.compose.project.config_files"]
			if files == "" {
				app.Issues = append(app.Issues, "compose_files_unavailable:"+c.ID)
			}
			for _, file := range strings.Split(files, ",") {
				if file == "" {
					continue
				}
				if !filepath.IsAbs(file) && filepath.IsAbs(working) {
					file = filepath.Join(working, file)
				}
				app.Paths = append(app.Paths, &agentv1.DiscoveredPath{Source: file, Kind: "compose"})
			}
			for _, file := range strings.Split(labels["com.docker.compose.project.environment_file"], ",") {
				// Restore starts Compose with /dev/null as the env file, so the app's .env cannot change it.
				if file != "" && file != os.DevNull {
					if !filepath.IsAbs(file) && filepath.IsAbs(working) {
						file = filepath.Join(working, file)
					}
					app.Paths = append(app.Paths, &agentv1.DiscoveredPath{Source: file, Kind: "env"})
				}
			}
		}
		for _, m := range c.Mounts {
			path := &agentv1.DiscoveredPath{Source: m.Source, Target: m.Destination, Kind: string(m.Type), Volume: m.Name, ContainerId: c.ID, ReadOnly: !m.RW}
			if m.Type != mount.TypeBind && m.Type != mount.TypeVolume {
				path.Status, path.Reason = "Never", "This mount has no persistent host data."
			} else if m.Type == mount.TypeVolume && m.Driver != "local" {
				path.Status, path.Reason = "Never", "This volume needs a supported local mount."
			} else if databasePath != "" && (databasePath == m.Destination || strings.HasPrefix(m.Destination, databasePath+"/")) {
				path.Status, path.Reason = "Dumped", "A native database dump replaces this data."
			}
			if m.Type == mount.TypeVolume && path.Status != "Never" {
				volume, err := api.VolumeInspect(ctx, m.Name, client.VolumeInspectOptions{})
				if err != nil {
					return nil, fmt.Errorf("inspect volume %s: %w", m.Name, err)
				}
				if len(volume.Volume.Options) != 0 {
					// Driver mounts can hide host paths behind a volume data directory.
					path.Status, path.Reason = "Never", "Volumes with driver mount options need an explicit backup method."
					if kind := volume.Volume.Options["type"]; host.Remote(kind) {
						path.Status, path.Reason = "Remote", "This volume mounts a network share ("+kind+"). Network shares are skipped by default."
					}
				}
			}
			app.Paths = append(app.Paths, path)
			if path.Status != "Never" && databasePath != "" && databasePath != m.Destination && strings.HasPrefix(databasePath, strings.TrimSuffix(m.Destination, "/")+"/") {
				data := proto.Clone(path).(*agentv1.DiscoveredPath)
				data.Source = filepath.Join(m.Source, strings.TrimPrefix(databasePath, strings.TrimSuffix(m.Destination, "/")+"/"))
				data.Target = databasePath
				data.Status, data.Reason = "Dumped", "A native database dump replaces this data."
				app.Paths = append(app.Paths, data)
			}
		}
	}
	for _, app := range inventory.Apps {
		slices.SortFunc(app.Containers, func(a, b *agentv1.DiscoveredContainer) int { return strings.Compare(a.Id, b.Id) })
		slices.SortFunc(app.Paths, func(a, b *agentv1.DiscoveredPath) int {
			return strings.Compare(a.Source+"\x00"+a.Kind+"\x00"+a.ContainerId+"\x00"+a.Target, b.Source+"\x00"+b.Kind+"\x00"+b.ContainerId+"\x00"+b.Target)
		})
		app.Paths = slices.CompactFunc(app.Paths, func(a, b *agentv1.DiscoveredPath) bool {
			return a.Source == b.Source && a.Kind == b.Kind && a.ContainerId == b.ContainerId && a.Target == b.Target
		})
		for _, path := range app.Paths {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			measure(ctx, path, table)
		}
		app.Paths = append(app.Paths, nestedMounts(ctx, app.Paths, table)...)
		slices.Sort(app.Issues)
		app.Issues = slices.Compact(app.Issues)
	}
	slices.SortFunc(inventory.Apps, func(a, b *agentv1.DiscoveredApp) int { return strings.Compare(a.Key, b.Key) })
	return inventory, nil
}

// nestedMounts lists each filesystem that is mounted below a copied, Large, or Remote
// path. A walk of the parent stops at it, so it gets its own path and status.
func nestedMounts(ctx context.Context, paths []*agentv1.DiscoveredPath, table []host.Mount) []*agentv1.DiscoveredPath {
	known := make(map[string]bool)
	for _, path := range paths {
		known[filepath.Clean(path.Source)] = true
	}
	var result []*agentv1.DiscoveredPath
	for _, parent := range paths {
		if !slices.Contains([]string{"Included", "Large", "Remote"}, parent.Status) || !slices.Contains([]string{"bind", "volume", "project"}, parent.Kind) {
			continue
		}
		resolved, err := filepath.EvalSymlinks(parent.Source)
		if err != nil {
			continue
		}
		for _, m := range host.Below(table, resolved) {
			if known[m.Point] || ctx.Err() != nil {
				continue
			}
			known[m.Point] = true
			nested := &agentv1.DiscoveredPath{Source: m.Point, Kind: "mount", ContainerId: parent.ContainerId, ReadOnly: parent.ReadOnly}
			if parent.Target != "" {
				relative, _ := filepath.Rel(resolved, m.Point)
				nested.Target = filepath.Join(parent.Target, relative)
			}
			measure(ctx, nested, table)
			result = append(result, nested)
		}
	}
	return result
}
