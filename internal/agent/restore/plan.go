// Package restore prepares app data and recovers interrupted app replacements.
package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/compose"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const operationLabel = "io.synod.restore.operation"

type failure struct {
	Code, Resource string
	Cause          error
}

func (e *failure) Error() string { return e.Code }
func (e *failure) Unwrap() error { return e.Cause }

type dataRoot struct {
	Source, Archive, Relative, Volume string
	Mode                              os.FileMode
	UserID, GroupID                   uint32
}

type sourceMapping struct {
	Root    int
	Subpath string
}

type plan struct {
	Snapshot   backup.Snapshot
	Definition *compose.Definition
	Containers []container.InspectResponse
	Services   map[string]string
	Networks   map[string]network.Network
	Data       []dataRoot
	Sources    map[string]sourceMapping
	Warnings   []string
}

// makePlan only reads the repository. Every writable path belongs to the new restore directory.
func makePlan(ctx context.Context, r *repository.Repository, saved backup.Snapshot, operationID uuid.UUID, directory, architecture string) (*plan, error) {
	p := &plan{Snapshot: saved, Sources: make(map[string]sourceMapping)}
	if saved.App.Definition == nil || len(saved.App.Definition.Containers) == 0 || len(saved.Images) != len(saved.App.Definition.Containers) {
		return nil, &failure{Code: "snapshot_invalid"}
	}
	raw, err := r.ReadFile(ctx, saved.ID, "definition/inspect.json", 4<<20)
	if err != nil || json.Unmarshal(raw, &p.Containers) != nil || len(p.Containers) != len(saved.App.Definition.Containers) {
		return nil, &failure{Code: "snapshot_invalid", Cause: err}
	}
	raw, err = r.ReadFile(ctx, saved.ID, "definition/networks.json", 4<<20)
	if err != nil || json.Unmarshal(raw, &p.Networks) != nil {
		return nil, &failure{Code: "snapshot_invalid", Cause: err}
	}
	project := saved.App.Definition.Name
	if saved.App.Definition.Kind == "container" {
		if len(p.Containers) != 1 || len(p.Containers[0].ID) != 64 {
			return nil, &failure{Code: "snapshot_invalid"}
		}
		project = "synod-" + p.Containers[0].ID[:12]
	}
	p.Definition, p.Services, err = compose.FromContainers(project, p.Containers)
	if err != nil {
		return nil, &failure{Code: "definition_unsupported", Resource: project, Cause: err}
	}
	hosts := make(map[string]bool)
	for _, requirement := range saved.Host {
		if p.Services[requirement.ContainerID] == "" || hosts[requirement.ContainerID] || (!requirement.PortsKnown && len(requirement.Ports) > 0) {
			return nil, &failure{Code: "snapshot_invalid"}
		}
		hosts[requirement.ContainerID] = true
		paths := make(map[string]bool)
		for _, path := range requirement.Paths {
			if !filepath.IsAbs(path.Path) || filepath.Clean(path.Path) != path.Path || paths[path.Path] || (!path.Known && (path.Mode != 0 || path.Device != 0 || path.UserID != 0 || path.GroupID != 0)) || os.FileMode(path.Mode)&^(os.ModeType|os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			paths[path.Path] = true
		}
	}
	images := make(map[string]backup.Image)
	variants := make(map[string]backup.ImageVariant)
	for _, image := range saved.Images {
		if image.Validate() != nil || images[image.ContainerID].Reference != "" || p.Services[image.ContainerID] == "" {
			return nil, &failure{Code: "snapshot_invalid"}
		}
		if !slices.Contains(image.Architectures, architecture) {
			return nil, &failure{Code: "image_platform_unsupported", Resource: image.Reference}
		}
		if image.LocalOnly {
			if _, err := v1.NewHash(image.Reference); err != nil {
				return nil, &failure{Code: "snapshot_invalid", Cause: err}
			}
		} else if _, err := name.NewDigest(image.Reference); err != nil {
			return nil, &failure{Code: "snapshot_invalid", Cause: err}
		}
		for _, variant := range image.Variants {
			if previous, ok := variants[variant.ID]; ok && previous != variant {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			variants[variant.ID] = variant
		}
		images[image.ContainerID] = image
	}
	paths := make(map[string]*agentv1.DiscoveredPath)
	for _, item := range saved.App.Definition.Paths {
		if item.ContainerId != "" {
			paths[item.ContainerId+":"+item.Target] = item
		}
	}
	volumes := make(map[string]string)
	for key, volume := range p.Definition.Volumes {
		volumes[volume.Name] = key
	}
	for _, c := range p.Containers {
		image := images[c.ID]
		if image.Reference == "" || image.SourceID != c.Image || !slices.ContainsFunc(saved.App.Definition.Containers, func(item *agentv1.DiscoveredContainer) bool { return item.Id == c.ID && item.ImageId == c.Image }) {
			return nil, &failure{Code: "snapshot_invalid"}
		}
		service := p.Definition.Services[p.Services[c.ID]]
		for _, variant := range image.Variants {
			if variant.Architecture == architecture {
				service.Image = variant.ID
			}
		}
		service.Platform = "linux/" + architecture
		if service.Labels == nil {
			service.Labels = make(map[string]string)
		}
		service.Labels[operationLabel] = operationID.String()
		p.Definition.Services[p.Services[c.ID]] = service
		for _, m := range c.Mounts {
			if m.Type != "volume" {
				continue
			}
			item := paths[c.ID+":"+m.Destination]
			if item == nil || item.Source != m.Source || item.Volume != m.Name {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			key := volumes[m.Name]
			volume := p.Definition.Volumes[key]
			if item.Status != "Never" && volume.Name == m.Name {
				volume.Name = "synod-" + operationID.String() + "-" + key
				p.Sources[m.Source] = sourceMapping{Root: len(p.Data)}
				p.Data = append(p.Data, dataRoot{Source: m.Source, Volume: volume.Name, Mode: os.ModeDir | 0755})
			}
			volume.External, volume.Driver = true, ""
			p.Definition.Volumes[key] = volume
		}
	}
	copies := slices.Clone(saved.Paths)
	slices.SortFunc(copies, func(a, b backup.Path) int {
		if n := len(a.Source) - len(b.Source); n != 0 {
			return n
		}
		return strings.Compare(a.Source, b.Source)
	})
	for _, copied := range copies {
		if !filepath.IsAbs(copied.Source) || filepath.Clean(copied.Source) != copied.Source || copied.Source == "/" || !strings.HasPrefix(copied.Archive, "paths/") {
			return nil, &failure{Code: "snapshot_invalid"}
		}
		info, err := r.Stat(ctx, saved.ID, copied.Archive)
		if err != nil || (!info.IsDir() && !info.Mode().IsRegular()) {
			return nil, &failure{Code: "snapshot_invalid", Cause: err}
		}
		if mapped, ok := p.Sources[copied.Source]; ok {
			root := &p.Data[mapped.Root]
			if root.Volume != "" && mapped.Subpath == "" {
				if !info.IsDir() || (root.Archive != "" && root.Archive != copied.Archive) {
					return nil, &failure{Code: "snapshot_invalid"}
				}
				root.Archive, root.Mode = copied.Archive, info.Mode()
			}
			continue
		}
		covered := false
		for i, root := range p.Data {
			if root.Archive == "" || !root.Mode.IsDir() || !strings.HasPrefix(copied.Source, root.Source+string(filepath.Separator)) {
				continue
			}
			relative, _ := filepath.Rel(root.Source, copied.Source)
			original, err := r.Stat(ctx, saved.ID, path.Join(root.Archive, filepath.ToSlash(relative)))
			if err == nil && original.Mode().Type() == info.Mode().Type() {
				p.Sources[copied.Source] = sourceMapping{Root: i, Subpath: relative}
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		relative := filepath.Join("data", fmt.Sprintf("path%d", len(p.Data)))
		if copied.Source == saved.App.Definition.ProjectDirectory && info.IsDir() {
			relative = "project"
		}
		if !info.IsDir() {
			relative = filepath.Join(relative, filepath.Base(copied.Source))
		}
		p.Sources[copied.Source] = sourceMapping{Root: len(p.Data)}
		p.Data = append(p.Data, dataRoot{Source: copied.Source, Archive: copied.Archive, Relative: relative, Mode: info.Mode()})
	}
	for _, c := range p.Containers {
		key := p.Services[c.ID]
		service := p.Definition.Services[key]
		for i, m := range service.Volumes {
			item := paths[c.ID+":"+m.Target]
			if item == nil {
				return nil, &failure{Code: "snapshot_invalid"}
			}
			if item.Status == "Never" {
				continue
			}
			mapped, ok := p.Sources[item.Source]
			if !ok {
				if item.Status != "Large" && item.Status != "Dumped" {
					return nil, &failure{Code: "source_path_unavailable", Resource: m.Target}
				}
				index := slices.IndexFunc(saved.MountSources, func(source backup.MountSource) bool { return source.Source == item.Source })
				if index < 0 {
					return nil, &failure{Code: "source_path_unavailable", Resource: m.Target}
				}
				source := saved.MountSources[index]
				mode := os.FileMode(source.Mode)
				if !mode.IsDir() && !mode.IsRegular() {
					return nil, &failure{Code: "source_path_unavailable", Resource: m.Target}
				}
				mapped = sourceMapping{Root: len(p.Data)}
				p.Sources[item.Source] = mapped
				p.Data = append(p.Data, dataRoot{Source: item.Source, Relative: filepath.Join("data", fmt.Sprintf("path%d", len(p.Data))), Mode: mode, UserID: source.UserID, GroupID: source.GroupID})
			}
			root := p.Data[mapped.Root]
			if root.Volume != "" {
				volumeKey := ""
				for candidate, volume := range p.Definition.Volumes {
					if volume.Name == root.Volume {
						volumeKey = candidate
						break
					}
				}
				if volumeKey == "" {
					return nil, &failure{Code: "snapshot_invalid"}
				}
				m.Type, m.Source, m.Bind = "volume", volumeKey, nil
				if m.Volume == nil {
					m.Volume = &compose.VolumeOptions{}
				}
				if mapped.Subpath != "" {
					m.Volume.Subpath = filepath.ToSlash(mapped.Subpath)
				}
				m.Volume.NoCopy = true
			} else {
				m.Source = filepath.Join(directory, root.Relative, mapped.Subpath)
				if m.Bind == nil {
					m.Bind = &compose.Bind{}
				}
				m.Bind.CreateHostPath = false
			}
			service.Volumes[i] = m
		}
		p.Definition.Services[key] = service
	}
	for _, key := range slices.Sorted(maps.Keys(p.Definition.Networks)) {
		name := p.Definition.Networks[key].Name
		if p.Networks[name].Name != name {
			return nil, &failure{Code: "snapshot_invalid"}
		}
	}
	if len(saved.Exclusions) > 0 {
		p.Warnings = append(p.Warnings, "excluded_paths")
	}
	return p, nil
}
