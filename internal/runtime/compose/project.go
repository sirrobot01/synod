package compose

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
)

var projectName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
var serviceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// FromContainers preserves observed service names and shared volumes and networks.
// It refuses replicas and dependencies whose saved settings are ambiguous.
func FromContainers(project string, containers []container.InspectResponse) (*Definition, map[string]string, error) {
	if !projectName.MatchString(project) || len(project) > 128 || len(containers) == 0 || len(containers) > 256 {
		return nil, nil, ErrDefinition
	}
	keys := make(map[string]string)
	services := make(map[string]bool)
	for _, c := range containers {
		if c.Config == nil || c.HostConfig == nil || c.ID == "" || keys[c.ID] != "" {
			return nil, nil, ErrDefinition
		}
		key := c.Config.Labels["com.docker.compose.service"]
		if key == "" && len(containers) == 1 {
			key = "app"
		}
		if !serviceName.MatchString(key) || services[key] {
			return nil, nil, ErrDefinition
		}
		services[key], keys[c.ID] = true, key
	}
	result := &Definition{Name: project, Services: make(map[string]Service), Volumes: make(map[string]Volume), Networks: make(map[string]Network)}
	volumes, networks := make(map[string]string), make(map[string]string)
	for _, c := range containers {
		host := *c.HostConfig
		c.HostConfig = &host
		modes := []string{string(host.NetworkMode), string(host.IpcMode), string(host.PidMode)}
		for i, mode := range modes {
			if id, ok := strings.CutPrefix(mode, "container:"); ok {
				key := keys[id]
				if key == "" || id == c.ID {
					return nil, nil, ErrDefinition
				}
				modes[i] = "service:" + key
			}
		}
		host.NetworkMode, host.IpcMode, host.PidMode = container.NetworkMode(modes[0]), container.IpcMode(modes[1]), container.PidMode(modes[2])
		one, err := FromContainer(c)
		if err != nil {
			return nil, nil, err
		}
		service := one.Services["app"]
		service.Labels = maps.Clone(service.Labels)
		maps.DeleteFunc(service.Labels, func(key, _ string) bool { return strings.HasPrefix(key, "com.docker.compose.") })
		for i, m := range service.Volumes {
			if m.Type != "volume" {
				continue
			}
			volume := one.Volumes[m.Source]
			key := volumes[volume.Name]
			if key == "" {
				key = fmt.Sprintf("volume%d", len(volumes))
				volumes[volume.Name], result.Volumes[key] = key, volume
			}
			if result.Volumes[key] != volume {
				return nil, nil, ErrDefinition
			}
			service.Volumes[i].Source = key
		}
		if len(service.Networks) > 0 {
			attachments := make(map[string]Attachment)
			for _, oldKey := range slices.Sorted(maps.Keys(service.Networks)) {
				network := one.Networks[oldKey]
				key := networks[network.Name]
				if key == "" {
					key = fmt.Sprintf("network%d", len(networks))
					networks[network.Name], result.Networks[key] = key, network
				}
				attachments[key] = service.Networks[oldKey]
			}
			service.Networks = attachments
		}
		if depends := c.Config.Labels["com.docker.compose.depends_on"]; depends != "" {
			service.DependsOn = make(map[string]Dependency)
			for item := range strings.SplitSeq(depends, ",") {
				parts := strings.Split(item, ":")
				if len(parts) != 3 || !services[parts[0]] || parts[0] == keys[c.ID] || !slices.Contains([]string{"service_started", "service_healthy", "service_completed_successfully"}, parts[1]) {
					return nil, nil, ErrDefinition
				}
				restart, err := strconv.ParseBool(parts[2])
				if err != nil {
					return nil, nil, ErrDefinition
				}
				service.DependsOn[parts[0]] = Dependency{Condition: parts[1], Restart: restart, Required: true}
			}
		}
		result.Services[keys[c.ID]] = service
	}
	return result, keys, nil
}
