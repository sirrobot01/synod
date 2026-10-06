package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
)

var ErrDefinition = errors.New("container configuration cannot be represented safely in Compose; use a Compose project for this app")

type composeDefinition struct {
	Name     string                    `json:"name"`
	Services map[string]composeService `json:"services"`
	Volumes  map[string]composeVolume  `json:"volumes,omitempty"`
	Networks map[string]composeNetwork `json:"networks,omitempty"`
}

type composeService struct {
	Image              string                       `json:"image"`
	ContainerName      string                       `json:"container_name"`
	Hostname           string                       `json:"hostname,omitempty"`
	DomainName         string                       `json:"domainname,omitempty"`
	User               string                       `json:"user,omitempty"`
	WorkingDir         string                       `json:"working_dir,omitempty"`
	Command            []string                     `json:"command"`
	Entrypoint         []string                     `json:"entrypoint"`
	Environment        []string                     `json:"environment,omitempty"`
	Labels             map[string]string            `json:"labels,omitempty"`
	TTY                bool                         `json:"tty,omitzero"`
	StdinOpen          bool                         `json:"stdin_open,omitzero"`
	Init               *bool                        `json:"init,omitempty"`
	StopSignal         string                       `json:"stop_signal,omitempty"`
	StopGracePeriod    string                       `json:"stop_grace_period,omitempty"`
	Healthcheck        *composeHealth               `json:"healthcheck,omitempty"`
	Restart            string                       `json:"restart,omitempty"`
	ReadOnly           bool                         `json:"read_only,omitzero"`
	Privileged         bool                         `json:"privileged,omitzero"`
	NetworkMode        string                       `json:"network_mode,omitempty"`
	Networks           map[string]composeAttachment `json:"networks,omitempty"`
	Ports              []composePort                `json:"ports,omitempty"`
	Expose             []string                     `json:"expose,omitempty"`
	Volumes            []composeMount               `json:"volumes,omitempty"`
	Tmpfs              []string                     `json:"tmpfs,omitempty"`
	DNS                []string                     `json:"dns,omitempty"`
	DNSOptions         []string                     `json:"dns_opt,omitempty"`
	DNSSearch          []string                     `json:"dns_search,omitempty"`
	ExtraHosts         []string                     `json:"extra_hosts,omitempty"`
	GroupAdd           []string                     `json:"group_add,omitempty"`
	CapAdd             []string                     `json:"cap_add,omitempty"`
	CapDrop            []string                     `json:"cap_drop,omitempty"`
	SecurityOpt        []string                     `json:"security_opt,omitempty"`
	Sysctls            map[string]string            `json:"sysctls,omitempty"`
	Devices            []string                     `json:"devices,omitempty"`
	DeviceCgroupRules  []string                     `json:"device_cgroup_rules,omitempty"`
	Ulimits            map[string]composeUlimit     `json:"ulimits,omitempty"`
	Logging            *composeLogging              `json:"logging,omitempty"`
	Runtime            string                       `json:"runtime,omitempty"`
	IPC                string                       `json:"ipc,omitempty"`
	PID                string                       `json:"pid,omitempty"`
	UTS                string                       `json:"uts,omitempty"`
	UserNS             string                       `json:"userns_mode,omitempty"`
	Cgroup             string                       `json:"cgroup,omitempty"`
	CgroupParent       string                       `json:"cgroup_parent,omitempty"`
	ShmSize            int64                        `json:"shm_size,omitzero"`
	CPUShares          int64                        `json:"cpu_shares,omitzero"`
	CPUs               float64                      `json:"cpus,omitzero"`
	CPUPeriod          int64                        `json:"cpu_period,omitzero"`
	CPUQuota           int64                        `json:"cpu_quota,omitzero"`
	CPURealtimePeriod  int64                        `json:"cpu_rt_period,omitzero"`
	CPURealtimeRuntime int64                        `json:"cpu_rt_runtime,omitzero"`
	CPUSet             string                       `json:"cpuset,omitempty"`
	Memory             int64                        `json:"mem_limit,omitzero"`
	MemoryReservation  int64                        `json:"mem_reservation,omitzero"`
	MemorySwap         int64                        `json:"memswap_limit,omitzero"`
	MemorySwappiness   *int64                       `json:"mem_swappiness,omitempty"`
	OOMKillDisable     *bool                        `json:"oom_kill_disable,omitempty"`
	OOMScoreAdj        int                          `json:"oom_score_adj,omitzero"`
	PidsLimit          *int64                       `json:"pids_limit,omitempty"`
}

type composeHealth struct {
	Test          []string `json:"test"`
	Interval      string   `json:"interval,omitempty"`
	Timeout       string   `json:"timeout,omitempty"`
	StartPeriod   string   `json:"start_period,omitempty"`
	StartInterval string   `json:"start_interval,omitempty"`
	Retries       int      `json:"retries,omitzero"`
}
type composePort struct {
	Target    uint16 `json:"target"`
	Published string `json:"published,omitempty"`
	HostIP    string `json:"host_ip,omitempty"`
	Protocol  string `json:"protocol"`
}
type composeMount struct {
	Type     string                `json:"type"`
	Source   string                `json:"source,omitempty"`
	Target   string                `json:"target"`
	ReadOnly bool                  `json:"read_only,omitzero"`
	Bind     *composeBind          `json:"bind,omitempty"`
	Volume   *composeVolumeOptions `json:"volume,omitempty"`
}
type composeBind struct {
	Propagation    string `json:"propagation,omitempty"`
	SELinux        string `json:"selinux,omitempty"`
	CreateHostPath bool   `json:"create_host_path"`
}
type composeVolumeOptions struct {
	NoCopy  bool   `json:"nocopy,omitzero"`
	Subpath string `json:"subpath,omitempty"`
}
type composeVolume struct {
	Name   string `json:"name"`
	Driver string `json:"driver,omitempty"`
}
type composeNetwork struct {
	Name     string `json:"name"`
	External bool   `json:"external"`
}
type composeAttachment struct {
	Aliases         []string          `json:"aliases,omitempty"`
	IPv4Address     string            `json:"ipv4_address,omitempty"`
	IPv6Address     string            `json:"ipv6_address,omitempty"`
	LinkLocalIPs    []string          `json:"link_local_ips,omitempty"`
	MACAddress      string            `json:"mac_address,omitempty"`
	DriverOpts      map[string]string `json:"driver_opts,omitempty"`
	GatewayPriority int               `json:"gw_priority,omitzero"`
}
type composeUlimit struct {
	Soft int64 `json:"soft"`
	Hard int64 `json:"hard"`
}
type composeLogging struct {
	Driver  string            `json:"driver"`
	Options map[string]string `json:"options,omitempty"`
}

// StandaloneCompose preserves supported runtime settings and escapes Compose interpolation.
// The full inspect response is also saved, inside the encrypted snapshot.
func StandaloneCompose(c container.InspectResponse) ([]byte, error) {
	if c.Config == nil || c.HostConfig == nil || c.NetworkSettings == nil || len(c.ID) < 12 || c.Config.Image == "" {
		return nil, ErrDefinition
	}
	config, host := c.Config, c.HostConfig
	// These options have no equivalent in the generated portable definition.
	if config.ArgsEscaped || config.StdinOnce || host.AutoRemove || len(host.VolumesFrom) > 0 || len(host.Links) > 0 || len(host.DeviceRequests) > 0 || len(host.StorageOpt) > 0 || host.Umask != nil || host.CpusetMems != "" || host.BlkioWeight != 0 || len(host.BlkioWeightDevice)+len(host.BlkioDeviceReadBps)+len(host.BlkioDeviceWriteBps)+len(host.BlkioDeviceReadIOps)+len(host.BlkioDeviceWriteIOps) > 0 || host.Cgroup != "" || len(host.Annotations) > 0 || host.Isolation != "" || host.CPUCount != 0 || host.CPUPercent != 0 || host.IOMaximumIOps != 0 || host.IOMaximumBandwidth != 0 {
		return nil, ErrDefinition
	}
	for _, mode := range []string{string(host.NetworkMode), string(host.IpcMode), string(host.PidMode)} {
		if strings.HasPrefix(mode, "container:") {
			return nil, ErrDefinition
		}
	}
	service := composeService{
		Image: config.Image,
		ContainerName: strings.TrimPrefix(c.Name,
			"/"),
		Hostname:           config.Hostname,
		DomainName:         config.Domainname,
		User:               config.User,
		WorkingDir:         config.WorkingDir,
		Command:            config.Cmd,
		Entrypoint:         config.Entrypoint,
		Environment:        config.Env,
		Labels:             config.Labels,
		TTY:                config.Tty,
		StdinOpen:          config.OpenStdin,
		Init:               host.Init,
		StopSignal:         config.StopSignal,
		ReadOnly:           host.ReadonlyRootfs,
		Privileged:         host.Privileged,
		CapAdd:             host.CapAdd,
		CapDrop:            host.CapDrop,
		SecurityOpt:        host.SecurityOpt,
		Sysctls:            host.Sysctls,
		GroupAdd:           host.GroupAdd,
		DNSOptions:         host.DNSOptions,
		DNSSearch:          host.DNSSearch,
		ExtraHosts:         host.ExtraHosts,
		DeviceCgroupRules:  host.DeviceCgroupRules,
		Runtime:            host.Runtime,
		IPC:                string(host.IpcMode),
		PID:                string(host.PidMode),
		UTS:                string(host.UTSMode),
		UserNS:             string(host.UsernsMode),
		Cgroup:             string(host.CgroupnsMode),
		CgroupParent:       host.CgroupParent,
		ShmSize:            host.ShmSize,
		CPUShares:          host.CPUShares,
		CPUs:               float64(host.NanoCPUs) / 1e9,
		CPUPeriod:          host.CPUPeriod,
		CPUQuota:           host.CPUQuota,
		CPURealtimePeriod:  host.CPURealtimePeriod,
		CPURealtimeRuntime: host.CPURealtimeRuntime,
		CPUSet:             host.CpusetCpus,
		Memory:             host.Memory,
		MemoryReservation:  host.MemoryReservation,
		MemorySwap:         host.MemorySwap,
		MemorySwappiness:   host.MemorySwappiness,
		OOMKillDisable:     host.OomKillDisable,
		OOMScoreAdj:        host.OomScoreAdj,
		PidsLimit:          host.PidsLimit,
	}
	// An explicit empty array prevents a newer image from supplying new defaults.
	if service.Command == nil {
		service.Command = []string{}
	}
	if service.Entrypoint == nil {
		service.Entrypoint = []string{}
	}
	if config.StopTimeout != nil {
		if *config.StopTimeout < 0 {
			return nil, ErrDefinition
		}
		service.StopGracePeriod = strconv.Itoa(*config.StopTimeout) + "s"
	}
	if health := config.Healthcheck; health != nil {
		service.Healthcheck = &composeHealth{Test: health.Test, Retries: health.Retries}
		if health.Interval > 0 {
			service.Healthcheck.Interval = health.Interval.String()
		}
		if health.Timeout > 0 {
			service.Healthcheck.Timeout = health.Timeout.String()
		}
		if health.StartPeriod > 0 {
			service.Healthcheck.StartPeriod = health.StartPeriod.String()
		}
		if health.StartInterval > 0 {
			service.Healthcheck.StartInterval = health.StartInterval.String()
		}
	}
	service.Restart = string(host.RestartPolicy.Name)
	if service.Restart == "on-failure" && host.RestartPolicy.MaximumRetryCount > 0 {
		service.Restart += ":" + strconv.Itoa(host.RestartPolicy.MaximumRetryCount)
	}
	for _, address := range host.DNS {
		service.DNS = append(service.DNS, address.String())
	}
	for port := range config.ExposedPorts {
		service.Expose = append(service.Expose, port.String())
	}
	slices.Sort(service.Expose)
	bindingsByPort := c.NetworkSettings.Ports
	if len(bindingsByPort) == 0 {
		bindingsByPort = host.PortBindings
	}
	for port, bindings := range bindingsByPort {
		for _, binding := range bindings {
			if !binding.HostIP.IsValid() {
				binding.HostIP = netip.IPv4Unspecified()
			}
			service.Ports = append(service.Ports, composePort{Target: port.Num(), Published: binding.HostPort, HostIP: binding.HostIP.String(), Protocol: string(port.Proto())})
		}
	}
	slices.SortFunc(service.Ports, func(a, b composePort) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	for _, device := range host.Devices {
		service.Devices = append(service.Devices, device.PathOnHost+":"+device.PathInContainer+":"+device.CgroupPermissions)
	}
	if len(host.Ulimits) > 0 {
		service.Ulimits = make(map[string]composeUlimit)
		for _, limit := range host.Ulimits {
			service.Ulimits[limit.Name] = composeUlimit{Soft: limit.Soft, Hard: limit.Hard}
		}
	}
	if host.LogConfig.Type != "" {
		service.Logging = &composeLogging{Driver: host.LogConfig.Type, Options: host.LogConfig.Config}
	}
	definition := composeDefinition{Name: "synod-" + c.ID[:12], Volumes: make(map[string]composeVolume), Networks: make(map[string]composeNetwork)}
	for _, m := range c.Mounts {
		item := composeMount{Type: string(m.Type), Source: m.Source, Target: m.Destination, ReadOnly: !m.RW}
		switch m.Type {
		case mount.TypeBind:
			item.Bind = &composeBind{Propagation: string(m.Propagation)}
			for option := range strings.SplitSeq(m.Mode, ",") {
				if option == "z" || option == "Z" {
					item.Bind.SELinux = option
				}
			}
		case mount.TypeVolume:
			key := fmt.Sprintf("volume%d", len(definition.Volumes))
			definition.Volumes[key] = composeVolume{Name: m.Name, Driver: m.Driver}
			item.Source = key
			item.Volume = &composeVolumeOptions{NoCopy: slices.Contains(strings.Split(m.Mode, ","), "nocopy")}
		case mount.TypeTmpfs:
			continue // HostConfig.Tmpfs below preserves the mount options.
		default:
			return nil, ErrDefinition
		}
		for _, original := range host.Mounts {
			if original.Target != m.Destination {
				continue
			}
			if original.BindOptions != nil && (original.BindOptions.NonRecursive || original.BindOptions.ReadOnlyForceRecursive || original.BindOptions.ReadOnlyNonRecursive) {
				return nil, ErrDefinition
			}
			if original.VolumeOptions != nil {
				if original.VolumeOptions.DriverConfig != nil || len(original.VolumeOptions.Labels) > 0 {
					return nil, ErrDefinition
				}
				item.Volume = &composeVolumeOptions{NoCopy: original.VolumeOptions.NoCopy, Subpath: original.VolumeOptions.Subpath}
			}
		}
		service.Volumes = append(service.Volumes, item)
	}
	for path, options := range host.Tmpfs {
		if options != "" {
			path += ":" + options
		}
		service.Tmpfs = append(service.Tmpfs, path)
	}
	slices.Sort(service.Tmpfs)
	for _, m := range host.Mounts {
		if m.Type == mount.TypeTmpfs {
			return nil, ErrDefinition
		}
	}
	mode := string(host.NetworkMode)
	if mode == "host" || mode == "none" || mode == "bridge" || mode == "default" || mode == "" {
		if mode == "default" || mode == "" {
			mode = "bridge"
		}
		service.NetworkMode = mode
	} else {
		service.Networks = make(map[string]composeAttachment)
		names := make([]string, 0, len(c.NetworkSettings.Networks))
		for name := range c.NetworkSettings.Networks {
			names = append(names, name)
		}
		slices.Sort(names)
		for i, name := range names {
			endpoint := c.NetworkSettings.Networks[name]
			if endpoint == nil || len(endpoint.Links) > 0 {
				return nil, ErrDefinition
			}
			key := fmt.Sprintf("network%d", i)
			definition.Networks[key] = composeNetwork{Name: name, External: true}
			attachment := composeAttachment{Aliases: endpoint.Aliases, DriverOpts: endpoint.DriverOpts, GatewayPriority: endpoint.GwPriority, MACAddress: endpoint.MacAddress.String()}
			if ipam := endpoint.IPAMConfig; ipam != nil {
				if ipam.IPv4Address.IsValid() {
					attachment.IPv4Address = ipam.IPv4Address.String()
				}
				if ipam.IPv6Address.IsValid() {
					attachment.IPv6Address = ipam.IPv6Address.String()
				}
				for _, address := range ipam.LinkLocalIPs {
					attachment.LinkLocalIPs = append(attachment.LinkLocalIPs, address.String())
				}
			}
			// Keep the observed MAC because inspect does not say who assigned it.
			service.Networks[key] = attachment
		}
	}
	definition.Services = map[string]composeService{"app": service}
	raw, err := json.Marshal(definition)
	if err != nil {
		return nil, ErrDefinition
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, ErrDefinition
	}
	return json.MarshalIndent(escapeCompose(value), "", "  ")
}

// escapeCompose changes JSON values only. Compose does not interpolate map keys.
func escapeCompose(value any) any {
	switch item := value.(type) {
	case string:
		return strings.ReplaceAll(item, "$", "$$")
	case []any:
		for i, child := range item {
			item[i] = escapeCompose(child)
		}
	case map[string]any:
		for key, child := range item {
			item[key] = escapeCompose(child)
		}
	}
	return value
}
