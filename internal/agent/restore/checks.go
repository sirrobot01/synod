package restore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	agentbackup "github.com/sirrobot01/synod/internal/agent/backup"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/runtime/host"
)

// probe is a TCP port that a drill connects to inside a service's network namespace.
type probe struct {
	Port uint16
	// Required ports served users on the source. An exposed image port can be unused.
	Required bool
}

// drillTarget holds what a drill checks for one service. Collect it before isolateDrill
// rebuilds the services.
type drillTarget struct {
	Service     string
	Probes      []probe
	Healthcheck bool
	Database    bool
	// Unprobed names a reason why the service has no usable port probe.
	Unprobed string
}

func drillTargets(p *plan) []drillTarget {
	var targets []drillTarget
	for _, c := range p.Containers {
		target := drillTarget{Service: p.Services[c.ID]}
		for _, item := range p.Snapshot.App.Definition.Containers {
			if item.Id == c.ID && item.Database != "" {
				target.Database = true
			}
		}
		target.Healthcheck = c.Config.Healthcheck != nil && len(c.Config.Healthcheck.Test) > 0 && c.Config.Healthcheck.Test[0] != "NONE"
		add := func(port uint16, required bool) {
			index := slices.IndexFunc(target.Probes, func(known probe) bool { return known.Port == port })
			if index < 0 {
				target.Probes = append(target.Probes, probe{Port: port, Required: required})
			} else if required {
				target.Probes[index].Required = true
			}
		}
		for binding := range c.HostConfig.PortBindings {
			if binding.Proto() == "tcp" {
				add(binding.Num(), true)
			}
		}
		if host.UsesHostNetwork(c, p.Containers) {
			for _, requirement := range p.Snapshot.Host {
				if requirement.ContainerID != c.ID {
					continue
				}
				for _, port := range requirement.Ports {
					if port.Protocol != "tcp" {
						continue
					}
					// A listener on one host address cannot start inside the drill namespace.
					if address := net.ParseIP(port.Address); address != nil && !address.IsUnspecified() && !address.IsLoopback() {
						target.Unprobed = "host_address_listener"
						continue
					}
					add(port.Port, true)
				}
			}
		}
		for exposed := range c.Config.ExposedPorts {
			if exposed.Proto() == "tcp" {
				add(exposed.Num(), false)
			}
		}
		slices.SortFunc(target.Probes, func(a, b probe) int { return int(a.Port) - int(b.Port) })
		targets = append(targets, target)
	}
	slices.SortFunc(targets, func(a, b drillTarget) int { return strings.Compare(a.Service, b.Service) })
	return targets
}

// checkData compares each restored data root with the snapshot before any app container
// starts. The counts include every regular file and its size.
func checkData(ctx context.Context, api *client.Client, r *repository.Repository, p *plan, directory string) []proof.Check {
	var checks []proof.Check
	for _, root := range p.Data {
		if root.Archive == "" {
			continue
		}
		check := proof.Check{Kind: proof.CheckDataFiles, Resource: root.Source, Status: proof.CheckFailed}
		files, size, err := r.Summary(ctx, p.Snapshot.ID, root.Archive)
		if err != nil {
			check.Detail = "snapshot_summary_unavailable"
			checks = append(checks, check)
			continue
		}
		restored := filepath.Join(directory, root.Relative)
		if root.Volume != "" {
			volume, err := api.VolumeInspect(ctx, root.Volume, client.VolumeInspectOptions{})
			if err != nil || volume.Volume.Mountpoint == "" {
				check.Status, check.Detail = proof.CheckUnchecked, "volume_unreadable"
				checks = append(checks, check)
				continue
			}
			restored = volume.Volume.Mountpoint
		}
		var gotFiles, gotBytes int64
		err = filepath.WalkDir(restored, func(_ string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type().IsRegular() {
				info, err := entry.Info()
				if err != nil {
					return err
				}
				gotFiles, gotBytes = gotFiles+1, gotBytes+info.Size()
			}
			return ctx.Err()
		})
		switch {
		case err != nil:
			check.Status, check.Detail = proof.CheckUnchecked, "restored_data_unreadable"
		case gotFiles != files || gotBytes != size:
			check.Detail = fmt.Sprintf("files=%d/%d bytes=%d/%d", gotFiles, files, gotBytes, size)
		default:
			check.Status, check.Detail = proof.CheckPassed, fmt.Sprintf("files=%d bytes=%d", files, size)
		}
		checks = append(checks, check)
	}
	return checks
}

// checkApp waits for health checks, database content, and port answers of the running drill.
// A failed check means that the restored app does not work although its containers run.
func checkApp(ctx context.Context, api *client.Client, p *plan, targets []drillTarget) []proof.Check {
	var checks []proof.Check
	deadline := time.Now().Add(5 * time.Minute)
	for _, target := range targets {
		name := p.Definition.Services[target.Service].ContainerName
		inspected, err := api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
		if err != nil || inspected.Container.State == nil {
			checks = append(checks, proof.Check{Kind: proof.CheckAppFunction, Resource: target.Service, Status: proof.CheckFailed, Detail: "container_missing"})
			continue
		}
		functional := false
		if target.Healthcheck {
			check := waitHealthy(ctx, api, name, deadline)
			check.Resource = target.Service
			checks = append(checks, check)
			functional = functional || check.Status == proof.CheckPassed
		}
		if target.Database {
			checks = append(checks, checkDatabase(ctx, api, p, inspected.Container, target.Service, deadline)...)
		}
		for _, port := range target.Probes {
			result := checkPort(ctx, inspected.Container.State.Pid, port, !target.Database, deadline)
			for i := range result {
				result[i].Resource = fmt.Sprintf("%s:%d", target.Service, port.Port)
				functional = functional || result[i].Status == proof.CheckPassed
			}
			checks = append(checks, result...)
		}
		if !functional && !target.Healthcheck && !target.Database {
			detail := "no_health_check_or_port"
			if target.Unprobed != "" {
				detail = target.Unprobed
			}
			checks = append(checks, proof.Check{Kind: proof.CheckAppFunction, Resource: target.Service, Status: proof.CheckUnchecked, Detail: detail})
		}
	}
	return checks
}

func waitHealthy(ctx context.Context, api *client.Client, name string, deadline time.Time) proof.Check {
	check := proof.Check{Kind: proof.CheckContainerHealth, Status: proof.CheckFailed}
	for {
		inspected, err := api.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
		if err != nil || inspected.Container.State == nil || inspected.Container.State.Health == nil {
			check.Detail = "health_unavailable"
			return check
		}
		switch inspected.Container.State.Health.Status {
		case container.Healthy:
			check.Status, check.Detail = proof.CheckPassed, "healthy"
			return check
		case container.Unhealthy:
			check.Detail = "unhealthy"
			return check
		}
		if time.Now().After(deadline) || sleep(ctx, time.Second) != nil {
			check.Detail = "health_timeout"
			return check
		}
	}
}

// checkDatabase compares the restored database with the inventory of the source.
// Every source database must exist. A database that had tables or keys must still have some.
func checkDatabase(ctx context.Context, api *client.Client, p *plan, restored container.InspectResponse, service string, deadline time.Time) []proof.Check {
	var checks []proof.Check
	for _, dump := range p.Snapshot.Dumps {
		if p.Services[dump.ContainerID] != service {
			continue
		}
		check := proof.Check{Kind: proof.CheckDatabaseContent, Resource: service, Status: proof.CheckFailed}
		if len(dump.Inventory) == 0 {
			check.Status, check.Detail = proof.CheckUnchecked, "source_inventory_missing"
			checks = append(checks, check)
			continue
		}
		var inventory map[string]int64
		var err error
		for {
			// The database can still recover from its crash-consistent files.
			if inventory, err = agentbackup.DatabaseInventory(ctx, api, restored, dump.Database); err == nil || time.Now().After(deadline) || sleep(ctx, 2*time.Second) != nil {
				break
			}
		}
		if err != nil {
			check.Detail = "database_unavailable"
			checks = append(checks, check)
			continue
		}
		var missing, empty []string
		var tables int64
		for name, count := range dump.Inventory {
			got, ok := inventory[name]
			if !ok {
				missing = append(missing, name)
			} else if count > 0 && got == 0 {
				empty = append(empty, name)
			}
			tables += got
		}
		switch {
		case len(missing) > 0:
			check.Detail = "missing_databases=" + strconv.Itoa(len(missing))
		case len(empty) > 0:
			check.Detail = "empty_databases=" + strconv.Itoa(len(empty))
		default:
			check.Status, check.Detail = proof.CheckPassed, fmt.Sprintf("databases=%d objects=%d", len(dump.Inventory), tables)
		}
		checks = append(checks, check)
	}
	return checks
}

// checkPort connects inside the container's network namespace. When the port answers
// HTTP, the response must not be a server error.
func checkPort(ctx context.Context, pid int, port probe, http bool, deadline time.Time) []proof.Check {
	check := proof.Check{Kind: proof.CheckPort, Status: proof.CheckFailed}
	var conn net.Conn
	var err error
	for {
		for _, address := range []string{"127.0.0.1", "::1"} {
			if conn, err = dialIn(ctx, pid, net.JoinHostPort(address, strconv.Itoa(int(port.Port)))); err == nil {
				break
			}
		}
		if err == nil || errors.Is(err, errNamespace) || time.Now().After(deadline) || sleep(ctx, 2*time.Second) != nil {
			break
		}
	}
	if err != nil {
		check.Detail = "no_answer"
		if errors.Is(err, errNamespace) {
			check.Status, check.Detail = proof.CheckUnchecked, "namespace_unavailable"
		} else if !port.Required {
			check.Status, check.Detail = proof.CheckUnchecked, "exposed_port_unused"
		}
		return []proof.Check{check}
	}
	defer conn.Close()
	check.Status, check.Detail = proof.CheckPassed, "connected"
	if !http {
		return []proof.Check{check}
	}
	return append([]proof.Check{check}, checkHTTP(conn)...)
}

func checkHTTP(conn net.Conn) []proof.Check {
	conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nUser-Agent: synod-drill\r\nConnection: close\r\n\r\n")); err != nil {
		return nil
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		// The port does not speak HTTP. The connection is the functional check.
		return nil
	}
	response.Body.Close()
	check := proof.Check{Kind: proof.CheckHTTP, Status: proof.CheckPassed, Detail: "status=" + strconv.Itoa(response.StatusCode)}
	if response.StatusCode >= 500 {
		check.Status = proof.CheckFailed
	}
	return []proof.Check{check}
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
