package provision

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replication"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/placement"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func (p *Planner) deployApp(ctx context.Context, claim operation.Claim) error {
	found, err := p.config.Store.GetApp(ctx, claim.InstallationID, claim.AppID)
	if err != nil {
		return err
	}
	spec := found.Spec
	database := spec.Tier == app.TierDatabase
	worker := spec.Tier == app.TierParallel && spec.Router == (app.RouterSpec{}) && len(spec.Service.Ports) == 0
	// A writer tier runs one primary copy and standbys that can take over.
	writer := database || spec.Tier == app.TierSingleWriter
	if database {
		if spec.Service, err = p.databaseService(spec); err != nil {
			return err
		}
	}
	if (!database && spec.Tier != app.TierParallel && spec.Tier != app.TierSingleWriter) || (!worker && (len(spec.Service.Ports) != 1 || (spec.Service.Ports[0].Protocol != "" && spec.Service.Ports[0].Protocol != "tcp"))) {
		return fmt.Errorf("unsupported app configuration")
	}
	resolved, err := p.config.ResolveImage(ctx, spec.Service)
	if err != nil {
		return err
	}
	if resolved.LocalOnly {
		return fmt.Errorf("local image evidence has no authenticated node binding")
	}
	service := resolved.Service
	snapshot, err := p.config.Store.ReadPlacementSnapshot(ctx, claim.InstallationID)
	if err != nil {
		return err
	}
	preferences := [][]uuid.UUID{nil, nil, nil}
	for index, names := range [][]string{spec.DevicePreference.Preferred, spec.DevicePreference.Allowed, spec.DevicePreference.Never} {
		for _, name := range names {
			if index == 1 && name == "*" && len(names) == 1 {
				continue
			}
			var matches []uuid.UUID
			for _, node := range snapshot.Nodes {
				if node.ID.String() == name || (node.Policy != nil && node.Policy.Labels["synod.name"] == name) {
					matches = append(matches, node.ID)
				}
			}
			if len(matches) != 1 {
				return fmt.Errorf("device preference %q matches %d devices", name, len(matches))
			}
			preferences[index] = append(preferences[index], matches[0])
		}
	}
	for _, node := range snapshot.Nodes {
		if node.Policy == nil || node.Policy.ClientAddress == "" || node.SessionID == uuid.Nil {
			if !slices.Contains(preferences[2], node.ID) {
				preferences[2] = append(preferences[2], node.ID)
			}
			continue
		}
		presence, err := p.config.Store.GetAgentPresence(ctx, claim.InstallationID, node.ID)
		if err != nil {
			return err
		}
		if (!slices.Contains(presence.Hello.Capabilities, "runtime.docker") || (writer && !slices.Contains(presence.Hello.Capabilities, replication.Capability))) && !slices.Contains(preferences[2], node.ID) {
			preferences[2] = append(preferences[2], node.ID)
		}
	}
	budget := placement.Vector{CPUMillis: service.Resources.CPUMillis, MemoryBytes: service.Resources.MemoryBytes, DiskBytes: service.Resources.DiskBytes}
	if spec.Tier == app.TierSingleWriter && budget.DiskBytes == 0 {
		budget.DiskBytes = 1 << 30
	}
	if budget.CPUMillis == 0 {
		budget.CPUMillis = 500
	}
	if budget.MemoryBytes == 0 {
		budget.MemoryBytes = 256 << 20
	}
	resourceID := uuid.New()
	resource := placement.Resource{ID: resourceID, ReservationID: uuid.New(), OwnedResourceIDs: []uuid.UUID{resourceID}, Kind: "service", Engine: "generic", SupportedArchitectures: service.SupportedArchitectures,
		PreferredNodes: preferences[0], AllowedNodes: preferences[1], ExcludedNodes: preferences[2], Budgets: placement.Budgets{Workload: &budget, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}, Ports: []placement.Port{{Name: "backend", Address: "0.0.0.0", Transport: "tcp"}}}
	if worker {
		resource.Ports = nil
	}
	if writer {
		resource.Role = "active"
		for _, mount := range service.Mounts {
			if mount.Type == "bind" {
				resource.RequiredPaths = append(resource.RequiredPaths, mount.Source)
			}
		}
	}
	request := placement.Request{Version: 1, InstallationID: claim.InstallationID, AppID: claim.AppID, OperationID: claim.OperationID, DesiredRevision: found.DesiredRevision, Profile: "compact", PortRange: p.config.EndpointPorts, Resources: []placement.Resource{resource}}
	// Refresh measured capacity before counting capable devices or reserving ingress.
	_, snapshot, err = p.config.Decider.Decide(ctx, request)
	if err != nil {
		return err
	}
	request.Resources = nil
	count := spec.Copies
	if writer {
		count += spec.Replication.Standbys
	}
	var capable []uuid.UUID
	if spec.CopiesMode == app.CopiesModeAllCapable {
		for _, node := range snapshot.Nodes {
			probe := request
			candidate := resource
			candidate.NodeAffinity = node.ID
			probe.Resources = []placement.Resource{candidate}
			if _, err := placement.Plan(probe, snapshot); err == nil {
				capable = append(capable, node.ID)
			}
		}
		count = len(capable)
	}
	if count < 1 || count > placement.MaxResources {
		return fmt.Errorf("app copy count %d exceeds planning bounds", count)
	}
	group := placement.Group{ID: "app-copies", DomainKey: "host", Soft: true}
	for index := range count {
		copy := resource
		copy.ID, copy.ReservationID = uuid.New(), uuid.New()
		copy.OwnedResourceIDs = []uuid.UUID{copy.ID}
		if writer && index > 0 {
			copy.Role = "standby"
			// A tier 2 standby only holds data. A tier 1 standby runs its engine.
			if !database {
				copy.Budgets = placement.Budgets{Workload: &placement.Vector{DiskBytes: budget.DiskBytes}, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}
			}
			copy.PreferredNodes = nil
			if len(copy.AllowedNodes) > 0 {
				copy.AllowedNodes = slices.Clone(copy.AllowedNodes)
				for _, node := range preferences[0] {
					if !slices.Contains(copy.AllowedNodes, node) {
						copy.AllowedNodes = append(copy.AllowedNodes, node)
					}
				}
			}
		}
		if spec.CopiesMode == app.CopiesModeAllCapable {
			copy.NodeAffinity = capable[index]
		}
		request.Resources = append(request.Resources, copy)
		group.Members = append(group.Members, copy.ID)
	}
	if count > 1 {
		request.Groups = []placement.Group{group}
		if writer {
			request.Groups = append(request.Groups, placement.Group{ID: "app-nodes", DomainKey: "node", Members: group.Members})
		}
	}
	if !worker {
		for _, node := range snapshot.Nodes {
			if node.Policy == nil || node.Policy.ClientAddress == "" || node.SessionID == uuid.Nil {
				continue
			}
			presence, err := p.config.Store.GetAgentPresence(ctx, claim.InstallationID, node.ID)
			if err != nil {
				return err
			}
			if !slices.Contains(presence.Hello.Capabilities, controlstream.RouterCapability) {
				continue
			}
			id := uuid.New()
			ingress := placement.Resource{ID: id, ReservationID: uuid.New(), OwnedResourceIDs: []uuid.UUID{id}, Kind: "service", Engine: "generic", SupportedArchitectures: []string{"amd64", "arm64"}, NodeAffinity: node.ID,
				Budgets: placement.Budgets{Workload: &placement.Vector{CPUMillis: 10, MemoryBytes: 8 << 20}, Supporting: &placement.Vector{}, Backup: &placement.Vector{}}, Ports: []placement.Port{{Name: "ingress", Address: "0.0.0.0", Transport: "tcp", Number: spec.Router.Port}}}
			probe := request
			probe.Resources, probe.Groups = []placement.Resource{ingress}, nil
			if database && spec.Router.ReaderPort != 0 {
				// A database with a reader route reserves both ports on each router device.
				reader := ingress
				reader.ID, reader.ReservationID = uuid.New(), uuid.New()
				reader.OwnedResourceIDs = []uuid.UUID{reader.ID}
				reader.Ports = []placement.Port{{Name: "reader", Address: "0.0.0.0", Transport: "tcp", Number: spec.Router.ReaderPort}}
				probe.Resources = append(probe.Resources, reader)
			}
			if _, err := placement.Plan(probe, snapshot); err != nil {
				continue
			}
			request.Resources = append(request.Resources, probe.Resources...)
		}
		if len(request.Resources) == count {
			return fmt.Errorf("no approved router device can reserve ingress port %d", spec.Router.Port)
		}
	}
	if len(request.Resources) > placement.MaxResources {
		return fmt.Errorf("app copies and ingress exceed %d reservations", placement.MaxResources)
	}
	decision, _, err := p.config.Decider.Decide(ctx, request)
	if err != nil {
		return err
	}
	var credentials []postgres.Credential
	if database {
		if credentials, err = p.databaseCredentials(ctx, found); err != nil {
			return err
		}
	}
	plan := &agentv1.ExecutionPlan{Version: 1}
	previous := map[uuid.UUID]string{}
	for index, resource := range request.Resources[:count] {
		if resource.Role == "standby" && !database {
			continue
		}
		at := slices.IndexFunc(decision.Assignments, func(a placement.Assignment) bool { return a.ResourceID == resource.ID })
		if at < 0 {
			return fmt.Errorf("app placement omitted copy %d", index)
		}
		assignment := decision.Assignments[at]
		if (!worker && len(assignment.Ports) != 1) || (worker && len(assignment.Ports) != 0) {
			return fmt.Errorf("app placement omitted backend port")
		}
		generic := &agentv1.GenericContainerConfig{Image: service.Image, AppName: found.Name, CopyIndex: uint32(index), Command: service.Command, CommandOverride: service.Command != nil, Entrypoint: service.Entrypoint, EntrypointOverride: service.Entrypoint != nil}
		for _, mount := range service.Mounts {
			generic.Mounts = append(generic.Mounts, &agentv1.Mount{Type: mount.Type, Source: mount.Source, Target: mount.Target, ReadOnly: mount.ReadOnly})
		}
		if health := service.HealthCheck; health != nil {
			generic.HealthCheck = &agentv1.HealthCheckConfig{Type: health.Type, Path: health.Path, Port: uint32(health.Port), Command: health.Command, IntervalSeconds: health.IntervalSeconds, TimeoutSeconds: health.TimeoutSeconds, UnhealthyThreshold: health.UnhealthyThreshold}
		}
		container := &agentv1.EnsureContainer{ImageDigest: service.Image, Generic: generic, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: budget.MemoryBytes, CpuMillis: budget.CPUMillis}
		if !worker {
			container.Ports = []*agentv1.PortBinding{{ContainerPort: uint32(service.Ports[0].ContainerPort), HostPort: uint32(assignment.Ports[0].Number), HostAddress: assignment.Ports[0].Address, Transport: "tcp"}}
		}
		for _, name := range slices.Sorted(maps.Keys(service.Environment)) {
			container.PlainEnv = append(container.PlainEnv, &agentv1.PlainEnv{Name: name, Value: service.Environment[name]})
		}
		var secrets []*agentv1.SecretReference
		if database {
			container, secrets = databaseContainer(spec, service, uint32(index), assignment.Ports[0].Address, assignment.Ports[0].Number, credentials)
		}
		var dependencies []string
		if id := previous[assignment.NodeID]; id != "" {
			dependencies = []string{id}
		}
		ensure, err := p.step(claim, epochFor(claim, assignment.NodeID), found.DesiredRevision, &agentv1.CommandInput{ResourceId: resource.ID.String(), Secrets: secrets, Payload: &agentv1.CommandInput_EnsureContainer{EnsureContainer: container}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_EXISTS, dependencies, beforeExecution)
		if err != nil {
			return err
		}
		start, err := p.step(claim, epochFor(claim, assignment.NodeID), found.DesiredRevision, &agentv1.CommandInput{ResourceId: resource.ID.String(), Payload: &agentv1.CommandInput_StartContainer{StartContainer: &agentv1.StartContainer{}}}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{ensure.Command.StepId}, beforeExecution)
		if err != nil {
			return err
		}
		plan.Steps = append(plan.Steps, ensure, start)
		previous[assignment.NodeID] = start.Command.StepId
	}
	_, err = p.config.Store.InstallPlacementPlan(ctx, claim, request, decision, plan)
	return err
}
