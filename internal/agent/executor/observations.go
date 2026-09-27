package executor

import (
	"context"
	"time"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Observations returns a bounded snapshot without blocking heartbeat delivery on Docker.
func (e *Executor) Observations() []*agentv1.Observation {
	e.mu.Lock()
	defer e.mu.Unlock()
	if time.Since(e.observedAt) > 15*time.Second {
		return nil
	}
	result := make([]*agentv1.Observation, len(e.observations))
	for i, observation := range e.observations {
		result[i] = proto.Clone(observation).(*agentv1.Observation)
	}
	return result
}

func (e *Executor) collectObservations(ctx context.Context) {
	read, cancel := context.WithTimeout(ctx, e.options.InspectionTimeout)
	defer cancel()
	resources, err := e.runtime.Inventory(read)
	var observations []*agentv1.Observation
	if err == nil {
		for _, resource := range resources {
			if resource.Health == "" {
				continue
			}
			observation := &agentv1.Observation{ResourceId: resource.ResourceID.String(), Lifecycle: resource.State, Health: resource.Health, Ports: resource.Ports, ObservedAt: timestamppb.Now()}
			observations = append(observations, observation)
		}
	}
	e.mu.Lock()
	e.observations, e.observedAt = observations, time.Now()
	e.mu.Unlock()
}
