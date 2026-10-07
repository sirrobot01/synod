package daemon

import (
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"testing"
)

func TestDestinationHealthMeasuresOnlyOwnedDirectories(t *testing.T) {
	node := uuid.New()
	id := uuid.New()
	entry := destination.Entry{Destination: destination.Destination{ID: id, NodeID: node, Location: repository.Location{Kind: "local", Path: t.TempDir()}}}
	plan := destination.Plan{Destinations: []destination.Entry{entry}}
	other := entry
	other.Destination.NodeID = uuid.New()
	plan.Destinations = append(plan.Destinations, other)
	missing := entry
	missing.Destination.Location.Path += "/missing"
	plan.Destinations = append(plan.Destinations, missing)
	report := destinationHealth(plan, node)
	if err := destination.ValidateHealth(report); err != nil {
		t.Fatal(err)
	}
	if len(report.Destinations) != 1 || report.Destinations[0].DestinationId != id.String() || report.Destinations[0].AvailableBytes == 0 {
		t.Fatal("owned filesystem was not measured")
	}
}
