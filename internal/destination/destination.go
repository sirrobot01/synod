// Package destination defines repository settings and their delivery to agents.
package destination

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/repository"
)

const MaxPlanBytes = 512 << 10
const MaxEntryBytes = 32 << 10
const MaxDestinations = 64

var ErrInvalid = errors.New("invalid destination settings")

// Destination contains public settings. NodeID owns a local path.
type Destination struct {
	ID        uuid.UUID           `json:"id"`
	Name      string              `json:"name"`
	NodeID    uuid.UUID           `json:"node_id,omitzero"`
	Location  repository.Location `json:"location"`
	CreatedAt time.Time           `json:"created_at"`
}

// Entry carries credentials only in private input and the authenticated agent stream.
type Entry struct {
	Destination Destination            `json:"destination"`
	Credentials repository.Credentials `json:"credentials"`
}

func (Entry) String() string   { return "[private destination settings]" }
func (Entry) GoString() string { return "[private destination settings]" }

// Plan replaces all settings on one device. An empty plan removes its cached credentials.
type Plan struct {
	Revision     int64   `json:"revision"`
	Destinations []Entry `json:"destinations"`
}

func (Plan) String() string   { return "[private destination plan]" }
func (Plan) GoString() string { return "[private destination plan]" }

type Delivery struct {
	NodeID         uuid.UUID  `json:"node_id"`
	NodeName       string     `json:"node_name"`
	Revision       int64      `json:"revision"`
	Current        bool       `json:"current"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitzero"`
}

type Listing struct {
	Revision     int64         `json:"revision"`
	Destinations []Destination `json:"destinations"`
	Delivery     []Delivery    `json:"delivery"`
}

func (e Entry) Validate() error {
	d := e.Destination
	if d.ID == uuid.Nil || len(d.Name) == 0 || len(d.Name) > 128 || strings.TrimSpace(d.Name) != d.Name || strings.ContainsFunc(d.Name, unicode.IsControl) {
		return ErrInvalid
	}
	if (d.Location.Kind == "local") != (d.NodeID != uuid.Nil) || repository.Validate(d.Location, e.Credentials) != nil {
		return ErrInvalid
	}
	e.Destination.CreatedAt = time.Time{}
	raw, err := json.Marshal(e)
	defer clear(raw)
	if err != nil || len(raw) > MaxEntryBytes {
		return ErrInvalid
	}
	return nil
}

func (p Plan) Validate(nodeID uuid.UUID) error {
	if p.Revision < 1 || nodeID == uuid.Nil || len(p.Destinations) > MaxDestinations {
		return ErrInvalid
	}
	ids := make(map[uuid.UUID]bool)
	for _, e := range p.Destinations {
		if e.Validate() != nil || ids[e.Destination.ID] || (e.Destination.NodeID != uuid.Nil && e.Destination.NodeID != nodeID) {
			return ErrInvalid
		}
		ids[e.Destination.ID] = true
	}
	raw, err := json.Marshal(p)
	defer clear(raw)
	if err != nil || len(raw) > MaxPlanBytes {
		return ErrInvalid
	}
	return nil
}

// OnlyOnSource reports whether every selected destination is on the app's device.
// The caller must supply the app's actual destinations, not all configured destinations.
func OnlyOnSource(nodeID uuid.UUID, selected []Destination) bool {
	if nodeID == uuid.Nil || len(selected) == 0 {
		return false
	}
	for _, d := range selected {
		if d.Location.Kind != "local" || d.NodeID != nodeID {
			return false
		}
	}
	return true
}
