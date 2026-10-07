// Package destination defines repository settings and their delivery to agents.
package destination

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/repository"
)

const MaxPlanBytes = 900 << 10
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
	InstallationID uuid.UUID         `json:"installation_id"`
	Revision       int64             `json:"revision"`
	Destinations   []Entry           `json:"destinations"`
	Peers          []Peer            `json:"peers,omitempty"`
	Ready          []uuid.UUID       `json:"ready,omitempty"`
	Schedules      []backup.Schedule `json:"schedules,omitempty"`
	Proofs         []proof.Schedule  `json:"proofs,omitempty"`
	Catalog        []byte            `json:"catalog,omitempty"`
}

func (Plan) String() string   { return "[private destination plan]" }
func (Plan) GoString() string { return "[private destination plan]" }

// Peer permits a specific enrolled certificate to use device destinations.
type Peer struct {
	NodeID            uuid.UUID `json:"node_id"`
	Address           string    `json:"address,omitempty"`
	CertificateSHA256 string    `json:"certificate_sha256"`
}

// ValidAddress accepts a host and port. An empty host is allowed only before advertisement.
func ValidAddress(address string, allowEmptyHost bool) bool {
	host, port, err := net.SplitHostPort(address)
	number, numberErr := strconv.Atoi(port)
	return len(address) <= 260 && len(host) <= 253 && err == nil && numberErr == nil && number > 0 && number <= 65535 && (host != "" || allowEmptyHost) && !strings.ContainsAny(host, "/\\%?#@ \t\r\n")
}

type Delivery struct {
	SessionID      string     `json:"-"`
	NodeID         uuid.UUID  `json:"node_id"`
	NodeName       string     `json:"node_name"`
	Revision       int64      `json:"revision"`
	Current        bool       `json:"current"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitzero"`
}

type Listing struct {
	Space        []Space        `json:"space"`
	Checks       []proof.Status `json:"checks,omitempty"`
	Revision     int64          `json:"revision"`
	Destinations []Destination  `json:"destinations"`
	Delivery     []Delivery     `json:"delivery"`
}

func (e Entry) Validate() error {
	d := e.Destination
	if d.ID == uuid.Nil || len(d.Name) == 0 || len(d.Name) > 128 || strings.TrimSpace(d.Name) != d.Name || strings.ContainsFunc(d.Name, unicode.IsControl) {
		return ErrInvalid
	}
	if (d.Location.Kind == "local" || d.Location.Kind == "device") != (d.NodeID != uuid.Nil) || repository.Validate(d.Location, e.Credentials) != nil {
		return ErrInvalid
	}
	if d.Location.Kind == "device" && (d.Location.Device.NodeID != d.NodeID || d.Location.Device.DestinationID != d.ID) {
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
	if p.InstallationID == uuid.Nil || p.Revision < 1 || nodeID == uuid.Nil || len(p.Destinations) > MaxDestinations {
		return ErrInvalid
	}
	ids := make(map[uuid.UUID]bool)
	for _, e := range p.Destinations {
		if e.Validate() != nil || ids[e.Destination.ID] || (e.Destination.Location.Kind == "local" && e.Destination.NodeID != nodeID) {
			return ErrInvalid
		}
		ids[e.Destination.ID] = true
	}
	if len(p.Peers) > 256 {
		return ErrInvalid
	}
	certificates := make(map[string]bool)
	for _, peer := range p.Peers {
		hash, err := hex.DecodeString(peer.CertificateSHA256)
		if peer.NodeID == uuid.Nil || (peer.Address != "" && !ValidAddress(peer.Address, false)) || err != nil || len(hash) != 32 || certificates[peer.CertificateSHA256] {
			return ErrInvalid
		}
		certificates[peer.CertificateSHA256] = true
	}
	ready := make(map[uuid.UUID]bool)
	for _, id := range p.Ready {
		if !ids[id] || ready[id] {
			return ErrInvalid
		}
		ready[id] = true
	}
	if len(p.Schedules) > 512 {
		return ErrInvalid
	}
	apps := make(map[uuid.UUID]bool)
	for _, schedule := range p.Schedules {
		if schedule.Validate() != nil || schedule.NodeID != nodeID || apps[schedule.AppID] || schedule.InstallationID != p.InstallationID {
			return ErrInvalid
		}
		apps[schedule.AppID] = true
	}
	if len(p.Proofs) > 576 {
		return ErrInvalid
	}
	proofIDs := make(map[uuid.UUID]bool)
	for _, schedule := range p.Proofs {
		if schedule.Validate() != nil || schedule.NodeID != nodeID || schedule.InstallationID != p.InstallationID || proofIDs[schedule.ID] || (schedule.DestinationID != uuid.Nil && !ids[schedule.DestinationID]) {
			return ErrInvalid
		}
		proofIDs[schedule.ID] = true
	}
	if len(p.Catalog) > 0 {
		catalog, err := repository.DecodeCatalog(p.Catalog)
		if err != nil || catalog.Generation != uint64(p.Revision) || p.InstallationID != catalog.InstallationID {
			return ErrInvalid
		}
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
		if (d.Location.Kind != "local" && d.Location.Kind != "device") || d.NodeID != nodeID {
			return false
		}
	}
	return true
}

// Connection resolves a repository from the durable plan and the agent's own identity.
func (p Plan) Connection(id uuid.UUID, identity *repository.DeviceIdentity) (repository.Location, repository.Credentials, error) {
	for _, entry := range p.Destinations {
		if entry.Destination.ID != id {
			continue
		}
		location, credentials := entry.Destination.Location, entry.Credentials
		if location.Kind != "device" {
			return location, credentials, nil
		}
		if identity == nil || location.Device == nil {
			return repository.Location{}, repository.Credentials{}, ErrInvalid
		}
		target := *location.Device
		target.CertificateSHA256 = nil
		for _, peer := range p.Peers {
			if peer.NodeID == target.NodeID && peer.Address != "" {
				target.Endpoint = "https://" + peer.Address
				target.CertificateSHA256 = append(target.CertificateSHA256, peer.CertificateSHA256)
			}
		}
		if target.Endpoint == "" {
			return repository.Location{}, repository.Credentials{}, ErrInvalid
		}
		location.Device = &target
		credentials.DeviceIdentity = identity
		return location, credentials, nil
	}
	return repository.Location{}, repository.Credentials{}, ErrInvalid
}
