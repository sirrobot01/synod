package backup

// HostRequirements records resources that a restore cannot copy to another device.
type HostRequirements struct {
	ContainerID string     `json:"container_id"`
	PortsKnown  bool       `json:"ports_known"`
	Ports       []HostPort `json:"ports,omitempty"`
	Paths       []HostPath `json:"paths,omitempty"`
}

type HostPort struct {
	Address  string `json:"address"`
	Port     uint16 `json:"port"`
	Protocol string `json:"protocol"`
}

// HostPath keeps the source type, permissions, owner, and device number.
// Known is false if stat failed.
type HostPath struct {
	Path    string `json:"path"`
	Known   bool   `json:"known"`
	Mode    uint32 `json:"mode"`
	UserID  uint32 `json:"uid"`
	GroupID uint32 `json:"gid"`
	Device  uint64 `json:"device,omitzero"`
}
