package backup

// Consistency records capture scope. Neither scope proves application recovery.
type Consistency string

const (
	ConsistencyUnverified Consistency = "unverified"
	ConsistencyFilesystem Consistency = "filesystem_snapshot"
)

// Limit codes. Each one prevents full protection and names the missing requirement.
const (
	AppConsistencyUnverified = "app_consistency_unverified"
	// DataExcluded means that the snapshot skipped Large paths, media, or files over 20 GiB.
	DataExcluded = "data_excluded"
	// ImagePlatformsLimited means that the registry was unavailable, so the snapshot holds
	// only the source architecture of a registry image.
	ImagePlatformsLimited = "image_platforms_limited"
	// HostRequirementsUnverified means that a host-network port or device check was not recorded.
	HostRequirementsUnverified = "host_requirements_unverified"
	// ContainerLayerData means that a container keeps data in its writable layer, which no backup copies.
	ContainerLayerData = "container_layer_data"
	// RestoreUnproven means that no complete restore drill passed for the app.
	RestoreUnproven = "restore_unproven"
)

// SnapshotLimits are the limits that an agent reports for one snapshot.
var SnapshotLimits = []string{DataExcluded, ImagePlatformsLimited, HostRequirementsUnverified, ContainerLayerData}

// Capture records how a source was read. A shared Point identifies one filesystem snapshot.
// An empty Point means that no common capture time was established for that source.
type Capture struct {
	Source string `json:"source"`
	Method string `json:"method"`
	Point  string `json:"point,omitempty"`
}

func (c Consistency) Validate() error {
	switch c {
	case "", ConsistencyUnverified, ConsistencyFilesystem:
		return nil
	default:
		return ErrInvalid
	}
}
