package backup

// Consistency records capture scope. Neither scope proves application recovery.
type Consistency string

const (
	ConsistencyUnverified Consistency = "unverified"
	ConsistencyFilesystem Consistency = "filesystem_snapshot"
)

const AppConsistencyUnverified = "app_consistency_unverified"

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
