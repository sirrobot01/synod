package backup

import (
	"uuid"

	"github.com/sirrobot01/synod/internal/backup"
)

// captureEvidence records scope without treating separate exports as one app transaction.
func (t *Tree) captureEvidence(method backup.Method, sqlite []string, dumps []backup.DatabaseDump) (backup.Consistency, []backup.Capture) {
	level := backup.ConsistencyFilesystem
	points := make(map[string]string)
	var captures []backup.Capture
	for _, root := range t.roots {
		capture := backup.Capture{Source: root.source, Method: "live_files"}
		if method == backup.Stop {
			capture.Method = "stopped_files"
		}
		if root.snapshot && root.capturePoint != "" {
			point, exists := points[root.capturePoint]
			if !exists {
				point = uuid.New().String()
				points[root.capturePoint] = point
			}
			capture.Method, capture.Point = "filesystem_snapshot", point
		} else {
			level = backup.ConsistencyUnverified
		}
		captures = append(captures, capture)
	}
	for _, source := range sqlite {
		captures = append(captures, backup.Capture{Source: source, Method: "sqlite_backup"})
	}
	for _, dump := range dumps {
		captures = append(captures, backup.Capture{Source: dump.ContainerID, Method: dump.Database + "_export"})
	}
	if len(points) != 1 || len(sqlite) > 0 || len(dumps) > 0 || len(t.Changing()) > 0 {
		level = backup.ConsistencyUnverified
	}
	return level, captures
}
