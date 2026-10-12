package backup

import (
	"uuid"

	"github.com/sirrobot01/synod/internal/backup"
)

// captureEvidence records scope. Only one filesystem snapshot of every root, including the
// database data directories, gives one capture time for the whole app.
func (t *Tree) captureEvidence(method backup.Method, sqlite []string, dumps []backup.DatabaseDump) (backup.Consistency, []backup.Capture) {
	level := backup.ConsistencyFilesystem
	points := make(map[string]string)
	var captures []backup.Capture
	for _, root := range t.roots {
		if root.database {
			continue
		}
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
	// Exports run at other moments. They agree with the files only as a fallback beside
	// database files that the shared snapshot holds.
	if len(points) != 1 || len(sqlite) > 0 || (len(dumps) > 0 && !t.databaseFiles) || len(t.Changing()) > 0 {
		level = backup.ConsistencyUnverified
	}
	return level, captures
}
