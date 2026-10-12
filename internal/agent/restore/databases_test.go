package restore

import (
	"errors"
	"testing"

	"github.com/sirrobot01/synod/internal/backup"
	"github.com/sirrobot01/synod/internal/inventory"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

func TestDatabaseFilesReplaceTheExportOnlyFromOneCapture(t *testing.T) {
	definition := &agentv1.DiscoveredApp{Paths: []*agentv1.DiscoveredPath{
		{Source: "/srv/db", ContainerId: "db", Status: "Dumped"},
		{Source: "/srv/db-wal", ContainerId: "db", Status: "Dumped"},
		{Source: "/srv/app", ContainerId: "app", Status: "Included"},
	}}
	saved := func(consistency backup.Consistency, sources ...string) backup.Snapshot {
		s := backup.Snapshot{App: inventory.App{Definition: definition}, Consistency: consistency}
		for _, source := range sources {
			s.Paths = append(s.Paths, backup.Path{Source: source})
		}
		return s
	}
	for _, test := range []struct {
		name    string
		saved   backup.Snapshot
		files   bool
		invalid bool
	}{
		{name: "export only", saved: saved(backup.ConsistencyUnverified, "/srv/app")},
		{name: "files from one snapshot", saved: saved(backup.ConsistencyFilesystem, "/srv/app", "/srv/db", "/srv/db-wal"), files: true},
		{name: "files without one capture", saved: saved(backup.ConsistencyUnverified, "/srv/db", "/srv/db-wal"), invalid: true},
		{name: "some database files", saved: saved(backup.ConsistencyFilesystem, "/srv/db"), invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			files, err := databaseFiles(test.saved, "db")
			var failed *failure
			if files != test.files || (err != nil) != test.invalid || (err != nil && (!errors.As(err, &failed) || failed.Code != "snapshot_invalid")) {
				t.Fatalf("files %t, error %v", files, err)
			}
		})
	}
}
