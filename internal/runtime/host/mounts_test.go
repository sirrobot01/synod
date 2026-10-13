package host

import (
	"strings"
	"testing"
)

const table = `22 1 8:1 / / rw - ext4 /dev/sda1 rw
40 22 0:50 / /srv/app rw - ext4 /dev/sdb1 rw
41 40 0:51 / /srv/app/media rw - nfs4 nas:/media rw
42 22 0:52 /share /mnt/user rw - fuse.shfs shfs rw
43 22 0:53 / /mnt/with\040space rw - cifs //nas/x rw
44 40 0:54 / /srv/app/media rw - fuse.sshfs host:/m rw
`

func TestMountTableFindsSharesAndNestedMounts(t *testing.T) {
	mounts, err := ParseMounts(strings.NewReader(table))
	if err != nil || len(mounts) != 6 {
		t.Fatalf("mounts: %d, %v", len(mounts), err)
	}
	if mounts[4].Point != "/mnt/with space" || mounts[3].Root != "/share" {
		t.Fatalf("escaped fields: %+v", mounts[3:5])
	}
	for path, want := range map[string]string{"/srv/app/data": "ext4", "/srv/app/media/movie": "fuse.sshfs", "/mnt/user/appdata": "fuse.shfs", "/etc": "ext4", "/mnt/with space/x": "cifs"} {
		if m, ok := Containing(mounts, path); !ok || m.Type != want {
			t.Errorf("Containing(%s) = %+v", path, m)
		}
	}
	below := Below(mounts, "/srv/app")
	if len(below) != 1 || below[0].Point != "/srv/app/media" || below[0].Type != "fuse.sshfs" {
		t.Fatalf("Below(/srv/app) = %+v", below)
	}
	if len(Below(mounts, "/srv/app/media")) != 0 {
		t.Fatal("a mount point is below itself")
	}
	for kind, want := range map[string]bool{"nfs": true, "nfs4": true, "cifs": true, "fuse.sshfs": true, "fuse.rclone": true, "fuse.mergerfs": false, "fuse.shfs": false, "ext4": false, "zfs": false} {
		if Remote(kind) != want {
			t.Errorf("Remote(%s) = %t", kind, !want)
		}
	}
}
