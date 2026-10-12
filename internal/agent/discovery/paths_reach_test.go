package discovery

import "testing"

func TestReachesForbiddenOnlyAboveHostPaths(t *testing.T) {
	for directory, want := range map[string]bool{
		"/":                                true,
		"/var":                             true, // /var/lib/docker and /var/run lie below it.
		"/var/lib":                         true,
		"/srv/app":                         false,
		"/home/main/apps/notes":            false,
		"/var/lib/docker/volumes/db/_data": false,
		"/mnt/user/appdata/plex":           false,
	} {
		if got := ReachesForbidden(directory); got != want {
			t.Errorf("ReachesForbidden(%q) = %t, want %t", directory, got, want)
		}
	}
}
