//go:build !linux

package backup

import "context"

// fsSnapshots is empty where Synod takes no filesystem snapshots.
type fsSnapshots struct{}

func (*Tree) useSnapshots(context.Context, string) (*fsSnapshots, error) { return nil, nil }
func (*fsSnapshots) Close() error                                        { return nil }

// RemoveStaleSnapshots does nothing where Synod takes no filesystem snapshots.
func RemoveStaleSnapshots(context.Context) error { return nil }
