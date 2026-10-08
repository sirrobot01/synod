//go:build !linux

package repository

import "os"

// Without syncfs, each upload syncs its own file and directory.
const deferredSync = false

func syncFilesystem(*os.Root) error { return nil }
