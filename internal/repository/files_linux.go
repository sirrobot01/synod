package repository

import (
	"os"

	"golang.org/x/sys/unix"
)

// deferredSync lets pack uploads skip their own sync. syncFilesystem makes them durable later.
const deferredSync = true

// startWriteback asks the kernel to write a pack now, without waiting for the disk.
// A slow disk then receives packs at a steady rate. Without it, dirty packs collect in
// memory, and the next syncFilesystem must write all of them at once.
func startWriteback(f *os.File) error {
	return unix.SyncFileRange(int(f.Fd()), 0, 0, unix.SYNC_FILE_RANGE_WRITE)
}

// syncFilesystem makes every completed write on the repository's filesystem durable.
func syncFilesystem(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}
