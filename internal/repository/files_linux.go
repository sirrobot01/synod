package repository

import (
	"os"

	"golang.org/x/sys/unix"
)

// deferredSync lets pack uploads skip their own sync. syncFilesystem makes them durable later.
const deferredSync = true

// syncFilesystem makes every completed write on the repository's filesystem durable.
func syncFilesystem(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return unix.Syncfs(int(f.Fd()))
}
