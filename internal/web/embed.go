// Package web carries the built browser application.
//
// The controller serves these files from its own origin, so the session cookie
// reaches the API without any script holding a credential. The assets are committed,
// so building the controller never needs a JavaScript toolchain.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var built embed.FS

// Assets returns the built application rooted at its index.
func Assets() (fs.FS, error) { return fs.Sub(built, "dist") }
