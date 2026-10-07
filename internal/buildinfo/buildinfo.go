// Package buildinfo holds the release identity shared by the CLI and agent.
package buildinfo

// The release build sets these values with linker flags.
var (
	Version  = "dev"
	Revision = "unknown"
)
