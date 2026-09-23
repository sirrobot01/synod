package postgres

import _ "embed"

const PatroniSourceDirectory = "/etc/synod-patroni"

//go:embed patroni_start.py
var patroniStartup string

// PatroniCommand restores private runtime files from protected local configuration before each start.
func PatroniCommand() []string {
	return []string{"/usr/bin/python3", "-c", patroniStartup, "synod-patroni", PatroniGuardProgram, PatroniApplicationProgram}
}
