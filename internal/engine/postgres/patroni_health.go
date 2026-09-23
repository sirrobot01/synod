package postgres

import _ "embed"

//go:embed patroni_health.py
var patroniHealth string

// PatroniHealthCommand proves native startup and application setup from protected local input.
func PatroniHealthCommand() []string {
	return []string{"/usr/local/bin/gosu", "postgres", "/usr/bin/python3", "-c", patroniHealth}
}
