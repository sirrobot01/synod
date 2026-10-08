//go:build darwin

package config

// DefaultControllerDir is where init writes the installation and the CLI reads its settings.
const DefaultControllerDir = "/Library/Application Support/Synod/controller"
