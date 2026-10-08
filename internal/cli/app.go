package cli

import (
	"cmp"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// app holds what the client commands share during one invocation: the API settings,
// the connection, and the one result that finish writes.
type app struct {
	settings       client.Config
	api            *client.Client
	result         Result
	json           bool
	stdout, stderr io.Writer
	// ran is set once a client command starts, so help alone writes no result.
	ran bool
}

func newApp(stdout, stderr io.Writer) *app {
	return &app{
		settings: client.Config{
			Endpoint:  os.Getenv("SYNOD_API_URL"),
			TokenFile: os.Getenv("SYNOD_ADMIN_TOKEN_FILE"),
			CAFile:    os.Getenv("SYNOD_CA_FILE"),
		},
		stdout: stdout,
		stderr: stderr,
	}
}

// localController reads the API settings of a controller on this device.
// Only root can read the default directory, so other users get no settings.
func localController(path string) client.Config {
	data, err := os.ReadFile(path)
	if err != nil {
		return client.Config{}
	}
	var saved struct {
		Listen         string `json:"listen"`
		AdminTokenFile string `json:"admin_token_file"`
		APICACertFile  string `json:"api_ca_cert_file"`
	}
	if json.Unmarshal(data, &saved) != nil {
		return client.Config{}
	}
	host, port, err := net.SplitHostPort(saved.Listen)
	if err != nil {
		return client.Config{}
	}
	// The API certificate also names 127.0.0.1, so a wildcard listener is reached there.
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return client.Config{Endpoint: "https://" + net.JoinHostPort(host, port), TokenFile: saved.AdminTokenFile, CAFile: saved.APICACertFile}
}

// bindFlags registers the client flags on the root, so they can come before the command.
func (a *app) bindFlags(flags *pflag.FlagSet) {
	flags.StringVar(&a.settings.Endpoint, "api-url", a.settings.Endpoint, "HTTPS API origin (SYNOD_API_URL)")
	flags.StringVar(&a.settings.TokenFile, "token-file", a.settings.TokenFile, "Private administrator token file (SYNOD_ADMIN_TOKEN_FILE)")
	flags.StringVar(&a.settings.CAFile, "ca-file", a.settings.CAFile, "Additional trusted API CA PEM file (SYNOD_CA_FILE)")
	// Environment values may contain invalid credentials. Never echo them in help.
	for _, name := range []string{"api-url", "token-file", "ca-file"} {
		flags.Lookup(name).DefValue = ""
	}
	flags.DurationVar(&a.settings.RequestTimeout, "request-timeout", 10*time.Second, "Timeout for each HTTPS attempt")
	flags.IntVar(&a.settings.Attempts, "attempts", 3, "Maximum attempts per request (1 to 5)")
	flags.BoolVar(&a.json, "json", false, "Write one JSON result to stdout")
}

// connect opens the API client before a client command runs.
func (a *app) connect(*cobra.Command, []string) error {
	a.ran = true
	// Without an API URL, use the controller on this device. Its token never goes to another URL.
	if a.settings.Endpoint == "" {
		local := localController(filepath.Join(config.DefaultControllerDir, "controller.json"))
		a.settings.Endpoint = cmp.Or(local.Endpoint, "https://localhost:8443")
		a.settings.TokenFile = cmp.Or(a.settings.TokenFile, local.TokenFile)
		a.settings.CAFile = cmp.Or(a.settings.CAFile, local.CAFile)
	}
	var err error
	a.api, err = client.New(a.settings)
	return err
}

// parseID parses a nonzero UUID. The refusal carries message and never the value.
func parseID(value, message string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, &client.Error{Code: "invalid_request", Message: message}
	}
	return id, nil
}
