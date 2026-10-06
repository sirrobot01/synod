package cli

import (
	"cmp"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
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
			Endpoint:  cmp.Or(os.Getenv("SYNOD_API_URL"), "https://localhost:8443"),
			TokenFile: os.Getenv("SYNOD_ADMIN_TOKEN_FILE"),
			CAFile:    os.Getenv("SYNOD_CA_FILE"),
		},
		stdout: stdout,
		stderr: stderr,
	}
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
