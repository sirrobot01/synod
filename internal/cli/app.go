package cli

import (
	"cmp"
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// app holds what the client commands share during one invocation: the API settings,
// the connection, and the one result that finish writes.
type app struct {
	settings          client.Config
	api               *client.Client
	result            Result
	json              bool
	timeout, interval time.Duration
	stdout, stderr    io.Writer
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
	flags.DurationVar(&a.timeout, "timeout", time.Minute, "Maximum operation wait time; does not cancel server work")
	flags.DurationVar(&a.interval, "poll-interval", time.Second, "Interval between operation reads while waiting")
}

// connect opens the API client before a client command runs.
func (a *app) connect(*cobra.Command, []string) error {
	a.ran = true
	if a.timeout <= 0 || a.interval <= 0 {
		return &client.Error{Code: "invalid_request", Message: "Use positive wait timeout and poll interval values."}
	}
	var err error
	a.api, err = client.New(a.settings)
	return err
}

// wait reads the operation until it ends or --timeout passes. The timeout never
// cancels the server work.
func (a *app) wait(ctx context.Context, id uuid.UUID) error {
	waiting, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	value, err := a.api.Wait(waiting, id, a.interval)
	if value.ID != uuid.Nil {
		a.result.Operation = &value
	}
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

// idempotencyKey returns key, or a new key when key is empty.
func idempotencyKey(key string) (string, error) {
	if key == "" {
		generated, err := uuid.NewRandom()
		if err != nil {
			return "", &client.Error{Code: "local_error", Message: "Create an idempotency key failed."}
		}
		key = generated.String()
	}
	if len(key) > 128 || strings.ContainsFunc(key, func(r rune) bool { return r < '!' || r > '~' }) {
		return "", &client.Error{Code: "invalid_request", Message: "Use an idempotency key with 1 to 128 visible ASCII bytes."}
	}
	return key, nil
}
