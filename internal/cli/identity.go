package cli

import (
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// readPassword takes a password from a protected file or from the terminal.
// A flag value would reach the process table and the shell history, so there is no flag.
func readPassword(file string, prompt string, stderr io.Writer) (string, error) {
	if file != "" {
		handle, err := os.OpenFile(file, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return "", &client.Error{Code: "invalid_request", Message: "The password file could not be opened."}
		}
		defer handle.Close()
		info, err := handle.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return "", &client.Error{Code: "invalid_request", Message: "The password file must be regular and private to its owner."}
		}
		data, err := io.ReadAll(io.LimitReader(handle, 1025))
		if err != nil || len(data) > 1024 {
			return "", &client.Error{Code: "invalid_request", Message: "The password file must hold at most 1024 bytes."}
		}
		return strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r"), nil
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", &client.Error{Code: "invalid_request", Message: "Use --password-file when stdin is not a terminal."}
	}
	if _, err := io.WriteString(stderr, prompt); err != nil {
		return "", err
	}
	entered, err := term.ReadPassword(int(os.Stdin.Fd()))
	if _, writeErr := io.WriteString(stderr, "\n"); writeErr != nil {
		return "", writeErr
	}
	if err != nil {
		return "", &client.Error{Code: "invalid_request", Message: "The password could not be read."}
	}
	return string(entered), nil
}

// newUserCommand administers operator accounts.
func newUserCommand(app *app) *cobra.Command {
	var passwordFile string
	group := &cobra.Command{Use: "user", Short: "Administer operator accounts", PersistentPreRunE: app.connect}
	group.PersistentFlags().StringVar(&passwordFile, "password-file", "", "Private file holding the password; otherwise prompt")

	create := &cobra.Command{Use: "create USERNAME", Short: "Create one operator account", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			password, err := readPassword(passwordFile, "New password: ", app.stderr)
			if err != nil {
				return err
			}
			user, err := app.api.CreateUser(cmd.Context(), strings.ToLower(args[0]), password)
			if err != nil {
				return err
			}
			app.result.User = &user
			return nil
		}}

	setPassword := &cobra.Command{Use: "set-password ID", Short: "Replace one password and end its other sessions", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a valid user ID.")
			if err != nil {
				return err
			}
			password, err := readPassword(passwordFile, "New password: ", app.stderr)
			if err != nil {
				return err
			}
			return app.api.SetUserPassword(cmd.Context(), id, password)
		}}

	disable := &cobra.Command{Use: "disable ID", Short: "Stop sign-ins and end every live session", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a valid user ID.")
			if err != nil {
				return err
			}
			return app.api.DisableUser(cmd.Context(), id)
		}}

	group.AddCommand(create, setPassword, disable)
	return group
}

// newTokenCommand administers API tokens for automation.
func newTokenCommand(app *app) *cobra.Command {
	var actorID, expires string
	group := &cobra.Command{Use: "token", Short: "Administer API tokens", PersistentPreRunE: app.connect}

	create := &cobra.Command{Use: "create NAME", Short: "Mint one token; its secret is shown once", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var deadline *time.Time
			if expires != "" {
				parsed, err := time.Parse(time.RFC3339, expires)
				if err != nil {
					return &client.Error{Code: "invalid_request", Message: "Use an RFC 3339 expiry, such as 2027-01-31T00:00:00Z."}
				}
				deadline = &parsed
			}
			token, err := app.api.CreateAPIToken(cmd.Context(), args[0], actorID, deadline)
			if err != nil {
				return err
			}
			app.result.Token = &token
			return nil
		}}
	create.Flags().StringVar(&actorID, "actor-id", "", "Audit identity that the token acts as; defaults to the caller")
	create.Flags().StringVar(&expires, "expires-at", "", "RFC 3339 expiry; omit for no expiry")

	list := &cobra.Command{Use: "list", Short: "Read every token without its secret", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			tokens, err := app.api.ListAPITokens(cmd.Context())
			if err != nil {
				return err
			}
			app.result.Tokens = tokens
			return nil
		}}

	revoke := &cobra.Command{Use: "revoke ID", Short: "End one token", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0], "Use a valid token ID.")
			if err != nil {
				return err
			}
			return app.api.RevokeAPIToken(cmd.Context(), id)
		}}

	group.AddCommand(create, list, revoke)
	return group
}
