package cli

import (
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/jointoken"
)

func newJoinTokenCommand(app *app) *cobra.Command {
	var ttl time.Duration
	var device string
	command := &cobra.Command{Use: "join-token", Short: "Create a one-use token that joins one agent node", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if ttl < time.Minute || ttl > 7*24*time.Hour {
				return &client.Error{Code: "invalid_request", Message: "Use a --ttl from 1m to 168h."}
			}
			// The agent pins the API CA, so the token can only name a CA this command trusts.
			fingerprint, err := apiCAFingerprint(app.settings.CAFile)
			if err != nil {
				return err
			}
			origin, err := url.Parse(app.settings.Endpoint)
			if err != nil || origin.Hostname() == "" {
				return &client.Error{Code: "invalid_request", Message: "Use an HTTPS API origin with a host."}
			}
			address := origin.Host
			if origin.Port() == "" {
				address = net.JoinHostPort(origin.Hostname(), "443")
			}
			if err := app.connect(cmd, nil); err != nil {
				return err
			}
			installation, err := app.api.GetInstallation(cmd.Context())
			if err != nil {
				return err
			}
			token := jointoken.Token{Address: address, InstallationID: installation.InstallationID, Fingerprint: fingerprint, Secret: jointoken.NewSecret()}
			if device != "" {
				node, err := app.api.ResolveNode(cmd.Context(), device)
				if err != nil {
					return err
				}
				token.NodeID = node.ID
			}
			registration, err := app.api.RegisterJoinToken(cmd.Context(), token.NodeID, token.Secret, int(ttl/time.Second))
			if err != nil {
				return err
			}
			app.result.EnrollmentToken = &registration
			app.result.JoinToken = jointoken.Encode(token)
			return nil
		}}
	command.Flags().DurationVar(&ttl, "ttl", 24*time.Hour, "How long the token stays valid (1m to 168h)")
	command.Flags().StringVar(&device, "device", "", "Recovered device name or UUID to join again")
	return command
}

// apiCAFingerprint pins the first CA certificate in the API trust file.
func apiCAFingerprint(path string) (string, error) {
	if path == "" {
		return "", &client.Error{Code: "invalid_request", Message: "Set --ca-file or SYNOD_CA_FILE to the API CA certificate. The join token pins it."}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", &client.Error{Code: "invalid_request", Message: "Read the API CA file: " + err.Error()}
	}
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if certificate, err := x509.ParseCertificate(block.Bytes); err == nil && certificate.IsCA {
			return jointoken.Fingerprint(certificate.Raw), nil
		}
	}
	return "", &client.Error{Code: "invalid_request", Message: "The API CA file holds no CA certificate."}
}
