package cli

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	appmodel "github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/spf13/cobra"
)

// newDatabaseCommand manages tier 1 databases. Each database is a tier 1 app.
func newDatabaseCommand(a *app) *cobra.Command {
	group := &cobra.Command{Use: "db", Short: "Create and manage replicated databases", PersistentPreRunE: a.connect}
	get := &cobra.Command{Use: "get NAME", Short: "Show the primary, standbys, and routes", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		item, err := a.findDatabase(cmd.Context(), args[0])
		if err == nil {
			a.result.App = &item
		}
		return err
	}}
	group.AddCommand(newDatabaseCreateCommand(a), get, newDatabaseCredentialsCommand(a), newAppActionCommand(a, "switchover"))
	for _, action := range []string{"stop", "start", "delete"} {
		group.AddCommand(newAppActionCommand(a, action))
	}
	return group
}

// findDatabase reads a tier 1 app by name or UUID.
func (a *app) findDatabase(ctx context.Context, nameOrID string) (appmodel.App, error) {
	var item appmodel.App
	var err error
	if id, parseErr := uuid.Parse(nameOrID); parseErr == nil && id != uuid.Nil {
		item, err = a.api.GetApp(ctx, id)
	} else {
		item, err = a.api.GetAppByName(ctx, nameOrID)
	}
	if err == nil && item.Tier != appmodel.TierDatabase {
		err = &client.Error{Code: "invalid_request", Message: "The app is not a database."}
	}
	return item, err
}

func newDatabaseCredentialsCommand(a *app) *cobra.Command {
	return &cobra.Command{Use: "credentials NAME", Short: "Reveal the client login and connection URLs through audited access", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			item, err := a.findDatabase(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(a.stderr, "The JSON result will contain a database password. Protect stdout. Use a private terminal or a file readable only by its owner. Do not send this output to logs or shared files."); err != nil {
				return &client.Error{Code: "local_error", Message: "Display protected-output guidance failed. No credential reveal request was sent."}
			}
			credential, err := a.api.RevealAppCredential(cmd.Context(), item.ID)
			if err != nil {
				return err
			}
			a.result.DatabaseCredential = &DatabaseCredential{Credential: credential, URLs: connectionURLs(item, credential)}
			return nil
		}}
}

// DatabaseCredential is the client login of a database and one URL for each route.
type DatabaseCredential struct {
	appmodel.Credential
	URLs []string `json:"urls"`
}

// connectionURLs names the writer route and the reader route on each device.
// Engine TLS uses the node certificate, which carries no host name, so PostgreSQL
// clients verify the installation CA only.
func connectionURLs(item appmodel.App, credential appmodel.Credential) []string {
	if item.Status == nil || item.Spec.Database == nil {
		return nil
	}
	login := url.UserPassword(credential.Username, credential.Password)
	ports := []uint16{item.Spec.Router.Port}
	if item.Spec.Router.ReaderPort != 0 {
		ports = append(ports, item.Spec.Router.ReaderPort)
	}
	var urls []string
	for _, endpoint := range item.Status.Endpoints {
		host, _, err := net.SplitHostPort(endpoint.Address)
		if err != nil {
			continue
		}
		for _, port := range ports {
			address := net.JoinHostPort(host, strconv.Itoa(int(port)))
			if item.Spec.Database.Engine == appmodel.EngineRedis {
				urls = append(urls, (&url.URL{Scheme: "rediss", User: login, Host: address}).String())
				continue
			}
			for _, database := range item.Spec.Database.Databases {
				urls = append(urls, (&url.URL{Scheme: "postgresql", User: login, Host: address, Path: "/" + database, RawQuery: "sslmode=verify-ca"}).String())
			}
		}
	}
	return urls
}

func newDatabaseCreateCommand(a *app) *cobra.Command {
	replication := appmodel.DefaultDatabaseReplication()
	database := appmodel.DatabaseSpec{}
	var preference appmodel.DevicePreference
	var writerPort, readerPort int
	var failoverAfter time.Duration
	var key string
	cmd := &cobra.Command{Use: "create NAME", Short: "Create a database with a primary and standbys on any architecture", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if writerPort < 1 || writerPort > 65535 || readerPort < 0 || readerPort > 65535 {
				return &client.Error{Code: "invalid_request", Message: "Use route ports from 1 to 65535."}
			}
			if replication.Standbys < 0 || replication.Standbys > 3 {
				return &client.Error{Code: "invalid_request", Message: "Use 0 to 3 standbys."}
			}
			if failoverAfter%time.Second != 0 || failoverAfter < time.Second {
				return &client.Error{Code: "invalid_request", Message: "Use a whole number of seconds for --failover-after."}
			}
			replication.FailoverAfterSeconds = uint32(failoverAfter / time.Second)
			if database.Engine == appmodel.EnginePostgres && len(database.Databases) == 0 {
				database.Databases = []string{strings.ReplaceAll(args[0], "-", "_")}
			}
			spec := appmodel.AppSpec{SchemaVersion: 1, Name: args[0], Tier: appmodel.TierDatabase, Copies: 1, DevicePreference: preference,
				Router: appmodel.RouterSpec{Protocol: "tcp", Port: uint16(writerPort), ReaderPort: uint16(readerPort)}, Replication: &replication, Database: &database}
			if err := spec.Validate(); err != nil {
				return &client.Error{Code: "invalid_request", Message: err.Error()}
			}
			key, err := idempotencyKey(key)
			if err != nil {
				return err
			}
			a.result.IdempotencyKey = key
			if _, err := a.stderr.Write([]byte("Idempotency key: " + key + "\n")); err != nil {
				return &client.Error{Code: "local_error", Message: "Disclose the retry key failed; no request was sent."}
			}
			result, err := a.api.CreateApp(cmd.Context(), spec, key)
			if err == nil {
				a.result.AppAdmission = &result
			}
			return err
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&database.Engine, "engine", appmodel.EnginePostgres, "Database engine: postgres or redis")
	flags.StringVar(&database.Version, "version", "", "Engine version, such as 17 (required)")
	flags.StringSliceVar(&database.Databases, "database", nil, "PostgreSQL database to create and replicate (default: the name with _ for -)")
	flags.IntVar(&replication.Standbys, "standbys", replication.Standbys, "Standby copies that can take over (0 to 3)")
	flags.StringSliceVar(&preference.Preferred, "prefer", nil, "Preferred device IDs or names, most preferred first")
	flags.StringSliceVar(&preference.Allowed, "allow", nil, "Devices the database may also use")
	flags.StringSliceVar(&preference.Never, "never", nil, "Devices the database must not use")
	flags.IntVar(&writerPort, "writer-port", 0, "Writer route port on every device (required)")
	flags.IntVar(&readerPort, "reader-port", 0, "Reader route port on every device (default: no reader route)")
	flags.DurationVar(&failoverAfter, "failover-after", time.Duration(replication.FailoverAfterSeconds)*time.Second, "Time without the primary before a standby takes over")
	flags.StringVar(&replication.Failback, "failback", replication.Failback, "auto or manual return to the preferred device")
	flags.StringVar(&key, "idempotency-key", "", "Reuse a prior key with the same request for recovery")
	_ = cmd.MarkFlagRequired("version")
	_ = cmd.MarkFlagRequired("writer-port")
	return cmd
}
