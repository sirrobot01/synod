package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/spf13/cobra"
)

func newDestinationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "destination", Short: "Manage encrypted backup destinations"}
	var kind, path, device, credentialsFile, host, user, knownHostsFile, endpoint, bucket, prefix, region string
	var port int
	add := &cobra.Command{Use: "add NAME", Short: "Save a destination and deliver its settings to approved agents", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		app.ran = true
		file, err := os.OpenFile(credentialsFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return &client.Error{Code: "invalid_credentials", Message: "Open the private credentials file failed."}
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) || info.Size() > destination.MaxEntryBytes {
			return &client.Error{Code: "invalid_credentials", Message: "Use a regular credentials file owned by this user, with mode 0600 and at most 32 KiB."}
		}
		raw, err := io.ReadAll(io.LimitReader(file, destination.MaxEntryBytes+1))
		if err != nil || len(raw) > destination.MaxEntryBytes {
			return &client.Error{Code: "invalid_credentials", Message: "Read the private credentials file failed."}
		}
		defer clear(raw)
		var credentials repository.Credentials
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&credentials) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) {
			return &client.Error{Code: "invalid_credentials", Message: "Use one credentials JSON object with known fields."}
		}
		entry := destination.Entry{Destination: destination.Destination{ID: uuid.New(), Name: args[0], Location: repository.Location{Kind: kind, Path: path}}, Credentials: credentials}
		switch kind {
		case "local":
			if device == "" {
				return &client.Error{Code: "invalid_request", Message: "A local destination needs --device and an absolute --path."}
			}
		case "sftp":
			if device != "" {
				return &client.Error{Code: "invalid_request", Message: "Use --device only for a local destination."}
			}
			file, err := os.OpenFile(knownHostsFile, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
			if err != nil {
				return &client.Error{Code: "invalid_request", Message: "Open the SSH known-hosts file failed."}
			}
			info, err := file.Stat()
			if err != nil || !info.Mode().IsRegular() || info.Size() > 16384 {
				file.Close()
				return &client.Error{Code: "invalid_request", Message: "Use a regular SSH known-hosts file of at most 16 KiB."}
			}
			hosts, readErr := io.ReadAll(io.LimitReader(file, 16385))
			if err := errors.Join(readErr, file.Close()); err != nil || len(hosts) > 16384 {
				return &client.Error{Code: "invalid_request", Message: "Use an SSH known-hosts file of at most 16 KiB."}
			}
			entry.Destination.Location.SFTP = &repository.SFTPLocation{Host: host, Port: port, User: user, KnownHosts: string(hosts)}
		case "s3":
			if device != "" {
				return &client.Error{Code: "invalid_request", Message: "Use --device only for a local destination."}
			}
			entry.Destination.Location.S3 = &repository.S3Location{Endpoint: endpoint, Bucket: bucket, Prefix: prefix, Region: region}
		default:
			return &client.Error{Code: "invalid_request", Message: "Use destination type local, sftp, or s3."}
		}
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		if kind == "local" {
			node, err := app.api.ResolveNode(cmd.Context(), device)
			if err != nil {
				return err
			}
			entry.Destination.NodeID = node.ID
		}
		if entry.Validate() != nil {
			return &client.Error{Code: "invalid_request", Message: "The destination settings are invalid. Check its type, path, connection settings, and credentials."}
		}
		saved, err := app.api.AddDestination(cmd.Context(), entry)
		if err != nil {
			return err
		}
		app.result.Destination = &saved
		app.result.Message = "Destination settings saved. Delivery to agents is pending. The connection has not been checked. No backup has run."
		if destination.OnlyOnSource(saved.NodeID, []destination.Destination{saved}) {
			app.result.Warnings = []string{"A backup stored only on device " + saved.NodeID.String() + " is lost if that device is lost. Add a destination on another device."}
		}
		return nil
	}}
	flags := add.Flags()
	flags.StringVar(&kind, "type", "local", "Repository type: local, sftp, or s3")
	flags.StringVar(&path, "path", "", "Absolute repository directory on the destination device or SFTP host")
	flags.StringVar(&device, "device", "", "Device ID or name that owns the local path")
	flags.StringVar(&credentialsFile, "credentials-file", "", "Private JSON file with the repository password and provider credentials")
	flags.StringVar(&host, "host", "", "SFTP host")
	flags.IntVar(&port, "port", 22, "SFTP port")
	flags.StringVar(&user, "user", "", "SFTP user")
	flags.StringVar(&knownHostsFile, "known-hosts-file", "", "SSH known-hosts file for SFTP host verification")
	flags.StringVar(&endpoint, "endpoint", "", "S3-compatible endpoint, including https:// or http://")
	flags.StringVar(&bucket, "bucket", "", "S3 bucket")
	flags.StringVar(&prefix, "prefix", "", "S3 object prefix")
	flags.StringVar(&region, "region", "", "S3 region")
	list := &cobra.Command{Use: "list", Short: "List destinations and agent delivery status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		result, err := app.api.ListDestinations(cmd.Context())
		if err == nil {
			app.result.Destinations = &result
		}
		return err
	}}
	remove := &cobra.Command{Use: "remove DESTINATION", Short: "Remove settings by ID or name and keep all repository data", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		id, err := uuid.Parse(args[0])
		if err != nil || id == uuid.Nil {
			listing, err := app.api.ListDestinations(cmd.Context())
			if err != nil {
				return err
			}
			for _, d := range listing.Destinations {
				if d.Name == args[0] {
					id = d.ID
					break
				}
			}
			if id == uuid.Nil {
				return &client.Error{Code: "not_found", Message: "The destination was not found."}
			}
		}
		if err := app.api.RemoveDestination(cmd.Context(), id); err != nil {
			return err
		}
		app.result.Message = "Destination settings removed. Repository data was kept. Offline agents remove cached credentials when they reconnect."
		return nil
	}}
	group.AddCommand(add, list, remove)
	return group
}
