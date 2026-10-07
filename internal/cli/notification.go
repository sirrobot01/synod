package cli

import (
	json "encoding/json/v2"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/spf13/cobra"
)

func newNotificationCommand(app *app) *cobra.Command {
	group := &cobra.Command{Use: "notification", Short: "Configure alerts and inspect delivery"}
	var path string
	configure := &cobra.Command{Use: "configure", Short: "Read a notification URL and thresholds from private JSON", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		app.ran = true
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return &client.Error{Code: "invalid_request", Message: "Open the private notification settings file failed."}
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) || info.Size() > 16384 {
			return &client.Error{Code: "invalid_request", Message: "Use a regular settings file owned by this user, with mode 0600 and at most 16 KiB."}
		}
		raw, err := io.ReadAll(io.LimitReader(file, 16385))
		defer clear(raw)
		settings := notification.DefaultSettings()
		if err != nil || len(raw) > 16384 || json.Unmarshal(raw, &settings, json.RejectUnknownMembers(true)) != nil || settings.URL == "" || notification.ValidateService(settings) != nil {
			return &client.Error{Code: "invalid_request", Message: "Use a JSON object with a valid notification URL and alert thresholds."}
		}
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		current, err := app.api.NotificationStatus(cmd.Context())
		if err != nil {
			return err
		}
		result, err := app.api.ConfigureNotifications(cmd.Context(), notification.Update{ID: uuid.New(), ExpectedRevision: current.Revision, Settings: settings})
		if err != nil {
			return err
		}
		app.result.Message = fmt.Sprintf("Notification settings saved at revision %d. Run synod notification test to check delivery.", result.Revision)
		return nil
	}}
	configure.Flags().StringVar(&path, "file", "", "Private JSON settings file (mode 0600)")
	status := &cobra.Command{Use: "status", Short: "Show public settings and the last 100 alerts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		result, err := app.api.NotificationStatus(cmd.Context())
		if err == nil {
			app.result.Notifications = &result
		}
		return err
	}}
	disable := &cobra.Command{Use: "disable", Short: "Disable notifications and cancel queued sends", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		current, err := app.api.NotificationStatus(cmd.Context())
		if err != nil {
			return err
		}
		settings := notification.Settings{OverdueGraceHours: current.Settings.OverdueGraceHours, MissingAfterHours: current.Settings.MissingAfterHours, FullPercent: current.Settings.FullPercent}
		if _, err := app.api.ConfigureNotifications(cmd.Context(), notification.Update{ID: uuid.New(), ExpectedRevision: current.Revision, Settings: settings}); err != nil {
			return err
		}
		app.result.Message = "Notifications disabled. Queued sends were cancelled. A send already in progress can still finish."
		return nil
	}}
	test := &cobra.Command{Use: "test", Short: "Queue one test alert", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if err := app.connect(cmd, nil); err != nil {
			return err
		}
		current, err := app.api.NotificationStatus(cmd.Context())
		if err != nil {
			return err
		}
		result, err := app.api.TestNotification(cmd.Context(), notification.TestRequest{ID: uuid.New(), ExpectedRevision: current.Revision})
		if err != nil {
			return err
		}
		app.result.Message = fmt.Sprintf("Test alert %s queued. Run synod notification status to check delivery.", result.ID)
		return nil
	}}
	group.AddCommand(configure, status, disable, test)
	return group
}
