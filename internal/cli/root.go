// Package cli builds the synod command tree. It holds the client commands and adds
// the server and agent commands.
package cli

import (
	"context"
	"io"

	agentcli "github.com/sirrobot01/synod/internal/agent/cli"
	"github.com/sirrobot01/synod/internal/controller"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Run runs one synod command and returns its exit status.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	app := newApp(stdout, stderr)
	server := controller.Command()
	agent := agentcli.Command()

	root := &cobra.Command{Use: "synod", Short: "Back up the Docker apps of your devices and restore them anywhere", SilenceErrors: true, SilenceUsage: true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true}}
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	app.bindFlags(root.PersistentFlags())
	root.AddCommand(
		newNodeCommand(app),
		newAppsCommand(app),
		newBackupCommand(app),
		newRestoreCommand(app),
		newProtectCommand(app),
		newDestinationCommand(app),
		newOperationCommand(app),
		newInstallationCommand(app),
		newUserCommand(app),
		newTokenCommand(app),
		server,
		agent,
	)
	// The client flags do not apply to the server and the agent, so their help omits them.
	for _, command := range []*cobra.Command{server, agent} {
		command.SetHelpFunc(func(cmd *cobra.Command, args []string) {
			root.PersistentFlags().VisitAll(func(flag *pflag.Flag) { flag.Hidden = true })
			root.HelpFunc()(cmd, args)
		})
	}

	selected, err := root.ExecuteContextC(ctx)
	// The server and the agent report their own failures. The client writes one result.
	for command := selected; command != nil; command = command.Parent() {
		switch command {
		case server:
			return controller.Report(err, stderr)
		case agent:
			return agentcli.Report(err, stderr)
		}
	}
	return app.finish(selected, err)
}
