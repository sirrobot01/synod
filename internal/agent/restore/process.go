package restore

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

// dockerCLI discards output because Compose and database errors can contain secrets.
func dockerCLI(ctx context.Context, api *client.Client, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"--host", api.DaemonHost()}, args...)...)
	for _, variable := range os.Environ() {
		key, _, _ := strings.Cut(variable, "=")
		if strings.HasPrefix(key, "COMPOSE_") || key == "DOCKER_HOST" || key == "DOCKER_CONTEXT" || key == "DOCKER_TLS" || key == "DOCKER_TLS_VERIFY" || key == "DOCKER_CERT_PATH" {
			continue
		}
		cmd.Env = append(cmd.Env, variable)
	}
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd.Run()
}

// execCommand runs a bounded command in a temporary restore container.
// The worker removes that container on cancellation, which also ends child processes.
func execCommand(ctx context.Context, api *client.Client, id string, command, env []string, output io.Writer) error {
	created, err := api.ExecCreate(ctx, id, client.ExecCreateOptions{Cmd: command, Env: env, User: "0", AttachStdout: true, AttachStderr: true})
	if err != nil {
		return err
	}
	attached, err := api.ExecAttach(ctx, created.ID, client.ExecAttachOptions{})
	if err != nil {
		return err
	}
	defer attached.Close()
	stop := context.AfterFunc(ctx, attached.Close)
	defer stop()
	if output == nil {
		output = io.Discard
	}
	if _, err := stdcopy.StdCopy(output, io.Discard, attached.Reader); err != nil {
		return err
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		status, err := api.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil {
			return err
		}
		if !status.Running {
			if status.ExitCode != 0 {
				return &failure{Code: "database_restore_failed"}
			}
			return nil
		}
		if time.Now().After(deadline) {
			return &failure{Code: "database_restore_failed"}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
