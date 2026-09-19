package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirrobot01/synod/internal/agent/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	cancel()
	os.Exit(code)
}
