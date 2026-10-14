package daemon

import (
	"context"
	"log/slog"
	"time"

	agentrestore "github.com/sirrobot01/synod/internal/agent/restore"
	"github.com/sirrobot01/synod/internal/agent/transport"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// serveFiles answers queued file requests until ctx ends. Each request uses its own stream.
func serveFiles(ctx context.Context, requests <-chan restore.FileRequest, worker *agentrestore.Worker, stream *transport.Client, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-requests:
			serveFile(ctx, request, worker, stream, logger)
		}
	}
}

// serveFile cancels the stream on any error, so the controller never takes a partial transfer as complete.
func serveFile(ctx context.Context, request restore.FileRequest, worker *agentrestore.Worker, stream *transport.Client, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Hour)
	defer cancel()
	files, err := stream.OpenFile(ctx)
	if err != nil {
		logger.Warn("file request failed: no stream to the controller", "app", request.AppID, "path", request.Path, "request", request.ID, "error", err)
		return
	}
	first := true
	err = worker.ServeFile(ctx, request, func(header, data []byte) error {
		chunk := &agentv1.FileChunk{HeaderJson: header, Data: data}
		if first {
			chunk.RequestId, first = request.ID.String(), false
		}
		return files.Send(chunk)
	})
	if err == nil {
		_, err = files.CloseAndRecv()
	}
	if err != nil {
		logger.Warn("file transfer incomplete", "app", request.AppID, "path", request.Path, "request", request.ID, "error", err)
	}
}
