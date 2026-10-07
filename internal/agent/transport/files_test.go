package transport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestFileStreamCompletionCancellationAndRevocation(t *testing.T) {
	for _, scenario := range []string{"complete", "short", "cancel", "revoked", "unsolicited", "duplicate"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t, func(context.Context, controlstream.Peer, *agentv1.AgentMessage) error { return nil }, func(config *controlstream.Config) {
				config.MaxSessions = 4
				config.Capabilities = append(config.Capabilities, "files.read")
			})
			f.hello.Capabilities = append(f.hello.Capabilities, "files.read")
			control, _ := f.stream(t, f.hello)
			connection, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(credentials.NewTLS(f.tls)))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			request := restore.FileRequest{ID: uuid.New(), InstallationID: uuid.MustParse(f.hello.InstallationId), NodeID: uuid.MustParse(f.hello.NodeId), AppID: uuid.New(), Action: restore.FileRead}
			payload := bytes.Repeat([]byte("saved-file"), 40000)
			header := restore.FileHeader{Format: "file", Size: int64(len(payload)), SnapshotID: uuid.New(), DestinationID: uuid.New()}
			raw, _ := json.Marshal(header)
			type outcome struct {
				data []byte
				err  error
			}
			done := make(chan outcome, 1)
			if scenario != "unsolicited" {
				go func() {
					_, body, err := f.server.ReadFile(ctx, request)
					if err != nil {
						done <- outcome{err: err}
						return
					}
					defer body.Close()
					data, err := io.ReadAll(body)
					done <- outcome{data, err}
				}()
				message, err := control.Recv()
				if err != nil || message.GetFileRequest() == nil {
					t.Fatal("file request not delivered", err)
				}
			}
			stream, err := agentv1.NewAgentServiceClient(connection).SendFile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&agentv1.FileChunk{RequestId: request.ID.String(), HeaderJson: raw}); err != nil {
				t.Fatal(err)
			}
			if scenario == "unsolicited" {
				if _, err := stream.CloseAndRecv(); err == nil {
					t.Fatal("accepted unsolicited content")
				}
				return
			}
			if err := stream.Send(&agentv1.FileChunk{Data: payload[:100]}); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "complete":
				for offset := 100; offset < len(payload); {
					end := min(offset+128<<10, len(payload))
					if err := stream.Send(&agentv1.FileChunk{Data: payload[offset:end]}); err != nil {
						t.Fatal(err)
					}
					offset = end
				}
			case "cancel":
				cancel()
			case "revoked":
				f.revoked.Store(true)
				// Keep the sender open. Authorization must stop the active read.
				if result := <-done; result.err == nil {
					t.Fatal("revoked transfer succeeded")
				}
				return
			case "duplicate":
				if err := stream.Send(&agentv1.FileChunk{RequestId: request.ID.String(), HeaderJson: raw}); err != nil {
					t.Fatal(err)
				}
			}
			_, transferErr := stream.CloseAndRecv()
			result := <-done
			if scenario == "complete" {
				if transferErr != nil || result.err != nil || !bytes.Equal(result.data, payload) {
					t.Fatalf("content changed: %v, %v (%d)", transferErr, result.err, len(result.data))
				}
			} else if transferErr == nil || result.err == nil {
				t.Fatal("incomplete transfer succeeded", transferErr, result.err)
			}
		})
	}
}
