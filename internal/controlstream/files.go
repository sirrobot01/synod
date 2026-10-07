package controlstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/restore"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileTransfer struct {
	request restore.FileRequest
	session string
	ctx     context.Context
	header  chan restore.FileHeader
	writer  *io.PipeWriter
	failed  chan error
	claimed bool
}

// ReadFile requests a repository read from the current agent session.
// The caller closes the body and cancels ctx when it no longer needs the transfer.
func (s *Server) ReadFile(ctx context.Context, request restore.FileRequest) (restore.FileHeader, io.ReadCloser, error) {
	if request.Validate() != nil || request.InstallationID != s.config.InstallationID {
		return restore.FileHeader{}, nil, ErrMessage
	}
	peer, ok := s.Session(request.NodeID)
	if !ok || !slices.Contains(peer.Capabilities, "files.read") {
		return restore.FileHeader{}, nil, ErrDisconnected
	}
	reader, writer := io.Pipe()
	transfer := &fileTransfer{request: request, session: peer.SessionID, ctx: ctx, header: make(chan restore.FileHeader, 1), writer: writer, failed: make(chan error, 1)}
	s.mu.Lock()
	if len(s.files) >= 32 || s.files[request.ID] != nil {
		s.mu.Unlock()
		reader.Close()
		writer.Close()
		return restore.FileHeader{}, nil, ErrBusy
	}
	s.files[request.ID] = transfer
	s.mu.Unlock()
	// Cancellation also releases a writer blocked by a slow or disconnected HTTP reader.
	stop := context.AfterFunc(ctx, func() {
		writer.CloseWithError(ctx.Err())
		s.mu.Lock()
		delete(s.files, request.ID)
		s.mu.Unlock()
	})
	raw, _ := json.Marshal(request)
	err := s.Send(request.NodeID, peer.SessionID, &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_FileRequest{FileRequest: &agentv1.FileRequest{RequestJson: raw}}})
	if err == nil {
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		select {
		case header := <-transfer.header:
			return header, reader, nil
		case err = <-transfer.failed:
		case <-ctx.Done():
			err = ctx.Err()
		case <-timer.C:
			err = context.DeadlineExceeded
		}
	}
	stop()
	reader.Close()
	writer.CloseWithError(err)
	s.mu.Lock()
	delete(s.files, request.ID)
	s.mu.Unlock()
	return restore.FileHeader{}, nil, err
}

// SendFile binds one agent answer to its pending request and current session.
func (s *Server) SendFile(stream grpc.ClientStreamingServer[agentv1.FileChunk, agentv1.FileAck]) error {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "file capacity reached")
	}
	identity, leaf, _, err := s.peerIdentity(stream.Context())
	if err != nil {
		return err
	}
	if err := s.authorize(stream.Context(), identity, leaf); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	started := make(chan *fileTransfer, 1)
	done := make(chan error, 1)
	// Returning from the RPC unblocks Recv, including a stalled or cancelled transfer.
	go func() { done <- s.receiveFile(ctx, identity.SubjectID, stream.Recv, started) }()
	timer := time.NewTimer(s.config.HelloTimeout)
	defer timer.Stop()
	tick := time.NewTicker(s.config.RecheckInterval)
	defer tick.Stop()
	var requestDone <-chan struct{}
	var transfer *fileTransfer
	for {
		select {
		case transfer = <-started:
			timer.Stop()
			requestDone = transfer.ctx.Done()
		case err := <-done:
			if err != nil {
				return status.Error(codes.Aborted, "file transfer incomplete")
			}
			return stream.SendAndClose(&agentv1.FileAck{})
		case <-timer.C:
			return status.Error(codes.DeadlineExceeded, "file header timeout")
		case <-ctx.Done():
			return status.Error(codes.Canceled, "file transfer cancelled")
		case <-requestDone:
			return status.Error(codes.Canceled, "file request cancelled")
		case <-tick.C:
			peer, online := s.Session(identity.SubjectID)
			if !online || (transfer != nil && peer.SessionID != transfer.session) {
				return status.Error(codes.Aborted, "agent session changed")
			}
			if err := s.authorize(ctx, identity, leaf); err != nil {
				return err
			}
		}
	}
}

func (s *Server) receiveFile(ctx context.Context, nodeID uuid.UUID, receive func() (*agentv1.FileChunk, error), started chan<- *fileTransfer) (err error) {
	first, err := receive()
	if err != nil {
		return err
	}
	id, err := uuid.Parse(first.GetRequestId())
	if err != nil || len(first.GetHeaderJson()) == 0 || len(first.HeaderJson) > 900<<10 || len(first.Data) != 0 {
		return ErrMessage
	}
	s.mu.Lock()
	transfer := s.files[id]
	current := s.sessions[nodeID]
	if transfer == nil || transfer.claimed || transfer.request.NodeID != nodeID || current == nil || current.peer.SessionID != transfer.session || current.ctx.Err() != nil || transfer.ctx.Err() != nil {
		s.mu.Unlock()
		return ErrMessage
	}
	transfer.claimed = true
	s.mu.Unlock()
	defer func() {
		transfer.writer.CloseWithError(err)
		if err != nil {
			transfer.failed <- err
		}
		s.mu.Lock()
		delete(s.files, id)
		s.mu.Unlock()
	}()
	stop := context.AfterFunc(ctx, func() { transfer.writer.CloseWithError(context.Canceled) })
	defer stop()
	started <- transfer
	var header restore.FileHeader
	if json.Unmarshal(first.HeaderJson, &header) != nil || header.Size < 0 {
		return ErrMessage
	}
	reading := transfer.request.Action == restore.FileRead && header.FailureCode == ""
	if reading && (header.SnapshotID == uuid.Nil || header.DestinationID == uuid.Nil || (header.Format != "file" && header.Format != "tar")) {
		return ErrMessage
	}
	if header.FailureCode == "" && transfer.request.Action != restore.FileSnapshots && ((transfer.request.SnapshotID != uuid.Nil && transfer.request.SnapshotID != header.SnapshotID) || (transfer.request.DestinationID != uuid.Nil && transfer.request.DestinationID != header.DestinationID)) {
		return ErrMessage
	}
	transfer.header <- header
	var size int64
	for {
		chunk, err := receive()
		if errors.Is(err, io.EOF) {
			if reading && header.Format == "file" && size != header.Size {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !reading || chunk.GetRequestId() != "" || len(chunk.GetHeaderJson()) != 0 || len(chunk.GetData()) == 0 || len(chunk.Data) > 256<<10 {
			return ErrMessage
		}
		if header.Format == "file" && int64(len(chunk.Data)) > header.Size-size {
			return ErrMessage
		}
		n, err := transfer.writer.Write(chunk.Data)
		size += int64(n)
		if err != nil {
			return err
		}
	}
}
