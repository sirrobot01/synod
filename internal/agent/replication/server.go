package replication

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxManifestBytes = 16 << 20
	maxMessageBytes  = maxManifestBytes + 1<<20
	incomingDir      = "incoming"
)

// Failure codes in a PushResult and in ReplicaStatus.
const (
	FailureEpochMismatch = "epoch_mismatch"
	FailureStale         = "stale_snapshot"
	FailureCorrupt       = "replica_corrupt"
	FailureApply         = "apply_failed"
	FailurePush          = "push_failed"
	// The active device reports these when it cannot take a snapshot.
	FailureFreeze   = "freeze_timeout"
	FailureSnapshot = "snapshot_failed"
)

var errNotReceived = errors.New("replication: block was not received")

// Server receives snapshots for the apps on which this device is a standby.
// Each app's replica lives in Root/<app ID>.
type Server struct {
	InstallationID uuid.UUID
	Root           string
	Table          *Table
	Status         *Status
	Logger         *slog.Logger

	agentv1.UnimplementedReplicationServiceServer
}

// Serve accepts pushes until ctx ends. Peers must present an agent certificate of this installation.
func (s *Server) Serve(ctx context.Context, listener net.Listener, certificate tls.Certificate, roots *x509.CertPool) error {
	if err := s.restoreStatus(ctx); err != nil {
		return err
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)), grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes))
	agentv1.RegisterReplicationServiceServer(server, s)
	stop := context.AfterFunc(ctx, server.GracefulStop)
	defer stop()
	err := server.Serve(listener)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// Push receives one snapshot: manifest, missing-block list, blocks, result.
func (s *Server) Push(stream grpc.BidiStreamingServer[agentv1.PushRequest, agentv1.PushResponse]) error {
	sender, err := s.sender(stream.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, "verified agent TLS required")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetManifest()
	if header == nil || len(header.Manifest) > maxManifestBytes {
		return status.Error(codes.InvalidArgument, "the first message must be a manifest")
	}
	assignment, ok := s.Table.Lookup(header.AppId)
	if !ok || assignment.Role != RoleStandby || len(assignment.Peers) != 1 || assignment.Peers[0].NodeId != sender.String() {
		return status.Error(codes.PermissionDenied, "this node is not the assigned active peer for the app")
	}
	var manifest replica.Manifest
	if err := json.Unmarshal(header.Manifest, &manifest); err != nil {
		return status.Error(codes.InvalidArgument, "invalid manifest")
	}
	if err := manifest.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	result := func(code string) error {
		if code != "" {
			s.Status.Fail(header.AppId, RoleStandby, code)
		}
		return stream.Send(&agentv1.PushResponse{Message: &agentv1.PushResponse_Result{Result: &agentv1.PushResult{Epoch: manifest.Epoch, Sequence: manifest.Sequence, FailureCode: code}}})
	}
	if manifest.Epoch != assignment.Epoch {
		return result(FailureEpochMismatch)
	}
	unlock, err := s.Table.LockApp(stream.Context(), header.AppId)
	if err != nil {
		return err
	}
	defer unlock()

	store := replica.Replica{Root: filepath.Join(s.Root, header.AppId)}
	if err := store.Recover(); err != nil {
		return status.Error(codes.Internal, "recover the replica")
	}
	current, err := store.Current()
	if err != nil {
		return status.Error(codes.Internal, "read the replica")
	}
	// A lost reply can repeat a completed transfer. A changed manifest is never a replay.
	if current != nil && reflect.DeepEqual(current, &manifest) {
		if err := store.Verify(stream.Context()); err != nil {
			return result(FailureCorrupt)
		}
		s.Status.Record(&agentv1.ReplicaStatus{AppId: header.AppId, Role: RoleStandby, Epoch: manifest.Epoch, Sequence: manifest.Sequence, CompletedAt: timestamppb.New(manifest.CreatedAt)})
		return result("")
	}
	if current != nil && (manifest.Epoch < current.Epoch || (manifest.Epoch == current.Epoch && manifest.Sequence <= current.Sequence)) {
		return result(FailureStale)
	}
	missing := replica.Missing(current, &manifest)
	refs := make([]*agentv1.BlockRef, len(missing))
	for i, ref := range missing {
		refs[i] = &agentv1.BlockRef{Path: ref.Path, Index: uint32(ref.Index)}
	}
	if err := stream.Send(&agentv1.PushResponse{Message: &agentv1.PushResponse_Missing{Missing: &agentv1.MissingBlocks{Blocks: refs}}}); err != nil {
		return err
	}
	received, err := s.receive(stream, store.Root, &manifest, missing)
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Join(store.Root, incomingDir))
	currentAssignment, ok := s.Table.Lookup(header.AppId)
	if !ok || currentAssignment.Role != RoleStandby || currentAssignment.Epoch != manifest.Epoch || currentAssignment.Peers[0].NodeId != sender.String() {
		return result(FailureEpochMismatch)
	}
	err = store.Apply(&manifest, func(ref replica.BlockRef) ([]byte, error) {
		i, ok := received[ref]
		if !ok {
			return nil, errNotReceived
		}
		return os.ReadFile(filepath.Join(store.Root, incomingDir, strconv.Itoa(i)))
	})
	switch {
	case errors.Is(err, errNotReceived):
		// A local block no longer matched its manifest. Start over with a complete copy.
		s.log().Warn("replica is corrupt; the next push sends every block", "app_id", header.AppId)
		if err := store.Discard(); err != nil {
			return status.Error(codes.Internal, "discard the replica")
		}
		return result(FailureCorrupt)
	case errors.Is(err, replica.ErrStale):
		return result(FailureStale)
	case err != nil:
		s.log().Warn("replica apply failed", "app_id", header.AppId, "error", err)
		return result(FailureApply)
	}
	s.Status.Record(&agentv1.ReplicaStatus{AppId: header.AppId, Role: RoleStandby, Epoch: manifest.Epoch, Sequence: manifest.Sequence, CompletedAt: timestamppb.Now()})
	return result("")
}

// receive stores each missing block on disk after it checks the block against the manifest.
func (s *Server) receive(stream grpc.BidiStreamingServer[agentv1.PushRequest, agentv1.PushResponse], root string, manifest *replica.Manifest, missing []replica.BlockRef) (map[replica.BlockRef]int, error) {
	files := map[string]replica.File{}
	for _, f := range manifest.Files {
		files[f.Path] = f
	}
	dir := filepath.Join(root, incomingDir)
	if err := os.RemoveAll(dir); err != nil {
		return nil, status.Error(codes.Internal, "prepare incoming blocks")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, status.Error(codes.Internal, "prepare incoming blocks")
	}
	received := make(map[replica.BlockRef]int, len(missing))
	for i, want := range missing {
		request, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		block := request.GetBlock()
		if block == nil || block.Path != want.Path || int(block.Index) != want.Index {
			return nil, status.Error(codes.InvalidArgument, "blocks must follow the missing-block order")
		}
		f := files[want.Path]
		size := min(int64(replica.BlockSize), f.Size-int64(want.Index)*replica.BlockSize)
		if int64(len(block.Data)) != size || sha256.Sum256(block.Data) != f.Blocks[want.Index] {
			return nil, status.Error(codes.DataLoss, fmt.Sprintf("block %d of %s does not match the manifest", want.Index, want.Path))
		}
		if err := os.WriteFile(filepath.Join(dir, strconv.Itoa(i)), block.Data, 0o600); err != nil {
			return nil, status.Error(codes.Internal, "store an incoming block")
		}
		received[want] = i
	}
	return received, nil
}

func (s *Server) sender(ctx context.Context) (uuid.UUID, error) {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return uuid.Nil, errors.New("no peer")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return uuid.Nil, errors.New("no TLS")
	}
	identity, err := pki.PeerIdentity(info.State, s.InstallationID, pki.Agent)
	if err != nil {
		return uuid.Nil, err
	}
	return identity.SubjectID, nil
}

func (s *Server) log() *slog.Logger {
	if s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

// restoreStatus makes a completed transfer visible after a process restart.
func (s *Server) restoreStatus(ctx context.Context) error {
	entries, err := os.ReadDir(s.Root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !canonical(entry.Name()) {
			continue
		}
		appID := entry.Name()
		unlock, err := s.Table.LockApp(ctx, appID)
		if err != nil {
			return err
		}
		store := replica.Replica{Root: filepath.Join(s.Root, appID)}
		err = store.Recover()
		var manifest *replica.Manifest
		if err == nil {
			manifest, err = store.Current()
		}
		if err == nil && manifest != nil && (manifest.Epoch == 0 || manifest.Epoch > math.MaxInt64 || manifest.Sequence == 0 || manifest.Sequence > math.MaxInt64 || manifest.CreatedAt.IsZero()) {
			err = replica.ErrBlock
		}
		if err == nil && manifest != nil {
			err = store.Verify(ctx)
		}
		unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			s.Status.Fail(appID, RoleStandby, FailureCorrupt)
			continue
		}
		if manifest != nil {
			s.Status.Record(&agentv1.ReplicaStatus{AppId: appID, Role: RoleStandby, Epoch: manifest.Epoch, Sequence: manifest.Sequence, CompletedAt: timestamppb.New(manifest.CreatedAt)})
		}
	}
	return nil
}
