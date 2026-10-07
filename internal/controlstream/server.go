// Package controlstream provides authenticated controller sessions for agents.
package controlstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

var ErrBusy = errors.New("transport queue full")
var ErrDisconnected = errors.New("transport session disconnected")
var ErrMessage = errors.New("invalid transport message")

// Peer is the authenticated identity of one connected agent session.
type Peer struct {
	RemoteAddress  string
	InstallationID uuid.UUID
	NodeID         uuid.UUID
	SessionID      string
	Capabilities   []string
}

// Config holds the settings of one control stream server.
type Config struct {
	InstallationID       uuid.UUID
	TLS                  *tls.Config
	Capabilities         []string
	MaxMessageBytes      int
	QueueSize            int
	MaxSessions          int
	HelloTimeout         time.Duration
	HeartbeatInterval    time.Duration
	StaleAfter           time.Duration
	RecheckInterval      time.Duration
	AuthorizationTimeout time.Duration
	Authorize            func(context.Context, uuid.UUID, uuid.UUID, *x509.Certificate) error
	// Handle must return nil only after required durable handling commits.
	// It must honor context cancellation. Errors are never sent to the peer verbatim.
	Handle func(context.Context, Peer, *agentv1.AgentMessage) error
	// Logger records the refusals the peer is never told the reason for. A refused
	// agent is otherwise invisible: it sees a fixed status and the controller keeps
	// no account of why. slog.Default() is used when none is supplied.
	Logger *slog.Logger
}

type session struct {
	peer     Peer
	ctx      context.Context
	cancel   context.CancelFunc
	outgoing chan *agentv1.ControllerMessage
}

// helloGuard orders durable registration and publication for one node.
type helloGuard struct {
	permit chan struct{}
	users  int
}

// Server accepts authenticated agent streams and tracks one session per node.
type Server struct {
	agentv1.UnimplementedAgentServiceServer
	config   Config
	grpc     *grpc.Server
	slots    chan struct{}
	mu       sync.Mutex
	sessions map[uuid.UUID]*session
	hellos   map[uuid.UUID]*helloGuard
	files    map[uuid.UUID]*fileTransfer
}

// New checks the server settings, applies defaults, and registers the gRPC service.
func New(config Config) (*Server, error) {
	if config.InstallationID == uuid.Nil || config.TLS == nil || config.TLS.ClientCAs == nil || len(config.TLS.Certificates) == 0 || config.TLS.GetConfigForClient != nil || config.TLS.InsecureSkipVerify || config.Authorize == nil || config.Handle == nil {
		return nil, errors.New("transport requires installation identity, mutual TLS, authorization, and durable handling")
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.QueueSize == 0 {
		config.QueueSize = 16
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = 128
	}
	if config.HelloTimeout == 0 {
		config.HelloTimeout = 5 * time.Second
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.StaleAfter == 0 {
		config.StaleAfter = 20 * time.Second
	}
	if config.RecheckInterval == 0 {
		config.RecheckInterval = time.Second
	}
	if config.AuthorizationTimeout == 0 {
		config.AuthorizationTimeout = 2 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.MaxMessageBytes < 1024 || config.MaxMessageBytes > 4<<20 || config.QueueSize < 1 || config.QueueSize > 256 || config.MaxSessions < 1 || config.MaxSessions > 4096 || config.HelloTimeout <= 0 || config.HeartbeatInterval <= 0 || config.StaleAfter <= config.HeartbeatInterval || config.RecheckInterval <= 0 || config.AuthorizationTimeout <= 0 {
		return nil, errors.New("invalid transport limits")
	}
	transportTLS := config.TLS.Clone()
	transportTLS.MinVersion = max(transportTLS.MinVersion, tls.VersionTLS12)
	transportTLS.ClientAuth = tls.RequireAndVerifyClientCert
	if transportTLS.MaxVersion != 0 && transportTLS.MaxVersion < transportTLS.MinVersion {
		return nil, errors.New("invalid TLS versions")
	}
	config.Capabilities = append([]string(nil), config.Capabilities...)
	server := &Server{config: config, slots: make(chan struct{}, 2*config.MaxSessions), sessions: make(map[uuid.UUID]*session), hellos: make(map[uuid.UUID]*helloGuard), files: make(map[uuid.UUID]*fileTransfer)}
	server.grpc = grpc.NewServer(grpc.Creds(credentials.NewTLS(transportTLS)), grpc.MaxRecvMsgSize(config.MaxMessageBytes), grpc.MaxSendMsgSize(config.MaxMessageBytes), grpc.MaxConcurrentStreams(uint32(config.MaxSessions)), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second}), grpc.KeepaliveParams(keepalive.ServerParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}))
	agentv1.RegisterAgentServiceServer(server.grpc, server)
	return server, nil
}

func (s *Server) Serve(listener net.Listener) error { return s.grpc.Serve(listener) }

// Stop closes agent streams.
func (s *Server) Stop() { s.grpc.Stop() }

// Session returns the live peer for a node and reports whether one is connected.
func (s *Server) Session(nodeID uuid.UUID) (Peer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[nodeID]
	if !ok || current.ctx.Err() != nil {
		return Peer{}, false
	}
	peer := current.peer
	peer.Capabilities = slices.Clone(peer.Capabilities)
	return peer, true
}

// Send binds delivery to the expected session. Success means queued, not admitted.
func (s *Server) Send(nodeID uuid.UUID, sessionID string, message *agentv1.ControllerMessage) error {
	if CheckControllerMessage(message) != nil || message.GetSession() != nil || proto.Size(message) > s.config.MaxMessageBytes {
		return ErrMessage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[nodeID]
	if !ok || current.peer.SessionID != sessionID || current.ctx.Err() != nil {
		return ErrDisconnected
	}
	select {
	case current.outgoing <- proto.Clone(message).(*agentv1.ControllerMessage):
		return nil
	default:
		return ErrBusy
	}
}

func (s *Server) authorize(ctx context.Context, identity pki.Identity, leaf *x509.Certificate) error {
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return status.Error(codes.Unauthenticated, "agent certificate expired")
	}
	check, cancel := context.WithTimeout(ctx, s.config.AuthorizationTimeout)
	defer cancel()
	if err := s.config.Authorize(check, identity.InstallationID, identity.SubjectID, leaf); err != nil || check.Err() != nil {
		return status.Error(codes.PermissionDenied, "agent authorization failed")
	}
	return nil
}

// peerIdentity reads the verified agent certificate of one gRPC stream.
func (s *Server) peerIdentity(ctx context.Context) (pki.Identity, *x509.Certificate, string, error) {
	remote, ok := peer.FromContext(ctx)
	if !ok {
		return pki.Identity{}, nil, "", status.Error(codes.Unauthenticated, "verified agent TLS required")
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return pki.Identity{}, nil, "", status.Error(codes.Unauthenticated, "verified agent TLS required")
	}
	identity, err := pki.PeerIdentity(info.State, s.config.InstallationID, pki.Agent)
	if err != nil {
		s.config.Logger.Warn("agent identity refused", "peer", remote.Addr, "error", err)
		return pki.Identity{}, nil, "", status.Error(codes.Unauthenticated, "invalid agent identity")
	}
	return identity, info.State.PeerCertificates[0], remote.Addr.String(), nil
}

// Connect runs one authenticated agent stream until it ends.
func (s *Server) Connect(stream grpc.BidiStreamingServer[agentv1.AgentMessage, agentv1.ControllerMessage]) error {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "session capacity reached")
	}
	identity, leaf, remoteAddress, err := s.peerIdentity(stream.Context())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	current := &session{peer: Peer{RemoteAddress: remoteAddress, InstallationID: identity.InstallationID, NodeID: identity.SubjectID, SessionID: uuid.NewString()}, ctx: ctx, cancel: cancel, outgoing: make(chan *agentv1.ControllerMessage, s.config.QueueSize)}
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.sessions[identity.SubjectID] == current {
			delete(s.sessions, identity.SubjectID)
		}
	}()
	if err := s.authorize(ctx, identity, leaf); err != nil {
		return err
	}
	done := make(chan error, 2)
	activity := make(chan struct{}, 1)
	// One goroutine owns Recv and durable application handling. Returning the RPC
	// cancels blocked gRPC I/O; application callbacks must honor their context.
	go func() { done <- s.receive(ctx, stream, current, identity, leaf, activity) }()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-current.outgoing:
				if err := s.authorize(ctx, identity, leaf); err != nil {
					done <- err
					return
				}
				if ctx.Err() != nil {
					return
				}
				if err := stream.Send(message); err != nil {
					s.config.Logger.Warn("agent stream send failed", "node_id", identity.SubjectID, "session_id", current.peer.SessionID, "error", err)
					done <- status.Error(codes.Unavailable, "stream send failed")
					return
				}
			}
		}
	}()
	helloDeadline := time.NewTimer(s.config.HelloTimeout)
	defer helloDeadline.Stop()
	stale := time.NewTimer(s.config.StaleAfter)
	defer stale.Stop()
	expiry := time.NewTimer(time.Until(leaf.NotAfter))
	defer expiry.Stop()
	tick := time.NewTicker(s.config.RecheckInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return status.Error(codes.Canceled, "session closed")
		case err := <-done:
			return err
		case <-helloDeadline.C:
			return status.Error(codes.DeadlineExceeded, "Hello timed out")
		case <-stale.C:
			return status.Error(codes.DeadlineExceeded, "agent session stale")
		case <-expiry.C:
			return status.Error(codes.Unauthenticated, "agent certificate expired")
		case <-activity:
			helloDeadline.Stop()
			stale.Reset(s.config.StaleAfter)
		case <-tick.C:
			if err := s.authorize(ctx, identity, leaf); err != nil {
				return err
			}
		}
	}
}

func (s *Server) receive(ctx context.Context, stream grpc.BidiStreamingServer[agentv1.AgentMessage, agentv1.ControllerMessage], current *session, identity pki.Identity, leaf *x509.Certificate, activity chan<- struct{}) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "Hello required")
	}
	hello := first.GetHello()
	if hello == nil || hello.InstallationId != identity.InstallationID.String() || hello.NodeId != identity.SubjectID.String() {
		return status.Error(codes.PermissionDenied, "Hello identity mismatch")
	}
	capabilities, err := NegotiateHello(hello, s.config.Capabilities)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "incompatible agent protocol")
	}
	current.peer.Capabilities = slices.Clone(capabilities)
	if err := s.authorize(ctx, identity, leaf); err != nil {
		return err
	}
	if err := s.publishSession(ctx, current, first, capabilities); err != nil {
		return err
	}
	select {
	case activity <- struct{}{}:
	default:
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			return status.Error(codes.Unavailable, "agent stream closed")
		}
		if CheckAgentMessage(message) != nil {
			return status.Error(codes.InvalidArgument, "invalid agent message")
		}
		if err := s.authorize(ctx, identity, leaf); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return status.Error(codes.Canceled, "session closed")
		}
		select {
		case activity <- struct{}{}:
		default:
		}
		callbackPeer := current.peer
		callbackPeer.Capabilities = slices.Clone(callbackPeer.Capabilities)
		if err := s.config.Handle(ctx, callbackPeer, message); err != nil || ctx.Err() != nil {
			if err != nil {
				s.config.Logger.Warn("agent message refused", "node_id", callbackPeer.NodeID, "session_id", callbackPeer.SessionID, "message", fmt.Sprintf("%T", message.Message), "error", err)
			}
			return status.Error(codes.Unavailable, "durable receive handling failed")
		}
	}
}

// publishSession keeps the durable and live session order equal for one node.
// Other nodes can register while this node waits for durable handling.
func (s *Server) publishSession(ctx context.Context, current *session, first *agentv1.AgentMessage, capabilities []string) error {
	s.mu.Lock()
	guard := s.hellos[current.peer.NodeID]
	if guard == nil {
		guard = &helloGuard{permit: make(chan struct{}, 1)}
		s.hellos[current.peer.NodeID] = guard
	}
	guard.users++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		guard.users--
		if guard.users == 0 {
			delete(s.hellos, current.peer.NodeID)
		}
	}()
	select {
	case guard.permit <- struct{}{}:
		defer func() { <-guard.permit }()
	case <-ctx.Done():
		return status.Error(codes.Canceled, "session closed")
	}
	if ctx.Err() != nil {
		return status.Error(codes.Canceled, "session closed")
	}
	callbackPeer := current.peer
	callbackPeer.Capabilities = slices.Clone(callbackPeer.Capabilities)
	if err := s.config.Handle(ctx, callbackPeer, first); err != nil || ctx.Err() != nil {
		if err != nil {
			s.config.Logger.Warn("agent Hello refused", "node_id", callbackPeer.NodeID, "session_id", callbackPeer.SessionID, "error", err)
		}
		return status.Error(codes.Unavailable, "Hello handling failed")
	}

	s.mu.Lock()
	if ctx.Err() != nil {
		s.mu.Unlock()
		return status.Error(codes.Canceled, "session closed")
	}
	old := s.sessions[current.peer.NodeID]
	if old == nil && len(s.sessions) >= s.config.MaxSessions {
		s.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "session capacity reached")
	}
	s.sessions[current.peer.NodeID] = current
	current.outgoing <- &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Session{Session: &agentv1.Session{ProtocolVersion: ProtocolVersion, SessionId: current.peer.SessionID, HeartbeatInterval: durationpb.New(s.config.HeartbeatInterval), StaleAfter: durationpb.New(s.config.StaleAfter), MaxMessageBytes: uint32(s.config.MaxMessageBytes), MaxQueuedMessages: uint32(s.config.QueueSize), Capabilities: capabilities}}}
	if old != nil {
		old.cancel()
	}
	s.mu.Unlock()
	return nil
}
