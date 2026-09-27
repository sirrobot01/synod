// Package transport maintains agent-initiated management streams without runtime policy.
package transport

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/journal"
	"github.com/sirrobot01/synod/internal/controlstream"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds the settings of one agent transport client.
type Config struct {
	Endpoints        []string
	TLS              *tls.Config
	Hello            *agentv1.Hello
	MaxMessageBytes  int
	QueueSize        int
	HandshakeTimeout time.Duration
	BackoffMin       time.Duration
	BackoffMax       time.Duration
	// Logger records why a connection ended. Run reconnects on its own, so a stream
	// that fails repeatedly is otherwise invisible.
	Logger *slog.Logger
	// Handle returns an optional response only after required durable handling commits.
	// Session is delivered first on each connection. Replay durable results with Send.
	// Callbacks must honor cancellation. They must not stop database processes on disconnect.
	Handle func(context.Context, *agentv1.ControllerMessage) (*agentv1.AgentMessage, error)
	// Observations returns an in-memory snapshot. It must not block on I/O.
	Observations func() []*agentv1.Observation
	// Replicas returns the tier 2 replica state for heartbeats. It must not block on I/O.
	Replicas func() []*agentv1.ReplicaStatus
	// WriterClock returns CLOCK_BOOTTIME nanoseconds, or zero when unavailable.
	WriterClock func() uint64
}

type connection struct {
	ctx      context.Context
	outgoing chan *agentv1.AgentMessage
	maxBytes int
}

// Client maintains one outbound management stream to a controller.
type Client struct {
	config  Config
	mu      sync.Mutex
	active  *connection
	running bool
}

// New checks the transport settings and returns a client that is ready to run.
func New(config Config) (*Client, error) {
	if len(config.Endpoints) == 0 || config.TLS == nil || config.TLS.RootCAs == nil || len(config.TLS.Certificates) == 0 || config.TLS.InsecureSkipVerify || config.Handle == nil || config.Hello == nil {
		return nil, errors.New("agent transport requires endpoints, verified TLS, Hello, and durable handling")
	}
	if slices.Contains(config.Endpoints, "") {
		return nil, errors.New("empty controller endpoint")
	}
	if _, err := operation.NegotiateHello(config.Hello, config.Hello.Capabilities); err != nil {
		return nil, fmt.Errorf("invalid agent Hello: %w", err)
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.QueueSize == 0 {
		config.QueueSize = 16
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = 5 * time.Second
	}
	if config.BackoffMin == 0 {
		config.BackoffMin = 100 * time.Millisecond
	}
	if config.BackoffMax == 0 {
		config.BackoffMax = 10 * time.Second
	}
	if config.MaxMessageBytes < 1024 || config.MaxMessageBytes > 4<<20 || config.QueueSize < 1 || config.QueueSize > 256 || config.HandshakeTimeout <= 0 || config.BackoffMin <= 0 || config.BackoffMax < config.BackoffMin || config.BackoffMax > time.Minute {
		return nil, errors.New("invalid agent transport limits")
	}
	if proto.Size(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: config.Hello}}) > config.MaxMessageBytes {
		return nil, controlstream.ErrMessage
	}
	config.Endpoints = slices.Clone(config.Endpoints)
	config.Hello = proto.Clone(config.Hello).(*agentv1.Hello)
	transportTLS := config.TLS.Clone()
	transportTLS.MinVersion = max(transportTLS.MinVersion, tls.VersionTLS12)
	if transportTLS.MaxVersion != 0 && transportTLS.MaxVersion < transportTLS.MinVersion {
		return nil, errors.New("invalid TLS versions")
	}
	installation, err := uuid.Parse(config.Hello.InstallationId)
	if err != nil {
		return nil, fmt.Errorf("invalid installation identity %q: %w", config.Hello.InstallationId, err)
	}
	previous := transportTLS.VerifyConnection
	transportTLS.VerifyConnection = func(state tls.ConnectionState) error {
		// VerifyConnection runs before HandshakeComplete becomes true.
		// Normal TLS verification has already built VerifiedChains here.
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || state.PeerCertificates[0].IsCA || len(state.PeerCertificates[0].URIs) != 1 {
			return errors.New("invalid controller identity")
		}
		uri := state.PeerCertificates[0].URIs[0]
		parts := strings.Split(uri.Path, "/")
		if len(parts) != 3 {
			return errors.New("invalid controller identity")
		}
		subject, err := uuid.Parse(parts[2])
		if err != nil {
			return fmt.Errorf("invalid controller identity: parse the subject in %q: %w", uri.Path, err)
		}
		expected, err := (pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: subject}).URI()
		if err != nil {
			return fmt.Errorf("invalid controller identity: build the expected URI: %w", err)
		}
		if uri.String() != expected.String() {
			return fmt.Errorf("invalid controller identity: the controller names %q and this agent expects %q", uri, expected)
		}
		if previous != nil {
			return previous(state)
		}
		return nil
	}
	config.TLS = transportTLS
	return &Client{config: config}, nil
}

// Send snapshots a message into the current bounded queue. The journal retains results.
func (c *Client) Send(message *agentv1.AgentMessage) error {
	return c.send(nil, message)
}

// A callback response belongs to the connection that delivered its request.
func (c *Client) send(expected *connection, message *agentv1.AgentMessage) error {
	if controlstream.CheckAgentMessage(message, c.config.Hello.InstallationId, c.config.Hello.NodeId) != nil {
		return controlstream.ErrMessage
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	active := c.active
	if active == nil || active.ctx.Err() != nil || (expected != nil && active != expected) {
		return controlstream.ErrDisconnected
	}
	if proto.Size(message) > active.maxBytes {
		return controlstream.ErrMessage
	}
	select {
	case active.outgoing <- proto.Clone(message).(*agentv1.AgentMessage):
		return nil
	default:
		return controlstream.ErrBusy
	}
}

// Run reconnects until cancellation. It never changes a database or local journal.
func (c *Client) Run(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("agent transport already running")
	}
	c.running = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.running = false; c.mu.Unlock() }()
	delay := c.config.BackoffMin
	for attempt := 0; ctx.Err() == nil; attempt++ {
		established := c.connect(ctx, c.config.Endpoints[attempt%len(c.config.Endpoints)])
		if ctx.Err() != nil {
			break
		}
		if established {
			delay = c.config.BackoffMin
		}
		jitter := time.Duration(rand.Int64N(int64(delay/2) + 1))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay/2 + jitter):
		}
		delay = min(delay*2, c.config.BackoffMax)
	}
	return ctx.Err()
}

func (c *Client) connect(parent context.Context, endpoint string) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	failure, sessionID := "client_setup_failed", ""
	// cause is what actually went wrong at the stage named by failure. Every path
	// out of here is a bare false, so without this the reason for a session that
	// will not start is lost at the one place it is known.
	var cause error
	// failed names the type of the controller message whose handler ended the
	// session. The handler error itself can carry private detail and is not logged.
	var failed string
	defer func() {
		if parent.Err() == nil && c.config.Logger != nil {
			c.config.Logger.Warn("agent_stream_ended", "session_id", sessionID, "reason", failure, "message", failed, "error", cause)
		}
	}()
	deadline := time.AfterFunc(c.config.HandshakeTimeout, cancel)
	defer deadline.Stop()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(c.config.TLS)), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.config.MaxMessageBytes), grpc.MaxCallSendMsgSize(c.config.MaxMessageBytes)))
	if err != nil {
		cause = err
		return false
	}
	defer conn.Close()
	failure = "connect_failed"
	stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx, grpc.WaitForReady(true))
	if err != nil {
		cause = err
		return false
	}
	failure = "hello_send_failed"
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: c.config.Hello}}); err != nil {
		cause = err
		return false
	}
	failure = "session_receive_failed"
	first, err := stream.Recv()
	if err != nil {
		cause = err
		return false
	}
	failure = "controller_identity_rejected"
	remote, ok := peer.FromContext(stream.Context())
	if !ok {
		cause = errors.New("the stream carries no peer")
		return false
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		cause = fmt.Errorf("the peer carries %T authentication and TLS is required", remote.AuthInfo)
		return false
	}
	if _, err := pki.PeerIdentity(info.State, uuid.MustParse(c.config.Hello.InstallationId), pki.Controller); err != nil {
		cause = err
		return false
	}
	expires := info.State.PeerCertificates[0].NotAfter
	failure = "session_rejected"
	session := first.GetSession()
	if session == nil || session.ProtocolVersion != operation.ProtocolVersion || session.HeartbeatInterval == nil || session.HeartbeatInterval.CheckValid() != nil || session.HeartbeatInterval.AsDuration() <= 0 || session.StaleAfter == nil || session.StaleAfter.CheckValid() != nil || session.StaleAfter.AsDuration() <= session.HeartbeatInterval.AsDuration() || session.MaxMessageBytes < 1024 || session.MaxMessageBytes > uint32(c.config.MaxMessageBytes) || session.MaxQueuedCommands == 0 || session.MaxQueuedCommands > 256 {
		cause = errors.New("the session offer is absent or outside the protocol's limits")
		return false
	}
	id, err := uuid.Parse(session.SessionId)
	if err != nil {
		cause = fmt.Errorf("parse the offered session %q: %w", session.SessionId, err)
		return false
	}
	if id == uuid.Nil || id.String() != session.SessionId {
		cause = fmt.Errorf("the offered session %q is not a canonical non-nil UUID", session.SessionId)
		return false
	}
	for _, capability := range session.Capabilities {
		if !slices.Contains(c.config.Hello.Capabilities, capability) {
			cause = fmt.Errorf("the session requires capability %q, which this agent does not offer", capability)
			return false
		}
	}
	if !deadline.Stop() || ctx.Err() != nil {
		cause = cmp.Or(ctx.Err(), errors.New("the handshake deadline expired"))
		return false
	}
	sessionID = session.SessionId
	active := &connection{ctx: ctx, outgoing: make(chan *agentv1.AgentMessage, min(c.config.QueueSize, int(session.MaxQueuedCommands))), maxBytes: int(session.MaxMessageBytes)}
	c.mu.Lock()
	c.active = active
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.active == active {
			c.active = nil
		}
		c.mu.Unlock()
	}()
	done := make(chan string, 2)
	handled := make(chan string, 1)
	// One writer owns stream.Send. Heartbeats bypass the reliable queue only when
	// selected; queued terminal results are never dropped to make room for metrics.
	go func() {
		tick := time.NewTicker(session.HeartbeatInterval.AsDuration())
		defer tick.Stop()
		var sequence uint64
		observationOffset := 0
		for {
			var message *agentv1.AgentMessage
			select {
			case <-ctx.Done():
				return
			case message = <-active.outgoing:
			case <-tick.C:
				sequence++
				message = &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: sequence, ObservedAt: timestamppb.Now()}}}
				if c.config.WriterClock != nil {
					message.GetHeartbeat().WriterClockNanos = c.config.WriterClock()
				}
				if c.config.Replicas != nil {
					heartbeat := message.GetHeartbeat()
					// Replica state is small. If it cannot fit, send the heartbeat without it.
					if heartbeat.Replicas = c.config.Replicas(); proto.Size(message) > active.maxBytes {
						heartbeat.Replicas = nil
					}
				}
				if c.config.Observations != nil {
					heartbeat := message.GetHeartbeat()
					observations := c.config.Observations()
					for range observations {
						observationOffset %= len(observations)
						observation := observations[observationOffset]
						heartbeat.Observations = append(heartbeat.Observations, observation)
						if proto.Size(message) > active.maxBytes {
							heartbeat.Observations = heartbeat.Observations[:len(heartbeat.Observations)-1]
							if len(heartbeat.Observations) != 0 {
								break
							}
						}
						observationOffset++
					}
				}
			}
			if err := stream.Send(message); err != nil {
				done <- "send_failed"
				return
			}
		}
	}()
	go func() {
		message := first
		for {
			if controlstream.CheckControllerMessage(message, c.config.Hello.InstallationId, c.config.Hello.NodeId) != nil {
				done <- "invalid_controller_message"
				return
			}
			response, err := c.config.Handle(ctx, message)

			// Keep ordinary admission refusals on this session. A failed session setup
			// or a fault joined with a refusal must still close the connection.
			refusal := ""
			switch {
			case errors.Is(err, operation.ErrStaleOwner):
				refusal = "stale_owner"
			case errors.Is(err, journal.ErrBusy), errors.Is(err, controlstream.ErrBusy):
				refusal = "agent_busy"
			}
			if message.GetSession() == nil && refusal != "" && !errors.Is(err, journal.ErrRecoveryRequired) && !errors.Is(err, controlstream.ErrMessage) {
				if c.config.Logger != nil {
					c.config.Logger.Warn("agent_message_refused", "session_id", session.SessionId, "reason", refusal, "message", fmt.Sprintf("%T", message.Message))
				}
				err, response = nil, nil
			}
			if err != nil {
				reason := "handler_failed"
				switch {
				case errors.Is(err, journal.ErrRecoveryRequired):
					reason = "recovery_required"
				case errors.Is(err, controlstream.ErrMessage):
					reason = "invalid_controller_message"
				case message.GetSession() != nil:
					reason = "session_installation_failed"
				}
				handled <- fmt.Sprintf("%T", message.Message)
				done <- reason
				return
			}
			if ctx.Err() != nil {
				return
			}
			if response != nil {
				if err := c.send(active, response); err != nil {
					done <- "response_not_queued"
					return
				}
			}
			message, err = stream.Recv()
			if err != nil {
				done <- "receive_failed"
				return
			}
			if message.GetSession() != nil {
				done <- "unexpected_session"
				return
			}
		}
	}()
	expiry := time.NewTimer(time.Until(expires))
	defer expiry.Stop()
	select {
	case <-ctx.Done():
		failure = "session_cancelled"
	case failure = <-done:
	case <-expiry.C:
		failure = "controller_certificate_expired"
	}
	select {
	case failed = <-handled:
	default:
	}
	return true
}
