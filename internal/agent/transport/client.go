// Package transport maintains agent-initiated management streams without runtime policy.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
	// Handle returns an optional response only after required durable handling commits.
	// Session is delivered first on each connection. Replay durable results with Send.
	// Callbacks must honor cancellation. They must not stop database processes on disconnect.
	Handle func(context.Context, *agentv1.ControllerMessage) (*agentv1.AgentMessage, error)
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
		return nil, errors.New("invalid agent Hello")
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
		return nil, errors.New("invalid installation identity")
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
			return errors.New("invalid controller identity")
		}
		expected, err := (pki.Identity{InstallationID: installation, Role: pki.Controller, SubjectID: subject}).URI()
		if err != nil || uri.String() != expected.String() {
			return errors.New("invalid controller identity")
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
	deadline := time.AfterFunc(c.config.HandshakeTimeout, cancel)
	defer deadline.Stop()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(c.config.TLS)), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.config.MaxMessageBytes), grpc.MaxCallSendMsgSize(c.config.MaxMessageBytes)))
	if err != nil {
		return false
	}
	defer conn.Close()
	stream, err := agentv1.NewAgentServiceClient(conn).Connect(ctx, grpc.WaitForReady(true))
	if err != nil {
		return false
	}
	if err := stream.Send(&agentv1.AgentMessage{Message: &agentv1.AgentMessage_Hello{Hello: c.config.Hello}}); err != nil {
		return false
	}
	first, err := stream.Recv()
	if err != nil {
		return false
	}
	remote, ok := peer.FromContext(stream.Context())
	if !ok {
		return false
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return false
	}
	if _, err := pki.PeerIdentity(info.State, uuid.MustParse(c.config.Hello.InstallationId), pki.Controller); err != nil {
		return false
	}
	expires := info.State.PeerCertificates[0].NotAfter
	session := first.GetSession()
	if session == nil || session.ProtocolVersion != operation.ProtocolVersion || session.HeartbeatInterval == nil || session.HeartbeatInterval.CheckValid() != nil || session.HeartbeatInterval.AsDuration() <= 0 || session.StaleAfter == nil || session.StaleAfter.CheckValid() != nil || session.StaleAfter.AsDuration() <= session.HeartbeatInterval.AsDuration() || session.MaxMessageBytes < 1024 || session.MaxMessageBytes > uint32(c.config.MaxMessageBytes) || session.MaxQueuedCommands == 0 || session.MaxQueuedCommands > 256 {
		return false
	}
	id, err := uuid.Parse(session.SessionId)
	if err != nil || id == uuid.Nil || id.String() != session.SessionId {
		return false
	}
	for _, capability := range session.Capabilities {
		if !slices.Contains(c.config.Hello.Capabilities, capability) {
			return false
		}
	}
	if !deadline.Stop() || ctx.Err() != nil {
		return false
	}
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
	done := make(chan error, 2)
	// One writer owns stream.Send. Heartbeats bypass the reliable queue only when
	// selected; queued terminal results are never dropped to make room for metrics.
	go func() {
		tick := time.NewTicker(session.HeartbeatInterval.AsDuration())
		defer tick.Stop()
		var sequence uint64
		for {
			var message *agentv1.AgentMessage
			select {
			case <-ctx.Done():
				return
			case message = <-active.outgoing:
			case <-tick.C:
				sequence++
				message = &agentv1.AgentMessage{Message: &agentv1.AgentMessage_Heartbeat{Heartbeat: &agentv1.Heartbeat{Sequence: sequence, ObservedAt: timestamppb.Now()}}}
			}
			if err := stream.Send(message); err != nil {
				done <- err
				return
			}
		}
	}()
	go func() {
		message := first
		for {
			if controlstream.CheckControllerMessage(message, c.config.Hello.InstallationId, c.config.Hello.NodeId) != nil {
				done <- controlstream.ErrMessage
				return
			}
			response, err := c.config.Handle(ctx, message)
			if err != nil {
				done <- errors.New("durable controller message handling failed")
				return
			}
			if ctx.Err() != nil {
				return
			}
			if response != nil {
				if err := c.send(active, response); err != nil {
					done <- err
					return
				}
			}
			message, err = stream.Recv()
			if err != nil {
				done <- err
				return
			}
			if message.GetSession() != nil {
				done <- controlstream.ErrMessage
				return
			}
		}
	}()
	expiry := time.NewTimer(time.Until(expires))
	defer expiry.Stop()
	select {
	case <-ctx.Done():
	case <-done:
	case <-expiry.C:
	}
	return true
}
