package repository

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"github.com/google/uuid"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func TestSFTPCatalogAndHostIdentity(t *testing.T) {
	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostKey)
	if err != nil {
		t.Fatal(err)
	}
	_, clientKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientSigner, err := ssh.NewSignerFromKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	keyBlock, err := ssh.MarshalPrivateKey(clientKey, "")
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() == "synod" && bytes.Equal(key.Marshal(), clientSigner.PublicKey().Marshal()) {
			return nil, nil
		}
		return nil, errors.New("unknown test key")
	}}
	serverConfig.AddHostKey(hostSigner)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var connections sync.Map
	var workers sync.WaitGroup
	drop := func() { connections.Range(func(key, value any) bool { key.(net.Conn).Close(); return true }) }
	t.Cleanup(func() { cancel(); listener.Close(); drop(); workers.Wait() })
	workers.Go(func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Store(raw, true)
			workers.Go(func() {
				defer connections.Delete(raw)
				defer raw.Close()
				if ctx.Err() != nil {
					return
				}
				conn, channels, requests, err := ssh.NewServerConn(raw, serverConfig)
				if err != nil {
					return
				}
				defer conn.Close()
				workers.Go(func() { ssh.DiscardRequests(requests) })
				for next := range channels {
					if next.ChannelType() != "session" {
						next.Reject(ssh.UnknownChannelType, "session required")
						continue
					}
					channel, requests, err := next.Accept()
					if err != nil {
						return
					}
					workers.Go(func() {
						defer channel.Close()
						for request := range requests {
							var subsystem struct{ Name string }
							ok := request.Type == "subsystem" && ssh.Unmarshal(request.Payload, &subsystem) == nil && subsystem.Name == "sftp"
							request.Reply(ok, nil)
							if ok {
								server, err := sftp.NewServer(channel)
								if err != nil {
									return
								}
								server.Serve()
								server.Close()
								return
							}
						}
					})
				}
			})
		}
	})
	host, portString, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portString)
	location := Location{Kind: "sftp", Path: filepath.Join(t.TempDir(), "repository"), SFTP: &SFTPLocation{Host: host, Port: port, User: "synod", KnownHosts: knownhosts.Line([]string{listener.Addr().String()}, hostSigner.PublicKey())}}
	credentials := Credentials{Password: "private-repository-password", SSHKey: string(pem.EncodeToMemory(keyBlock))}
	r, err := Create(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	catalog := testCatalog()
	if err := r.WriteCatalog(t.Context(), catalog); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadCatalog(t.Context())
	if err != nil || got.InstallationID != catalog.InstallationID {
		t.Fatalf("SFTP reopen: %v", err)
	}
	drop()
	catalog.Generation = 2
	interrupted, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	if err := r.WriteCatalog(interrupted, catalog); err == nil {
		t.Fatal("lost SFTP connection accepted a catalog")
	}
	r.Close(t.Context())
	r, err = Open(t.Context(), location, credentials)
	if err != nil {
		t.Fatal(err)
	}
	got, err = r.ReadCatalog(t.Context())
	if err != nil || got.Generation != 1 {
		t.Fatalf("failed upload changed the catalog: %v", err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	if state, err := ClaimMaintenanceAt(t.Context(), location, credentials, catalog.InstallationID, node, 1, -MaintenanceHandover-time.Minute); err != nil || state != MaintenanceClaimed {
		t.Fatalf("claim SFTP maintenance: %s, %v", state, err)
	}
	if state, err := Maintain(t.Context(), location, credentials, catalog.InstallationID, node, 1); err != nil || state != MaintenanceCompleted {
		t.Fatalf("SFTP maintenance: %s, %v", state, err)
	}
	wrongLocation := location
	other := *location.SFTP
	other.KnownHosts = knownhosts.Line([]string{listener.Addr().String()}, clientSigner.PublicKey())
	wrongLocation.SFTP = &other
	if r, err := Open(t.Context(), wrongLocation, credentials); err == nil {
		r.Close(t.Context())
		t.Fatal("wrong SFTP host key accepted")
	}
	wrongCredentials := credentials
	wrongBlock, _ := ssh.MarshalPrivateKey(hostKey, "")
	wrongCredentials.SSHKey = string(pem.EncodeToMemory(wrongBlock))
	if r, err := Open(t.Context(), location, wrongCredentials); err == nil {
		r.Close(t.Context())
		t.Fatal("wrong SFTP client key accepted")
	}
}

func TestSFTPHandshakeCancellation(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(key, "")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		for {
			if _, err := conn.Read(buffer); err != nil {
				return
			}
		}
	}()
	host, portString, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portString)
	location := Location{Kind: "sftp", Path: "/test", SFTP: &SFTPLocation{Host: host, Port: port, User: "synod", KnownHosts: knownhosts.Line([]string{listener.Addr().String()}, signer.PublicKey())}}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if r, err := Open(ctx, location, Credentials{Password: "test-password", SSHKey: string(pem.EncodeToMemory(block))}); !errors.Is(err, context.DeadlineExceeded) {
		if r != nil {
			r.Close(t.Context())
		}
		t.Fatalf("handshake did not honor cancellation: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancelled handshake was not bounded")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled SFTP connection remains open")
	}
}
