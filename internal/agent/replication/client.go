package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/agent/replica"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client pushes snapshots from the active device.
type Client struct {
	InstallationID uuid.UUID
	Certificate    tls.Certificate
	Roots          *x509.CertPool
}

// Push sends one snapshot to one standby and waits until the standby has applied it.
// Blocks are read from snapshot, which must not change during the push.
func (c Client) Push(ctx context.Context, standby *agentv1.ReplicaPeer, appID string, manifest *replica.Manifest, snapshot string) (*agentv1.PushResult, error) {
	node, err := uuid.Parse(standby.NodeId)
	if err != nil {
		return nil, err
	}
	expected, err := (pki.Identity{InstallationID: c.InstallationID, Role: pki.Agent, SubjectID: node}).URI()
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{c.Certificate},
		// Agent certificates carry no server name. VerifyConnection checks the chain and the node identity instead.
		InsecureSkipVerify: true,
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("replication: the standby sent no certificate")
			}
			leaf := state.PeerCertificates[0]
			intermediates := x509.NewCertPool()
			for _, cert := range state.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: c.Roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return fmt.Errorf("replication: verify the standby certificate: %w", err)
			}
			if leaf.IsCA || len(leaf.URIs) != 1 || leaf.URIs[0].String() != expected.String() {
				return fmt.Errorf("replication: the standby is not node %s", node)
			}
			return nil
		},
	}
	conn, err := grpc.NewClient(standby.Address, grpc.WithTransportCredentials(credentials.NewTLS(config)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes), grpc.MaxCallSendMsgSize(maxMessageBytes)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := agentv1.NewReplicationServiceClient(conn).Push(ctx)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&agentv1.PushRequest{Message: &agentv1.PushRequest_Manifest{Manifest: &agentv1.PushManifest{AppId: appID, Manifest: data}}}); err != nil {
		return nil, err
	}
	response, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if result := response.GetResult(); result != nil {
		return result, nil
	}
	missing := response.GetMissing()
	if missing == nil {
		return nil, errors.New("replication: the standby sent no missing-block list")
	}
	root, err := os.OpenRoot(snapshot)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	sizes := map[string]int64{}
	for _, f := range manifest.Files {
		sizes[f.Path] = f.Size
	}
	buf := make([]byte, replica.BlockSize)
	for _, ref := range missing.Blocks {
		size, ok := sizes[ref.Path]
		offset := int64(ref.Index) * replica.BlockSize
		if !ok || offset >= size {
			return nil, fmt.Errorf("replication: the standby asked for an unknown block %s/%d", ref.Path, ref.Index)
		}
		n := min(int64(replica.BlockSize), size-offset)
		if err := readBlock(root, ref.Path, offset, buf[:n]); err != nil {
			return nil, err
		}
		if err := stream.Send(&agentv1.PushRequest{Message: &agentv1.PushRequest_Block{Block: &agentv1.ReplicaBlock{Path: ref.Path, Index: ref.Index, Data: buf[:n]}}}); err != nil {
			return nil, err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	response, err = stream.Recv()
	if err != nil {
		return nil, err
	}
	result := response.GetResult()
	if result == nil {
		return nil, errors.New("replication: the standby sent no result")
	}
	return result, nil
}

func readBlock(root *os.Root, path string, offset int64, buf []byte) error {
	file, err := root.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.ReadAt(buf, offset); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
