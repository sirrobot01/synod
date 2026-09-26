package executor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"time"

	"github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// errEtcdMembershipInterrupted reports a membership change that the agent lost before it saw the
// result. The change leaves no native work behind, so the command fails. Each action succeeds
// at once when its result already holds, so a new plan can repeat it.
var errEtcdMembershipInterrupted = errors.New("etcd_membership_interrupted")

// etcdMembership changes the cluster's etcd membership through the local etcd member. Like a
// switchover, it is a network action and does not go to the container runtime.
func (e *Executor) etcdMembership(ctx context.Context, command *agentv1.Command, secrets runtime.Secrets) (runtime.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, command.Input.Timeout.AsDuration())
	defer cancel()
	p := command.Input.GetEtcdMembership()
	material, err := postgres.DecodePatroniTLSMaterial(secrets["patroni_member_tls"])
	if err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	roots := x509.NewCertPool()
	pair, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if !roots.AppendCertsFromPEM(material.CA) || err != nil {
		return runtime.Result{}, runtime.ErrConfigurationConflict
	}
	// Validation accepted the address and the port before the command was admitted.
	address := netip.MustParseAddr(p.Address)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://" + net.JoinHostPort(address.String(), strconv.Itoa(int(p.ClientPort)))},
		TLS:         &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: address.String(), Certificates: []tls.Certificate{pair}},
		DialTimeout: 10 * time.Second, Logger: zap.NewNop(), Context: ctx,
	})
	if err != nil {
		return runtime.Result{}, err
	}
	defer client.Close()
	if err := changeEtcdMembership(ctx, client, p); err != nil {
		return runtime.Result{}, err
	}
	return runtime.Result{Effects: []*agentv1.Effect{{
		CommandId: command.CommandId, ResourceId: command.Input.ResourceId,
		NativeOperationId: "etcd-membership:" + command.CommandId, State: "etcd_" + p.Action,
	}}}, nil
}

// changeEtcdMembership applies one membership action and returns once its result holds.
//
// A learner has no name until it starts, so add_learner finds it by peer URL. promote and remove
// find the member by name. promote waits for the learner to join and catch up with the leader.
func changeEtcdMembership(ctx context.Context, cluster clientv3.Cluster, p *agentv1.EtcdMembership) error {
	for {
		members, err := cluster.MemberList(ctx)
		if err != nil {
			return err
		}
		switch p.Action {
		case "add_learner":
			for _, member := range members.Members {
				if slices.Contains(member.PeerURLs, p.PeerUrl) {
					if member.Name != "" && member.Name != p.Name {
						return fmt.Errorf("etcd peer %s belongs to member %s", p.PeerUrl, member.Name)
					}
					return nil
				}
			}
			_, err := cluster.MemberAddAsLearner(ctx, []string{p.PeerUrl})
			return err
		case "remove", "promote":
			index := slices.IndexFunc(members.Members, func(member *etcdserverpb.Member) bool { return member.Name == p.Name })
			switch {
			case index < 0 && p.Action == "remove":
				return nil
			case p.Action == "remove":
				_, err := cluster.MemberRemove(ctx, members.Members[index].ID)
				return err
			case index >= 0 && !members.Members[index].IsLearner:
				return nil
			case index >= 0:
				// A learner that has not caught up is refused. It is tried again until the timeout.
				if _, err := cluster.MemberPromote(ctx, members.Members[index].ID); err == nil {
					return nil
				}
			}
			// A started learner publishes its name once it joins, so an absent name is awaited too.
		default:
			return fmt.Errorf("etcd membership action %q is unknown", p.Action)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
