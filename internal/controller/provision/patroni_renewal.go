package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	enginepostgres "github.com/sirrobot01/synod/internal/engine/postgres"
	"github.com/sirrobot01/synod/internal/operation"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// renewCertificates issues new versions of the member and DCS certificates and recreates each
// member container with them.
//
// A running cluster renews one member at a time. The two other members prove readiness first,
// so the cluster keeps a writer while one member restarts. A stopped cluster recreates its
// members without starting them.
func (p *Planner) renewCertificates(ctx context.Context, claim operation.Claim, op operation.Operation) error {
	native := p.config.Patroni
	if native == nil {
		return errors.New("Patroni HA is not configured on this controller")
	}
	// An installed plan holds the renewed versions. A repeat must not issue others.
	if op.PlanVersion != 0 {
		return nil
	}
	found, err := p.config.Store.GetCluster(ctx, claim.InstallationID, claim.ClusterID)
	if err != nil {
		return err
	}
	if found.Spec.Engine != cluster.PostgreSQL || found.Spec.PostgreSQL == nil || found.Spec.PostgreSQL.Topology != "patroni" {
		return ErrUnplannable
	}
	running, err := p.members(ctx, claim)
	if err != nil {
		return err
	}
	if len(running) != patroniMembers {
		return ErrUnplannable
	}
	for _, each := range running {
		if each.Role != "patroni" || each.State != "active" {
			return fmt.Errorf("%w: member %s is %s %s", ErrUnplannable, each.MemberID, each.State, each.Role)
		}
	}
	now := time.Now()
	intent := postgres.ClusterIntent{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}
	// seal records the next version of one certificate credential.
	seal := func(reference *agentv1.SecretReference, member uuid.UUID, material enginepostgres.PatroniTLSMaterial) (*agentv1.SecretReference, error) {
		id, err := uuid.Parse(reference.CredentialId)
		if err != nil {
			return nil, err
		}
		latest, err := p.config.Store.ReadLatestCredentialVersion(ctx, claim.InstallationID, claim.ClusterID, id)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(material)
		if err != nil {
			return nil, err
		}
		expires, err := material.Expiry()
		if err != nil {
			return nil, err
		}
		credential := postgres.Credential{CredentialID: id, MemberID: member, Version: latest + 1, Purpose: reference.Purpose, ExpiresAt: expires}
		credential.Envelope, err = p.config.Keyring.Seal(secret.Binding{
			InstallationID: claim.InstallationID, OwnerType: "cluster", OwnerID: claim.ClusterID,
			CredentialID: credential.CredentialID, Version: credential.Version,
		}, encoded)
		if err != nil {
			return nil, err
		}
		intent.Credentials = append(intent.Credentials, credential)
		return &agentv1.SecretReference{CredentialId: reference.CredentialId, Version: credential.Version, Purpose: reference.Purpose}, nil
	}
	stored := make([]*agentv1.EnsureContainer, len(running))
	next := make([]*agentv1.EnsureContainer, len(running))
	addresses := make([]netip.Addr, len(running))
	for index, each := range running {
		container, err := p.config.Store.ReadMemberContainer(ctx, claim.InstallationID, claim.ClusterID, each.ResourceID)
		if err != nil {
			return err
		}
		if addresses[index], err = netip.ParseAddr(container.GetPostgres().GetPatroni().GetAddress()); err != nil {
			return fmt.Errorf("%w: member %s stores no Patroni address", ErrUnplannable, each.MemberID)
		}
		stored[index] = container
	}
	var dcs *agentv1.SecretReference
	for index, each := range running {
		container, address := stored[index], addresses[index]
		endpoints, err := patroniEndpoints(found, address, addresses)
		if err != nil {
			return err
		}
		renewed := proto.Clone(container).(*agentv1.EnsureContainer)
		for position, reference := range renewed.Postgres.Patroni.Credentials {
			switch reference.Purpose {
			case postgres.PatroniMemberTLSCredential:
				material, err := enginepostgres.IssuePatroniMemberMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID, ResourceID: each.ResourceID}, address, now, patroniLeafLifetime, endpoints...)
				if err != nil {
					return err
				}
				if renewed.Postgres.Patroni.Credentials[position], err = seal(reference, each.ResourceID, material); err != nil {
					return err
				}
			case postgres.PatroniDCSTLSCredential:
				// The DCS client certificate is one credential for the cluster.
				if dcs == nil {
					material, err := enginepostgres.IssuePatroniDCSMaterial(native.CA, pki.PatroniIdentity{InstallationID: claim.InstallationID, ClusterID: claim.ClusterID}, native.EtcdTrust, now, patroniLeafLifetime)
					if err != nil {
						return err
					}
					if dcs, err = seal(reference, uuid.Nil, material); err != nil {
						return err
					}
				}
				if reference.CredentialId != dcs.CredentialId {
					return fmt.Errorf("%w: members hold different DCS credentials", ErrUnplannable)
				}
				renewed.Postgres.Patroni.Credentials[position] = proto.Clone(dcs).(*agentv1.SecretReference)
			}
		}
		next[index] = renewed
	}
	recreate := func(index int, after []string) (*agentv1.PlanStep, error) {
		each := running[index]
		return p.step(claim, epochFor(claim, each.NodeID), op.DesiredRevision, &agentv1.CommandInput{
			ResourceId: each.ResourceID.String(), Secrets: next[index].Postgres.Patroni.Credentials,
			Payload: &agentv1.CommandInput_RecreatePatroniMember{RecreatePatroniMember: &agentv1.RecreatePatroniMember{Previous: stored[index], Next: next[index]}},
		}, agentv1.Postcondition_POSTCONDITION_PATRONI_MEMBER_RECREATED, after, beforeExecution)
	}
	var steps []*agentv1.PlanStep
	if found.DesiredLifecycle == cluster.DesiredStopped {
		// Stopped members have no writer to keep. Each is recreated and stays stopped. The steps form
		// one chain, so the route steps that follow come after every member's step on its node.
		var previous []string
		for index := range running {
			step, err := recreate(index, previous)
			if err != nil {
				return err
			}
			steps, previous = append(steps, step), []string{step.Command.StepId}
		}
	} else {
		application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
		if err != nil {
			return err
		}
		var previous []string
		for index, each := range running {
			var peers []string
			for other := range running {
				if other == index {
					continue
				}
				// A renewal keeps the address and port that a readiness check reads.
				check, err := p.patroniReadiness(claim, op, application, running[other], stored[other], "any", previous)
				if err != nil {
					return err
				}
				steps, peers = append(steps, check), append(peers, check.Command.StepId)
			}
			epoch := epochFor(claim, each.NodeID)
			stop, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: each.ResourceID.String(),
				Payload:    &agentv1.CommandInput_StopContainer{StopContainer: &agentv1.StopContainer{GracePeriod: durationpb.New(p.config.StopGrace)}},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_STOPPED, peers, agentv1.CancellationBoundary_CANCELLATION_BOUNDARY_SAFE_STOP)
			if err != nil {
				return err
			}
			renewed, err := recreate(index, []string{stop.Command.StepId})
			if err != nil {
				return err
			}
			start, err := p.step(claim, epoch, op.DesiredRevision, &agentv1.CommandInput{
				ResourceId: each.ResourceID.String(),
				Payload:    &agentv1.CommandInput_StartPatroniMember{StartPatroniMember: &agentv1.StartPatroniMember{Container: next[index]}},
			}, agentv1.Postcondition_POSTCONDITION_CONTAINER_RUNNING, []string{renewed.Command.StepId}, beforeExecution)
			if err != nil {
				return err
			}
			ready, err := p.patroniReadiness(claim, op, application, each, next[index], "any", []string{start.Command.StepId})
			if err != nil {
				return err
			}
			steps = append(steps, stop, renewed, start, ready)
			previous = []string{ready.Command.StepId}
		}
		// A final check of every member proves one writer after the last restart. The checks form one
		// chain, so the route steps that follow come after every check on its node.
		for index, each := range running {
			check, err := p.patroniReadiness(claim, op, application, each, next[index], "any", previous)
			if err != nil {
				return err
			}
			steps, previous = append(steps, check), []string{check.Command.StepId}
		}
	}
	// Each route presents its member's certificate, so each member's pooler and router are
	// replaced with the renewed one, one member at a time. A running cluster checks each router.
	if routed(running) {
		application, err := p.config.Store.ReadClusterCredential(ctx, claim.InstallationID, claim.ClusterID, postgres.ApplicationCredential)
		if err != nil {
			return err
		}
		routes := make([]routeMember, len(running))
		for index, each := range running {
			if routes[index], err = routeMemberFrom(each, next[index]); err != nil {
				return err
			}
		}
		start := found.DesiredLifecycle != cluster.DesiredStopped
		var tail []*agentv1.PlanStep
		for index, each := range routes {
			pooler, router := p.routeConfigs(routes, index, application)
			for _, route := range []struct {
				kind      string
				container *agentv1.EnsureContainer
			}{{"pooler", pooler}, {"router", router}} {
				replaced, err := p.routeReplace(claim, op, each.node, routeID(each.resource, route.kind), route.container, start)
				if err != nil {
					return err
				}
				tail = append(tail, replaced...)
			}
			if start {
				checks, err := p.routeCheckSteps(claim, op, routes[index:index+1], application, nil)
				if err != nil {
					return err
				}
				tail = append(tail, checks...)
			}
		}
		steps = append(steps, chainAfter(steps[len(steps)-1].Command.StepId, tail)...)
	}
	plan := &agentv1.ExecutionPlan{Version: 1, Steps: steps}
	if err := operation.ValidatePlan(plan, op); err != nil {
		return err
	}
	return p.config.Store.InstallRenewalPlan(ctx, claim, plan, intent)
}
