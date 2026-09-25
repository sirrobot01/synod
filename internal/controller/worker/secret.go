package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ErrSecretUnresolvable reports a plan naming a credential this controller cannot
// open. The command is not sent: an engine with no credential either refuses every
// client or accepts every client.
var ErrSecretUnresolvable = errors.New("secret_unresolvable")

// pushSecrets sends the credentials one command names, before the command itself. The
// material travels on the same stream and is never part of the command, which keeps it
// out of the plan, the stored command bytes, the input hash, and the agent's journal.
//
// It is sent every time rather than once per session: tracking what a remote process
// still holds in memory is a guess that fails silently the one time it is wrong.
func (w *Worker) pushSecrets(ctx context.Context, nodeID uuid.UUID, sessionID string, command *agentv1.Command) error {
	if len(command.Input.Secrets) == 0 {
		return nil
	}
	if w.config.Keyring == nil {
		return fmt.Errorf("%w: this controller holds no keyring", ErrSecretUnresolvable)
	}
	clusterID, err := uuid.Parse(command.Epoch.ClusterId)
	if err != nil {
		return err
	}
	for _, reference := range command.Input.Secrets {
		id, err := uuid.Parse(reference.CredentialId)
		if err != nil || id.String() != reference.CredentialId {
			return ErrSecretUnresolvable
		}
		credential, err := w.config.Store.ReadCredentialVersion(ctx, w.config.Claim.InstallationID, clusterID, id, reference.Version, reference.Purpose)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrSecretUnresolvable, reference.Purpose)
		}
		if credential.MemberID != uuid.Nil && credential.MemberID.String() != command.Input.ResourceId {
			// A member credential also serves a resource the member's reservation owns on this node.
			resource, err := uuid.Parse(command.Input.ResourceId)
			if err != nil {
				return ErrSecretUnresolvable
			}
			owned, err := w.config.Store.MemberOwnsResource(ctx, w.config.Claim.InstallationID, clusterID, credential.MemberID, resource, nodeID)
			if err != nil || !owned {
				return fmt.Errorf("%w: member identity mismatch", ErrSecretUnresolvable)
			}
		}
		binding := secret.Binding{
			InstallationID: w.config.Claim.InstallationID, OwnerType: "cluster", OwnerID: clusterID,
			CredentialID: credential.CredentialID, Version: credential.Version,
		}
		material, err := w.config.Keyring.Open(binding, credential.Envelope)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrSecretUnresolvable, reference.Purpose)
		}
		message := &agentv1.ControllerMessage{Message: &agentv1.ControllerMessage_Secret{Secret: &agentv1.SecretMaterial{
			CredentialId: credential.CredentialID.String(), Version: credential.Version,
			Purpose: credential.Purpose, Material: material,
		}}}
		if err := w.config.Transport.Send(nodeID, sessionID, message); err != nil {
			return err
		}
	}
	return nil
}
