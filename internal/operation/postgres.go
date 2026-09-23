package operation

import (
	"fmt"
	"net/netip"
	"slices"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func validatePostgresCloneVerification(command *agentv1.Command, config *agentv1.PostgresContainerConfig, cloneID string) error {
	if config == nil || config.ReplicaSource == nil || config.ReplicaSource.ResourceId == command.Input.ResourceId || len(command.Input.Secrets) != 0 {
		return fmt.Errorf("postgres clone verification input is invalid")
	}
	id, err := uuid.Parse(cloneID)
	if err != nil || id == uuid.Nil || id.String() != cloneID || cloneID == command.CommandId {
		return fmt.Errorf("postgres original clone command is invalid")
	}
	// Verification needs configuration identities, but delivers no secret material.
	return validatePostgresConfiguration(config, []*agentv1.SecretReference{
		{CredentialId: config.ApplicationCredentialId, Version: config.ApplicationCredentialVersion, Purpose: "application"},
		{CredentialId: config.ReplicationCredentialId, Version: config.ReplicationCredentialVersion, Purpose: "replication"},
	})
}

func validatePostgresConfiguration(config *agentv1.PostgresContainerConfig, references []*agentv1.SecretReference) error {
	if config == nil || config.Port == 0 || config.Port > 65535 || config.ApplicationCredentialVersion < 1 || config.SuperuserCredentialVersion < 1 || config.ReplicationCredentialVersion < 1 || config.ApplicationCredentialId == config.SuperuserCredentialId || config.ReplicationCredentialId == config.ApplicationCredentialId || config.ReplicationCredentialId == config.SuperuserCredentialId {
		return fmt.Errorf("postgres configuration is invalid")
	}
	for _, value := range []string{config.ApplicationCredentialId, config.SuperuserCredentialId, config.ReplicationCredentialId} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return fmt.Errorf("postgres credential identity is invalid")
		}
	}
	count := 3
	if native := config.Patroni; native != nil {
		if config.ReplicaSource != nil || len(native.Credentials) != 8 || len(references) != 8 {
			return fmt.Errorf("Patroni requires eight bound credentials and no standalone replica source")
		}
		purposes := []string{"superuser", "application", "replication", "patroni_rewind", "patroni_dcs", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"}
		identities, used := make(map[string]bool), make(map[string]bool)
		for _, reference := range native.Credentials {
			if reference == nil || !slices.Contains(purposes, reference.Purpose) || used[reference.Purpose] || identities[reference.CredentialId] || reference.Version < 1 {
				return fmt.Errorf("Patroni credential binding is invalid")
			}
			id, err := uuid.Parse(reference.CredentialId)
			if err != nil || id == uuid.Nil || id.String() != reference.CredentialId || !slices.ContainsFunc(references, func(candidate *agentv1.SecretReference) bool { return proto.Equal(reference, candidate) }) {
				return fmt.Errorf("Patroni credential identity does not match command input")
			}
			identities[reference.CredentialId], used[reference.Purpose] = true, true
		}
		count = 8
	}
	if source := config.ReplicaSource; source != nil {
		address, err := netip.ParseAddr(source.Address)
		id, idErr := uuid.Parse(source.ResourceId)
		if err != nil || address.String() != source.Address || address.Zone() != "" || (!address.IsGlobalUnicast() && !address.IsLoopback()) || source.Port == 0 || source.Port > 65535 || idErr != nil || id == uuid.Nil || id.String() != source.ResourceId {
			return fmt.Errorf("postgres replica source is invalid")
		}
		count = 2
	}
	if len(references) != count || (config.ReplicaSource == nil && !slices.ContainsFunc(references, func(reference *agentv1.SecretReference) bool {
		return reference != nil && reference.Purpose == "superuser" && reference.CredentialId == config.SuperuserCredentialId && reference.Version == config.SuperuserCredentialVersion
	})) || !slices.ContainsFunc(references, func(reference *agentv1.SecretReference) bool {
		return reference != nil && reference.Purpose == "application" && reference.CredentialId == config.ApplicationCredentialId && reference.Version == config.ApplicationCredentialVersion
	}) || !slices.ContainsFunc(references, func(reference *agentv1.SecretReference) bool {
		return reference != nil && reference.Purpose == "replication" && reference.CredentialId == config.ReplicationCredentialId && reference.Version == config.ReplicationCredentialVersion
	}) {
		return fmt.Errorf("postgres requires matching credentials for its role")
	}
	return nil
}
