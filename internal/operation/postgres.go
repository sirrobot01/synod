package operation

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

// validatePostgresConfiguration checks the credential identities of a PostgreSQL container.
// Standalone binds its superuser and application credentials. Only Patroni replicates,
// so only Patroni binds a replication credential and its native credentials.
func validatePostgresConfiguration(config *agentv1.PostgresContainerConfig, references []*agentv1.SecretReference) error {
	if config == nil || config.Port == 0 || config.Port > 65535 || config.ApplicationCredentialVersion < 1 || config.SuperuserCredentialVersion < 1 || config.ApplicationCredentialId == config.SuperuserCredentialId {
		return fmt.Errorf("postgres configuration is invalid")
	}
	bound := []*agentv1.SecretReference{
		{CredentialId: config.SuperuserCredentialId, Version: config.SuperuserCredentialVersion, Purpose: "superuser"},
		{CredentialId: config.ApplicationCredentialId, Version: config.ApplicationCredentialVersion, Purpose: "application"},
	}
	native := config.Patroni
	if native == nil {
		if config.ReplicationCredentialId != "" || config.ReplicationCredentialVersion != 0 || config.SynchronousReplication {
			return fmt.Errorf("standalone postgres has no replication settings")
		}
	} else {
		if config.ReplicationCredentialVersion < 1 || config.ReplicationCredentialId == config.ApplicationCredentialId || config.ReplicationCredentialId == config.SuperuserCredentialId {
			return fmt.Errorf("postgres configuration is invalid")
		}
		bound = append(bound, &agentv1.SecretReference{CredentialId: config.ReplicationCredentialId, Version: config.ReplicationCredentialVersion, Purpose: "replication"})
	}
	for _, reference := range bound {
		id, err := uuid.Parse(reference.CredentialId)
		if err != nil || id == uuid.Nil || id.String() != reference.CredentialId {
			return fmt.Errorf("postgres credential identity is invalid")
		}
	}
	count := len(bound)
	if native != nil {
		if len(native.Credentials) != 7 || len(references) != 7 {
			return fmt.Errorf("Patroni requires seven bound credentials")
		}
		purposes := []string{"superuser", "application", "replication", "patroni_rewind", "patroni_rest", "patroni_member_tls", "patroni_dcs_tls"}
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
		count = 7
	}
	if len(references) != count {
		return fmt.Errorf("postgres requires matching credentials for its role")
	}
	for _, reference := range bound {
		if !slices.ContainsFunc(references, func(candidate *agentv1.SecretReference) bool {
			return candidate != nil && candidate.Purpose == reference.Purpose && candidate.CredentialId == reference.CredentialId && candidate.Version == reference.Version
		}) {
			return fmt.Errorf("postgres requires matching credentials for its role")
		}
	}
	return nil
}
