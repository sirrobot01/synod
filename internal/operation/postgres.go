package operation

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// validatePostgresConfiguration checks the credential identities of a PostgreSQL container.
// It binds exactly its superuser and application credentials.
func validatePostgresConfiguration(config *agentv1.PostgresContainerConfig, references []*agentv1.SecretReference) error {
	if config == nil || config.Port == 0 || config.Port > 65535 || config.ApplicationCredentialVersion < 1 || config.SuperuserCredentialVersion < 1 || config.ApplicationCredentialId == config.SuperuserCredentialId {
		return fmt.Errorf("postgres configuration is invalid")
	}
	bound := []*agentv1.SecretReference{
		{CredentialId: config.SuperuserCredentialId, Version: config.SuperuserCredentialVersion, Purpose: "superuser"},
		{CredentialId: config.ApplicationCredentialId, Version: config.ApplicationCredentialVersion, Purpose: "application"},
	}
	for _, reference := range bound {
		id, err := uuid.Parse(reference.CredentialId)
		if err != nil || id == uuid.Nil || id.String() != reference.CredentialId {
			return fmt.Errorf("postgres credential identity is invalid")
		}
	}
	if len(references) != len(bound) {
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
