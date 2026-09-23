package executor

import (
	"errors"

	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// ErrSecretUnavailable reports a command whose credential this session was never sent.
// Starting an engine with no credential produces a database that either refuses every
// client or accepts every client.
var ErrSecretUnavailable = errors.New("secret_unavailable")

// errSecretMessage is a malformed push: the controller sent something unusable.
var errSecretMessage = errors.New("invalid secret material")

// maxSessionSecrets bounds what one session holds.
const maxSessionSecrets = 64

// sessionSecret is one credential this session was sent.
type sessionSecret struct {
	credentialID string
	version      int64
	material     []byte
}

// acceptSecret takes one credential into session memory. It is never written down, so
// it ends with the session and a reconnect is sent it again.
func (e *Executor) acceptSecret(current *session, material *agentv1.SecretMaterial) error {
	if material == nil || material.CredentialId == "" || material.Version < 1 ||
		material.Purpose == "" || len(material.Material) == 0 {
		return errSecretMessage
	}
	if current.secrets == nil {
		current.secrets = make(map[string]sessionSecret, 4)
	}
	if _, held := current.secrets[material.Purpose]; !held && len(current.secrets) >= maxSessionSecrets {
		return errSecretMessage
	}
	current.secrets[material.Purpose] = sessionSecret{
		credentialID: material.CredentialId,
		version:      material.Version,
		material:     material.Material,
	}
	return nil
}

// resolve answers the credentials one command's references name.
//
// A reference must match by credential identity and version, not only by purpose: a
// rotation in flight means two versions exist, and the wrong one configures an engine
// with a credential nobody expects it to have.
func (e *Executor) resolve(command *agentv1.Command) (runtime.Secrets, error) {
	if len(command.Input.Secrets) == 0 {
		return nil, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.session
	if current == nil {
		return nil, ErrSecretUnavailable
	}
	resolved := make(runtime.Secrets, len(command.Input.Secrets))
	for _, reference := range command.Input.Secrets {
		held, ok := current.secrets[reference.Purpose]
		if !ok || held.credentialID != reference.CredentialId || held.version != reference.Version {
			return nil, ErrSecretUnavailable
		}
		resolved[reference.Purpose] = held.material
	}
	return resolved, nil
}
