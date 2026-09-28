package docker

import (
	"slices"
	"strings"

	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// secretEnvironment adds the credential-backed variables. The names come from the
// command; the values come from the caller's argument and are never part of any
// message, so nothing that serializes a command can capture one.
//
// A named variable with no resolved credential is refused rather than left empty.
func secretEnvironment(imageEnv []string, p *agentv1.EnsureContainer, secrets runtime.Secrets) ([]string, error) {
	if len(p.SecretEnv) == 0 {
		return imageEnv, nil
	}
	if len(p.SecretEnv) > maxSecretEnv {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	environment := slices.Clone(imageEnv)
	seen := make(map[string]bool, len(p.SecretEnv))
	for _, wanted := range p.SecretEnv {
		if wanted == nil || !validEnvName(wanted.Name) || wanted.Purpose == "" || seen[wanted.Name] {
			return nil, runtime.ErrUnsupportedConfiguration
		}
		// A second assignment leaves which one wins to the container runtime.
		if shadowsImage(imageEnv, wanted.Name) {
			return nil, runtime.ErrConfigurationConflict
		}
		material, ok := secrets[wanted.Purpose]
		// Engine start scripts write credentials into configuration files, where a
		// control byte would end the entry early.
		if !ok || len(material) == 0 || slices.ContainsFunc(material, func(b byte) bool { return b < ' ' || b == 0x7f }) {
			return nil, runtime.ErrConfigurationConflict
		}
		seen[wanted.Name] = true
		environment = append(environment, wanted.Name+"="+string(material))
	}
	return environment, nil
}

// plainEnvironment adds the non-secret variables an engine image reads at first start.
// It is applied where the options are built, not where the credentials are, so a
// verification rebuild can reproduce them and detect a value changed by hand.
func plainEnvironment(imageEnv []string, p *agentv1.EnsureContainer) []string {
	environment := slices.Clone(imageEnv)
	for _, env := range p.PlainEnv {
		environment = slices.DeleteFunc(environment, func(entry string) bool { name, _, _ := strings.Cut(entry, "="); return name == env.Name })
		environment = append(environment, env.Name+"="+env.Value)
	}
	return environment
}

// shadowsImage reports a variable the image already sets.
func shadowsImage(imageEnv []string, name string) bool {
	return slices.ContainsFunc(imageEnv, func(entry string) bool {
		found, _, _ := strings.Cut(entry, "=")
		return found == name
	})
}

// maxSecretEnv bounds one container. A database copy reads a handful of credentials.
const maxSecretEnv = 8

// validEnvName refuses a name that would change what the assignment means.
func validEnvName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// sameEnvironment compares a live container against a rebuild that holds no
// credentials. The image's own variables must match exactly and every
// credential-backed one must be present; their values are never compared or read.
func sameEnvironment(live, rebuilt []string, p *agentv1.EnsureContainer) bool {
	secretNames := make([]string, 0, len(p.SecretEnv))
	for _, wanted := range p.SecretEnv {
		if wanted != nil {
			secretNames = append(secretNames, wanted.Name)
		}
	}
	var plain []string
	found := make(map[string]bool, len(secretNames))
	for _, entry := range live {
		name, _, _ := strings.Cut(entry, "=")
		if slices.Contains(secretNames, name) {
			if found[name] {
				return false
			}
			found[name] = true
			continue
		}
		plain = append(plain, entry)
	}
	if len(found) != len(secretNames) {
		return false
	}
	return slices.Equal(plain, rebuilt)
}
