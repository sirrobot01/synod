package docker

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"strconv"
	"strings"

	"github.com/sirrobot01/synod/internal/engine/postgres"
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
		if !ok || len(material) == 0 {
			return nil, runtime.ErrConfigurationConflict
		}
		seen[wanted.Name] = true
		if p.Redis != nil && wanted.Name == "REDIS_REPLICATION_PASSWORD" {
			for _, value := range material {
				if value < ' ' || value == 0x7f {
					return nil, runtime.ErrConfigurationConflict
				}
			}
		}
		if p.Postgres != nil && wanted.Name == "SYNOD_REPL_PASSWORD" && (len(material) > 1024 || slices.ContainsFunc(material, func(b byte) bool { return b < ' ' || b > '~' })) {
			return nil, runtime.ErrConfigurationConflict
		}
		if p.Postgres != nil && (wanted.Name == "SYNOD_APP_VERIFIER" || wanted.Name == "SYNOD_REPL_VERIFIER") {
			for _, value := range material {
				if value < ' ' || value > '~' {
					return nil, runtime.ErrConfigurationConflict
				}
			}
			identity, version, purpose := p.Postgres.ApplicationCredentialId, p.Postgres.ApplicationCredentialVersion, "application"
			if wanted.Name == "SYNOD_REPL_VERIFIER" {
				identity, version, purpose = p.Postgres.ReplicationCredentialId, p.Postgres.ReplicationCredentialVersion, "replication"
			}
			verifier, err := postgresVerifier(material, identity, version, purpose)
			if err != nil {
				return nil, err
			}
			environment = append(environment, wanted.Name+"="+verifier)
			continue
		}
		environment = append(environment, wanted.Name+"="+string(material))
	}
	return environment, nil
}

// plainEnvironment adds the non-secret variables an engine image reads at first start.
// It is applied where the options are built, not where the credentials are, so a
// verification rebuild can reproduce them and detect a value changed by hand.
func plainEnvironment(imageEnv []string, p *agentv1.EnsureContainer) ([]string, error) {
	if p.Postgres != nil {
		if len(p.PlainEnv) != 0 {
			return nil, runtime.ErrUnsupportedConfiguration
		}
		environment := slices.Clone(imageEnv)
		for _, entry := range []string{"POSTGRES_USER=synod_admin", "POSTGRES_DB=app", "PGPORT=" + strconv.FormatUint(uint64(p.Postgres.Port), 10)} {
			name, _, _ := strings.Cut(entry, "=")
			if shadowsImage(environment, name) {
				return nil, runtime.ErrConfigurationConflict
			}
			environment = append(environment, entry)
		}
		foundData := false
		for _, entry := range environment {
			if value, ok := strings.CutPrefix(entry, "PGDATA="); ok {
				if foundData || value != postgres.DataDirectory {
					return nil, runtime.ErrUnsupportedConfiguration
				}
				foundData = true
			}
		}
		if !foundData {
			return nil, runtime.ErrUnsupportedConfiguration
		}
		return environment, nil
	}
	if len(p.PlainEnv) == 0 {
		return imageEnv, nil
	}
	if len(p.PlainEnv) > maxSecretEnv {
		return nil, runtime.ErrUnsupportedConfiguration
	}
	environment := slices.Clone(imageEnv)
	seen := make(map[string]bool, len(p.PlainEnv))
	for _, plain := range p.PlainEnv {
		if plain == nil || !validEnvName(plain.Name) || seen[plain.Name] || !validEnvValue(plain.Value) {
			return nil, runtime.ErrUnsupportedConfiguration
		}
		if shadowsImage(imageEnv, plain.Name) {
			return nil, runtime.ErrConfigurationConflict
		}
		seen[plain.Name] = true
		environment = append(environment, plain.Name+"="+plain.Value)
	}
	return environment, nil
}

// shadowsImage reports a variable the image already sets.
func shadowsImage(imageEnv []string, name string) bool {
	return slices.ContainsFunc(imageEnv, func(entry string) bool {
		found, _, _ := strings.Cut(entry, "=")
		return found == name
	})
}

// validEnvValue refuses a control character, which would end the assignment somewhere
// other than where it looks like it ends.
func validEnvValue(value string) bool {
	if value == "" || len(value) > 1024 {
		return false
	}
	return !strings.ContainsFunc(value, func(r rune) bool { return r < ' ' || r == 0x7f })
}

// maxSecretEnv bounds one container. An engine reads a handful of credentials.
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

// postgresVerifier binds a stable SCRAM salt to one declared credential version and purpose.
func postgresVerifier(material []byte, identity string, version int64, purpose string) (string, error) {
	salt := sha256.Sum256([]byte("synod-postgres-" + purpose + "-scram-v1\x00" + identity + "\x00" + strconv.FormatInt(version, 10)))
	salted, err := pbkdf2.Key(sha256.New, string(material), salt[:16], 4096, 32)
	if err != nil {
		return "", &runtime.Error{Kind: runtime.ErrConfigurationConflict, Cause: err}
	}
	client := hmac.New(sha256.New, salted)
	client.Write([]byte("Client Key"))
	clientKey := client.Sum(nil)
	storedKey := sha256.Sum256(clientKey)
	server := hmac.New(sha256.New, salted)
	server.Write([]byte("Server Key"))
	serverKey := server.Sum(nil)
	verifier := "SCRAM-SHA-256$4096:" + base64.StdEncoding.EncodeToString(salt[:16]) + "$" + base64.StdEncoding.EncodeToString(storedKey[:]) + ":" + base64.StdEncoding.EncodeToString(serverKey)
	clear(salted)
	clear(clientKey)
	clear(serverKey)
	return verifier, nil
}
