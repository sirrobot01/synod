package provision

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/app"
	"github.com/sirrobot01/synod/internal/secret"
	"github.com/sirrobot01/synod/internal/store/postgres"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// DatabaseImage pins one multi-architecture engine image for tier 1 apps.
type DatabaseImage struct {
	Engine  string
	Version string
	Digest  string
}

// databaseImage answers the pinned image for an engine and a requested version.
// A major version such as "17" matches the pinned release "17.11".
func (p *Planner) databaseImage(spec *app.DatabaseSpec) (DatabaseImage, error) {
	for _, image := range p.config.DatabaseImages {
		if image.Engine == spec.Engine && (image.Version == spec.Version || strings.HasPrefix(image.Version, spec.Version+".")) {
			return image, nil
		}
	}
	return DatabaseImage{}, fmt.Errorf("%w: %s %s", ErrNoImage, spec.Engine, spec.Version)
}

// Container ports and roles of the tier 1 engines.
const (
	postgresPort    = 5432
	redisPort       = 6379
	replicationRole = "synod_replication"
	applicationRole = "app"
)

// databaseService is the container service of a tier 1 app. The catalog supplies it.
func (p *Planner) databaseService(spec app.AppSpec) (app.ServiceSpec, error) {
	image, err := p.databaseImage(spec.Database)
	if err != nil {
		return app.ServiceSpec{}, err
	}
	port := uint16(postgresPort)
	if spec.Database.Engine == app.EngineRedis {
		port = redisPort
	}
	return app.ServiceSpec{Image: image.Digest, Ports: []app.PortSpec{{ContainerPort: port, Protocol: "tcp"}},
		Resources: app.ResourceSpec{CPUMillis: p.config.Budget.CPUMillis, MemoryBytes: p.config.Budget.MemoryBytes, DiskBytes: p.config.Budget.DiskBytes}}, nil
}

// databaseCredentials returns the sealed credentials of a tier 1 app. It creates the
// missing ones and records them before any command that needs them is sent.
func (p *Planner) databaseCredentials(ctx context.Context, found app.App) ([]postgres.Credential, error) {
	if p.config.Keyring == nil {
		return nil, fmt.Errorf("tier 1 apps need a credential keyring")
	}
	roles := map[string]string{postgres.SuperuserCredential: superuserRole, postgres.ReplicationCredential: replicationRole}
	if found.Spec.Database.Engine == app.EnginePostgres {
		roles[postgres.ApplicationCredential] = applicationRole
	}
	var credentials []postgres.Credential
	for _, purpose := range []string{postgres.SuperuserCredential, postgres.ReplicationCredential, postgres.ApplicationCredential} {
		role, ok := roles[purpose]
		if !ok {
			continue
		}
		existing, err := p.config.Store.ReadAppCredential(ctx, found.InstallationID, found.ID, purpose)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			credentials = append(credentials, *existing)
			continue
		}
		password, err := secret.NewPassword()
		if err != nil {
			return nil, err
		}
		credential := postgres.Credential{CredentialID: uuid.New(), Version: 1, Purpose: purpose, RoleName: role}
		binding := secret.Binding{InstallationID: found.InstallationID, OwnerType: "app", OwnerID: found.ID, CredentialID: credential.CredentialID, Version: credential.Version}
		if credential.Envelope, err = p.config.Keyring.Seal(binding, []byte(password)); err != nil {
			return nil, err
		}
		credentials = append(credentials, credential)
	}
	if err := p.config.Store.RecordAppCredentials(ctx, found.InstallationID, found.ID, credentials); err != nil {
		return nil, err
	}
	return credentials, nil
}

// postgresStartup copies the TLS files to a private path the server user owns, then
// starts PostgreSQL with logical replication and TLS.
const postgresStartup = `set -e
mkdir -p /tls
cp /tls-src/server.crt /tls-src/server.key /tls-src/ca.crt /tls/
chown -R postgres:postgres /tls
chmod 600 /tls/server.key
exec docker-entrypoint.sh postgres -c listen_addresses=* -c wal_level=logical -c ssl=on -c ssl_cert_file=/tls/server.crt -c ssl_key_file=/tls/server.key -c ssl_ca_file=/tls/ca.crt "$@"`

// redisStartup writes the ACL users and the replication login from the environment into
// private files, so no password appears in the server's arguments.
const redisStartup = `set -e
umask 077
mkdir -p /tls /run/synod-redis
cp /tls-src/server.crt /tls-src/server.key /tls-src/ca.crt /tls/
printf 'user default on >%s ~* &* +@all\nuser synod_replication on >%s +psync +replconf +ping\n' "$REDIS_ADMIN_PASSWORD" "$REDIS_REPLICATION_PASSWORD" > /tls/users.acl
printf 'masteruser synod_replication\nmasterauth %s\n' "$REDIS_REPLICATION_PASSWORD" > /tls/redis.conf
unset REDIS_ADMIN_PASSWORD REDIS_REPLICATION_PASSWORD
chown -R redis /tls /run/synod-redis /data
exec docker-entrypoint.sh redis-server /tls/redis.conf --port 0 --tls-port 6379 --tls-cert-file /tls/server.crt --tls-key-file /tls/server.key --tls-ca-cert-file /tls/ca.crt --tls-replication yes --tls-auth-clients optional --appendonly yes --aclfile /tls/users.acl --unixsocket /run/synod-redis/redis.sock --unixsocketperm 700`

// databaseContainer builds one tier 1 copy. Every copy runs the same container; the
// agent supervisor gives it its role. Credentials travel as secret references.
func databaseContainer(spec app.AppSpec, service app.ServiceSpec, copyIndex uint32, hostAddress string, hostPort uint16, credentials []postgres.Credential) (*agentv1.EnsureContainer, []*agentv1.SecretReference) {
	generic := &agentv1.GenericContainerConfig{Image: service.Image, AppName: spec.Name, CopyIndex: copyIndex, CommandOverride: true, EntrypointOverride: true, Entrypoint: []string{"bash", "-c"},
		HealthCheck: &agentv1.HealthCheckConfig{Type: "tcp", Port: uint32(service.Ports[0].ContainerPort), IntervalSeconds: 5, TimeoutSeconds: 3, UnhealthyThreshold: 3}}
	container := &agentv1.EnsureContainer{ImageDigest: service.Image, Generic: generic, NetworkMode: agentv1.NetworkMode_NETWORK_MODE_BRIDGE, MemoryBytes: service.Resources.MemoryBytes, CpuMillis: service.Resources.CPUMillis,
		Ports: []*agentv1.PortBinding{{ContainerPort: uint32(service.Ports[0].ContainerPort), HostPort: uint32(hostPort), HostAddress: hostAddress, Transport: "tcp"}}}
	purposes := map[string]string{}
	if spec.Database.Engine == app.EnginePostgres {
		// Slots and senders cover one slot per standby and database, plus table sync slots.
		// Workers cover the apply and sync workers when this copy is a standby.
		databases := len(spec.Database.Databases)
		senders := strconv.Itoa(10 + 4*max(spec.Replication.Standbys, 1)*databases)
		workers := strconv.Itoa(4 + 3*databases)
		generic.Command = []string{postgresStartup, "synod-postgres", "-c", "max_replication_slots=" + senders, "-c", "max_wal_senders=" + senders,
			"-c", "max_logical_replication_workers=" + workers, "-c", "max_worker_processes=" + strconv.Itoa(8+3*databases)}
		generic.Mounts = []*agentv1.Mount{{Type: "engine", Source: "data", Target: "/var/lib/postgresql/data"}, {Type: "engine", Source: "tls", Target: "/tls-src", ReadOnly: true}}
		container.PlainEnv = []*agentv1.PlainEnv{{Name: "POSTGRES_INITDB_ARGS", Value: "--auth-local=trust --auth-host=scram-sha-256"}, {Name: "POSTGRES_USER", Value: superuserRole}}
		container.SecretEnv = []*agentv1.SecretEnv{{Name: "POSTGRES_PASSWORD", Purpose: postgres.SuperuserCredential}}
		purposes[postgres.SuperuserCredential] = ""
	} else {
		generic.Command = []string{redisStartup, "synod-redis"}
		generic.Mounts = []*agentv1.Mount{{Type: "engine", Source: "data", Target: "/data"}, {Type: "engine", Source: "tls", Target: "/tls-src", ReadOnly: true}}
		container.SecretEnv = []*agentv1.SecretEnv{{Name: "REDIS_ADMIN_PASSWORD", Purpose: postgres.SuperuserCredential}, {Name: "REDIS_REPLICATION_PASSWORD", Purpose: postgres.ReplicationCredential}}
		purposes[postgres.SuperuserCredential], purposes[postgres.ReplicationCredential] = "", ""
	}
	var references []*agentv1.SecretReference
	for _, credential := range credentials {
		if _, ok := purposes[credential.Purpose]; ok {
			references = append(references, &agentv1.SecretReference{CredentialId: credential.CredentialID.String(), Version: credential.Version, Purpose: credential.Purpose})
		}
	}
	return container, references
}
