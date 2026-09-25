# Synod

Synod is a self-hosted database control plane for ordinary Linux servers.
It provisions and operates dedicated PostgreSQL and Redis clusters without Kubernetes.
Applications use normal database protocols.

## Status

Synod is under active development. v0.1 is not released.
The required release includes distributed controllers and agents, native database HA, backups,
restore, PostgreSQL PITR and upgrades, Redis Cluster, an API, a CLI, and a minimal web UI.
A completed development milestone does not imply that the release is ready.

## Development

Use the Go version declared in go.mod.
Read [AGENTS.md](AGENTS.md) before making changes.

### Task commands

[Task](https://taskfile.dev) runs the local commands. Install it, then run `task` to
list them. Every command below also works without Task. The sections that follow give
the direct form.

| Command | Action |
| --- | --- |
| `task build` | Build the three binaries into `dist/` |
| `task build:web` | Build the browser application into `internal/web/dist` |
| `task check` | Vet the Go code and type-check the browser application |
| `task test` | Run the Go tests that need no dependency |
| `task test:all` | Run the whole Go suite with PostgreSQL, etcd, and race detection |
| `task test:web` | Run the browser application tests |
| `task dev` | Run the controller on `https://127.0.0.1:8443` |
| `task dev:web` | Serve the browser application with hot reload |
| `task dev:user -- alice` | Create the first operator account |
| `task cli -- db get ID` | Run the command line against the development controller |
| `task dev:reset` | Empty the development metadata database |
| `task dev:down` | Stop the development containers and keep their data |

A test command passes its arguments straight to `go test`, and those arguments replace
the package list. Pass the whole tail:

```sh
task test -- -run TestName ./internal/api/...
```

### Development controller in one command

`task dev` starts a local PostgreSQL container, writes development credentials to
`.dev/`, and runs the controller. The first run creates the credentials.

```sh
task dev                                    # in one terminal
task dev:user -- alice                      # in another; it prompts for a password
```

Then open `https://127.0.0.1:8443` and sign in. The certificate is self-signed, so the
browser warns once.

The credentials in `.dev/` are for one workstation. The certificate is self-signed and
the development database trusts any password. Never use them anywhere else. Git ignores
the directory.

This development command starts no worker. Worker dispatch needs the agent transport,
etcd, a credential wrapping key, and an approved node.
Use `synodd bootstrap --dir DIRECTORY` to create installation material.
Use `task dev:etcd` to start a local etcd node.

Run `task dev:reset` after you change a migration. AGENTS.md requires a dropped
development database, never a repair migration.

### Browser application with hot reload

`task dev:web` serves the application on `https://localhost:5173` and sends `/v1` to the
controller. Run `task dev` in another terminal first.

The development server uses the same certificate as the controller, because the session
cookie carries the `__Host-` prefix and a browser stores it only over HTTPS.

## Test fixtures

Two scripts start the dependencies that the tests need.
Docker must be available.
Each script uses a pinned image and a random localhost port.
Each script removes its container after the command exits.
Pass a command after the script name to run a focused check.

Run `./scripts/test-postgres.sh` for an isolated PostgreSQL database.
It exports `SYNOD_TEST_DATABASE_URL` for the test process.

Run `./scripts/test-etcd.sh` for an isolated etcd node.
It exports `SYNOD_TEST_ETCD_ENDPOINT` for the test process.
With no command it runs `./scripts/test-postgres.sh`, which starts both dependencies.

Run the whole suite with both dependencies:

```sh
./scripts/test-etcd.sh ./scripts/test-postgres.sh go test -race ./...
```

A test that needs a dependency skips when its variable is absent.

## Development controller

The current controller accepts standalone PostgreSQL and Redis requests.
It records desired state and a pending operation in PostgreSQL.
A configured worker provisions dedicated database containers on approved agents.
The API-only development command leaves operations pending.
The development version catalog contains PostgreSQL 17.11 and 18.6, and Redis 8.2.7.
These versions are local test candidates. They are not a qualified release matrix.

Build the controller with `go build -o dist/synodd ./cmd/synodd`.
Create a metadata PostgreSQL database before startup.
Use one stable installation UUID for all controllers in that installation.
Keep that UUID with the installation recovery records.
Use a trusted TLS certificate for the API address.
Store the TLS key and administrator token in files with mode 0600.
The token must contain 32 to 256 visible ASCII bytes.
One final line ending is permitted in its file.

Set the following environment variables:

```sh
export SYNOD_INSTALLATION_ID='<stable-uuid>'
export SYNOD_DATABASE_URL='postgres://synod@localhost/synod?sslmode=verify-full'
export SYNOD_ADMIN_TOKEN_FILE='/path/to/admin.token'
export SYNOD_TLS_CERT_FILE='/path/to/api.crt'
export SYNOD_TLS_KEY_FILE='/path/to/api.key'
./dist/synodd
```

The default API address is `127.0.0.1:8443`.
Use `--listen` to change it.
Use `--help` to list the settings.
Explicit flags override environment variables.
Environment variables override a JSON file supplied with `--config` or `SYNOD_CONFIG`.
The JSON keys use the names shown in [the controller configuration](internal/config/controller.go).
Use a protected file or environment for a database URL that contains a password.
Startup applies the metadata schema migrations.

See [the initial API schema](api/openapi.json).
Send the administrator token in `Authorization: Bearer <token>`.
Send one `Idempotency-Key` header with every create request.
Use 1 to 128 visible ASCII bytes for the key.
Keep that key and the same request body until the result is known.
A retry returns the original cluster and operation IDs.
A changed request with the same key returns HTTP 409.
An uncertain commit returns HTTP 503 with `outcome_unknown`.
Retry that result with the same key and body.
An accepted request returns HTTP 202 and an operation URL in `Location`.

Example request body for `POST /v1/clusters`:

```json
{
  "schema_version": 1,
  "name": "example",
  "engine": "postgres",
  "postgresql": {"version": "17.11", "topology": "standalone"}
}
```

Read `GET /v1/clusters/{id}` to inspect the cluster and its revision ETag.
Read `GET /v1/operations/{id}` to inspect its operation.
An operation with `plan_version: 0` has no executable plan.
An operation stays pending until a configured worker can produce and run its plan.

## Operator access

The controller authenticates three credentials.
Agents use mutual TLS. The command line uses an API token.
A browser uses a session cookie that a username and a password create.

The bootstrap token file creates the first account.
Keep it after that. It is the only credential that works when no account can sign in.

```sh
synod user create alice --password-file /path/to/password
synod token create "deploy pipeline"
```

`synod token create` prints the secret once. Store it then, because no later call repeats it.
A password never uses a flag, because a flag value reaches the process table.
Supply `--password-file` with a private file, or answer the prompt.

Set `SYNOD_PUBLIC_ORIGIN` to the browser origin of the API before you serve a web UI.
A session request that changes state must prove its origin.

## Running operations

The controller runs an operation only when you supply etcd endpoints and the agent transport.
Without them it admits requests and records desired state, and no worker starts.

```sh
export SYNOD_ETCD_ENDPOINTS='etcd.example.com:2379'
export SYNOD_ETCD_PREFIX='/synod'
export SYNOD_ETCD_CA_FILE='/path/to/etcd-ca.crt'
export SYNOD_ETCD_USERNAME='synod-controller'
export SYNOD_ETCD_PASSWORD_FILE='/path/to/etcd-password'
```

Use `--etcd-endpoints` and `--etcd-prefix` to override them.
Separate several endpoints with commas.
Use plain host and port pairs. Do not put credentials or URL schemes in an endpoint.
Every controller in one installation must use the same prefix.

Create the etcd account before controller startup. Enable etcd authentication.
Grant the account read and write access only to `/synod/<installation-uuid>/`.
Use your configured prefix in place of `/synod`.
Keep this account separate from Patroni accounts.
The controller does not create accounts or change etcd permissions.

Keep the password file private to its owner.
Use 32 to 256 visible ASCII bytes. One final LF or CRLF is permitted.
The controller verifies the server certificate against the CA bundle and endpoint name.
For mutual TLS, also set `SYNOD_ETCD_TLS_CERT_FILE` and `SYNOD_ETCD_TLS_KEY_FILE`.
The client certificate must be current and permit client authentication.
Keep its key file private to its owner.
These settings also have matching `--etcd-*` flags and JSON fields.
Restart the controller after you change the files or settings.

A local development etcd can use explicit plaintext mode.
Remove TLS and account settings before you select this mode.

```sh
export SYNOD_ETCD_ENDPOINTS='127.0.0.1:2379'
export SYNOD_ETCD_ALLOW_PLAINTEXT=true
```

This mode has no transport encryption or authentication. Startup logs identify it.
Secure mode refuses a server with authentication disabled.

One controller elects itself owner of a cluster, claims that cluster's operation,
and runs one worker for it.
A second controller waits for the election and takes over after the claim expires.
Shutdown stops management dispatch. It never stops native work that an agent accepted.

The controller produces standalone PostgreSQL and Redis plans when it has a credential
wrapping key and a complete placement profile. The approved node must match that profile.
Use `--secret-key-file`, `--secret-key-id`, and the `--placement-*` flags to configure them.
`--placement-architecture` accepts `amd64`, `arm64`, or both separated by a comma, in preference order.
Each cluster stays on nodes of one architecture, because PostgreSQL replication needs one architecture.
With both architectures, set the Patroni image to an image index digest that covers both.
Use `--engine-endpoint-first-port` and `--engine-endpoint-last-port` to set the reserved port range.

New Redis clusters have separate admin, application, and replication credentials.
The replication role can only run the native replication handshake commands and PING.
The compact Redis profile supports one native replica with RDB, AOF, or combined persistence.
It does not provide automatic failover.

```sh
synod db scale CLUSTER_ID --replicas 1 --wait
synod db members CLUSTER_ID
```

If an add fails or is cancelled, correct its cause and run the scale command again.
Use a new idempotency key after a terminal failure.
The new operation reports the prior operation ID in `retry_of`.
It keeps the existing member, volume, node, port, image, and credential versions.
A retry after a dispatched start requires existing storage.
A `storage_lost` result requires explicit recovery. The retry does not initialize replacement data.
This retry applies to the current failed add. An intervening lifecycle change ends this retry path.

For an uncertain response, keep the original key and expected revision.
That request returns the original operation.
Use `synod db scale CLUSTER_ID --replicas 0` to preview replica removal.
Review the plan before you confirm it. Removal retains the replica volume.

## Agent configuration

An agent reads one JSON file. Give it with `--config`.
The file must belong to the agent user. It must have no group or other permission.
Unknown fields are refused.

| Field | Required | Meaning |
| --- | --- | --- |
| `installation_id` | yes | The installation UUID. |
| `node_id` | yes | The node UUID from the enrollment token. |
| `enrollment_dir` | yes | The completed enrollment state directory. |
| `journal_dir` | yes | The journal directory. It must not be inside `enrollment_dir`. |
| `recovery_generation` | yes | The installation recovery generation UUID. Read it with `synod installation get`. |
| `controller_endpoints` | yes | One to eight controller agent addresses as `host:port`. |
| `storage_id` | yes | The storage pool identity. Use the same value as `synod node approve --storage-id`. |
| `storage_path` | no | The filesystem that capacity is measured on. The default is `journal_dir`. |
| `min_free_bytes` | no | The free space that new work must leave on `storage_path`. |
| `docker_socket` | no | The Docker socket path. The default is `/var/run/docker.sock`. |

Put `storage_path` on the filesystem that holds the Docker volumes.
Without `storage_id`, the agent cannot report capacity, so it refuses to start.

`synod installation get` shows `installation_id` and `recovery_generation`.
The recovery generation can change after controller recovery. Read it when you write a new configuration.
It is available after a controller with etcd and the agent transport starts.

Run `synod-agent journal init --config FILE` once after enrollment.
Then run `synod-agent run --config FILE`.

A journal or inventory refusal prints its cause in the `detail` field of the error.
Other errors print fixed text only, because an argument error can repeat raw input.

Before the first release, the agent protocol can change without a migration.
A new agent refuses a journal that holds commands from an older build.
It reports `journal_recovery_required`, and `detail` names the saved command it refused.
Upgrade an agent only when its node has no cluster. Then create a new journal directory.

## Find a database cluster

List one page of clusters:

```sh
synod db list --limit 50
synod db list --limit 50 --json
```

The `cluster` command is an alias for `db`.
Use `--cursor TOKEN` with the returned `next_cursor` to read the next page.
Use `--include-deleted` to include completed deletion records.
Keep the same deletion filter when you use a cursor.
Each command reads one page. The maximum page size is 200.

The list shows stored desired and observed states.
It does not run a live database probe.
Concurrent changes can affect later pages.
Start a new list without a cursor to see newer clusters.
The web **Clusters** screen uses the same public API.

## Database connections

Set the node's client address with `synod node approve --client-address ADDRESS`.
Supply the other required node policy flags with that command.
Use a canonical IPv4 or IPv6 address that clients can reach.
Private LAN addresses are valid. Loopback and wildcard addresses are refused.
A node with no client address can run work, but endpoint inspection reports that the address is missing.

After a create operation succeeds, inspect its connection metadata:

```sh
synod endpoint get CLUSTER_ID
synod credential list CLUSTER_ID
```

The endpoint command reports the reserved address and port.
It also reports the stored desired and observed lifecycle state.
It does not run a live connection probe.
The current direct standalone profile has one node and no database TLS.
The response reports `availability_profile: single_node` and `tls_mode: disabled`.
Router failover and secure database endpoints remain required release work.

Credential inspection returns names and versions. It returns no password.
Use an explicit reveal when a client needs the password:

```sh
synod credential reveal CLUSTER_ID --purpose application
```

Reveal prints a JSON response with a plaintext password.
Keep that output private. Do not copy it into logs or shared files.
The server records the actor and credential version before it returns the secret.
Both engines use `synod_app` for application access.
The PostgreSQL application role owns the `app` database.
It can create tables and manage data in that database.
It cannot create roles, create other databases, or read server files.
Use `--purpose superuser` for explicit administrative access.
PostgreSQL verifies its stored setup before each start.
It refuses to replace a missing application database after setup has completed.
A confirmed setup failure stays failed across container restarts.
Ordinary start does not clear that failure.
Recovery for that state is not yet available in the CLI.

Use the reported host, port, database, and username with a native database client.
Supply its password through the client's protected input mechanism.
Do not put the password in a command-line URL.

## Cluster lifecycle

Use `synod db stop CLUSTER_ID --wait` to stop a cluster.
Use `synod db start CLUSTER_ID --wait` to start it again.
Both commands read the current revision before admission.
Use `--expected-revision` to supply a revision explicitly.

Use `synod db delete CLUSTER_ID` to see the deletion plan.
This preview exits without deleting the cluster.
Review the target and retained volumes.
Run the command again with the displayed retry flags and `--yes` to confirm.
Deletion retains data. It releases reservations only after removal is confirmed.
Purge is a separate action.

Keep the reported idempotency key, expected revision, and plan token after an uncertain reply.
Use the same values for the retry. An accepted delete can replay after its preview expires.
A new deletion needs an unexpired preview.

## Web application

The controller serves a browser application from its own API origin.
Open the API address in a browser and sign in with an operator account.

The Clusters screen lists actual clusters with **Refresh**, **Load more**, and **Show deleted** controls.
The cluster screen shows declared endpoints and credential metadata.
Select **Reveal** to make an audited request for a password.
Select **Hide** to clear the displayed password.
Navigation also clears it.

Start, stop, and delete requests return an operation link.
Deletion requires a server preview and the exact cluster name.
Deletion retains data.
The browser saves lifecycle request identities in session storage.
After an uncertain reply, select **Retry same request**.
Do not clear that storage before you resolve the request outcome.
Unsupported views and upgrade previews remain marked as examples.

The application is built from `web/` and committed under `internal/web/dist`.
Building the controller therefore needs no JavaScript toolchain.

Rebuild it after a change under `web/`:

```sh
task build:web && task test:web
```

Or without Task:

```sh
cd web && npm install && npm run build && npm test
```

The content security policy allows this origin only.
It forbids inline script and every foreign origin.
