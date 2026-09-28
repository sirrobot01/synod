# Synod

Synod pools the devices of a home lab and runs any container on them.
Devices can be amd64 or arm64, and some devices are online only part of the time.
Apps run in three tiers: databases (tier 1), single-writer apps (tier 2), and parallel apps (tier 3).
Clients use normal protocols through a router on every device.

## Status

Synod is under active development. v0.1 is not released.
The plan for v0.1 is in the product brief. It includes the three app tiers, backups,
upgrades, an install script, and a web UI around apps and devices.
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
| `task build` | Build the `synod` binary into `dist/` |
| `task build:web` | Build the browser application into `internal/web/dist` |
| `task check` | Vet the Go code and type-check the browser application |
| `task test` | Run the Go tests that need no dependency |
| `task test:all` | Run the whole Go suite with PostgreSQL and race detection |
| `task test:web` | Run the browser application tests |
| `task dev` | Run the controller on `https://127.0.0.1:8443` |
| `task dev:web` | Serve the browser application with hot reload |
| `task dev:user -- alice` | Create the first operator account |
| `task cli -- db get NAME` | Run the command line against the development controller |
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
a credential wrapping key, and an approved node.
Use `synod server init --dir DIRECTORY` to create installation material.

Run `task dev:reset` after you change a migration. AGENTS.md requires a dropped
development database, never a repair migration.

### Browser application with hot reload

`task dev:web` serves the application on `https://localhost:5173` and sends `/v1` to the
controller. Run `task dev` in another terminal first.

The development server uses the same certificate as the controller, because the session
cookie carries the `__Host-` prefix and a browser stores it only over HTTPS.

## Test fixtures

`./scripts/test-postgres.sh` starts the PostgreSQL database that the tests need.
Docker must be available.
The script uses a pinned image and a random localhost port.
It removes its container after the command exits.
It exports `SYNOD_TEST_DATABASE_URL` for the test process.
Pass a command after the script name to run a focused check.

Run the whole suite:

```sh
./scripts/test-postgres.sh go test -race -p 1 ./...
```

A test that needs a dependency skips when its variable is absent.

## Development controller

The controller accepts app requests.
It records desired state and a pending operation in PostgreSQL.
A configured worker runs the app containers on approved agents.
The API-only development command leaves operations pending.
The database catalog contains PostgreSQL 17 (17.11) and Redis 8.2 (8.2.7).
These versions are local test candidates. They are not a qualified release matrix.

Build Synod with `go build -o dist/synod ./cmd/synod`.
One binary holds the CLI, the controller (`synod server`), and the agent (`synod agent`).
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
./dist/synod server
```

The default API address is `127.0.0.1:8443`.
Use `--listen` to change it.
Use `synod server --help` to list the settings.
Explicit flags override environment variables.
Environment variables override a JSON file supplied with `--config` or `SYNOD_CONFIG`.
The JSON keys use the names shown in [the controller configuration](internal/config/controller.go).
Use a protected file or environment for a database URL that contains a password.
Startup applies the metadata schema migrations.

See [the initial API schema](api/openapi.json). It does not describe the app endpoints yet.
Send the administrator token in `Authorization: Bearer <token>`.
Send one `Idempotency-Key` header with every create request.
Use 1 to 128 visible ASCII bytes for the key.
Keep that key and the same request body until the result is known.
A retry returns the original app and operation IDs.
A changed request with the same key returns HTTP 409.
An uncertain commit returns HTTP 503 with `outcome_unknown`.
Retry that result with the same key and body.
An accepted request returns HTTP 202 and an operation URL in `Location`.

Example request body for `POST /v1/apps` that creates a database:

```json
{
  "schema_version": 1,
  "name": "photos-db",
  "tier": 1,
  "copies": 1,
  "database": {"engine": "postgres", "version": "17", "databases": ["immich"]},
  "replication": {"standbys": 1, "interval_seconds": 0, "failover_after_seconds": 90, "failback": "manual", "failback_stable_seconds": 600},
  "router": {"protocol": "tcp", "port": 5432, "reader_port": 5433}
}
```

Read `GET /v1/apps/{id}` to inspect the app, its copies, and its routes.
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

The controller runs an operation only when you supply the agent transport.
Without it the controller admits requests and records desired state, and no worker starts.
Controllers elect app owners in the metadata PostgreSQL database.

One controller elects itself owner of an app, claims that app's operation,
and runs one worker for it.
A second controller waits for the election and takes over after the claim expires.
Shutdown stops management dispatch. It never stops native work that an agent accepted.

Database apps need a credential wrapping key.
Use `--secret-key-file` and `--secret-key-id` to configure it.
Placement reads the node shape from the approved node records.
Use `--engine-endpoint-first-port` and `--engine-endpoint-last-port` to set the host ports of database copies.

## Databases

A database is a tier 1 app. The engine replicates its data to standby copies.
PostgreSQL uses logical replication. Redis uses replica mode.
A primary and its standbys can run on different architectures.

Create a PostgreSQL database with one standby:

```sh
synod db create photos-db --version 17 --standbys 1 --prefer server --allow odin --writer-port 5432 --reader-port 5433
```

The version is a major version that the engine catalog serves.
The default engine is `postgres`. Use `--engine redis` for Redis.
`--database` names a PostgreSQL database. The default is the app name with `-` replaced by `_`.
The writer route and the optional reader route listen on every device.

Read the primary, the standbys, and the routes:

```sh
synod db get photos-db
```

Each standby reports its state and its lag.
A standby that copies its data is not usable for failover.

Read the client login and the connection URLs:

```sh
synod db credentials photos-db
```

The result is always JSON and contains a password. Keep that output private.
Do not copy it into logs or shared files.
The server records the actor and the credential version before it returns the password.
PostgreSQL clients log in as `app`. Redis clients log in as `default`.
Engine TLS uses the node certificate, which carries no host name.
Clients verify the installation CA only. PostgreSQL URLs use `sslmode=verify-ca`.

Move the primary to another device after the standby catches up:

```sh
synod db switchover photos-db --to odin
```

Use `synod db stop`, `synod db start`, and `synod db delete` with the app ID.
Add `--yes` to confirm a delete.
Delete removes the containers and the app data directories.

## Join a node

`synod server init` prints a join token when it can reach the metadata database.
A join token enrolls one node. It is valid for seven days.
To get a token for another node, run this command:

```sh
synod node join-token --ttl 24h
```

The command needs `SYNOD_CA_FILE` or `--ca-file`. The join token pins that API CA.

On the new node, run this command:

```sh
synod agent join TOKEN
```

The command does these steps:
1. It verifies the API CA against the fingerprint in the token.
2. It enrolls the node and keeps the node ID in `node-id`.
3. It initializes the agent journal.
4. It writes `agent.json` and `synod-agent.service`.

The default directory is `/var/lib/synod/agent`. Use `--dir` to change it.
Use `--storage-path` and `--storage-id` to name the storage pool.
If the command stops before it writes `agent.json`, run it again with the same token and directory.
The controller must run before a node can join, because the agent needs the recovery generation.

Start the agent. Then approve the node:

```sh
synod node approve NODE_ID
```

Approval takes the architecture, runtime version, storage ID, and headroom from the facts that the agent measured.
The default headroom is one tenth of each measured resource.
The node can run all engine versions that the controller serves.
Use the flags of `synod node approve` to change a value.
If the agent has not reported its facts, approval fails with `node_facts_unavailable`.

## Agent configuration

`synod agent join` writes this file. You can also write it yourself.

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

Put `storage_path` on the filesystem that holds the Docker data and the app directories.
Without `storage_id`, the agent cannot report capacity, so it refuses to start.

`synod installation get` shows `installation_id` and `recovery_generation`.
The recovery generation can change after controller recovery. Read it when you write a new configuration.
It is available after a controller with the agent transport starts.

Run `synod agent journal init --config FILE` once after enrollment.
Then run `synod agent run --config FILE`.

A journal or inventory refusal prints its cause in the `detail` field of the error.
Other errors print fixed text only, because an argument error can repeat raw input.

Before the first release, the agent protocol can change without a migration.
A new agent refuses a journal that holds commands from an older build.
It reports `journal_recovery_required`, and `detail` names the saved command it refused.
Upgrade an agent only when its node has no app copy. Then create a new journal directory.

## Client addresses

Set the node's client address with `synod node approve --client-address ADDRESS`.
Supply the other required node policy flags with that command.
Use a canonical IPv4 or IPv6 address that clients can reach.
Private LAN addresses are valid. Loopback and wildcard addresses are refused.
App endpoints and database URLs use this address.

## Web application

The controller serves a browser application from its own API origin.
Open the API address in a browser and sign in with an operator account.

Settings manages accounts and API tokens through the API.
An operation link opens the live state of that operation.
The other screens show examples. Each example is marked.
The web UI has no app screens yet. Use the command line for apps and databases.

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
