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

This controller starts no worker. Worker dispatch needs the agent transport, and no
command generates the installation CA or the agent certificates yet. `task dev:etcd`
starts an etcd node for when that material exists.

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
It does not provision database containers yet.
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
The current admission milestone leaves it pending.

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
export SYNOD_ETCD_ENDPOINTS='127.0.0.1:2379'
export SYNOD_ETCD_PREFIX='/synod'
```

Use `--etcd-endpoints` and `--etcd-prefix` to override them.
Separate several endpoints with commas.
Every controller in one installation must use the same prefix.

One controller elects itself owner of a cluster, claims that cluster's operation,
and runs one worker for it.
A second controller waits for the election and takes over after the claim expires.
Shutdown stops management dispatch. It never stops native work that an agent accepted.

The controller does not produce execution plans yet.
It skips an operation until a plan exists for it.

## Web application

The controller serves a browser application from its own API origin.
Open the API address in a browser and sign in with an operator account.

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
