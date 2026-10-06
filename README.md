# Synod

Synod backs up the Docker apps of a home lab and restores them on another device.
Devices can use amd64 or arm64.
Some devices can be offline between backups.

## Status

Synod is under development. v0.1 is not released.
The current build creates an installation, joins devices, and approves them.
It provides user accounts, API tokens, operation records, and a web shell.
The controller stores its metadata in SQLite through `modernc.org/sqlite`.
It requires no PostgreSQL server or etcd service.

App discovery, backups, restore, repository recovery, and the recovery kit are not available yet.
The current build does not protect app data.
Synod does not place running apps or provide automatic failover.

## Build and checks

Use the Go version in [go.mod](go.mod).
Read [AGENTS.md](AGENTS.md) before you change code.

```sh
go build -o dist/synod ./cmd/synod
go vet ./...
go test -race -p 1 -count=1 ./...
```

The controller tests use SQLite files in temporary directories.
They need no external database.
A test that needs Docker requires `SYNOD_TEST_DOCKER_HOST`.
Set it to the Unix socket URL of your test daemon.
Use disposable test resources.

Build and check the browser application:

```sh
cd web
npm ci
npx svelte-check
npx vitest run
npm run build
```

The build writes to `internal/web/dist`.
Commit that output with a web change.

## Local development

[Task](https://taskfile.dev) runs the commands in [Taskfile.yml](Taskfile.yml).
Run `task` to list them.

| Command | Action |
| --- | --- |
| `task build` | Build the binary into `dist/`. |
| `task build:web` | Build the web application. |
| `task check` | Run Go vet and the web type check. |
| `task test` | Run Go tests with race detection, one package at a time. |
| `task test:web` | Run the web tests. |
| `task dev` | Create a local installation and run its controller. |
| `task dev:web` | Run the web development server. |
| `task dev:user -- alice` | Create an operator account. |
| `task cli -- node list` | List the joined devices. |
| `task dev:reset` | Delete the local installation in `.dev/`. |
| `task proto` | Generate Go code from the agent protocol. |

Run the controller in one terminal:

```sh
task dev
```

Create an account from another terminal:

```sh
task dev:user -- alice
```

Open `https://127.0.0.1:8443`.
The local certificate uses a private CA.
Check that this is your local controller before you accept a browser certificate warning.
Sign in with the account that you created.

The `.dev/` directory holds the SQLite database and the installation credentials.
Git ignores this directory.
Keep it private.

For a focused Go test, pass the package list after the flags:

```sh
task test -- -run TestName ./internal/api/...
```

A changed migration causes a checksum error on an existing development database.
Run `task dev:reset` only when you can delete that installation.
Then run `task dev` to create a new one.

## Create an installation

One binary holds the CLI, the controller, and the agent.
Build it before you create service files.
Use an address that the other devices can reach.

```sh
./dist/synod server init --dir /private/path/synod --address 192.168.0.20
./dist/synod server --config /private/path/synod/controller.json
```

Replace the example path and address with your own values.
Init writes the certificates, keys, administrator token, SQLite database, and controller configuration.
It prints a join token for one device.
That token expires after seven days.
Init refuses to replace existing installation material.

The API listens on port 8443.
The agent transport listens on port 8444.
Use `--listen` and `--agent-listen` to change these addresses when you start the controller.
A join token must name the API address that the device can reach.

On Linux, init also writes a systemd service file.
It installs the file if it can write to the systemd directory.
Use `--systemd-dir /path/to/units` to select another directory.
Init does not start the service.
Start it through systemd only after you check its paths.

The controller accepts a JSON configuration file with `--config` or `SYNOD_CONFIG`.
Environment variables override the file.
Flags override environment variables.
Use `synod server --help` for the available settings.
The `database_path` setting, or `SYNOD_DATABASE_PATH`, must name an absolute file path.
Keep the database in a private directory on a local disk.
Database files must belong to the current user and have owner-only permissions.
SQLite uses WAL mode, full synchronous writes, and foreign key checks.

## Join and approve devices

Run this command on a device that has Docker:

```sh
synod agent join TOKEN
```

Replace `TOKEN` with the token from init.
The command creates a private agent directory and writes the configuration and service file.
The default directory is `/var/lib/synod/agent`.
Use `--dir` to select another directory.
The default Docker socket is `/var/run/docker.sock`.
Use `--docker-socket` to select another socket.
The agent user must have access to that socket.

Start the agent with its saved configuration:

```sh
synod agent run --config /var/lib/synod/agent/agent.json
```

Set the administrator shell variables that init printed.
Then list the devices and approve the new device by its name or ID:

```sh
synod node list
synod node approve DEVICE
```

The agent must connect and report its facts before approval.
The Nodes screen can also approve it.
To join another device, create a new one-use token:

```sh
synod node join-token
```

Agents use mutual TLS after enrollment.
A device keeps its identity and certificates in its enrollment directory.

## Operator access

The CLI uses the token file in `SYNOD_ADMIN_TOKEN_FILE`.
Set `SYNOD_API_URL` to the controller HTTPS origin.
Set `SYNOD_CA_FILE` to the API CA certificate.
Init prints the paths for its installation.

Create an account or an API token:

```sh
synod user create alice
synod token create "backup automation"
```

The user command prompts for a password.
Use `--password-file` to read it from a private file.
An API token secret appears once.
Store it when the command prints it.
Keep the bootstrap administrator token private for account recovery.

A browser signs in with a username and password.
Set `--public-origin` to its API origin when needed.
A browser request that changes state must prove that origin.

## Operation records

The operation core stores work and its result across controller restarts.
Read recent records with these commands:

```sh
synod operation list
synod operation get ID
synod operation wait ID
```

Interrupting `wait` does not cancel the work.
A failed or cancelled operation gives `wait` a nonzero exit status.
The current build has no backup worker that creates these records.
The agent journal records input before execution and keeps results until acknowledgement.
A running journal record requires inspection after a restart.
The backup worker will use this journal in a later task.

See [the API schema](api/openapi.json) for enrollment, device, and operation routes.
It does not yet describe account and session routes.
