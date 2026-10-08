# Develop Synod

This page is for contributors. Users read the [user guide](user-guide.md).
Read [AGENTS.md](../AGENTS.md) before you change code.

## Build and checks

Use the Go version in [go.mod](../go.mod).

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

[Task](https://taskfile.dev) runs the commands in [Taskfile.yml](../Taskfile.yml).
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

## Build release files

The release script builds static Linux binaries for amd64 and arm64.
It uses the Go version in `go.mod` and the committed web assets.
It records the version and source revision in both binaries.
The agent reports the same version to the controller.

Run the code and web checks above before you build a release.
Commit the source and the web build.
Then run:

```sh
sh scripts/build-release.sh v0.1.0-rc.1
```

The output is `dist/releases/v0.1.0-rc.1/`.
It contains `synod-linux-amd64`, `synod-linux-arm64`, `install.sh`, `release.json`, and `SHA256SUMS`.
The script refuses an existing output directory.
A dirty tree requires the explicit `--allow-dirty` test option as the third argument.
The revision of that build has a `-dirty` suffix.
Do not publish a test build as a release.

## Tests and proofs

Docker tests, MinIO tests, and real-host proofs are slow.
Run them when the owner asks, usually before a release.
Real-host proofs follow `tmp/tasks/HOSTS.md` and use disposable test projects only.

Run the release proof on the approved hosts:

```sh
python3 scripts/proof-release.py --release dist/releases/VERSION --evidence /path/to/new-private-evidence
```

The proof installs the release, protects a test app, deletes it with its device state, and restores it on the other device.

Test the installer without a system change:

```sh
python3 scripts/test-install.py
```

Run the real-host proof only on the approved disposable proof paths:

```sh
python3 scripts/proof-install.py --release dist/releases/v0.1.0-rc.1 --evidence /tmp/synod-install-proof.json
```

The upgrade proof uses two release directories and disposable Linux state:

```sh
python3 scripts/proof-upgrade.py --old-release /path/to/old-release --new-release /path/to/new-release --evidence /tmp/synod-upgrade-proof
```

To run the local Docker discovery test, load its pinned image first:

```sh
docker pull alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock go test -race -p 1 -count=1 ./internal/agent/discovery
```

The test creates private containers and a volume.
It removes them after the test.

Run the file restore proof with a built binary:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock python3 scripts/proof-files.py ./dist/synod
```

The proof uses a disposable Compose project and temporary controller and agent processes.
It removes its files and containers when it exits.
Add `--recover` to delete the test controller, agent state, and source files.
The proof then rebuilds from the repository, restores the app, and makes a new backup.

Run the local delivery proof with a built binary:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock python3 scripts/proof-checks.py ./dist/synod --notifications
```

The proof uses a local HTTP receiver and disposable Docker containers.
It checks delivery after a controller restart and after a forced drill failure.

Run the M4 proof on the approved Linux hosts:

```sh
python3 scripts/proof-alerts.py --binaries /path/to/linux-binaries --evidence /path/to/new-private-evidence
```

The binary directory must contain `synod-linux-amd64` and `synod-linux-arm64`.
The proof uses the hosts and SSH settings in `scripts/proof-m1.py`.
It starts only disposable projects and a loopback HTTP receiver.
It checks database rows, drill isolation, alert delivery, cleanup, and existing container start times.
Get host approval before you run it.

Run the repository checks with a temporary MinIO service:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock ./scripts/test-minio.sh go test -race -p 1 -count=1 -timeout=2m ./internal/repository
```

The fixture builds MinIO from source revision `9e49d5e7a648`.
It keeps the local test image for later runs.
It binds a free port to loopback and removes the container after the command.
The SFTP test starts its own SSH server on loopback.

Run the SQLite source-path proof on Linux with a disposable container:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock ./scripts/test-sqlite.sh
```

The test uses a pinned Go image and a read-only module cache.
It restores a live WAL database and compares committed writes.
It also checks overlapping paths, sidecar links, corruption, and cancellation.

Run the app restore tests on Linux with a disposable container:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock ./scripts/test-restore.sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock ./scripts/test-restore.sh -test.run TestRestoreRebuilds
```

The test container reads named volume data from `/var/lib/docker/volumes`, as a root agent does.
It uses the Docker CLI and the Compose plugin from a pinned image.
Its work directory is `/var/tmp` on the Docker host, so restored bind paths have the same path in both places.
The test restores a lost Compose project with a bind path, a named volume, a PostgreSQL dump, a network, and a published port.
It checks file content, owners, modes, and database rows.
It also checks the conflict refusal, a replace with a safety snapshot, and recovery after an interrupted replace.
On macOS, `go test` runs the same tests without the named volume.

Run the database and disaster recovery proof on the approved Linux hosts:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/synod-proof-binaries/synod-linux-amd64 ./cmd/synod
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/synod-proof-binaries/synod-linux-arm64 ./cmd/synod
python3 scripts/proof-recovery.py --binaries /tmp/synod-proof-binaries --evidence /tmp/synod-recovery-proof
```

The evidence directory must be new.
The hosts need the pinned PostgreSQL and Alpine images named in the proof scripts.
The proof writes to a disposable PostgreSQL app on server during backup.
It deletes its controller, agent state, source project, and source volume.
It rebuilds the controller from the repository on odin.
It restores the amd64 app on arm64 and checks the saved rows, role, and files.
It checks that existing running containers keep their start times.
It removes its temporary processes, containers, volumes, and directories.
