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

## Tests

GitHub Actions runs the checks in `.github/workflows/test.yml` on each push and pull request:

- `gofmt`, `go vet`, and the Go tests.
- The web type check, tests, and build. The committed `internal/web/dist` must match the build.
- A snapshot release and an install with `install.sh`.

Docker and MinIO tests are slow. They skip themselves unless you set their variables.
Run them when you need them, usually before a release:

- In GitHub, run the **test** workflow by hand and select **heavy**.
- On a Linux host, run them as root, because restore tests read named volumes in `/var/lib/docker`:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///var/run/docker.sock go test -race -p 1 -count=1 ./...
```

For S3 tests, start a MinIO server and set `SYNOD_TEST_S3_ENDPOINT`, `SYNOD_TEST_S3_ACCESS_KEY`, and `SYNOD_TEST_S3_SECRET_KEY`.
The SFTP test starts its own SSH server.

## Release

GoReleaser builds the release from `.goreleaser.yaml`.
It builds static Linux binaries for amd64 and arm64 from the committed web assets.
The binaries report the tag and commit in `synod --version`.

Build the release files locally without publishing:

```sh
task release:snapshot
```

To publish a release, push a version tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The **release** workflow runs the tests. Then it publishes `synod-linux-amd64`, `synod-linux-arm64`, `SHA256SUMS`, and `install.sh` to the GitHub release.
`install.sh` downloads these files and verifies their checksums.
