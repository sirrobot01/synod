# Development

This page is for contributors. Read [AGENTS.md](../AGENTS.md) before you change code.

## Build and test

Use the Go version in [go.mod](../go.mod).

```sh
go build -o dist/synod ./cmd/synod
go vet ./...
go test -race -p 1 -count=1 ./...
```

Always use `-p 1`. Docker tests fail on timing without it.

The web application is in `web/`:

```sh
cd web
npm ci
npx svelte-check
npx vitest run
npm run build
```

The build writes to `internal/web/dist`. Commit that output with each web change.

## Heavy tests

Docker and MinIO tests skip themselves unless you set their variables.
Run them before a release.

| Test | Set |
| --- | --- |
| Docker | `SYNOD_TEST_DOCKER_HOST`, for example `unix:///var/run/docker.sock` |
| S3 | `SYNOD_TEST_S3_ENDPOINT`, `SYNOD_TEST_S3_ACCESS_KEY`, `SYNOD_TEST_S3_SECRET_KEY` for a MinIO server |
| btrfs | `SYNOD_TEST_BTRFS`, an empty directory at the top of a disposable btrfs mount |
| LVM | `SYNOD_TEST_LVM=1` on a disposable Linux runner with LVM, ext4, and XFS tools |
| App consistency | `SYNOD_TEST_SNAPSHOT_DIR`, a directory on a disposable btrfs, LVM, or ZFS filesystem. Also set the Docker variable. Set `SYNOD_TEST_DATABASE_DIR` to test another ZFS dataset in the same pool. |

The SFTP test starts its own SSH server.
On Linux, run the Docker tests as root. Restore tests read volumes in `/var/lib/docker`.
Pull the pinned test images listed in the **heavy** job of `.github/workflows/test.yml` first.

```sh
SYNOD_TEST_DOCKER_HOST=unix:///var/run/docker.sock go test -race -p 1 -count=1 ./...
```

In GitHub, you can also run the **test** workflow by hand and select **heavy**.
It checks Docker, MinIO, and all three filesystem types.
The app consistency test writes linked PostgreSQL rows and files during a backup.
It restores the database files from the snapshot and checks each saved row against its file.

## Local controller

[Task](https://taskfile.dev) runs the commands in [Taskfile.yml](../Taskfile.yml). Run `task` to list them.

| Command | Does |
| --- | --- |
| `task build` | Build the binary into `dist/`. |
| `task build:web` | Build the web application. |
| `task check` | Run `go vet` and the web type check. |
| `task test` | Run the Go tests. Add `-- -run TestName ./internal/api/...` for one test. |
| `task test:web` | Run the web tests. |
| `task dev` | Create a local installation in `.dev/` and run its controller. |
| `task dev:web` | Run the web development server. |
| `task dev:user -- alice` | Create an account. |
| `task cli -- node list` | Run a CLI command against the local controller. |
| `task dev:reset` | Delete the local installation. |
| `task proto` | Generate Go code from the agent protocol. |

1. Run `task dev`.
2. In another terminal, run `task dev:user -- alice`.
3. Open `https://127.0.0.1:8443` and sign in.

`.dev/` holds the database and the installation keys. Keep it private.
A changed migration fails the checksum check on an existing `.dev/` database. Run `task dev:reset`, then `task dev`.

## Continuous integration

On each push and pull request, `.github/workflows/test.yml` runs:

- `gofmt`, `go vet`, and the Go tests.
- The web checks and build. The committed `internal/web/dist` must match the build.
- A snapshot release and an install with `install.sh` on Linux and macOS.
- The macOS controller tests, the launchd setup, and an upgrade of a running controller.

## Release

GoReleaser builds Linux and macOS binaries for amd64 and arm64 from `.goreleaser.yaml`.
The builds do not use cgo.

To build the release files without publishing them:

```sh
task release:snapshot
```

To publish a release, push a version tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The **release** workflow requires all jobs of the **test** workflow, with **heavy** enabled.
These jobs check the Go code, web build, installers, Docker, MinIO, and filesystem snapshots.
Only then does it publish the binaries, `SHA256SUMS`, `install.sh`, and the agent image.

Before the first release, complete the real-host restore and controller recovery checks.
Test the same commit that you will tag.
Record the commands, results, and limits in the task tracker.

The v0.1.0 tag fixes the first supported database and repository formats.
After that tag, do not edit an applied migration.
Add a migration for each required schema change.
Test upgrades from the released version with its saved state and backups.
Do not require users to delete their database to upgrade.
