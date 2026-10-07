# Synod

Synod backs up the Docker apps of a home lab and restores them on another device.
Devices can use amd64 or arm64.
Some devices can be offline between backups.

## Status

Synod is under development. v0.1 is not released.
The current build creates an installation, joins devices, and approves them.
It discovers Docker apps and their paths.
It provides user accounts, API tokens, operation records, and a web shell.
The controller stores its metadata in SQLite through `modernc.org/sqlite`.
It requires no PostgreSQL server or etcd service.

On-demand and scheduled Stop and Live backups are available for file-based apps.
They use encrypted local, Synod-device, SFTP, or S3 repositories.
App restore to the same device or another device is available.
File restore is available through the CLI and API.
Repository recovery and the recovery kit are not available yet.
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

## Recover after controller loss

Keep the repository password outside Synod.
Stop the old controller and agents before recovery.
Use a new private directory for the recovered controller.
For a local or Synod-device repository, run recovery on the host with the repository files:

```sh
synod server init --dir /private/path/recovered --address 192.168.0.20 \
  --from-repository /mnt/backups/synod --password-file /private/path/password
synod server --config /private/path/recovered/controller.json
```

The password file must have mode `0600`.
Recovery opens an existing repository.
It never creates a replacement repository.
It restores the installation ID, devices, apps, destinations, and protection settings.
It creates new controller keys and a new administrator token.
It does not restore users, old API tokens, device approval, or activity records.
The snapshot commands read the saved copies from the repository.

The repository can move to another path.
If the catalog has several destinations, use `--repository-destination NAME_OR_UUID` to identify the surviving copy.
For a local repository, the printed join token uses the saved ID of its device.
Run that join command on the host with the repository files.
Use a new agent `--dir`.
Start the agent and approve it.
To join each other saved device, create a token with its saved name or ID:

```sh
synod node join-token --device DEVICE
synod agent join TOKEN --dir /private/path/new-agent
synod agent run --config /private/path/new-agent/agent.json
synod node approve DEVICE
synod snapshot list APP
synod restore APP --to DEVICE
```

Run the agent commands on the device.
Run the other commands with the new administrator settings that init printed.
Saved apps stay missing until discovery finds them again.
You can restore a missing app to an approved device.
A recovered local destination is available only to its owner device.
A recovered Synod-device destination becomes available when its owner joins and receives approval.

For S3 or SFTP, pass a private connection JSON file to `--from-repository`.
Use the `location` and `credentials` fields:

```json
{
  "location": {
    "kind": "s3",
    "s3": {"endpoint": "https://s3.example.com", "bucket": "backups", "prefix": "synod"}
  },
  "credentials": {
    "password": "REPOSITORY_PASSWORD",
    "access_key_id": "ACCESS_KEY",
    "secret_access_key": "SECRET_KEY"
  }
}
```

The connection file must have mode `0600`.
You can omit its password and use `--password-file` instead.
The fields use the same format as destination settings.
For a Synod-device repository, use its directory and password directly.
Old device certificates are not needed.

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

## Discover apps

The agent scans Docker once a minute.
It reads running and stopped containers.
Compose project labels group services into one app.
Each standalone container is one app.
The agent must run on the Docker host to measure local paths.
Docker Desktop stores named volumes inside its VM, so a Mac agent cannot measure them.

```sh
synod app list
synod --json app list
```

The list shows project files, environment file paths, bind mounts, and named volumes.
It shows image references, local digests, and the local image platform in JSON.
It does not claim that a local image supports another platform.
File contents, container environment values, and volume driver options do not leave the agent during discovery.

Each path has a proposed status:

| Status | Current meaning |
| --- | --- |
| Included | App files or state selected for each backup. |
| Large | Media or more than 20 GiB. Skipped by default. |
| Dumped | A detected database data path. Dump execution is not available yet. |
| Never | A host system path or an unsupported mount. |

Discovery checks host system exclusions before it scans files.
It does not follow links during a directory scan.
Media paths are not scanned for size.
A size scan stops after five seconds, 100,000 entries, or more than 20 GiB.
An incomplete size is shown as unknown or as a lower bound.
Sizes are file lengths, not disk usage.
A failed Docker scan keeps the last app list and shows a failure code.
A complete scan marks absent apps as missing and keeps their records.
The scan time remains visible when a device disconnects.
An inventory report supports up to 512 apps and 900 KiB per device.
An excessive report shows `inventory_limit` and keeps the prior list.
The CLI accepts an app list of up to 16 MiB.

To run the local Docker discovery test, load its pinned image first:

```sh
docker pull alpine@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock go test -race -p 1 -count=1 ./internal/agent/discovery
```

The test creates private containers and a volume.
It removes them after the test.

## Destinations

Put the repository password in a private JSON file.
Set its mode to `0600`.
Keep a separate copy of the password for recovery.
The CLI does not print saved credentials.

```json
{"password":"use-a-long-random-repository-password"}
```

Add a local directory on an approved device:

```sh
synod destination add home --device server --path /srv/backups/synod --credentials-file ./repository.json
synod destination list
synod destination remove home
```

The local path belongs to the selected device.
A copy on that device does not protect its own apps from device loss.
Add a destination on another device for that protection.
A local destination is sent only to its owner.
Synod-device, SFTP, and S3 settings are sent to all approved agents.
Pending devices receive no destination credentials.

To use a directory on another Synod device, select type `device`:

```sh
synod destination add peer --type device --device odin --path /srv/synod-backups --credentials-file ./repository.json
```

The destination agent serves that directory over mutual TLS.
The directory must be empty or hold a Kopia filesystem repository.
The service creates the directory when a repository client first connects.
It adds a small `.synod-repository` marker.
It does not expose other directories.
The stored repository also opens through the local provider with its password.

Each agent listens on port 8445 by default.
The controller uses the address from the agent connection.
For another route, set `--destination-address HOST:PORT` during `agent join`.
Use `--destination-listen HOST:PORT` to change the listener.
Existing agents use `destination_address` and `destination_listen` in `agent.json`.
An address change is sent to peers after the agent reconnects.
Peers use the stored address during a controller outage.

Each request checks the cached list of approved certificate fingerprints.
The client checks the destination certificate and its exact device identity.
Access changes take effect when an agent receives the new plan.
Expired certificates are always refused.
The service accepts at most eight requests at once.
Each blob is limited to 64 MiB.
Each directory listing is limited to 20,000 entries.
Kopia divides its repository into directories.
An interrupted upload cannot replace a complete object.

For SFTP, add `ssh_key` to the credentials JSON.
Use the full private key text as a JSON string.
Use a separate known-hosts file to verify the server key.

```sh
synod destination add remote --type sftp --host backup.example --port 22 --user synod --path /srv/synod --known-hosts-file ./known_hosts --credentials-file ./sftp.json
```

For S3, add `access_key_id` and `secret_access_key` to the credentials JSON.
Add `session_token` only when the provider requires it.

```sh
synod destination add cloud --type s3 --endpoint https://s3.example --bucket backups --prefix synod/ --credentials-file ./s3.json
```

A successful add saves settings only.
It does not check the connection or show backup success.
`destination list` shows when each approved agent has saved the current settings.
A reconnect requires a new acknowledgement.
There is a limit of 64 destinations and 384 KiB of total input.
Each destination accepts at most 32 KiB of input.

The controller wraps credentials with its installation key.
The agent wraps its local copy with a key derived from its enrolled private key.
The agent journal is the enrollment directory path with `.db` appended.
Keep that private key with the journal.
The agent saves a plan before it acknowledges delivery.
It can read saved settings while the controller is down.
A key replacement must rewrap the journal cache.

Removing a destination keeps all repository data.
It removes the saved credentials from the active controller record.
An offline agent removes its copy when it receives the next plan.
Removal does not revoke credentials at the storage provider.
A destination cannot be removed while a backup operation or an enabled plan uses it.

## Protect apps on a schedule

Review the paths and the proposed settings before confirmation.
Use a destination on another device to protect against source device loss.

```sh
synod protect --all --destination peer
# Repeat the same command with the revision shown in the preview.
synod protect --all --destination peer --confirm REVISION
synod app status
```

Protect all also enables automatic protection for new apps.
Select one app to change only that app.
Use `--device` if its name occurs on more than one device.
An explicit opt-out stays off when Synod discovers the app again.

```sh
synod protect notes --destination peer --destination cloud --at 03:30 --time-zone Africa/Lagos
synod protect notes --off
synod protect --all --off
```

These commands show a preview.
Repeat the same command with `--confirm REVISION` to save it.
A changed plan requires a new review.
Omitted flags keep saved choices.
Use `--off` to stop future admissions after settings reach the agent.
Admitted work still finishes. Existing snapshots remain available.

The defaults are Auto, 03:00 UTC, 7 daily, 4 weekly, and 6 monthly snapshots.
Auto uses Stop for file apps.
Auto uses Dump for apps with a detected database.
Use `--method live` only when the app permits a live copy.
Use `--keep-daily`, `--keep-weekly`, and `--keep-monthly` to change retention.
Retention always keeps the newest copy, the copy just made, and Kopia pins.
It expires older snapshot records after a successful backup.
Content space reclamation remains a later maintenance task.

A new plan runs at once, then once each day at the saved time.
A part-time device takes one current backup when it returns.
It does not replay every missed day.
A skipped daylight-saving time moves forward by the clock gap.
A repeated daylight-saving time has one slot.
The agent saves schedules and destination credentials in its encrypted cache.
An initialized repository remains usable during a controller outage.
The agent reports saved results when the controller returns.
First repository setup requires the controller to assign one owner.

App status shows pending settings, blocked apps, per-destination results, and warnings.
The data-loss window starts at the last good backup's start time.
No reported good copy means that the window is unknown.
`catalog_write_failed` means that the backup did not save current recovery settings.
`retention_failed` means that the backup did not expire older copies.
The next backup retries these steps.

## Back up an app

Select an app from `synod app list`.
Add a destination before its first backup.

```sh
synod backup notes --destination peer
synod backup notes --device server --destination peer --method live
synod backup APP_ID --destination DESTINATION_ID --wait=false
synod operation get OPERATION_ID
synod operation wait OPERATION_ID
```

The default method is Auto.
Auto selects Dump for database apps and Stop for other apps.
With Stop, Synod keeps database containers running for their native dumps.
Synod first uploads the files while the app runs.
It then stops the running file containers and reads the final files again.
Kopia sends only new content blocks.
Synod restarts the containers before it publishes the snapshot.
The result shows the snapshot ID, file bytes, and measured downtime.
File containers that were stopped or paused before the backup keep that state.

Live copies files while the containers run.
Use it only when the app permits a live copy.
On Linux agents, Live and Dump find SQLite files by their file header.
They copy each database with the SQLite backup API.
WAL writes can continue while Synod holds a read snapshot.
Overlapping app paths use the same database copy.
The encrypted manifest lists the copied SQLite files and the excluded sidecars.
The copy uses an unlinked temporary file until the upload ends.
One app can contain up to 256 SQLite files in a Live or Dump backup.
Use Stop for SQLite apps on other agent platforms.
Dump requires a detected database. Use Auto for other apps.
Dump keeps the app running and replaces database data paths with native dumps.
PostgreSQL uses `pg_dumpall`. MySQL and MariaDB use their native SQL dump tools.
Redis uses `redis-cli --rdb`.
Synod waits for an existing Redis background save to end before it requests a dump.
Dumps stream directly into the encrypted repository. Synod creates no plaintext dump files.
Each dump records its source container and format in the snapshot manifest.
MySQL and MariaDB hold a read lock during the dump. Writes wait until the dump ends.
PostgreSQL copies each database in its own transaction. The databases do not share one snapshot point.
The native client and `timeout` must be available in the container.
With BusyBox `timeout`, the container must also provide `setsid`.
SQL clients use the local socket and the official image's administrator environment variables.
Password variables with a `_FILE` suffix are supported.
Redis uses its local port, `REDISCLI_AUTH`, `REDIS_PASSWORD`, or `--requirepass`.
Custom sockets, Redis ACL users, and credentials stored only in custom configuration files need more support.
Discovery reads `PGDATA`, `-D`, `--datadir`, and `--dir` for data paths.
Data paths stored only in custom configuration files are not detected.
A failed dump prevents snapshot publication. The result contains no tool error output.
A stopped or paused database must be started before a backup.
Each dump has a 30-minute deadline inside its container.

Each snapshot holds the discovered app manifest, included paths, and private container definitions.
It also holds network settings and image platform evidence.
Synod checks the running image digest. It does not use the current value of an image tag.
If the registry is unavailable, the snapshot records only the cached image and its architecture.
A standalone container also gets a generated Compose file.
An unsupported standalone setting causes a failure.
The full Docker inspect data stays in the encrypted snapshot.
Host paths, media, and excluded database paths remain excluded inside parent copies.
The manifest records additional exclusions found during the copy.
A backup does not copy its own destination or the agent identity files.

The command waits by default.
Use `--wait=false` to return after admission.
An interruption of the command does not cancel the operation.
Use an app ID or `--device` when app names repeat.
Use `--destination` when the app can reach more than one destination.
The first job creates a repository if it does not exist.
Repository creation has one owner at a time, even if destination names refer to the same directory.
Other agents open the repository after that owner confirms its connection.

The agent saves work before execution.
It saves restart intent before each container stop.
After a crash, it resumes affected containers before it takes more work.
A lost stop response causes a confirmed stop before restart.
A failed restart stays pending in the journal and is retried.
An admitted backup continues through a controller outage.
Its result survives an agent restart and remains until the controller acknowledges it.

## Restore files

List the snapshots of an app. Then list its saved paths.

```sh
synod snapshot list notes
synod file list notes
synod file list notes /srv/notes --snapshot SNAPSHOT_ID
synod file get notes /srv/notes/config.yml --output ./config.yml
synod file get notes /srv/notes --output ./notes.tar
synod file list notes @databases
synod file get notes @databases/notes-db.sql --output ./notes-db.sql
```

Use the original absolute path shown in the listing.
Database dumps appear under `@databases`.
A file download contains the saved file bytes.
A directory download is a tar archive with saved owners, modes, times, and links.
It includes separately saved paths below that directory.
Extract an archive into a new empty directory.
Synod does not follow saved symbolic links when you browse or download a file.

Use `--snapshot` to select an older snapshot.
Without this flag, Synod selects the newest available snapshot.
Use `--destination` to select one destination by name or ID.
Use `--device` to select the source device when app names repeat.
Use `--via` to read through a specific online device.
By default, Synod prefers the source device, then another approved online device.
The selected device must have current destination settings.
Each listing covers only destinations that this device can open.
A local destination is accessible only through its own device.
The response identifies the snapshot and destination that were read.
It reports a warning if a destination could not be read.

Downloads pass through the controller without temporary server files.
The CLI writes a private temporary file beside the requested output file.
It publishes the output only after the complete transfer.
It refuses an existing output file.
Cancellation or connection loss removes the partial download.
A download can run for up to 12 hours.
Snapshot and path listings are bounded.
The response reports `truncated` when a listing exceeds 1,000 snapshots, 4,000 paths, or the metadata size limit.
Use a more specific path or destination when the listing is incomplete.

The administrator API provides these read endpoints:

- `GET /v1/apps/APP_ID/snapshots`
- `GET /v1/apps/APP_ID/files?path=/srv/notes`
- `GET /v1/apps/APP_ID/files/content?path=/srv/notes/config.yml`

The optional `snapshot`, `destination`, and `via` query values use IDs.
The content endpoint returns file bytes or a tar archive.
Its headers include `X-Synod-Snapshot-ID`, `X-Synod-Destination-ID`, and `X-Synod-Format`.
Clients must check the final `X-Synod-Transfer: complete` trailer.
Discard content if this trailer is absent.

Run the file restore proof with a built binary:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock python3 scripts/proof-files.py ./dist/synod
```

The proof uses a disposable Compose project and temporary controller and agent processes.
It removes its files and containers when it exits.
Add `--recover` to delete the test controller, agent state, and source files.
The proof then rebuilds from the repository, restores the app, and makes a new backup.


## Restore an app

Select the app from `synod app list`.
Select the target device from `synod node list`.

```sh
synod restore notes --to odin
synod restore notes --to server --snapshot SNAPSHOT_ID
synod restore notes --to server --replace
synod restore APP_ID --to odin --destination peer --wait=false
```

The default snapshot is the newest snapshot in all destinations that the target can open.
Use `--destination` to read one destination only.
The target agent does all restore work. The snapshot data does not go through the controller.

Synod checks these conditions before it changes the target:

- Each image supports the target architecture. The failure names the image.
- The target can pull each image by its saved digest. A cached image of the target architecture is also accepted.
- No other container has an app container name or the Compose project name.
- No running container publishes a port that the app publishes.
- An existing network with a saved network name uses the saved driver.
- The Docker Compose plugin is available.

The agent writes the restore into a new private directory.
The directory is `restores/OPERATION_ID` beside the agent enrollment directory.
The default is `/var/lib/synod/agent/restores/OPERATION_ID`.
The `project` directory holds the saved project files.
The `data` directory holds the saved bind paths.
Each saved volume becomes a new volume named `synod-OPERATION_ID-volumeN`.
Synod applies the saved owners, modes, and times.
A Large or Dumped bind path becomes an empty path with its saved owner and mode.
A Never path keeps its original host path.

Synod loads each database dump into the new data before the app starts.
The load container has no network and no published ports.
It ignores the image's initialization scripts.
PostgreSQL loads the `pg_dumpall` output as the original administrator user.
MySQL and MariaDB load the SQL dump as root.
Redis loads the RDB file. Synod then enables the append-only file again if the app used it.

Synod writes `compose.json` from the saved container settings.
The file keeps the project name, the service names, the container names, and the published ports.
It uses the image digest and the target platform.
Docker Compose then starts the project without a build or a pull.
Synod creates each missing network with its saved driver and options.
It keeps a saved subnet only when a container uses a static address on that network.
A container must run, or exit with code 0, three seconds after the start.
A standalone container becomes the Compose project `synod-CONTAINER_ID_PREFIX`.
The result shows the new app ID and the project directory.

Synod does not overwrite an existing app unless you use `--replace`.
With `--replace`, Synod first prepares the complete restore beside the running app.
It then takes a safety snapshot of the existing app in the same destination.
It stops and removes the existing containers only after the safety snapshot succeeds.
The volumes and bind paths of the replaced app stay on the device.
If the restored app does not start, Synod removes it.
Synod then creates the replaced containers again from their saved settings and starts the containers that ran before.
The result shows the safety snapshot ID.

A failed restore removes its directory, its volumes, its networks, and its containers.
The warning `cleanup_pending` means that some of these items remain.
The agent journal saves each phase of the restore.
After an agent restart, the agent recovers the replaced app before it takes more work.
A recovery that cannot finish stays in the `waiting` state and is retried.

Limits:

- Host networking is not supported.
- The port check reads Docker containers only. A host process that uses a port causes a start failure.
- An image that was available only on the source device cannot be restored on another device.
- The safety snapshot is reported in the restore result only. It is not a separate backup operation.

## Repository library

The repository library uses Kopia v0.23.1.
It creates and opens encrypted repositories on a local path, a Synod device, SFTP, or S3.
Destination commands save the connection settings.
They do not create a repository or start a backup.

The recovery catalog is stored inside the encrypted repository.
It holds the installation ID, devices, apps, destination credentials, and protection settings.
Each backup writes its cached catalog after the app resumes.
Settings changed after that write reach the repository with a later backup.
An offline agent cannot replace a newer catalog generation.
A new catalog generation keeps the previous generations.
A repeated write with the same content has no effect.
A stale generation or a conflicting generation fails.
A repository cannot silently change to another installation.
Controller recovery from this catalog remains a later task.

Opening a repository needs its location, access credentials, and repository password.
It needs no saved controller state.
A private temporary connection file is removed when the repository opens.
The library keeps no local repository cache.

SFTP uses a private key and a saved host key.
It requires the server's atomic POSIX rename extension.
A cancelled SFTP request closes its connection.
Open the repository again before another request.
S3 verifies TLS for an HTTPS endpoint.
An explicit HTTP endpoint is supported for local test services.
Provider errors do not print credentials.
Catalog reads, writes, and connection setup have a 30-second deadline.
Kopia can take up to one retry sleep to observe cancellation.

Run the repository checks with a temporary MinIO service:

```sh
SYNOD_TEST_DOCKER_HOST=unix:///path/to/docker.sock ./scripts/test-minio.sh go test -race -p 1 -count=1 -timeout=2m ./internal/repository
```

The fixture builds MinIO from source revision `9e49d5e7a648`.
It keeps the local test image for later runs.
It binds a free port to loopback and removes the container after the command.
The SFTP test starts its own SSH server on loopback.

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

See [the API schema](api/openapi.json) for enrollment, device, app, and operation routes.
It does not yet describe account and session routes.

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
