# Reference

This page gives the settings, behaviour, and limits of Synod.
For setup steps, read the [user guide](user-guide.md).

- [Components](#components)
- [Install and upgrade](#install-and-upgrade)
- [Controller](#controller)
- [Devices](#devices)
- [App discovery](#app-discovery)
- [Destinations](#destinations)
- [Protection plans](#protection-plans)
- [Backup methods](#backup-methods)
- [Protection status](#protection-status)
- [Restore files](#restore-files)
- [Restore an app](#restore-an-app)
- [Restore tests and repository checks](#restore-tests-and-repository-checks)
- [Notifications](#notifications)
- [Recover after controller loss](#recover-after-controller-loss)
- [Repository](#repository)
- [Accounts and API](#accounts-and-api)
- [Logs](#logs)
- [Limits](#limits)

## Components

One `synod` binary holds the CLI, the controller, and the agent.

| Component | Runs on | Does |
| --- | --- | --- |
| Controller | One Linux device, in Docker or as a service, or a Mac | Stores settings, serves the web UI and API, and sends work to agents. |
| Agent | Each Linux device with apps | Finds apps, backs them up, restores them, and can store backups for other devices. |
| CLI | Any device | Talks to the controller API. |

The agents do the backup and restore work. Backup data does not go through the controller.
An agent keeps its schedules and destination settings, so backups continue while the controller is down.

| Port | Listener | Change it with |
| --- | --- | --- |
| 8443 | Controller web UI and API | `synod server --listen` |
| 8444 | Controller agent transport | `synod server --agent-listen` |
| 8445 | Agent destination service | `synod agent join --destination-listen` |

| Path | Content |
| --- | --- |
| `/var/lib/synod/controller` | Controller state on Linux. |
| `/Library/Application Support/Synod/controller` | Controller state on macOS, with `controller.log`. |
| `/var/lib/synod/agent` | Agent identity, configuration, and journal. |
| `/var/lib/synod/apps` | Restored apps. |
| `/var/lib/synod/cache` | Encrypted repository metadata, up to about 576 MiB for each destination. |

## Install and upgrade

```sh
curl -fsSL https://synod.biodun.dev/install.sh | sh
```

To give options, use `sh -s -- OPTIONS`:

| Option | Use |
| --- | --- |
| `--version v0.1.0` | Install this version. The default is the latest release. |
| `--install-dir DIR` | Install into this directory. The default is `/usr/local/bin`. |
| `--from-dir DIR` | Install from release files in a local directory. Needs `--version`. |
| `--base-url URL` | Download from another HTTPS release server. Needs `--version`. |

The installer needs `curl` and `sha256sum` or `shasum`.
It checks the binary against `SHA256SUMS` before it changes anything.
It replaces `synod` in one atomic rename and keeps the old binary as `synod.previous`.
It does not create state or change Docker.

To upgrade, run the installer again on each device, the controller device first.
Use the same version on all devices.
The installer restarts the running `synod-server` and `synod-agent` services, or the `dev.biodun.synod.server` launchd service on macOS.
Do not run `server init` or `agent join` again. An upgrade keeps all identities.

After an upgrade, check the controller:

```sh
synod installation get
synod --json node list
synod app status
```

`installation get` shows the controller version. `node list` shows `agent_version` for each device.

If a stopped installer leaves `.synod-install.lock` in the install directory, make sure no installer runs. Then remove it with `rmdir`.

## Controller

### Controller in Docker

The image `ghcr.io/sirrobot01/synod` runs the controller with `command: server`.
Mount `/var/lib/synod/controller` to keep its state.
Use `network_mode: host`, so that the controller finds the device address and other devices reach ports 8443 and 8444.

On its first start, the controller creates the installation in `/var/lib/synod/controller`.
Later starts use the saved installation.
The first start:

- listens on all interfaces,
- writes the recovery kit to `recovery-kit.txt`, but not to the log. The web UI shows it once when you create the first account.
- writes a join token for one device to `join.token` and to the log.

| Variable | Use on the first start |
| --- | --- |
| `SYNOD_ADDRESS` | Address or hostname that devices use. The default is the address of the default route. Set it on a bridge network. The controller saves it as `address`. |
| `SYNOD_FROM_REPOSITORY` | Recover from this repository directory or connection file. See [Recover after controller loss](#recover-after-controller-loss). |
| `SYNOD_PASSWORD_FILE` | The private file with the repository password, for a recovery. |
| `SYNOD_REPOSITORY_DESTINATION` | The saved destination to recover from, when there are several. |

If the first start stops before it completes, empty the directory and start again.

`synod server` without options does the same on a host.

### Controller as a service

```sh
synod server init
```

| Option | Use |
| --- | --- |
| `--address HOST` | Address or hostname that devices use. The default is the address of the default route. |
| `--dir DIR` | Controller directory. |
| `--systemd-dir DIR` | Linux only. Write the service file here and do not start it. |
| `--launchd-dir DIR` | macOS only. Write the service file here and do not start it. |
| `--from-repository`, `--password-file`, `--repository-destination` | Recover from a repository. See [Recover after controller loss](#recover-after-controller-loss). |

Init writes the certificates, keys, administrator token, database, and `controller.json`.
It prints the recovery kit and a join token that is valid for seven days.
It refuses to replace an existing installation.

On Linux, init installs and starts the `synod-server` systemd service.
On macOS, run init as root. It installs and starts the `dev.biodun.synod.server` launchd service. To restart it, run `sudo launchctl kickstart -k system/dev.biodun.synod.server`.

Without a service manager, start the controller yourself:

```sh
synod server --config /var/lib/synod/controller/controller.json
```

Run `synod server --help` for all controller settings.
Flags override environment variables. Environment variables override the configuration file.

### CLI connection

For a controller in Docker, run the CLI in its container: `docker exec synod synod COMMAND`.
As root on the controller, the CLI reads `controller.json` from the default controller directory.
On another device, or for another directory, set these variables:

| Variable | Value |
| --- | --- |
| `SYNOD_API_URL` | The controller HTTPS address. |
| `SYNOD_CA_FILE` | `api-ca.crt` from the controller directory. |
| `SYNOD_ADMIN_TOKEN_FILE` | A private file with an API token. |

Add `--json` to a command for JSON output.

## Devices

```sh
synod agent join 'TOKEN'
synod node list
synod node approve DEVICE
synod node join-token
```

| `agent join` option | Use |
| --- | --- |
| `--dir DIR` | Agent directory. The default is `/var/lib/synod/agent`. |
| `--docker-socket PATH` | The default is `/var/run/docker.sock`. |
| `--destination-address HOST:PORT` | Address that other agents use to reach this device's destination service. The default is the address that the controller sees. |
| `--destination-listen HOST:PORT` | Destination service listener. The default is `:8445`. |
| `--systemd-dir DIR` | Write the service file here and do not start it. |

Join installs and starts the `synod-agent` service.
Without systemd, run `synod agent run --config /var/lib/synod/agent/agent.json`.
If join fails, run it again with the same token and directory.

In the web UI, **Devices → Add device** creates a token and shows a Compose file and commands with the token filled in. It shows the device when it joins, and approves it.

Join tokens name the `address` setting of the controller with the API port.
Without `address`, they name the listen address, or the host that the browser or CLI used.
Set `address` in `controller.json`, or `SYNOD_ADDRESS`, when devices reach the controller at another address.

A device must connect and report its facts before you can approve it.
A token from `node join-token` joins one device. It is valid for 24 hours. Use `--ttl` to change this, up to 168 hours.

### Rejoin and renew

`synod node join-token --device DEVICE` makes a token for one existing device. Use it:

- with `synod agent join` when the device lost its agent state. The device keeps its ID, destinations, and apps. Its old key stops working. Approve it again.
- with `synod agent renew` when the device certificate expired. The device keeps its key and approval.

### Certificates

Device and controller transport certificates last 30 days. The API certificate lasts one year.
Synod renews each certificate when less than 10 days remain.
A device that is offline for more than 20 days can miss its renewal. Its agent then stops. Use `synod agent renew`.

### Agent in Docker

The agent image is `ghcr.io/sirrobot01/synod`.
The container needs:

| Setting | Why |
| --- | --- |
| `privileged: true` | Filesystem snapshots and network checks inside app containers. |
| `pid: host` | Host checks read paths and sockets as the Docker daemon sees them. Without it, a restore of an app with host paths or published ports stops with `host_requirements_unverified`. |
| `network_mode: host` | Other agents can reach its destination service, and host port checks see the host ports. |
| Docker socket, `/var/lib/docker`, and `/var/lib/synod` mounts | Access to Docker, volume data, and agent state. |
| App directories at the same path as on the host | Docker reads restored bind paths on the host, so the paths must agree. |

`SYNOD_JOIN_TOKEN` joins the device on its first start. Later starts ignore it.
`SYNOD_JOIN_TOKEN_FILE` names a file with the token instead. The agent waits until the file exists. An agent in the same Compose file as the controller uses `/var/lib/synod/controller/join.token`.

## App discovery

The agent scans Docker once a minute, including stopped containers.
A Compose project is one app. A standalone container is one app.
Discovery reports paths, sizes, and image references. It never sends file contents or environment values to the controller.

```sh
synod app list
```

Each path has a status:

| Status | Meaning | Backed up |
| --- | --- | --- |
| Included | App files or state. | Yes |
| Dumped | A database data path. A native dump replaces it. | As a dump |
| Large | Media, or more than 20 GiB. | Only with `--include-large` |
| Remote | A network share, such as NFS, SMB, sshfs, or rclone. | Only with `--include-large` |
| Never | A host system path or a mount without host data. | No |

- A filesystem mounted below an app path is listed as its own path. Backups stop at each mount point.
- Local FUSE pools, such as mergerfs and the Unraid user share, are local, not Remote.
- A size scan stops after five seconds, 100,000 entries, or 20 GiB. An incomplete size shows as unknown or as a minimum.
- A measured size is kept for 10 minutes.
- Synod detects PostgreSQL, MySQL, MariaDB, and Redis from the image name. It reads data paths from `PGDATA`, `-D`, `--datadir`, and `--dir`.

## Destinations

A destination is an encrypted repository.

| Type | Stores backups | Example |
| --- | --- | --- |
| `device` | In a directory on another Synod device, served on port 8445 | `synod destination add peer --type device --device odin --path /srv/synod-backups` |
| `local` | In a directory on one device, for that device only | `synod destination add home --device server --path /srv/backups` |
| `sftp` | On an SFTP server | `synod destination add remote --type sftp --host backup.example --user synod --path /srv/synod --known-hosts-file ./known_hosts --credentials-file ./sftp.json` |
| `s3` | In an S3-compatible bucket | `synod destination add cloud --type s3 --endpoint https://s3.example --bucket backups --prefix synod/ --credentials-file ./s3.json` |

A local destination does not protect a device against its own loss.
A Mac cannot hold a `device` or `local` destination.

The credentials file is private JSON with mode `0600`:

| Field | For |
| --- | --- |
| `ssh_key` | SFTP. The full private key text. |
| `access_key_id`, `secret_access_key`, `session_token` | S3. `session_token` is optional. |
| `password` | Optional. A custom repository password. Without it, the recovery kit password is used. Keep a custom password with the kit. |

Adding a destination saves settings only. It does not test the connection.
`synod destination list` shows when each agent received the settings, the free space, and the maintenance device.
Removing a destination keeps its repository data. You cannot remove a destination that a plan or a running backup uses.

A `device` directory must be empty or hold a Synod repository.
The destination service gives access only to approved devices and only to that directory.

## Protection plans

```sh
synod protect --all --destination peer
synod protect --all --destination peer --confirm REVISION
synod protect notes --destination peer --destination cloud --at 03:30 --time-zone Africa/Lagos
synod protect notes --off
```

Each command shows a preview. Run it again with `--confirm REVISION` to save it.
`--all` also sets the plan for new apps.
An app that you turn off stays off when it is found again.
Flags that you omit keep their saved values.

| Option | Default |
| --- | --- |
| `--destination NAME` | Required for a new plan. Repeat it for more destinations. |
| `--at HH:MM`, `--time-zone ZONE` | `03:00`, `UTC` |
| `--method auto\|dump\|live\|stop` | `auto` |
| `--keep-daily`, `--keep-weekly`, `--keep-monthly` | 7, 4, 6 |
| `--include-large PATH`, `--exclude-large PATH` | Large and Remote paths are skipped. |
| `--device DEVICE` | Selects the app when its name exists on more than one device. |
| `--off` | Stops future backups. Keeps snapshots. |

A new plan backs up at once, then each day at the saved time.
A device that was offline takes one backup when it returns. It does not repeat each missed day.
Retention always keeps the newest backup.

To back up now:

```sh
synod backup notes --destination peer
```

`--wait=false` returns at once. Stopping the command does not cancel the backup.

## Backup methods

| Method | Behaviour |
| --- | --- |
| `auto` | `dump` for apps with a detected database, `live` for other apps. Never stops an app. |
| `dump` | Native database dumps, and a live copy of the other files. |
| `live` | A copy of the files while the app runs. |
| `stop` | A live copy, then a short stop to copy the changed files again. Database containers keep running for their dumps. |

### Live copies

- On btrfs, ZFS, and LVM (ext4 or XFS), Synod copies from a read-only filesystem snapshot. All files in a snapshot come from one moment.
- On other filesystems, Synod reads files in place. It copies a changed file again, up to three times. A file that still changes is saved as read, and the backup reports `files_changed_during_copy`.
- SQLite databases are copied with the SQLite backup API.
- Synod skips a file whose size, modification time, mode, and owner did not change, as rsync does.

LVM snapshots need the LVM tools and free space in the volume group: 10% of the volume, from 64 MiB to 1 GiB.

### Database dumps

| Database | Tool |
| --- | --- |
| PostgreSQL | `pg_dumpall` |
| MySQL, MariaDB | `mysqldump` or `mariadb-dump` with `--single-transaction` |
| Redis | `redis-cli --rdb` |

- Dumps go directly into the encrypted repository. No plaintext dump is written to disk.
- Synod uses the official image's administrator variables, including `_FILE` variants. Redis uses `REDISCLI_AUTH`, `REDIS_PASSWORD`, or `--requirepass`.
- The database container must run and have `sh`, `timeout`, and the native client.
- A dump has a 30-minute limit. A failed dump fails the backup.
- MySQL and MariaDB dumps are consistent for InnoDB only. PostgreSQL dumps each database separately.

When one filesystem snapshot holds all app data and all database data, Synod also copies the database files from it.
A restore then uses these files, as after a power cut, and keeps the dumps as a fallback.

### Images

Each backup saves the image of each container.
For a registry image, Synod saves the amd64 and arm64 variants, so a restore works on both.
For a local-only image, or when the registry is unavailable, Synod saves the source architecture only.
An unchanged image is not copied again.

## Protection status

```sh
synod app status
```

An app is **protected** when its last good backup has no limits and a restore test passed.
Otherwise it is **partial**, with one code for each limit:

| Limit | Cause | Fix |
| --- | --- | --- |
| `app_consistency_unverified` | The app data is not in one filesystem snapshot. | Put the app data and Docker volumes on one btrfs subvolume, ZFS pool, or LVM volume. |
| `data_excluded` | Large paths, network shares, or files over 20 GiB were skipped. | `synod protect APP --include-large PATH` |
| `image_platforms_limited` | Only the source architecture of an image is saved. | Back up again with registry access. |
| `host_requirements_unverified` | The agent could not record the host ports or devices of a host-network service. | Run the agent as root. |
| `container_layer_data` | A container keeps more than 64 MiB in its writable layer. Backups do not copy it. | Move the data to a volume or bind mount. |
| `restore_unproven` | No restore test passed all checks. | Wait for the test, or give each service a health check or a port. |

Other status codes:

| Code | Meaning |
| --- | --- |
| `catalog_write_failed` | The backup did not save the current recovery settings. The next backup retries. |
| `retention_failed` | The backup did not remove expired backups. The next backup retries. |

## Restore files

```sh
synod snapshot list notes
synod file list notes
synod file list notes /srv/notes
synod file get notes /srv/notes/config.yml --output ./config.yml
synod file get notes /srv/notes --output ./notes.tar
synod file get notes @databases/notes-db.sql --output ./notes-db.sql
```

- Use the original absolute path. Database dumps are under `@databases`.
- A directory downloads as a tar archive with owners, modes, and times.
- The output file must not exist. A stopped download leaves no partial file.
- Options: `--snapshot`, `--destination`, `--device`, and `--via DEVICE` to read through a specific online device.
- The web UI can download files up to 64 MiB.

## Restore an app

```sh
synod restore notes --to odin
synod restore notes --to server --snapshot SNAPSHOT_ID
synod restore notes --to server --replace
```

The default is the newest backup in all destinations that the target can read.
Use `--destination` to read one destination.

### Checks before restore

Synod changes nothing on the target until these checks pass:

- Each image has a saved archive for the target architecture.
- No other container uses an app container name or the project name.
- No running container publishes a port of the app.
- Existing networks with saved names use the saved driver.
- Host devices and host paths that the backup did not copy exist with the saved type, owner, and permissions.
- For host-network apps, the saved TCP and UDP ports are free.

### Result on the target

| Item | Location |
| --- | --- |
| Project files | `/var/lib/synod/apps/PROJECT/project` |
| Bind mount data | `/var/lib/synod/apps/PROJECT/data` |
| Compose file | `/var/lib/synod/apps/PROJECT/compose.json` |
| Volumes | New volumes named `synod-OPERATION_ID-volumeN` |

If `apps/PROJECT` exists, the directory gets the first eight characters of the operation ID, for example `PROJECT-1a2b3c4d`.

- Images load from the repository. Restore does not use a registry.
- Database dumps load into the new data before the app starts.
- Compose starts the app from `compose.json`. It does not read the app's `.env` file, because `compose.json` holds all settings.
- Skipped Large and Dumped paths become empty directories. A skipped network share stays at its original host path, so mount the same share on the target.
- After the start, Synod runs the same checks as a restore test. A failed check does not undo the restore. It adds the warning `restore_check_failed`.
- A failed restore removes what it created. The warning `cleanup_pending` means that some items remain.

### Replace

With `--replace`, Synod:

1. prepares the restore beside the running app,
2. takes a safety backup of the running app,
3. removes the old containers and starts the restored app.

The old volumes, bind paths, and project directory stay on the device.
If the restored app does not start, Synod removes it and starts the old containers again.

## Restore tests and repository checks

Each protected app gets a restore test each month. Each destination gets a content check each week.

```sh
synod restore notes --to server --drill
synod destination check peer --on server
```

A restore test restores the latest backup into a temporary copy on an internal network with no route out.
The source app keeps running.
After 30 seconds, it runs these checks:

| Check | Passes when |
| --- | --- |
| `data_files` | Each restored path has the saved file count and size. |
| `database_content` | Each saved database exists and still has tables or keys. |
| `container_health` | A container with a Docker health check becomes healthy. |
| `port_tcp` | A published or exposed port accepts a connection. |
| `http_response` | An HTTP port answers with a status below 500. |
| `app_function` | Unchecked when a service has no health check and no port. |

A failed check fails the test. An unchecked check leaves the proof incomplete.
To make a service fully testable, give it a health check or a port.
The test then removes all temporary containers, volumes, and directories.

A test refuses privileged containers, host devices, shared host namespaces, added capabilities, and host paths that the backup did not copy.

A content check reads a random 1% of the repository and verifies it.

## Notifications

Synod sends alerts through [Shoutrrr v0.20.0](https://shoutrrr.nickfedor.com/v0.20.0/services/overview/): ntfy, Gotify, Discord, Slack, email, and webhooks.

Write the settings to a private file:

```json
{
  "url": "ntfy://ntfy.example.com/private-topic",
  "overdue_grace_hours": 24,
  "missing_after_hours": 168,
  "full_percent": 90
}
```

```sh
chmod 600 notification.json
synod notification configure --file notification.json
synod notification test
synod notification status
synod notification disable
```

| Alert | When |
| --- | --- |
| Backup or restore test failed | Once for each failed operation. |
| Backup overdue | A device is online and a backup is late by more than the grace period. |
| Destination almost full | A `device` or `local` destination passes `full_percent`. S3 and SFTP space is unknown. |
| Device missing | A device has not connected for `missing_after_hours`. |
| Maintenance failed | Repository maintenance failed. |

An alert is sent once until its condition clears.
The URL is stored encrypted and never appears in status output or logs.

## Recover after controller loss

You need the recovery kit password and one surviving destination.
Stop the old controller and agents first, if they still run.

For a controller in Docker, set `SYNOD_FROM_REPOSITORY` and `SYNOD_PASSWORD_FILE` on its first start. See [Controller in Docker](#controller-in-docker).
The paths are paths inside the container, so mount the repository and the password file.

For a controller as a service, run init on the device that has the repository files:

```sh
synod server init --from-repository /srv/synod-backups --password-file /root/repository-password
```

For S3 or SFTP, give a private connection file instead of the directory:

```json
{
  "location": {
    "kind": "s3",
    "s3": {"endpoint": "https://s3.example.com", "bucket": "backups", "prefix": "synod"}
  },
  "credentials": {
    "access_key_id": "ACCESS_KEY",
    "secret_access_key": "SECRET_KEY"
  }
}
```

- Password files and connection files must have mode `0600`.
- If several destinations were saved, use `--repository-destination NAME` to select the surviving one.
- Recovery restores the installation ID, devices, apps, destinations, and plans.
- It does not restore user accounts, API tokens, device approvals, or activity history.

Then join each device again with a new agent directory:

```sh
synod node join-token --device DEVICE
synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent
synod node approve DEVICE
```

Restored apps in `/var/lib/synod/apps` keep running.
An app stays missing until discovery finds it again. You can restore a missing app.

## Repository

Synod uses [Kopia](https://kopia.io) v0.23.1 repositories.

- Encryption is ChaCha20-Poly1305. Hashes are BLAKE2b.
- Each repository also holds an encrypted recovery catalog: devices, apps, destinations, and plans. Each backup updates it.
- A repository belongs to one installation. Synod refuses a repository of another installation.

### Space

Retention removes expired backups after each backup.
Daily maintenance then frees the space that no kept backup uses.
Space returns after about three daily runs.

One device runs maintenance for each destination:

- For `device` and `local`, the device that holds the repository.
- For S3 and SFTP, the first approved device that connected in the last seven days. Another device takes over after seven days without a connection.

## Accounts and API

### First account

Until an account exists, the web UI shows a setup page to the first visitor.
It creates an administrator account, signs in, and shows the recovery kit once.
After that, the setup page refuses all requests.
Anybody who reaches the controller before you can use the setup page. Start the controller on a private network.
`synod user create` also creates the first account. The setup page then stays closed.

### Other accounts and tokens

```sh
synod user create alice
synod user set-password USER_ID
synod user disable USER_ID
synod token create "backup automation"
synod token list
synod token revoke TOKEN_ID
```

A token secret is shown once.
Keep the administrator token from init private. Use it to recover access.
For a browser on another address, set `--public-origin` on the controller.

The API schema is in [api/openapi.json](../api/openapi.json). It does not yet describe accounts and sessions.

## Logs

The controller and each agent write one line for each event.

| Event | Where |
| --- | --- |
| Start and result of each backup, restore, restore test, repository check, and maintenance run, with the app or destination, the duration, and on failure the code and its cause | Agent |
| Each operation result, with the app, the device, and the code | Controller |
| Connection to the controller: connected, lost, or cannot connect | Agent |
| A refused or failed API request, with method, path, status, and reason | Controller |
| Agent and controller start | Both |

A failure that repeats, such as a controller that is not reachable, is logged again only when its reason changes or after five minutes.
Logs never hold passwords, tokens, keys, notification URLs, or file contents.

| Setting | Controller | Agent |
| --- | --- | --- |
| Level: `debug`, `info`, `warn`, or `error` | `--log-level`, `SYNOD_LOG_LEVEL`, `log_level` | `--log-level`, `SYNOD_LOG_LEVEL`, `log_level` in `agent.json` |
| Format: `text` or `json` | `--log-format`, `SYNOD_LOG_FORMAT`, `log_format` | `--log-format`, `SYNOD_LOG_FORMAT`, `log_format` in `agent.json` |

The default is `info` and `text`. The `debug` level adds the time of each backup step.
A text line looks like this:

```text
time="2026-10-14 03:12:41" level=INFO msg="backup finished" app=compose:immich destination="odin disk" duration=41.2s operation=… method=dump bytes=197834112 files_read=12 files_unchanged=40211
```

## Limits

- Agents run on Linux only. Docker Desktop is not supported.
- One controller for each installation.
- A Mac controller is not reachable while the Mac sleeps. Agents continue their schedules.
- MongoDB and databases with custom connection settings have no native dumps. Their files are copied live.
- A restore does not change saved host addresses, device paths, or host mounts to fit a different device.
- Host port checks cover the ports that the app used during the backup.
- An image restores only on an architecture that the backup saved.
- A restore test without network access cannot prove features that need the internet or the LAN.
- A host-network service that listens on one host address cannot start in a restore test. Its function check is unchecked.
- A program that rewrites a file and keeps its size and modification time defeats change detection.
- An agent in Docker uses filesystem snapshots only when the whole filesystem is mounted at the same path. The image has no ZFS tools.
- A backup copy that runs for more than 24 hours can fail, because maintenance can remove its new data. The next backup copies the data again.
- Maintenance runs in the agent job queue. A long run delays other jobs on that device.
- The web UI downloads files up to 64 MiB. Use `synod file get` for larger files.
