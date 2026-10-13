# Synod user guide

This guide sets up Synod, protects your apps, and restores them.
For the full behaviour of each feature, read the [reference](reference.md).

## Before you start

You need:

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- One Linux device or Mac for the controller. A Linux controller can also run apps.
  A Mac controller needs no Docker. It manages agents on Linux devices.
- A second device or storage service for your backups. A copy on the same device does not survive the loss of that device.

Open these ports on your private network:

| Port | Use |
| --- | --- |
| 8443 | Web UI and API |
| 8444 | Agent connections to the controller |
| 8445 | Backups stored on another Synod device |

Run all commands as root (`sudo -i`).
The examples use `192.168.0.20` for the controller. Use your own address.

## 1. Install

Run this on each device:

```sh
curl -fsSL https://synod.biodun.dev/install.sh | sh
synod --version
```

The installer gets the latest release, checks its checksums, and installs `/usr/local/bin/synod`.
It does not start any service.
To select a version, add `-s -- --version v0.1.0` after `sh`.

## 2. Create the controller

On the device that runs the controller:

```sh
synod server init
```

On Linux, init creates `/var/lib/synod/controller` and starts the `synod-server` systemd service.
On macOS, it creates `/Library/Application Support/Synod/controller` and starts the `dev.biodun.synod.server` launchd service.
Both services start again after a reboot.
It uses the address of the device's default network route. To select another address, add `--address 192.168.0.20`.

Init prints a **recovery kit**. It holds the repository password.
Store the kit in a password manager or on paper, away from your devices.
Then delete `recovery-kit.txt` from the controller directory.
Without this password, nobody can read your backups.

Run `synod` commands on the controller as root. They find the controller settings by themselves.

## 3. Join your devices

Init also prints a join token. Run this on the first Linux Docker device:

```sh
synod agent join 'TOKEN'
```

Join starts the `synod-agent` service.
For each other device, create a token on the controller and join with it:

```sh
synod node join-token
```

Then approve each device:

```sh
synod node list
synod node approve DEVICE
```

A token joins one device and expires after seven days.

### Run the agent in Docker

Use the agent image on a device without systemd, such as Unraid or Synology.
You do not need to install the binary on that device.
Create a `compose.yaml` file:

```yaml
services:
  synod-agent:
    image: ghcr.io/sirrobot01/synod:latest
    container_name: synod-agent
    restart: unless-stopped
    privileged: true
    pid: host
    network_mode: host
    environment:
      SYNOD_JOIN_TOKEN: "TOKEN"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/lib/docker:/var/lib/docker
      - /var/lib/synod:/var/lib/synod
      # Add each directory that holds app files, at the same path.
      - /srv:/srv
```

Replace `TOKEN` with a join token. Then start the agent:

```sh
docker compose up -d
```

The agent joins on its first start. Later starts ignore the token.
Approve the device as for any other device.

Mount every directory at the same path as on the host.
Docker reads restored bind paths on the host, so the paths must agree.
Add each directory that holds Compose projects or bind mounts, for example `/mnt/user/appdata` on Unraid or `/volume1/docker` on Synology.
A backup of an app fails with `source_unavailable` when one of its paths is not mounted.

The container needs these settings:

- `privileged` lets the agent take filesystem snapshots and enter container namespaces for drill checks.
- `pid: host` lets the agent find app processes and their listening ports.
- `network_mode: host` lets other agents reach a destination on this device and lets host checks see the host ports.

To upgrade the agent, pull the new image and start the container again:

```sh
docker compose pull && docker compose up -d
```

## 4. Open the web UI

```sh
synod user create admin
```

Open the address that init printed, such as `https://192.168.0.20:8443`, and sign in.
The controller uses its own certificate authority.
To remove the browser warning, trust `api-ca.crt` from the controller directory on your computer.

You can do the next steps in the web UI or with the CLI.

## 5. Add a destination

A destination stores your encrypted backups.
To keep backups in a directory on another device:

```sh
synod destination add peer --type device --device DEVICE --path /srv/synod-backups
```

For S3 or SFTP, see [Destinations](reference.md#destinations).
A new destination uses the password from your recovery kit.

## 6. Protect your apps

Synod finds your apps within one minute.

```sh
synod app list
synod protect --all --destination peer
```

The command shows a preview. Check that your app data is **Included** or **Dumped**.
**Large** paths, such as media, are skipped. Add one with `--include-large PATH`.
**Remote** paths are network shares, such as an NFS or SMB mount from a NAS. They are skipped, because their data already lives on the NAS. Add one with `--include-large PATH`. A restore attaches a skipped share at its original path, so mount the same share on the target device.
Then confirm with the revision from the preview:

```sh
synod protect --all --destination peer --confirm REVISION
```

Synod backs up each app at once, then every day at 03:00 UTC.
It keeps 7 daily, 4 weekly, and 6 monthly copies.
The default backup method never stops or pauses your apps.
Databases (PostgreSQL, MySQL, MariaDB, Redis) are saved with native dumps while they run.
Other data is copied while the app runs.
On supported btrfs, ZFS, and LVM filesystems, Synod copies from filesystem snapshots.
Each filesystem has its own capture time.
On other disks, Synod reads a file again when it changed during the copy.
An app gets full protection when one filesystem snapshot holds all of its data.
This includes the database data directories.
Synod then also copies the database files from that snapshot.
A restore uses these files, as after a power cut. The dumps stay as a fallback.
Put the app data and the Docker volumes on one btrfs subvolume, ZFS dataset, or LVM volume to get full protection.
Other copies have partial protection.
Synod does not pause writes to gain full-protection status.
New apps receive the backup schedule automatically.

Check the status:

```sh
synod app status
```

Each month, Synod tests a restore of each app. Each week, it checks part of each destination.

## 7. Set up alerts (optional)

In the web UI, open **Settings → Notifications**.
Enter a [Shoutrrr URL](https://shoutrrr.nickfedor.com/v0.20.0/services/overview/), for example for ntfy, Discord, Slack, or email.
Send a test notification.
Synod alerts you about failed backups and restore tests, overdue backups, full destinations, and missing devices.

## Restore

### Restore a file

```sh
synod file list APP
synod file get APP /saved/path/config.yml --output ./config.yml
```

Use `--snapshot SNAPSHOT_ID` for an older copy. List the copies with `synod snapshot list APP`.

### Restore an app

Restore on any approved device, also after the original device is lost:

```sh
synod restore APP --to DEVICE
```

Synod restores the newest copy. It checks images, names, and ports before it changes anything.
Saved images come from the repository. A restore does not need the registry or an existing image cache.
To overwrite an existing app, add `--replace`. Synod first takes a safety backup of that app.

In the web UI, use **Snapshots & restore**.

### Recover after you lose the controller

You need the recovery kit and one surviving destination.
On the new controller device:

```sh
umask 077; printf '%s' 'PASSWORD_FROM_THE_KIT' > "$HOME/repository-password"
synod server init --from-repository /srv/synod-backups --password-file "$HOME/repository-password"
```

Use the repository directory for `--from-repository`.
For S3 or SFTP, see [Recover after controller loss](reference.md#recover-after-controller-loss).
Init prints a join token for the device with the repository.
For each other device, create a token on the controller:

```sh
synod node join-token --device DEVICE
```

Join each device again with a new agent directory. Do not reuse the old one:

```sh
synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent
```

Approve each device. Then restore your apps.

## Upgrade

Run the install command again on each device:

```sh
curl -fsSL https://synod.biodun.dev/install.sh | sh
```

It installs the latest release and restarts the Synod services on that device.
Upgrade the controller device first.
Do not run `server init` or `agent join` again. Your devices keep their identities.
For an agent that runs in Docker, see [Run the agent in Docker](#run-the-agent-in-docker).

## Certificates

Synod renews its certificates automatically.
A device that was offline for more than 20 days can miss its renewal. Its agent then stops.
To fix it, run `synod node join-token --device DEVICE` on the controller. Then run this on the device:

```sh
synod agent renew 'TOKEN'
```

Renew stops the agent, gets a new certificate, and starts the agent again.
For an agent in Docker, stop the container and renew in a new container:

```sh
docker compose stop
docker compose run --rm synod-agent agent renew 'TOKEN'
docker compose up -d
```

## Troubleshoot

```sh
synod app status
synod operation list
synod operation get OPERATION_ID
```

On Linux, read the service logs:

```sh
journalctl -u synod-agent -n 100 --no-pager
journalctl -u synod-server -n 100 --no-pager
```

For an agent in Docker, read the container log:

```sh
docker logs --tail 100 synod-agent
```

On macOS, read the controller log:

```sh
tail -n 100 "/Library/Application Support/Synod/controller/controller.log"
```

| Problem | Action |
| --- | --- |
| Device does not connect | Check its address, port 8444, clock, and agent log. |
| `source_unavailable` | Read the `backup_source_unavailable` line in the agent log. Run the agent as root. |
| `repository_open_failed` | Check that the destination is online. Run another backup. |
| `files_changed_during_copy` | Some files kept changing during the backup. They were saved as read. The snapshot lists them. If the app needs an exact copy, use `synod protect APP --method stop`. |
| `repository_installation_conflict` | The repository belongs to another installation. Use an empty directory. |
| Image refused for an architecture | Restore on a device with a matching architecture. |
| Port or name conflict | Select a target with a free port and name. Use `--replace` only to replace the existing app. A host process can also hold a port. |
| `host_requirements_unverified` | Run the agent as root on the Docker host. Take a new backup while the app runs. |
| `host_port_unavailable` | Use a target with the saved host address and port. |
| `device_unavailable` | Connect the required device at the saved path. Its type, device number, permissions, and owner must match. |
| `mount_unavailable` | Provide the required Never mount or external volume on the target. A bind source must have its saved type, permissions, and owner. |

Do not send passwords, tokens, or private keys with a support report.

## Current limits

- Agents require Linux. Docker Desktop on macOS and Windows is not supported.
- Keep the Mac awake for access to its controller. Agents keep their saved backup schedules while it sleeps.
- One controller per installation.
- Host-network restore needs verified TCP and UDP port records. The target must provide the saved host addresses, devices, and Never mounts.
- An image can be restored only on a platform whose archive is saved in the repository.
- MongoDB and databases with custom connection settings have no native dumps.
- Browser downloads are limited to 64 MiB. Use `synod file get` for larger files.
- A restore test without network access cannot prove every app feature.
- An agent in Docker uses a filesystem snapshot only when the whole filesystem is mounted at the same path in the container. Otherwise it copies live and the app shows `app_consistency_unverified`.
- The agent image has btrfs and LVM tools. It has no ZFS tools, so an agent in Docker does not take ZFS snapshots.

The [reference](reference.md) lists all limits.
