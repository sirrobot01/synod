# User guide

This guide sets up Synod, protects your apps, and restores them.
The [reference](reference.md) gives all settings and limits.

Run all commands as root (`sudo -i`).
The examples use `192.168.0.20` for the controller. Use your own address.

## Before you start

You need:

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- One Linux device or Mac for the controller. A Linux controller can also run apps.
- A second device or a storage service for your backups. A backup on the same device does not survive the loss of that device.

Open these ports on your private network:

| Port | Use |
| --- | --- |
| 8443 | Web UI and API |
| 8444 | Agent connections to the controller |
| 8445 | Backups stored on another Synod device |

## 1. Install

On each device:

```sh
curl -fsSL https://synod.biodun.dev/install.sh | sh
synod --version
```

The installer checks the download and installs `/usr/local/bin/synod`. It does not start a service.

## 2. Create the controller

On one device:

```sh
synod server init
```

Init starts the controller as a service. The service starts again after a reboot.
To use another address than the default route address, add `--address 192.168.0.20`.

Init prints a **recovery kit** with the repository password.
Without this password, nobody can read your backups.

1. Store the kit in a password manager or on paper.
2. Delete `recovery-kit.txt` from the controller directory.

## 3. Join your devices

On each Linux device with apps, join with a token:

```sh
synod agent join 'TOKEN'
```

Use the token from init for the first device.
For each other device, run `synod node join-token` on the controller.
Each token joins one device.

Then approve the devices on the controller:

```sh
synod node list
synod node approve DEVICE
```

To run the agent in a container, for example on Unraid or Synology, see [Run the agent in Docker](#run-the-agent-in-docker).

## 4. Sign in to the web UI

```sh
synod user create admin
```

Open the address that init printed, for example `https://192.168.0.20:8443`.
The controller uses its own certificate authority.
To remove the browser warning, trust `api-ca.crt` from the controller directory.

You can do the next steps in the web UI or with the CLI.

## 5. Add a destination

A destination stores your encrypted backups.
To store them in a directory on another device:

```sh
synod destination add peer --type device --device DEVICE --path /srv/synod-backups
```

For S3 or SFTP, see [Destinations](reference.md#destinations).

## 6. Protect your apps

Synod finds your apps within one minute.

```sh
synod app list
synod protect --all --destination peer
```

The command shows a preview. Check that your app data is **Included** or **Dumped**.
Large paths, such as media, and network shares are skipped.
To include one, add `--include-large PATH`.

Save the plan with the revision from the preview:

```sh
synod protect --all --destination peer --confirm REVISION
```

Synod backs up each app at once, then every day at 03:00 UTC.
It keeps 7 daily, 4 weekly, and 6 monthly backups.
Apps keep running during a backup.
New apps get the same plan.

Check the result:

```sh
synod app status
```

An app shows **protected** when its last backup is complete and a restore test passed.
Otherwise it shows **partial** and the reason.
See [Protection status](reference.md#protection-status).

## 7. Set up alerts (optional)

1. In the web UI, open **Settings**.
2. Enter a [Shoutrrr URL](https://shoutrrr.nickfedor.com/v0.20.0/services/overview/), for example for ntfy, Discord, or email.
3. Send a test notification.

Synod alerts you about failed backups and restore tests, overdue backups, full destinations, and missing devices.

## Restore a file

```sh
synod file list APP
synod file get APP /saved/path/config.yml --output ./config.yml
```

For an older copy, add `--snapshot SNAPSHOT_ID`. List the copies with `synod snapshot list APP`.

## Restore an app

Restore on any approved device, also after the original device is lost:

```sh
synod restore APP --to DEVICE
```

Synod restores the newest backup. It needs no registry access.
It checks images, names, and ports before it changes anything.
The restored app goes into `/var/lib/synod/apps/APP` on the target.

To replace an app that exists on the target, add `--replace`.
Synod first takes a safety backup of that app.

In the web UI, use **Snapshots & restore**.

## Recover after you lose the controller

You need the recovery kit and one surviving destination.

1. On the new controller device, write the password to a private file:

   ```sh
   umask 077; printf '%s' 'PASSWORD' > "$HOME/repository-password"
   ```

2. Create the controller from the repository:

   ```sh
   synod server init --from-repository /srv/synod-backups --password-file "$HOME/repository-password"
   ```

   For S3 or SFTP, see [Recover after controller loss](reference.md#recover-after-controller-loss).

3. On the device with the repository, join with the token that init printed and a new agent directory:

   ```sh
   synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent
   ```

4. For each other device, run `synod node join-token --device DEVICE` on the controller. Join the device with that token and a new `--dir`.
5. Approve each device. Then restore your apps.

Restored apps keep running, because they are outside the agent directory.

## Upgrade

Run the installer again on each device. Start with the controller device:

```sh
curl -fsSL https://synod.biodun.dev/install.sh | sh
```

The installer restarts the Synod services.
Do not run `server init` or `agent join` again.

## Run the agent in Docker

Use the agent image on a device without systemd, such as Unraid or Synology.
Create `compose.yaml`:

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
      # Add each directory with app files, at the same path.
      - /srv:/srv
```

Replace `TOKEN` with a join token. Then start it and approve the device:

```sh
docker compose up -d
```

Mount each directory with Compose projects or bind mounts at the same path as on the host.
For example, mount `/mnt/user/appdata` on Unraid or `/volume1/docker` on Synology.
A backup fails with `source_unavailable` when a path is not mounted.

Keep `privileged`, `pid: host`, and `network_mode: host`.
The agent needs them for filesystem snapshots, host port checks, and restore tests.

To upgrade, run `docker compose pull && docker compose up -d`.

## Fix a device

**A device lost its agent state**, for example after a reinstall.
Run `synod node join-token --device DEVICE` on the controller.
Join the device with that token, then approve it again.
It keeps its ID, destinations, and apps.

**An agent stopped with a certificate expiry message.**
This happens when a device was offline for more than 20 days.
Run `synod node join-token --device DEVICE` on the controller. Then run this on the device:

```sh
synod agent renew 'TOKEN'
```

For an agent in Docker:

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

Logs:

| Where | Command |
| --- | --- |
| Linux agent | `journalctl -u synod-agent -n 100 --no-pager` |
| Linux controller | `journalctl -u synod-server -n 100 --no-pager` |
| Agent in Docker | `docker logs --tail 100 synod-agent` |
| macOS controller | `tail -n 100 "/Library/Application Support/Synod/controller/controller.log"` |

| Problem | Action |
| --- | --- |
| A device does not connect | Check its address, port 8444, clock, and agent log. |
| `source_unavailable` | Run the agent as root. In Docker, mount the missing path. |
| `repository_open_failed` | Check that the destination is online. Then back up again. |
| `files_changed_during_copy` | Files changed during the copy and were saved as read. If the app needs an exact copy, use `synod protect APP --method stop`. |
| `repository_installation_conflict` | The directory holds another installation's repository. Use an empty directory. |
| `image_platform_unsupported` | Restore on a device with a supported architecture. |
| A port or name conflict | Select another target, or add `--replace` to replace the existing app. |
| `host_requirements_unverified` | Run the agent as root on the Docker host. Back up again while the app runs. |
| `host_port_unavailable` | Use a target where the saved host address and port are free. |
| `device_unavailable` or `mount_unavailable` | Provide the saved device or host path on the target, with the same type, owner, and permissions. |

Do not send passwords, tokens, or private keys with a support report.
