# User guide

This guide sets up Synod, protects your apps, and restores them.
The [reference](reference.md) gives all settings and limits.

The examples use `192.168.0.20` for the first device. Use your own address.

## Before you start

You need:

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- A second device or a storage service for your backups. A backup on the same device does not survive the loss of that device.

Open these ports on your private network:

| Port | Use |
| --- | --- |
| 8443 | Web UI and API |
| 8444 | Agent connections to the controller |
| 8445 | Backups stored on another Synod device |

## 1. Start Synod

On your first device, create `compose.yaml`:

```yaml
services:
  synod:
    image: ghcr.io/sirrobot01/synod:latest
    container_name: synod
    command: server
    restart: unless-stopped
    network_mode: host
    volumes:
      - /var/lib/synod/controller:/var/lib/synod/controller

  synod-agent:
    image: ghcr.io/sirrobot01/synod:latest
    container_name: synod-agent
    restart: unless-stopped
    privileged: true
    pid: host
    network_mode: host
    environment:
      SYNOD_JOIN_TOKEN_FILE: /var/lib/synod/controller/join.token
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/lib/docker:/var/lib/docker
      - /var/lib/synod:/var/lib/synod
      # Add each directory with app files, at the same path.
      - /srv:/srv
```

Mount each directory with Compose projects or bind mounts at the same path as on the host.
For example, mount `/mnt/user/appdata` on Unraid or `/volume1/docker` on Synology.

Start it:

```sh
docker compose up -d
```

On its first start, the `synod` controller creates your installation.
The `synod-agent` joins it with the token that the controller writes.

## 2. Create your account

Open `https://192.168.0.20:8443`.
The controller uses its own certificate authority, so the browser shows a warning.
To remove it, trust `/var/lib/synod/controller/api-ca.crt` on your computer.

1. Create the administrator account.
2. Save the **recovery kit**. It holds the password of your backups. Without it, nobody can read your backups.
   Store it in a password manager or on paper, away from your devices.
   Synod shows the kit only once. A copy stays in `/var/lib/synod/controller/recovery-kit.txt` until you delete it.
3. In **Devices**, approve your device.

## 3. Add your other devices

On the controller device, create a join token for each other device:

```sh
docker exec synod synod node join-token
```

On the other device, use the `synod-agent` service from step 1.
Replace `SYNOD_JOIN_TOKEN_FILE` with `SYNOD_JOIN_TOKEN: "TOKEN"`.
Then approve the device in **Devices**.

You can do the next steps in the web UI or with the CLI.
To run a CLI command on a controller in Docker, put `docker exec synod` before it.

## 4. Add a destination

A destination stores your encrypted backups.
To store them in a directory on another device:

```sh
synod destination add peer --type device --device DEVICE --path /srv/synod-backups
```

For S3 or SFTP, see [Destinations](reference.md#destinations).

## 5. Protect your apps

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

## 6. Set up alerts (optional)

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
   umask 077; printf '%s' 'PASSWORD' > /root/repository-password
   ```

2. In the `synod` service of `compose.yaml`, add the repository and the password:

   ```yaml
       environment:
         SYNOD_FROM_REPOSITORY: /srv/synod-backups
         SYNOD_PASSWORD_FILE: /run/repository-password
       volumes:
         - /var/lib/synod/controller:/var/lib/synod/controller
         - /srv/synod-backups:/srv/synod-backups
         - /root/repository-password:/run/repository-password:ro
   ```

   For S3 or SFTP, see [Recover after controller loss](reference.md#recover-after-controller-loss).

3. Start it with `docker compose up -d`.
   The controller rebuilds the installation from the repository.
   The `synod-agent` on this device joins again with its old device ID.
4. For each other device, run `docker exec synod synod node join-token --device DEVICE`.
   On that device, delete `/var/lib/synod/agent`, and start the agent with the new token.
5. Approve each device. Then restore your apps.

Restored apps keep running, because they are outside the agent directory.
After the recovery, you can remove the recovery settings from `compose.yaml`.

## Upgrade

On each device, start with the controller device:

```sh
docker compose pull && docker compose up -d
```

## Agent container settings

Keep `privileged`, `pid: host`, and `network_mode: host` on the `synod-agent` service.
The agent needs them for filesystem snapshots, host port checks, and restore tests.

Mount each directory with Compose projects or bind mounts at the same path as on the host.
Docker reads restored bind paths on the host, so the paths must agree.
A backup fails with `source_unavailable` when a path is not mounted.

## Install without Docker

You can also install Synod as a binary with systemd services.
A Mac can run the controller this way, but not the agent.
Run these commands as root.

1. Install Synod on each device:

   ```sh
   curl -fsSL https://synod.biodun.dev/install.sh | sh
   ```

2. On one device, create the controller. It runs as the `synod-server` service:

   ```sh
   synod server init
   ```

   Init prints the recovery kit and a join token.
   To use another address than the default route address, add `--address 192.168.0.20`.

3. On each device with apps, join the agent. It runs as the `synod-agent` service:

   ```sh
   synod agent join 'TOKEN'
   ```

   For each other device, run `synod node join-token` on the controller.

4. Open the address that init printed and continue with [step 2](#2-create-your-account).

Without Docker, run CLI commands directly on the controller.
To upgrade, run the install command again on each device.
To recover, run `synod server init --from-repository /srv/synod-backups --password-file /root/repository-password`.
Then join each device with `synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent`.

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
docker compose stop synod-agent
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
| Controller in Docker | `docker logs --tail 100 synod` |
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
