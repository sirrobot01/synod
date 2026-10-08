# Synod user guide

This guide sets up Synod, protects your apps, and restores them.
For the full behaviour of each feature, read the [reference](reference.md).

## Before you start

You need:

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- One device for the controller. It can also run apps.
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

```sh
synod server init --dir /var/lib/synod/controller --address 192.168.0.20
systemctl enable --now synod-server
```

Init prints a **recovery kit**. It holds the repository password.
Store the kit in a password manager or on paper, away from your devices.
Then delete `recovery-kit.txt` from the controller directory.
Without this password, nobody can read your backups.

Set these variables in each shell where you run `synod` commands on the controller:

```sh
export SYNOD_API_URL=https://192.168.0.20:8443
export SYNOD_CA_FILE=/var/lib/synod/controller/api-ca.crt
export SYNOD_ADMIN_TOKEN_FILE=/var/lib/synod/controller/admin.token
```

## 3. Join your devices

Init also prints a join token. Run this on the first Docker device:

```sh
synod agent join 'TOKEN'
systemctl enable --now synod-agent
```

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

## 4. Open the web UI

```sh
synod user create admin
```

Open `https://192.168.0.20:8443` and sign in.
The controller uses its own certificate authority.
To remove the browser warning, trust `/var/lib/synod/controller/api-ca.crt` on your computer.

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
Then confirm with the revision from the preview:

```sh
synod protect --all --destination peer --confirm REVISION
```

Synod backs up each app at once, then every day at 03:00 UTC.
It keeps 7 daily, 4 weekly, and 6 monthly copies.
Databases (PostgreSQL, MySQL, MariaDB, Redis) are saved with native dumps while they run.
Other apps stop only for a short final copy.
New apps are protected automatically.

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
To overwrite an existing app, add `--replace`. Synod first takes a safety backup of that app.

In the web UI, use **Snapshots & restore**.

### Recover after you lose the controller

You need the recovery kit and one surviving destination.
On a new controller device:

```sh
umask 077; printf '%s' 'PASSWORD_FROM_THE_KIT' > /root/repository-password
synod server init --dir /var/lib/synod/recovered --address 192.168.0.20 \
  --from-repository /srv/synod-backups --password-file /root/repository-password
systemctl enable --now synod-server
```

Use the repository directory for `--from-repository`.
For S3 or SFTP, see [Recover after controller loss](reference.md#recover-after-controller-loss).
Init prints a join token for the device with the repository.
Join each device again with a new agent directory. Do not reuse the old one:

```sh
synod node join-token --device DEVICE          # on the controller, for each other device
synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent
systemctl enable --now synod-agent
```

Approve each device. Then restore your apps.

## Upgrade

Stop the agents, then the controller.
Run the install command again on each device. It installs the latest release.
Start the controller, then the agents.

```sh
systemctl stop synod-agent
systemctl stop synod-server        # controller device only
curl -fsSL https://synod.biodun.dev/install.sh | sh
systemctl start synod-server       # controller device only
systemctl start synod-agent
```

Stop the agents on all devices before you stop the controller.
Do not run `server init` or `agent join` again. Your devices keep their identities.

## Certificates

Synod renews its certificates automatically.
A device that was offline for more than 20 days can miss its renewal. Its agent then stops.
To fix it, run `synod node join-token --device DEVICE` on the controller. Then run this on the device:

```sh
systemctl stop synod-agent
synod agent renew 'TOKEN'
systemctl start synod-agent
```

## Troubleshoot

```sh
synod app status
synod operation list
synod operation get OPERATION_ID
journalctl -u synod-agent -n 100 --no-pager
journalctl -u synod-server -n 100 --no-pager
```

| Problem | Action |
| --- | --- |
| Device does not connect | Check its address, port 8444, clock, and agent log. |
| `source_unavailable` | Read the `backup_source_unavailable` line in the agent log. Run the agent as root. |
| `repository_open_failed` | Check that the destination is online. Run another backup. |
| `repository_installation_conflict` | The repository belongs to another installation. Use an empty directory. |
| Image refused for an architecture | Restore on a device with a matching architecture. |
| Port or name conflict | Stop the conflicting container, or use `--replace`. |

Do not send passwords, tokens, or private keys with a support report.

## Current limits

- Linux only. Docker Desktop on macOS and Windows is not supported.
- One controller per installation.
- Apps with host networking cannot be restored.
- An image that exists only on the lost device cannot be restored elsewhere.
- MongoDB and databases with custom connection settings have no native dumps.
- Browser downloads are limited to 64 MiB. Use `synod file get` for larger files.
- A restore test without network access cannot prove every app feature.

The [reference](reference.md) lists all limits.
