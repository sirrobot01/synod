# Use Synod

Synod backs up Docker apps and restores them on another Linux device.
A device can use amd64 or arm64.
One controller serves the web UI and holds the installation records.
An agent on each Docker host discovers apps and does the backup work.
The controller device also needs an agent if it runs apps or stores a device repository.

Synod is under development. No v0.1 release is available yet.
Read [Current limits](#current-limits) before use.

Go to:

- [Install](#install-the-binary) and [create the controller](#create-the-controller).
- [Join devices](#join-each-docker-device) and [open the web UI](#open-the-web-ui).
- [Add a destination](#add-a-destination) and [protect apps](#review-and-protect-apps).
- [Test a backup](#prove-the-first-backup) and [configure alerts](#configure-alerts-with-shoutrrr).
- [Restore an app](#restore-after-device-loss) or [recover the controller](#recover-when-the-controller-is-lost).
- [Upgrade](#upgrade-synod) or [resolve a failure](#read-status-and-resolve-a-failure).

## Before you start

Each app device needs Docker Engine and the Docker Compose plugin.
The agent must run on the Docker host.
Its user must read the app files and use the Docker socket.
It must write restored files with their saved owners.
The service examples below use a root shell on Linux.

Choose an address that the other devices can reach.
Keep that address stable.
Permit these connections on your private network:

| Port | Listener | Clients |
| --- | --- | --- |
| TCP 8443 | Controller HTTPS API and web UI | Operators and joining agents |
| TCP 8444 | Controller agent transport | Joined agents |
| TCP 8445 | Agent destination service | Agents that use a Synod-device destination |

Use a destination on another device to survive loss of the source device.
A second directory on the source device is not a second device.
Do not put the repository inside the app data that Synod copies.

The examples use `192.168.0.20` for the controller.
Replace it with your address.
Replace uppercase placeholders, such as `APP_ID`, with the values that Synod shows.

## Install the binary

Build or obtain the release files described in [Build release files](../README.md#build-release-files).
Copy the release directory to each Linux device.
The example version below must match its `release.json` file.

Open a root shell on the device:

```sh
sudo -i
```

Install from the copied directory:

```sh
sh /path/to/release/install.sh --version v0.1.0-rc.1 --from-dir /path/to/release --install-dir /usr/local/bin
synod --version
docker info
docker compose version
```

The installer verifies the release checksums.
It installs the matching amd64 or arm64 binary.
It does not install Docker or start a Synod service.
For a custom directory, add it to `PATH`.
For download options, see [Install a release binary](../README.md#install-a-release-binary).

## Create the controller

Run these commands on the controller device:

```sh
synod server init --dir /var/lib/synod/controller --address 192.168.0.20
systemctl cat synod-server
```

Check the executable and configuration paths in `ExecStart`.
Then load and start the service:

```sh
systemctl daemon-reload
systemctl enable --now synod-server
systemctl status synod-server
```

Init prints a recovery kit, one join token, and three administrator settings.
The recovery kit holds the installation ID, the repository password, and the recovery steps.
Init also writes the kit to `recovery-kit.txt` in the controller directory.
Copy the kit to a password manager or to paper, away from your Synod devices.
Then delete `recovery-kit.txt`.
Without the repository password, nobody can read the backups, including Synod.
The join token expires after seven days and permits one enrollment.
Keep it private.
Init creates the controller state in the selected directory.
Do not delete this directory during an upgrade.

In the controller's administrator shell, set the printed values:

```sh
export SYNOD_API_URL=https://192.168.0.20:8443
export SYNOD_CA_FILE=/var/lib/synod/controller/api-ca.crt
export SYNOD_ADMIN_TOKEN_FILE=/var/lib/synod/controller/admin.token
synod installation get
```

Set these variables again in a new administrator shell.
The token file must remain private and belong to the user who runs the CLI.
For remote administration, copy the API CA and token through a trusted connection.
Keep the copied token in an owner-only file.
Do not put the token value in a command argument.

For a host without systemd, run the controller in the foreground:

```sh
synod server --config /var/lib/synod/controller/controller.json
```

Use one controller process for this directory.

## Join each Docker device

Run this command in a root shell on the device:

```sh
synod agent join 'TOKEN' --dir /var/lib/synod/agent
systemctl cat synod-agent
```

Check the executable and configuration paths.
Then start the agent:

```sh
systemctl daemon-reload
systemctl enable --now synod-agent
systemctl status synod-agent
```

The default Docker socket is `/var/run/docker.sock`.
Use `--docker-socket` during join to select another socket.
For a routed destination address, use `--destination-address HOST:8445` during join.

From the controller's administrator shell:

```sh
synod node list
synod node approve DEVICE_ID
```

Wait for the agent to connect before approval.
Check the device name and architecture.
Copy its ID from the list.
Create a new token for each additional device:

```sh
synod node join-token
```

Repeat the join and approval steps on that device.
For a host without systemd, start its agent in the foreground:

```sh
synod agent run --config /var/lib/synod/agent/agent.json
```

## Open the web UI

Create an operator account from the administrator shell:

```sh
synod user create admin
```

Enter the account password at the prompt.
Open `https://192.168.0.20:8443` in your browser.
The API uses a private certificate authority.
Get `api-ca.crt` from the controller through a trusted connection.
Trust that CA in your browser or operating system.
Sign in with the operator account.

The web UI and CLI use the same installation.
Use **Devices** to inspect and approve devices.
Use **Apps** to review protection.
Use **Activity** to read operation results.

## Add a destination

Each destination holds an encrypted repository.
A new destination uses the repository password from your recovery kit.
You need the kit and the location of one surviving destination to recover.
For S3 or SFTP, also keep the access keys or SSH key outside your Synod devices.

To connect an existing repository with its own password, put that password in a private credentials file.
This custom password is not in your recovery kit. Store it with the kit.

```sh
umask 077
${EDITOR:-vi} /root/synod-repository.json
chmod 600 /root/synod-repository.json
```

Use this JSON form for a custom password:

```json
{"password":"EXISTING_REPOSITORY_PASSWORD"}
```

For a directory on another joined device:

```sh
synod destination add peer --type device --device DESTINATION_DEVICE_ID --path /srv/synod-backups
synod destination list
```

The path belongs to the selected device.
Use an empty directory for a new repository.
The destination agent serves only its configured repository directories over mutual TLS.
Wait until the settings are current on each device.
Adding a destination does not prove that it can store a backup.

The other destination types use these commands:

```sh
synod destination add local-disk --type local --device DEVICE_ID --path /mnt/backup/synod
synod destination add cloud --type s3 --endpoint https://s3.example.com --bucket backups --prefix synod/ --credentials-file /root/synod-s3.json
synod destination add remote --type sftp --host backup.example.com --port 22 --user synod --path /srv/synod --known-hosts-file /root/known_hosts --credentials-file /root/synod-sftp.json
```

For S3, put `access_key_id` and `secret_access_key` in the private credentials JSON.
Add `session_token` if the provider requires it.
For SFTP, add `ssh_key` with the full private key as a JSON string.
Verify the SSH server key before you add it to `known_hosts`.
The SFTP server must support atomic POSIX rename.

A local destination is available only through its owner device.
A Synod-device destination is available to approved peer agents.
In the web UI, **Destinations → Add destination** accepts the same settings.

### Backup speed

The first backup copies all included data.
Later backups read only the files that changed.
Synod treats a file as unchanged when its size, modification time, mode, and owner are the same as in the previous snapshot.
rsync uses the same rule.
The Stop method reads the data while the app runs, then reads again only the files that changed before the app stopped.
The agent keeps encrypted repository metadata in the `cache` directory beside its state directory.

### Space reclamation

Retention removes expired snapshot records after each backup.
Repository maintenance then removes the data that no kept snapshot uses.
One device runs maintenance for each destination, once each day.
For a local or Synod-device destination, this is the device that holds the repository.
For an S3 or SFTP destination, this is the earliest approved device that connected in the last 7 days.
If that device is missing for 7 days, the controller gives maintenance to another device.
The new device waits six hours before its first full run, so two devices never run maintenance at the same time.
Maintenance keeps all data that is less than 24 hours old, so backups that run at the same time stay complete.
Space returns after approximately three daily runs.
Data in small packs of less than 12 MiB can stay longer.
`synod destination list` and **Destinations** show the maintenance device and its last run.
A failed run sends a `maintenance_failed` alert.

## Review and protect apps

Wait for discovery. It runs once a minute.
Inspect the discovered apps:

```sh
synod app list
synod protect --all --destination peer
```

The protect command first shows a preview.
Review every path and its reason:

| Status | Backup behavior |
| --- | --- |
| Included | Copy the selected app data. |
| Large | Skip media paths and paths larger than 20 GiB by default. |
| Dumped | Save a native database dump in place of the live database directory. |
| Never | Exclude host system paths, sockets, and unsupported mounts. |

Check that required app data is included or dumped.
Do not assume that skipped media is protected.

To copy a Large path, include it for one app:

```sh
synod protect photos --include-large /srv/photos/library
synod protect photos --include-large /srv/photos/library --confirm REVISION
```

You can include a whole Large path.
You can also include a skipped folder or file inside an Included path, such as `/srv/app/uploads`.
Synod refuses a folder inside a Large path. Include the whole Large path instead.
Synod refuses Never and Dumped paths.
Use `--exclude-large PATH` to stop copying a path.
Omitted flags keep the saved choice.
Make sure that the destination has space for the included data.
In the web UI, **Protect** shows a **Large paths** list for one app.

Repeat the command with the revision shown in the preview:

```sh
synod protect --all --destination peer --confirm REVISION
synod app status
```

If the revision changed, review the new preview before confirmation.
**Protect all** also enables protection for newly discovered apps.
To protect selected apps only, use an app name or ID in place of `--all`.
In the web UI, open **Apps → Protect all** or select one app.
Confirm the same path and destination review.

The default method is Auto.
It uses native dumps for PostgreSQL, MySQL, MariaDB, and Redis.
For other apps, it copies files, stops the app for a final copy, and starts it again.
Use Live only if the app permits a live file copy.
Live and Dump use the SQLite backup API for detected SQLite files on Linux.
See [Copy behavior and database limits](../README.md#back-up-an-app) for custom database settings.

The default schedule is daily at 03:00 UTC.
The default retention is 7 daily, 4 weekly, and 6 monthly snapshots.
To change one app, preview the new settings:

```sh
synod protect APP_ID --destination peer --at 03:30 --time-zone Africa/Lagos
```

Repeat the exact command with `--confirm REVISION` to save it.
A new plan runs a backup at once.
An offline device takes one current backup when it returns.
Agents keep schedules and encrypted settings during a controller outage.
The destination must still be reachable.

To stop future scheduled backups, preview `synod protect APP_ID --off`.
Confirm its revision to save the change.
Already admitted work finishes. Saved snapshots remain available.

## Prove the first backup

Run a backup and inspect the result:

```sh
synod backup APP_ID --destination peer
synod snapshot list APP_ID --destination peer
synod file list APP_ID --destination peer
```

A backup result shows the snapshot ID and any warnings.
Use the saved paths from `file list` for a file restore:

```sh
synod file get APP_ID /saved/path/config.json --destination peer --output /root/restored-config.json
```

Compare the restored file with the expected content.
The output path must not exist.
For a directory path, the output is a tar archive.
Use `--snapshot SNAPSHOT_ID` to choose an older copy.
Use `--via DEVICE_ID` to read through an online device with repository access.

Test the complete app on a compatible device:

```sh
synod restore APP_ID --to TARGET_DEVICE_ID --destination peer --drill
synod app status
```

A drill restores the data into a temporary area.
It starts app containers without network access or published ports.
It checks that they stay up for 30 seconds.
It then removes the temporary containers, volumes, and files.
A successful drill records **Restore tested on DATE**.
A later failure keeps that date and shows the latest failure separately.
A drill does not test every app feature.
Apps that need network access or unsafe host access can fail a drill.

Agents schedule weekly content samples and monthly restore drills.
To run a content sample now:

```sh
synod destination check peer --on DEVICE_ID
```

This reads one percent of repository content, with at least one item.
It does not verify every stored item.

## Configure alerts with Shoutrrr

Open **Settings → Notifications** in the web UI.
Enter a supported Shoutrrr URL and save it.
Send a test notification.
Read its delivery state in **Activity**.
A queued test is not yet a delivered test.

For the CLI, create an owner-only JSON file:

```json
{
  "url": "ntfy://ntfy.example.com/private-topic",
  "overdue_grace_hours": 24,
  "missing_after_hours": 168,
  "full_percent": 90
}
```

The example uses the [Shoutrrr v0.20.0 ntfy URL format](https://shoutrrr.nickfedor.com/v0.20.0/services/push/ntfy/).
Replace the host and topic with your own values.

```sh
chmod 600 /root/synod-notification.json
synod notification configure --file /root/synod-notification.json
synod notification test
synod notification status
```

Keep the URL out of shell history and Git.
Supported services include ntfy, Gotify, Discord, Slack, SMTP, and generic HTTP webhooks.
See [Notification settings](../README.md#configure-notifications) for URL guidance and retry behavior.

Synod alerts on failed backups, failed drills, failed repository maintenance, overdue backups, almost-full destinations, and missing devices.
An offline device does not produce overdue alerts.
The default missing-device delay is seven days.
S3 and SFTP free space remains unknown.
A local or device capacity report measures filesystem space, not provider quotas.

## Restore after device loss

Keep the destination online.
Join and approve the target device if it is new.
Select the lost app by its saved ID.
A missing source device does not prevent a restore from another destination.

```sh
synod app list
synod node list
synod restore APP_ID --to TARGET_DEVICE_ID --destination peer
```

The default is the newest snapshot that the target can read.
Use `--snapshot SNAPSHOT_ID` for a chosen snapshot.
Synod checks image architecture, image availability, container names, and Docker port conflicts.
The target needs the Docker Compose plugin.
An unsupported image causes a refusal that names the image.

The result shows the new app ID and project directory.
The default restore directory is `/var/lib/synod/agent/restores/OPERATION_ID`.
Open the restored app and check its data.
Keep that directory while the restored app uses it.

Synod refuses an existing app unless you explicitly use `--replace`:

```sh
synod restore APP_ID --to TARGET_DEVICE_ID --destination peer --replace
```

Replacement first takes a safety snapshot of the existing app.
It does not replace the app if that snapshot fails.
The result identifies the safety snapshot.
Read [Restore details](../README.md#restore-an-app) before replacement.

In the web UI, open **Snapshots & restore**.
Select the app, snapshot, destination, and target.
A replacement requires the app name as confirmation.
Read the final result in **Activity**.

## Recover when the controller is lost

You need the surviving repository, the password from your recovery kit, and its storage access credentials.
A destination that you added with a custom password needs that password instead.
You do not need the old controller database or certificates.
Stop old Synod agents and the old controller if they still run.
Use a new controller directory.

For a local or Synod-device repository, run this on the host with its files:

```sh
umask 077; printf '%s' 'PASSWORD_FROM_THE_KIT' > /root/repository-password
synod server init --dir /var/lib/synod/recovered --address 192.168.0.20 --from-repository /mnt/surviving-repository --password-file /root/repository-password
systemctl daemon-reload
systemctl start synod-server
```

The password file contains only the repository password.
Recovery opens an existing repository and reads its encrypted catalog.
It keeps installation, app, device, destination, and protection IDs.
It creates new controller keys and an administrator token.
It writes a new `recovery-kit.txt` with the same password and does not print the password.
Use the new administrator settings that init prints.

The catalog reflects the most recent successful catalog write during backup.
Settings changed after that write are not yet recoverable from that copy.
If several destinations match, pass `--repository-destination NAME_OR_UUID`.

Use the printed token to join the host with the repository files.
Use a new agent directory:

```sh
synod agent join 'TOKEN' --dir /var/lib/synod/recovered-agent
systemctl daemon-reload
systemctl start synod-agent
```

From the new controller's administrator shell, approve the saved device ID.
Create bound tokens for other saved devices:

```sh
synod node list
synod node approve DEVICE_ID
synod node join-token --device OTHER_SAVED_DEVICE_ID
```

Join each device with its own token and a new agent directory.
Approve it after it connects.
Wait until destination settings are current.
Then inspect and restore a saved app:

```sh
synod destination list
synod snapshot list APP_ID --destination DESTINATION_ID --via DEVICE_ID
synod restore APP_ID --to DEVICE_ID --destination DESTINATION_ID
```

Recovery does not restore users, API tokens, prior approval, or activity records.
Create a new operator account for the web UI.
Saved apps remain missing until discovery finds them.
A missing app can still be restored.

For S3 or SFTP, pass a private connection JSON file to `--from-repository`.
See [Remote repository recovery](../README.md#recover-after-controller-loss) for its fields.

## Upgrade Synod

Use the [upgrade procedure](../README.md#upgrade-the-controller-and-agents).
It stops agents and the controller, verifies the release, and replaces each binary.
Keep the existing state directories and keys.
Start the controller first, then the agents.
Check the running versions and destination acknowledgements.
The installer does not perform these service steps for you.

## Keep certificates current

Synod renews its certificates automatically.
Device and controller transport certificates last 30 days.
The controller API certificate lasts one year.
Each certificate is renewed when less than 10 days remain.
A device keeps its key, identity, approval, and encrypted cache.
Other devices receive the new certificate in their settings before the device uses it.

A device that is offline for more than 20 days can miss its renewal.
Its agent then stops with a certificate expiry message.
To recover the device, run this on the controller:

```sh
synod node join-token --device DEVICE
```

Then run these commands on the device:

```sh
systemctl stop synod-agent
synod agent renew 'TOKEN' --dir /var/lib/synod/agent
systemctl start synod-agent
```

The token works only for that device and only with its existing key.

## Read status and resolve a failure

```sh
synod app status
synod destination list
synod operation list
synod operation get OPERATION_ID
synod operation wait OPERATION_ID
```

A data-loss window measures time since the last good backup started.
An unknown window means that no good backup has been reported.
A good backup and a good restore test are separate facts.
Read warnings even when a snapshot exists.
Interrupting `operation wait` does not cancel the operation.

| Symptom | Next action |
| --- | --- |
| Pending settings after restart | Keep both services running. Wait for current destination acknowledgements. |
| Device does not connect | Check its address, clock, port 8444, and service log. |
| Destination cannot be read | Check the owner device or storage service, credentials, and free space. |
| `app_definition_incomplete` | Restore access to the Compose project files on the source host. Wait for discovery. |
| `catalog_write_failed` | Fix repository access and run another backup. The current recovery settings were not saved. |
| `retention_failed` | Fix repository access and run another backup. Older snapshot records were not expired. |
| `maintenance_failed` | Fix repository access from the maintenance device. Check its clock. Maintenance stops when the device clock differs from the storage clock by more than 5 minutes. The next daily run tries again. |
| `maintenance_not_owner` | Wait for current settings on the device. Another device owns maintenance for this destination. |
| Architecture refusal | Select a compatible target or supply the required image for that architecture. |
| Port or name conflict | Remove the conflict yourself, or review an explicit app replacement. |
| `cleanup_pending` | Keep the target agent running. Read the operation result before you remove any resources. |
| Migration checksum error | Use the matching build or recover into new state. Do not reset the only copy of a controller without a surviving repository and password. |

For systemd installations, inspect the relevant log:

```sh
journalctl -u synod-server -n 100 --no-pager
journalctl -u synod-agent -n 100 --no-pager
```

Do not send repository credentials, tokens, or private keys with a support report.

## Current limits

- Linux hosts only. Docker Desktop is a development environment.
- One controller per installation.
- Host-network apps cannot be restored.
- Local-only images need to exist on the target with the correct architecture.
- MongoDB and custom database connection settings have no native dump support.
- Browser file downloads are limited to 64 MiB. Use the CLI for larger downloads.
- A backup copy pass that runs for more than 24 hours can lose new data to concurrent maintenance. Synod checks every object before it saves a snapshot, so that backup fails and saves no damaged snapshot. The next backup copies the data again.
- Maintenance runs in the agent's job queue. A long maintenance run delays other jobs on that device.
- A program that rewrites a file and keeps its old size and modification time defeats change detection. Synod copies that file again only after its size or time changes.
- A local destination syncs each repository file to disk. On a slow disk, a Synod-device destination is faster, because it syncs once before each index write.
- An isolated drill cannot prove app behavior that needs network access.

See the [README](../README.md) for detailed behavior and developer checks.
