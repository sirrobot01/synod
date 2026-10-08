# Synod

Synod backs up the Docker apps of your home lab and restores them on any of your devices.

- **Finds your apps.** Synod discovers Compose projects and containers, with their files, volumes, and databases.
- **Backs them up safely.** It makes native dumps of PostgreSQL, MySQL, MariaDB, and Redis. It stops other apps only for a short final copy.
- **Restores anywhere.** Restore an app on the same device or on another one, amd64 or arm64.
- **Proves your backups.** Agents test a restore each month and check repository content each week.
- **Fast and encrypted.** Later backups read only changed files, like rsync. Backups go to another device, a local disk, SFTP, or S3.
- **Needs only a password.** The recovery kit and one surviving backup rebuild everything.

Synod runs on Linux (amd64 and arm64) with Docker Engine and the Compose plugin.
v0.1 is not released yet.

## Quick start

Run these commands as root on Linux.

1. Install Synod on each device:

   ```sh
   curl -fsSL https://synod.biodun.dev/install.sh | sh
   ```

2. Create the controller on one device:

   ```sh
   synod server init
   ```

   Init starts the controller. Save the recovery kit that it prints, away from your devices. Copy the join token.

3. Join each Docker device, the controller device included:

   ```sh
   synod agent join 'TOKEN'
   ```

   Join starts the agent. Each token joins one device. On the controller, run `synod node join-token` for the next one.

4. On the controller, create your account:

   ```sh
   synod user create admin
   ```

5. Open the web UI address that init printed, such as `https://192.168.0.20:8443`, and sign in.
   Approve your devices in **Devices**. Add a destination on another device in **Destinations**.
   Then select **Apps → Protect all**.

To restore an app, open **Snapshots & restore**. Select the app and a target device.

## Documentation

- [User guide](docs/user-guide.md): install, protect, restore, and recover.
- [Reference](docs/reference.md): full behaviour and limits of each feature.
- [Development](docs/development.md): build, test, and release.
