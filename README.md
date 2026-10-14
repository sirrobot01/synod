# Synod

Synod backs up the Docker apps in your home lab and restores them on any of your devices.

- Finds your Compose projects and containers, with their files, volumes, and databases.
- Backs up without stopping apps. PostgreSQL, MySQL, MariaDB, and Redis get native dumps.
- Restores an app on the same device or on another one, amd64 or arm64.
- Tests a restore of each app every month.
- Encrypts every backup. Later backups copy only changed files.
- Stores backups on another device, a local disk, SFTP, or S3.
- Rebuilds everything from one backup and your recovery kit password.

v0.1 is not released yet.

## Requirements

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- One Linux device or Mac for the controller. A Mac controller does not need Docker.

## Quick start

Run these commands as root.

1. Install Synod on each device:

   ```sh
   curl -fsSL https://synod.biodun.dev/install.sh | sh
   ```

2. On one device, create the controller:

   ```sh
   synod server init
   ```

   Save the recovery kit that it prints. Keep it away from your devices.

3. On each Linux device with apps, join the agent with the token from init:

   ```sh
   synod agent join 'TOKEN'
   ```

   A token joins one device. Run `synod node join-token` on the controller to get the next one.

4. On the controller, create your account:

   ```sh
   synod user create admin
   ```

5. Open the address that init printed, for example `https://192.168.0.20:8443`. Then:
   1. In **Devices**, approve your devices.
   2. In **Destinations**, add a destination on another device.
   3. In **Apps**, select **Protect all**.

To restore an app, open **Snapshots & restore**.

## Documentation

- [User guide](docs/user-guide.md): set up, protect, restore, and recover.
- [Reference](docs/reference.md): all settings, behaviour, and limits.
- [Development](docs/development.md): build, test, and release.
