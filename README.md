# Synod

Synod backs up the Docker apps in your home lab and restores them on any of your devices.

- Finds your Compose projects and containers, with their files, volumes, and databases.
- Backs up without stopping apps. PostgreSQL, MySQL, MariaDB, and Redis get native dumps.
- Restores an app on the same device or on another one, amd64 or arm64.
- Tests a restore of each app every month.
- Encrypts every backup. Later backups copy only changed files.
- Stores backups on another device, a local disk, SFTP, or S3.
- Rebuilds everything from one backup and your recovery kit password.

## Requirements

- Linux devices (amd64 or arm64) with Docker Engine and the Docker Compose plugin.
- One Linux device or Mac for the controller. A Mac controller does not need Docker.

## Quick start

1. On one device, install Synod and create the controller as root:

   ```sh
   curl -fsSL https://synod.biodun.dev/install.sh | sh
   synod server init
   ```

   Save the recovery kit that init prints. Keep it away from your devices.

2. On each Linux device with apps, start the agent with Docker Compose.
   Use the join token from init for the first device. Run `synod node join-token` on the controller for each other device.

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

   ```sh
   docker compose up -d
   ```

   To run the agent as a systemd service instead, install Synod on the device and run `synod agent join 'TOKEN'` as root.

3. On the controller, create your account:

   ```sh
   synod user create admin
   ```

4. Open the address that init printed, for example `https://192.168.0.20:8443`. Then:
   1. In **Devices**, approve your devices.
   2. In **Destinations**, add a destination on another device.
   3. In **Apps**, select **Protect all**.

To restore an app, open **Snapshots & restore**.

## Documentation

- [User guide](docs/user-guide.md): set up, protect, restore, and recover.
- [Reference](docs/reference.md): all settings, behaviour, and limits.
- [Development](docs/development.md): build, test, and release.
