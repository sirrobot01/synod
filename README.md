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

## Quick start

1. On your first device, create `compose.yaml`:

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

   `synod` is the controller. `synod-agent` backs up the apps on this device.

2. Start it:

   ```sh
   docker compose up -d
   ```

3. Open `https://DEVICE-ADDRESS:8443`. Create your account and save the recovery kit.
   The kit holds the password of your backups. Keep it away from your devices.

4. In the web UI:
   1. In **Devices**, approve your device.
   2. In **Destinations**, add a destination on another device.
   3. In **Apps**, select **Protect all**.

To add another device, select **Add device** in **Devices**. Run the Compose file that it shows on that device.

To restore an app, open **Snapshots & restore**.

To install Synod without Docker, see the [user guide](docs/user-guide.md#install-without-docker).

## Documentation

- [User guide](docs/user-guide.md): set up, protect, restore, and recover.
- [Reference](docs/reference.md): all settings, behaviour, and limits.
- [Development](docs/development.md): build, test, and release.
