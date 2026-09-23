package redis

import (
	"strconv"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

const ConfigDirectory = "/run/synod-redis"

// DataDirectory is the directory the generated configuration names with `dir`.
const DataDirectory = "/data"

// DataMarkerFile proves Synod prepared this data directory.
//
// Redis writes no file that is always present. A dump appears only after a save, and a
// cluster with no persistence never writes one. An empty directory is therefore not
// proof of a new volume, so Synod writes its own marker and requires it on every later
// start. The agent reads it before the container runs, so a lost directory cannot be
// re-marked by the startup script.
const DataMarkerFile = DataDirectory + "/.synod-redis-volume"

// ContainerCommand configures a validated Redis instance before it accepts clients.
func ContainerCommand(config *agentv1.RedisContainerConfig) []string {
	save, appendonly := `""`, "no"
	if config.Persistence == "rdb" || config.Persistence == "rdb+aof" {
		save = "60 1"
	}
	if config.Persistence == "aof" || config.Persistence == "rdb+aof" {
		appendonly = "yes"
	}
	upstream, upstreamPort := "", "0"
	if config.Upstream != nil {
		upstream = config.Upstream.Address
		upstreamPort = strconv.FormatUint(uint64(config.Upstream.Port), 10)
	}
	return []string{"/bin/sh", "-ec", redisStartup, "synod-redis",
		strconv.FormatUint(uint64(config.Port), 10), save, appendonly,
		strconv.FormatInt(config.MaxmemoryBytes, 10), config.EvictionPolicy, upstream, upstreamPort}
}

// The script is fixed product code. Only validated settings become positional arguments.
const redisStartup = `
: "${REDIS_ADMIN_PASSWORD:?missing admin credential}"
: "${REDIS_APP_PASSWORD:?missing application credential}"
: "${REDIS_REPLICATION_PASSWORD:?missing replication credential}"
umask 077
admin_hash=$(printf '%s' "$REDIS_ADMIN_PASSWORD" | sha256sum)
app_hash=$(printf '%s' "$REDIS_APP_PASSWORD" | sha256sum)
replication_hash=$(printf '%s' "$REDIS_REPLICATION_PASSWORD" | sha256sum)
admin_hash=${admin_hash%% *}
app_hash=${app_hash%% *}
replication_hash=${replication_hash%% *}
printf 'bind 0.0.0.0\nprotected-mode yes\nport %s\ndir /data\nsave %s\nappendonly %s\nappendfsync everysec\nmaxmemory %s\nmaxmemory-policy %s\nuser default off\nuser synod_admin on #%s ~* &* +@all\nuser synod_app on #%s ~* &* +@read +@write +@connection -@dangerous\nuser synod_repl reset on #%s -@all +ping +replconf +psync\nreplica-read-only yes\nreplica-serve-stale-data no\n' \
 "$1" "$2" "$3" "$4" "$5" "$admin_hash" "$app_hash" "$replication_hash" > /run/synod-redis/redis.conf
if [ -n "$6" ]; then
    masterauth=$(printf '%s' "$REDIS_REPLICATION_PASSWORD" | sed 's/\\/\\\\/g; s/"/\\"/g')
    printf 'replicaof %s %s\nmasteruser synod_repl\nmasterauth "%s"\n' "$6" "$7" "$masterauth" >> /run/synod-redis/redis.conf
    unset masterauth
fi
unset REDIS_ADMIN_PASSWORD REDIS_APP_PASSWORD REDIS_REPLICATION_PASSWORD
chown -R redis:redis /run/synod-redis
chmod 700 /run/synod-redis
if [ ! -s /data/.synod-redis-volume ]; then
    printf 'synod\n' > /data/.synod-redis-volume
    sync -f /data/.synod-redis-volume
fi
chown redis:redis /data/.synod-redis-volume
exec docker-entrypoint.sh redis-server /run/synod-redis/redis.conf
`
