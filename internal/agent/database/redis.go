package database

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
)

// RedisSocket is where the supervisor reaches Redis inside its container.
const RedisSocket = "/run/synod-redis/redis.sock"

// Redis holds what the Redis steps need. The passwords come from the credential store.
type Redis struct {
	AdminPassword       string
	ReplicationPassword string
}

// RedisStatus is what one replica reports.
type RedisStatus struct {
	State      string
	LagSeconds float64
}

func (r Redis) validate() error {
	if !passwordPattern.MatchString(r.AdminPassword) || !passwordPattern.MatchString(r.ReplicationPassword) {
		return ErrConfiguration
	}
	return nil
}

// cli runs one Redis command as the default user over the local socket.
// A non-empty last reads the final argument from stdin, so a secret stays out of the process list.
func (r Redis) cli(ctx context.Context, x Exec, last string, args ...string) ([]byte, error) {
	command := []string{"redis-cli", "-s", RedisSocket, "--no-raw"}
	if last != "" {
		command = append(command, "-x")
	}
	out, err := x(ctx, []string{"REDISCLI_AUTH=" + r.AdminPassword}, last, append(command, args...)...)
	if err != nil {
		return out, err
	}
	if bytes.HasPrefix(out, []byte("(error)")) {
		return out, fmt.Errorf("database: redis refused %s: %s", args[0], strings.TrimSpace(string(out)))
	}
	return out, nil
}

func (r Redis) info(ctx context.Context, x Exec) (map[string]string, error) {
	out, err := x(ctx, []string{"REDISCLI_AUTH=" + r.AdminPassword}, "", "redis-cli", "-s", RedisSocket, "INFO", "replication")
	if err != nil {
		return nil, err
	}
	fields := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		if key, value, ok := strings.Cut(strings.TrimSpace(scanner.Text()), ":"); ok {
			fields[key] = value
		}
	}
	return fields, scanner.Err()
}

// EnsurePrimary makes this copy accept writes. It also promotes a replica.
func (r Redis) EnsurePrimary(ctx context.Context, x Exec) error {
	if err := r.validate(); err != nil {
		return err
	}
	fields, err := r.info(ctx, x)
	if err != nil {
		return err
	}
	if fields["role"] == "master" {
		return nil
	}
	_, err = r.cli(ctx, x, "", "REPLICAOF", "NO", "ONE")
	return err
}

// EnsureReplica follows the primary. It repeats nothing when the copy already follows it.
func (r Redis) EnsureReplica(ctx context.Context, x Exec, primary Endpoint) error {
	if err := r.validate(); err != nil {
		return err
	}
	if !hostPattern.MatchString(primary.Host) || primary.Port == 0 {
		return ErrConfiguration
	}
	fields, err := r.info(ctx, x)
	if err != nil {
		return err
	}
	port := strconv.Itoa(int(primary.Port))
	if fields["role"] == "slave" && fields["master_host"] == primary.Host && fields["master_port"] == port {
		return nil
	}
	if _, err := r.cli(ctx, x, "", "CONFIG", "SET", "masteruser", "synod_replication"); err != nil {
		return err
	}
	if _, err := r.cli(ctx, x, r.ReplicationPassword, "CONFIG", "SET", "masterauth"); err != nil {
		return err
	}
	_, err = r.cli(ctx, x, "", "REPLICAOF", primary.Host, port)
	return err
}

// Status reads the replication state of one replica.
func (r Redis) Status(ctx context.Context, x Exec) (RedisStatus, error) {
	fields, err := r.info(ctx, x)
	if err != nil {
		return RedisStatus{}, err
	}
	status := RedisStatus{State: StateFailed, LagSeconds: -1}
	switch {
	case fields["role"] != "slave":
	case fields["master_sync_in_progress"] == "1":
		status.State = StateCopying
	case fields["master_link_status"] == "up":
		status.State = StateStreaming
		if lag, err := strconv.ParseFloat(fields["master_last_io_seconds_ago"], 64); err == nil {
			status.LagSeconds = lag
		}
	case fields["master_link_status"] == "down":
		status.State = StateDisconnected
	}
	return status, nil
}
