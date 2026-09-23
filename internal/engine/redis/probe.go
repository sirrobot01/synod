package redis

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirrobot01/synod/internal/engine"
)

const probeTimeout = 3 * time.Second

// Probe checks authenticated Redis access through the published endpoint.
func (Engine) Probe(ctx context.Context, endpoint engine.Endpoint, password []byte, revision int64) engine.Observation {
	observed := engine.Observation{ObservedRevision: revision, ObservedAt: time.Now().UTC()}
	if endpoint.Address == "" || endpoint.Port == 0 || endpoint.Database != "0" || (endpoint.Role != "synod_app" && endpoint.Role != "synod_admin") {
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: "endpoint_not_declared"}
		return observed
	}
	if check := endpoint.RedisReplication; check != nil {
		valid := endpoint.Role == "synod_admin"
		switch check.ExpectedRole {
		case "primary":
			valid = valid && check.Upstream == nil
		case "replica":
			if check.Upstream == nil {
				valid = false
			} else {
				address, err := netip.ParseAddr(check.Upstream.Address)
				valid = valid && err == nil && address.String() == check.Upstream.Address &&
					!address.Is4In6() && address.Zone() == "" && (address.IsGlobalUnicast() || address.IsLoopback()) &&
					check.Upstream.Port > 0 && check.Upstream.Port <= 65535
			}
		default:
			valid = false
		}
		if !valid {
			observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: "redis_replication_invalid_request"}
			return observed
		}
	}
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	address := net.JoinHostPort(endpoint.Address, strconv.Itoa(int(endpoint.Port)))
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		observed.Reachable = engine.Probe{State: engine.ProbeFailed, Reason: "endpoint_refused_connection"}
		return observed
	}
	_ = conn.Close()
	observed.Reachable = engine.Probe{State: engine.ProbePassed}
	observed.ProcessLive = engine.Probe{State: engine.ProbePassed}
	if len(password) == 0 {
		observed.Ready = engine.Probe{State: engine.ProbeUnavailable, Reason: "credential_unavailable"}
		return observed
	}
	client := goredis.NewClient(&goredis.Options{
		Addr: address, Username: endpoint.Role, Password: string(password), DB: 0,
		Protocol: 2, DisableIdentity: true, MaxRetries: -1, DialerRetries: 1,
		DialTimeout: probeTimeout, ReadTimeout: probeTimeout, WriteTimeout: probeTimeout,
		ContextTimeoutEnabled: true, PoolSize: 1, MaxActiveConns: 1,
	})
	defer client.Close()
	if endpoint.RedisReplication != nil {
		observed.Ready = replicationReady(ctx, client, endpoint.RedisReplication)
		if observed.Ready.State != engine.ProbePassed {
			return observed
		}
	}
	answer, err := client.Ping(ctx).Result()
	if err != nil || answer != "PONG" {
		reason := "database_not_ready"
		if goredis.HasErrorPrefix(err, "WRONGPASS") || goredis.HasErrorPrefix(err, "NOAUTH") {
			reason = "authentication_rejected"
		}
		observed.Ready = engine.Probe{State: engine.ProbeFailed, Reason: reason}
		return observed
	}
	observed.Ready = engine.Probe{State: engine.ProbePassed}
	observed.Endpoint = engine.Probe{State: engine.ProbePassed}
	return observed
}

// replicationReady never returns native response text to the operation receipt.
func replicationReady(ctx context.Context, client *goredis.Client, expected *engine.RedisReplicationCheck) engine.Probe {
	failed := engine.Probe{State: engine.ProbeFailed, Reason: "redis_replication_invalid_response"}
	reply, err := client.Do(ctx, "ROLE").Slice()
	if err != nil {
		failed.Reason = "redis_role_unavailable"
		if goredis.HasErrorPrefix(err, "WRONGPASS") || goredis.HasErrorPrefix(err, "NOAUTH") {
			failed.Reason = "authentication_rejected"
		}
		return failed
	}
	if len(reply) == 0 {
		return failed
	}
	role, ok := reply[0].(string)
	if !ok {
		return failed
	}
	if role != "master" && role != "slave" {
		failed.Reason = "redis_role_mismatch"
		return failed
	}
	if (expected.ExpectedRole == "primary") != (role == "master") {
		failed.Reason = "redis_role_mismatch"
		return failed
	}
	if role == "master" {
		if len(reply) != 3 {
			return failed
		}
		offset, validOffset := reply[1].(int64)
		replicas, validReplicas := reply[2].([]any)
		if !validOffset || offset < 0 || !validReplicas {
			return failed
		}
		for _, item := range replicas {
			fields, ok := item.([]any)
			if !ok || len(fields) != 3 {
				return failed
			}
			address, validAddress := fields[0].(string)
			port, validPort := fields[1].(string)
			received, validReceived := fields[2].(string)
			if !validAddress || address == "" || !validPort || !validReceived {
				return failed
			}
			if _, err := strconv.ParseUint(port, 10, 16); err != nil {
				return failed
			}
			if _, err := strconv.ParseUint(received, 10, 63); err != nil {
				return failed
			}
		}
		return engine.Probe{State: engine.ProbePassed}
	}
	if len(reply) != 5 {
		return failed
	}
	address, validAddress := reply[1].(string)
	port, validPort := reply[2].(int64)
	link, validLink := reply[3].(string)
	offset, validOffset := reply[4].(int64)
	if !validAddress || !validPort || port <= 0 || port > 65535 || !validLink || !validOffset || offset < -1 {
		return failed
	}
	if address != expected.Upstream.Address || port != int64(expected.Upstream.Port) {
		failed.Reason = "redis_upstream_mismatch"
		return failed
	}
	if link == "sync" {
		failed.Reason = "redis_replication_syncing"
		return failed
	}
	if link != "connected" {
		if link == "none" || link == "connect" || link == "connecting" || link == "handshake" {
			failed.Reason = "redis_replication_link_down"
		}
		return failed
	}
	if offset < 0 {
		return failed
	}
	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		failed.Reason = "redis_replication_unavailable"
		return failed
	}
	if len(info) > 64*1024 {
		return failed
	}
	fields := make(map[string]string)
	for line := range strings.SplitSeq(info, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || key == "" {
			return failed
		}
		if _, duplicate := fields[key]; duplicate {
			return failed
		}
		fields[key] = value
	}
	// Recheck role and upstream in this reply; ROLE and INFO are separate reads.
	if fields["role"] != "slave" {
		if fields["role"] == "master" {
			failed.Reason = "redis_role_mismatch"
		}
		return failed
	}
	if fields["master_host"] == "" || fields["master_port"] == "" {
		return failed
	}
	if fields["master_host"] != expected.Upstream.Address || fields["master_port"] != strconv.FormatUint(uint64(expected.Upstream.Port), 10) {
		failed.Reason = "redis_upstream_mismatch"
		return failed
	}
	if fields["master_sync_in_progress"] != "0" && fields["master_sync_in_progress"] != "1" {
		return failed
	}
	if fields["master_sync_in_progress"] == "1" {
		failed.Reason = "redis_replication_syncing"
		return failed
	}
	if fields["master_link_status"] != "up" {
		if fields["master_link_status"] == "down" {
			failed.Reason = "redis_replication_link_down"
		}
		return failed
	}
	return engine.Probe{State: engine.ProbePassed}
}
