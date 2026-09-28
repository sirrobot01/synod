package database

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"
)

// RedisPosition binds an applied offset to one replication history.
type RedisPosition struct {
	ReplicationID string
	Offset        int64
}

// PauseWrites stops writes for at most one minute and reads the final replication position.
// The caller must resume writes after an error if it still holds the writer lease.
func (r Redis) PauseWrites(ctx context.Context, x Exec, pause time.Duration) (position RedisPosition, err error) {
	if err := r.validate(); err != nil {
		return position, err
	}
	if pause < time.Millisecond || pause > time.Minute {
		return position, ErrConfiguration
	}
	fields, err := r.info(ctx, x)
	if err != nil {
		return position, err
	}
	if fields["role"] != "master" {
		return position, ErrConfiguration
	}

	if _, err := r.cli(ctx, x, "", "CLIENT", "PAUSE", strconv.FormatInt(pause.Milliseconds(), 10), "WRITE"); err != nil {
		return position, err
	}
	fields, err = r.info(ctx, x)
	if err != nil {
		return position, err
	}
	position.ReplicationID = fields["master_replid"]
	position.Offset, err = strconv.ParseInt(fields["master_repl_offset"], 10, 64)
	id, decodeErr := hex.DecodeString(position.ReplicationID)
	if err != nil || decodeErr != nil || len(id) != 20 || position.Offset < 0 || fields["role"] != "master" {
		return RedisPosition{}, ErrConfiguration
	}
	return position, nil
}

// CaughtUp checks the replica's applied offset and replication history.
func (r Redis) CaughtUp(ctx context.Context, x Exec, position RedisPosition) (bool, error) {
	if err := r.validate(); err != nil {
		return false, err
	}
	id, err := hex.DecodeString(position.ReplicationID)
	if err != nil || len(id) != 20 || position.Offset < 0 {
		return false, ErrConfiguration
	}
	fields, err := r.info(ctx, x)
	if err != nil {
		return false, err
	}
	if fields["role"] != "slave" || fields["master_sync_in_progress"] != "0" || fields["master_link_status"] != "up" || fields["master_replid"] != position.ReplicationID {
		return false, nil
	}
	offset, err := strconv.ParseInt(fields["slave_repl_offset"], 10, 64)
	if err != nil {
		return false, err
	}
	return offset >= position.Offset, nil
}

func (r Redis) ResumeWrites(ctx context.Context, x Exec) error {
	if err := r.validate(); err != nil {
		return err
	}
	_, err := r.cli(ctx, x, "", "CLIENT", "UNPAUSE")
	return err
}
