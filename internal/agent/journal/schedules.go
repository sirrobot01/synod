package journal

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"time"

	"github.com/sirrobot01/synod/internal/proof"
	"github.com/sirrobot01/synod/internal/secret"
)

// AdmitScheduled saves one current job per app and destination from the encrypted plan.
// The daily ID prevents replay. A pending job or a newer good snapshot suppresses catch-up.
func (j *Journal) AdmitScheduled(ctx context.Context, ring *secret.Keyring, now time.Time) (int, error) {
	plan, err := j.DestinationPlan(ctx, ring)
	if err != nil {
		return 0, err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM destination_plan WHERE singleton=1`).Scan(&revision); err != nil {
		return 0, err
	}
	if revision != plan.Revision {
		return 0, nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM records WHERE state!='terminal'`).Scan(&count); err != nil {
		return 0, err
	}
	admitted := 0
	for _, schedule := range plan.Schedules {
		slot := schedule.Due(now)
		if slot.IsZero() {
			continue
		}
		for _, destinationID := range schedule.Settings.Destinations {
			if count >= 64 {
				break
			}
			available := false
			for _, entry := range plan.Destinations {
				if entry.Destination.ID == destinationID {
					available = true
					break
				}
			}
			if !available {
				continue
			}
			ready := slices.Contains(plan.Ready, destinationID)
			if !ready {
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM opened_destinations WHERE id=?)`, destinationID).Scan(&ready); err != nil {
					return 0, err
				}
			}
			if !ready {
				continue
			}
			job, err := schedule.Request(destinationID, slot)
			if err != nil {
				return 0, ErrConflict
			}
			var skip bool
			if err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM records WHERE id=?) OR
 EXISTS(SELECT 1 FROM backup_history WHERE app_id=? AND destination_id=? AND last_success>=?) OR
 EXISTS(SELECT 1 FROM records WHERE state!='terminal' AND json_extract(CASE WHEN json_valid(CAST(input AS TEXT)) THEN CAST(input AS TEXT) ELSE '{}' END,'$.app_id')=? AND json_extract(CASE WHEN json_valid(CAST(input AS TEXT)) THEN CAST(input AS TEXT) ELSE '{}' END,'$.destination_id')=?)`, job.ID, job.AppID, destinationID, slot.UnixMilli(), job.AppID, destinationID).Scan(&skip); err != nil {
				return 0, err
			}
			if skip {
				continue
			}
			raw, _ := json.Marshal(job)
			hash := sha256.Sum256(raw)
			if _, err := tx.ExecContext(ctx, `INSERT INTO records (id,input_hash,state,input) VALUES (?,?,'queued',?)`, job.ID, hash[:], raw); err != nil {
				return 0, err
			}
			count++
			admitted++
		}
	}
	for _, schedule := range plan.Proofs {
		if count >= 64 {
			break
		}
		slot := schedule.Due(now)
		if slot.IsZero() {
			continue
		}
		ready := false
		if schedule.Kind == proof.Drill {
			// A first drill waits until this agent has made a good snapshot.
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM backup_history WHERE app_id=?)`, schedule.AppID).Scan(&ready); err != nil {
				return 0, err
			}
		} else {
			ready = slices.Contains(plan.Ready, schedule.DestinationID)
			if !ready {
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM opened_destinations WHERE id=?)`, schedule.DestinationID).Scan(&ready); err != nil {
					return 0, err
				}
			}
		}
		if !ready {
			continue
		}
		job, err := schedule.Request(slot)
		if err != nil {
			return 0, ErrConflict
		}
		var skip bool
		if err := tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM records WHERE id=?) OR
 EXISTS(SELECT 1 FROM proof_history WHERE kind=? AND app_id=? AND destination_id=? AND last_success>=?) OR
 EXISTS(SELECT 1 FROM records WHERE state!='terminal' AND json_extract(CASE WHEN json_valid(CAST(input AS TEXT)) THEN CAST(input AS TEXT) ELSE '{}' END,'$.kind')=? AND (?!='restore_drill' AND json_extract(CASE WHEN json_valid(CAST(input AS TEXT)) THEN CAST(input AS TEXT) ELSE '{}' END,'$.destination_id')=? OR ?='restore_drill' AND json_extract(CASE WHEN json_valid(CAST(input AS TEXT)) THEN CAST(input AS TEXT) ELSE '{}' END,'$.app_id')=?))`, job.ID, job.Kind, job.AppID, job.DestinationID, slot.UnixMilli(), job.Kind, job.Kind, job.DestinationID, job.Kind, job.AppID).Scan(&skip); err != nil {
			return 0, err
		}
		if skip {
			continue
		}
		raw, _ := json.Marshal(job)
		hash := sha256.Sum256(raw)
		if _, err := tx.ExecContext(ctx, `INSERT INTO records (id,input_hash,state,input) VALUES (?,?,'queued',?)`, job.ID, hash[:], raw); err != nil {
			return 0, err
		}
		count++
		admitted++
	}
	return admitted, tx.Commit()
}
