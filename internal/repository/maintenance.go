package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

const (
	// MaintenanceRunLimit bounds one maintenance run.
	MaintenanceRunLimit = 4 * time.Hour
	// MaintenanceHandover is the wait after a claim. A run of the previous owner ends before it.
	MaintenanceHandover = MaintenanceRunLimit + 2*time.Hour
)

// MaintenanceState tells what a maintenance request did.
type MaintenanceState string

const (
	MaintenanceCompleted   MaintenanceState = "completed"
	MaintenanceClaimed     MaintenanceState = "claimed"
	MaintenanceHandingOver MaintenanceState = "handing_over"
)

// ErrMaintenanceNotOwner means a newer plan gave maintenance of the repository to another device.
var ErrMaintenanceNotOwner = errors.New("another device owns repository maintenance")

// maintenanceOwner is saved in the repository, so the record survives the loss of the controller.
type maintenanceOwner struct {
	NodeID   uuid.UUID `json:"node_id"`
	Revision int64     `json:"revision"`
}

// Maintain runs full Kopia maintenance when nodeID is the one recorded owner of the repository.
// A device that the plan at revision names as owner claims a repository with an older record.
// It runs maintenance only after MaintenanceHandover, so it never overlaps the previous owner.
// Kopia's full safety delays keep content that concurrent backups can still reference.
func Maintain(ctx context.Context, location Location, credentials Credentials, installationID, nodeID uuid.UUID, revision int64) (MaintenanceState, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil || revision < 1 {
		return "", &Error{Action: "maintain", Cause: ErrCatalogInvalid}
	}
	connect, cancel := context.WithTimeout(ctx, 30*time.Second)
	st, err := storage(connect, location, credentials, false)
	if err != nil {
		cancel()
		return "", &Error{Action: "open storage", Cause: errors.Join(err, connect.Err())}
	}
	defer st.Close(context.WithoutCancel(ctx))
	// Kopia records maintenance ownership as user@host. The node ID makes it unique to this device.
	r, err := open(connect, st, credentials.Password, nodeID.String(), nil)
	cancel()
	if err != nil {
		return "", err
	}
	defer r.Close(context.WithoutCancel(ctx))
	return r.maintain(ctx, installationID, nodeID, revision)
}

// maintain requires a connection whose Kopia host is nodeID.
func (r *Repository) maintain(ctx context.Context, installationID, nodeID uuid.UUID, revision int64) (MaintenanceState, error) {
	if r.repo.ClientOptions().Hostname != nodeID.String() {
		return "", &Error{Action: "maintain", Cause: errors.New("connection is not bound to the maintenance device")}
	}
	if err := r.CheckInstallation(ctx, installationID); err != nil {
		return "", err
	}
	r.write.Lock()
	defer r.write.Unlock()
	owner, claimedAt, err := r.maintenanceOwner(ctx, installationID)
	if err != nil {
		return "", err
	}
	if owner.NodeID != nodeID {
		if owner.Revision >= revision {
			return "", ErrMaintenanceNotOwner
		}
		err := repo.WriteSession(ctx, r.repo, repo.WriteSessionOptions{Purpose: "claim Synod maintenance"}, func(ctx context.Context, w repo.RepositoryWriter) error {
			if _, err := w.ReplaceManifests(ctx, map[string]string{"type": "synod.maintenance", "installation": installationID.String()}, maintenanceOwner{NodeID: nodeID, Revision: revision}); err != nil {
				return err
			}
			params, err := maintenance.GetParams(ctx, w)
			if err != nil {
				return err
			}
			params.Owner = w.ClientOptions().UsernameAtHost()
			return maintenance.SetParams(ctx, w, params)
		})
		if fatal := r.fatal.Load(); fatal != nil {
			err = errors.Join(err, *fatal)
		}
		if err != nil {
			return "", &Error{Action: "claim maintenance", Cause: errors.Join(err, ctx.Err())}
		}
		return MaintenanceClaimed, nil
	}
	if r.repo.Time().Sub(claimedAt) < MaintenanceHandover {
		return MaintenanceHandingOver, nil
	}
	direct, ok := r.repo.(repo.DirectRepository)
	if !ok {
		return "", &Error{Action: "maintain", Cause: errors.New("direct repository required")}
	}
	run, cancel := context.WithTimeout(ctx, MaintenanceRunLimit)
	defer cancel()
	err = repo.DirectWriteSession(run, direct, repo.WriteSessionOptions{Purpose: "Synod maintenance"}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
		return snapshotmaintenance.Run(ctx, w, maintenance.ModeFull, false, maintenance.SafetyFull)
	})
	if fatal := r.fatal.Load(); fatal != nil {
		err = errors.Join(err, *fatal)
	}
	if err != nil {
		return "", &Error{Action: "maintain", Cause: errors.Join(err, run.Err())}
	}
	return MaintenanceCompleted, nil
}

// maintenanceOwner returns the record with the newest plan revision and the time of its claim.
func (r *Repository) maintenanceOwner(ctx context.Context, installationID uuid.UUID) (maintenanceOwner, time.Time, error) {
	entries, err := r.repo.FindManifests(ctx, map[string]string{"type": "synod.maintenance", "installation": installationID.String()})
	if err != nil {
		return maintenanceOwner{}, time.Time{}, &Error{Action: "read maintenance owner", Cause: err}
	}
	var owner maintenanceOwner
	var claimedAt time.Time
	for _, entry := range entries {
		var saved maintenanceOwner
		if _, err := r.repo.GetManifest(ctx, entry.ID, &saved); err != nil {
			return maintenanceOwner{}, time.Time{}, &Error{Action: "read maintenance owner", Cause: err}
		}
		if saved.NodeID == uuid.Nil || saved.Revision < 1 {
			return maintenanceOwner{}, time.Time{}, &Error{Action: "read maintenance owner", Cause: ErrCatalogInvalid}
		}
		if saved.Revision > owner.Revision || (saved.Revision == owner.Revision && entry.ModTime.After(claimedAt)) {
			owner, claimedAt = saved, entry.ModTime
		}
	}
	return owner, claimedAt, nil
}
