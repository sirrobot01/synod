package journal

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
	"github.com/sirrobot01/synod/internal/secret"
)

// DestinationKeyring derives a separate cache key from the enrolled private key.
// Keep the enrolled key with the journal. A key replacement must rewrap the cache.
func DestinationKeyring(installationID, nodeID uuid.UUID, privateKey any) (*secret.Keyring, error) {
	if installationID == uuid.Nil || nodeID == uuid.Nil {
		return nil, ErrConflict
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, secret.ErrKeyUnavailable
	}
	defer clear(der)
	key, err := hkdf.Key(sha256.New, der, installationID[:], "synod.destination-cache.v1/"+nodeID.String(), 32)
	if err != nil {
		return nil, secret.ErrKeyUnavailable
	}
	defer clear(key)
	id := uuid.NewSHA1(nodeID, []byte("synod.destination-cache.v1"))
	return secret.NewKeyring(id, map[uuid.UUID][]byte{id: key})
}

// SaveDestinationPlan commits an encrypted replacement before the agent acknowledges it.
func (j *Journal) SaveDestinationPlan(ctx context.Context, ring *secret.Keyring, plan destination.Plan) error {
	if plan.InstallationID != j.installationID || plan.Validate(j.nodeID) != nil {
		return ErrConflict
	}
	input, err := json.Marshal(plan)
	if err != nil {
		return ErrConflict
	}
	defer clear(input)
	binding := secret.Binding{InstallationID: j.installationID, OwnerType: "destination_plan", OwnerID: j.nodeID, CredentialID: j.nodeID, Version: plan.Revision}
	envelope, err := ring.Seal(binding, input)
	if err != nil {
		return err
	}
	sealed, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous int64
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT revision,envelope FROM destination_plan WHERE singleton=1`).Scan(&previous, &raw)
	if err == nil {
		if previous > plan.Revision {
			return ErrConflict
		}
		if previous == plan.Revision {
			var saved secret.Envelope
			if err := json.Unmarshal(raw, &saved); err != nil {
				return ErrConflict
			}
			plain, err := ring.Open(binding, saved)
			if err != nil {
				return err
			}
			defer clear(plain)
			if !bytes.Equal(plain, input) {
				return ErrConflict
			}
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO destination_plan (singleton,revision,envelope) VALUES (1,?,?) ON CONFLICT(singleton) DO UPDATE SET revision=excluded.revision,envelope=excluded.envelope`, plan.Revision, sealed); err != nil {
		return err
	}
	return tx.Commit()
}

// DestinationPlan returns the last committed settings, including during a controller outage.
func (j *Journal) DestinationPlan(ctx context.Context, ring *secret.Keyring) (destination.Plan, error) {
	var revision int64
	var raw []byte
	if err := j.db.QueryRowContext(ctx, `SELECT revision,envelope FROM destination_plan WHERE singleton=1`).Scan(&revision, &raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return destination.Plan{}, ErrNotFound
		}
		return destination.Plan{}, err
	}
	var envelope secret.Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return destination.Plan{}, ErrConflict
	}
	plain, err := ring.Open(secret.Binding{InstallationID: j.installationID, OwnerType: "destination_plan", OwnerID: j.nodeID, CredentialID: j.nodeID, Version: revision}, envelope)
	if err != nil {
		return destination.Plan{}, err
	}
	defer clear(plain)
	var plan destination.Plan
	if err := json.Unmarshal(plain, &plan); err != nil || plan.Revision != revision || plan.InstallationID != j.installationID || plan.Validate(j.nodeID) != nil {
		return destination.Plan{}, ErrConflict
	}
	return plan, nil
}
