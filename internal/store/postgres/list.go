package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

type clusterCursor struct {
	Version        int       `json:"version"`
	InstallationID uuid.UUID `json:"installation_id"`
	IncludeDeleted bool      `json:"include_deleted"`
	CreatedAt      time.Time `json:"created_at"`
	ID             uuid.UUID `json:"id"`
}

// ListClusters reads one installation page in descending creation and ID order.
func (s *Store) ListClusters(ctx context.Context, installationID uuid.UUID, options cluster.ListOptions) (cluster.Page, error) {
	if installationID == uuid.Nil || options.Limit < 1 || options.Limit > 200 || len(options.Cursor) > 1024 {
		return cluster.Page{}, &AdmissionError{Code: InvalidRequest}
	}
	var cursor clusterCursor
	var before *time.Time
	if options.Cursor != "" {
		encoded, err := base64.RawURLEncoding.Strict().DecodeString(options.Cursor)
		if err != nil || json.Unmarshal(encoded, &cursor) != nil || cursor.Version != 1 ||
			cursor.InstallationID != installationID || cursor.IncludeDeleted != options.IncludeDeleted ||
			cursor.ID == uuid.Nil || cursor.CreatedAt.IsZero() {
			return cluster.Page{}, &AdmissionError{Code: InvalidRequest}
		}
		cursor.CreatedAt = cursor.CreatedAt.UTC()
		canonical, err := json.Marshal(cursor)
		if err != nil || base64.RawURLEncoding.EncodeToString(canonical) != options.Cursor {
			return cluster.Page{}, &AdmissionError{Code: InvalidRequest}
		}
		before = &cursor.CreatedAt
	}
	rows, err := s.pool.Query(ctx, `SELECT id, installation_id, spec, desired_revision, desired_lifecycle,
		observed_revision, observed_lifecycle, execution_epoch, created_at, updated_at, deleted_at
		FROM clusters WHERE installation_id=$1 AND ($2 OR deleted_at IS NULL)
		AND ($3::timestamptz IS NULL OR (created_at,id)<($3,$4))
		ORDER BY created_at DESC, id DESC LIMIT $5`, installationID, options.IncludeDeleted, before, cursor.ID, options.Limit+1)
	if err != nil {
		return cluster.Page{}, admissionError(err)
	}
	defer rows.Close()
	page := cluster.Page{Items: make([]cluster.Cluster, 0, options.Limit)}
	for rows.Next() {
		var item cluster.Cluster
		var spec []byte
		if err := rows.Scan(&item.ID, &item.InstallationID, &spec, &item.DesiredRevision, &item.DesiredLifecycle,
			&item.ObservedRevision, &item.ObservedLifecycle, &item.ExecutionEpoch, &item.CreatedAt, &item.UpdatedAt, &item.DeletedAt); err != nil {
			return cluster.Page{}, admissionError(err)
		}
		if len(page.Items) == options.Limit {
			last := page.Items[len(page.Items)-1]
			encoded, err := json.Marshal(clusterCursor{Version: 1, InstallationID: installationID, IncludeDeleted: options.IncludeDeleted, CreatedAt: last.CreatedAt.UTC(), ID: last.ID})
			if err != nil {
				return cluster.Page{}, admissionError(err)
			}
			page.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
			break
		}
		if err := json.Unmarshal(spec, &item.Spec); err != nil {
			return cluster.Page{}, admissionError(err)
		}
		page.Items = append(page.Items, item)
	}
	if err := rows.Err(); err != nil {
		return cluster.Page{}, admissionError(err)
	}
	return page, nil
}
