package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ResolveNodeName rejects duplicate device names.
func (s *Store) ResolveNodeName(ctx context.Context, installation uuid.UUID, name string) (uuid.UUID, error) {
	if len(name) == 0 || len(name) > 256 {
		return uuid.Nil, &AdmissionError{Code: InvalidRequest}
	}
	rows, err := s.pool.Query(ctx, `SELECT n.id FROM nodes n JOIN node_placement_policies p ON p.installation_id=n.installation_id AND p.node_id=n.id
 WHERE n.installation_id=$1 AND n.revoked_at IS NULL AND convert_from(p.policy_bytes,'UTF8')::jsonb->'labels'->>'synod.name'=$2 LIMIT 2`, installation, name)
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return uuid.Nil, admissionError(err)
	}
	if len(ids) == 0 {
		return uuid.Nil, &AdmissionError{Code: NotFound}
	}
	if len(ids) != 1 {
		return uuid.Nil, &AdmissionError{Code: InvalidRequest}
	}
	return ids[0], nil
}
