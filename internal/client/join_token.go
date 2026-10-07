package client

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// EnrollmentToken contains public registration metadata, never the secret token.
type EnrollmentToken struct {
	ID        uuid.UUID `json:"id"`
	NodeID    uuid.UUID `json:"node_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Replayed  bool      `json:"replayed"`
}

type enrollmentTokenRequest struct {
	NodeID     uuid.UUID `json:"node_id"`
	Token      string    `json:"token"`
	TTLSeconds int       `json:"ttl_seconds"`
}

// RegisterJoinToken can bind a token to a recovered device ID.
func (c *Client) RegisterJoinToken(ctx context.Context, nodeID uuid.UUID, secret string, ttlSeconds int) (EnrollmentToken, error) {
	var result EnrollmentToken
	err := c.request(ctx, http.MethodPost, "/v1/enrollment-tokens", enrollmentTokenRequest{NodeID: nodeID, Token: secret, TTLSeconds: ttlSeconds}, uuid.NewString(), http.StatusCreated, &result)
	if err == nil && (result.ID == uuid.Nil || result.NodeID != nodeID || result.ExpiresAt.IsZero()) {
		err = &Error{Code: "invalid_response", Message: "The API returned an incomplete join token registration."}
	}
	return result, err
}
