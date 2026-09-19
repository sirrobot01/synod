package client

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// User is one operator account as the API reports it.
type User struct {
	ID       uuid.UUID `json:"id"`
	Username string    `json:"username"`
	Role     string    `json:"role"`
	Disabled bool      `json:"disabled"`
}

// APIToken is one stored token. Plaintext appears only when the token is created.
type APIToken struct {
	ID         uuid.UUID  `json:"id"`
	ActorID    string     `json:"actor_id"`
	Name       string     `json:"name"`
	Role       string     `json:"role"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	// Plaintext is disclosed once, by the create call, and is never stored.
	Plaintext string `json:"token,omitempty"`
}

// CreateUser adds one operator account.
func (c *Client) CreateUser(ctx context.Context, username, password string) (User, error) {
	var value User
	body := struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}{username, password}
	err := c.request(ctx, http.MethodPost, "/v1/users", body, "", http.StatusCreated, &value)
	return value, err
}

// SetUserPassword replaces one password and ends every other session of that account.
func (c *Client) SetUserPassword(ctx context.Context, id uuid.UUID, password string) error {
	body := struct {
		Password string `json:"password"`
	}{password}
	return c.request(ctx, http.MethodPost, "/v1/users/"+id.String()+"/actions/set-password", body, "", http.StatusNoContent, nil)
}

// DisableUser stops new sign-ins and ends every live session of the account.
func (c *Client) DisableUser(ctx context.Context, id uuid.UUID) error {
	return c.request(ctx, http.MethodPost, "/v1/users/"+id.String()+"/actions/disable", nil, "", http.StatusNoContent, nil)
}

// CreateAPIToken mints one token. The reply carries the plaintext exactly once.
func (c *Client) CreateAPIToken(ctx context.Context, name, actorID string, expires *time.Time) (APIToken, error) {
	var value APIToken
	body := struct {
		Name      string     `json:"name"`
		ActorID   string     `json:"actor_id,omitempty"`
		ExpiresAt *time.Time `json:"expires_at,omitempty"`
	}{name, actorID, expires}
	err := c.request(ctx, http.MethodPost, "/v1/tokens", body, "", http.StatusCreated, &value)
	return value, err
}

// ListAPITokens reads every token without any secret.
func (c *Client) ListAPITokens(ctx context.Context) ([]APIToken, error) {
	var value []APIToken
	err := c.request(ctx, http.MethodGet, "/v1/tokens", nil, "", http.StatusOK, &value)
	return value, err
}

// RevokeAPIToken ends one token. Repeat revocation is accepted.
func (c *Client) RevokeAPIToken(ctx context.Context, id uuid.UUID) error {
	return c.request(ctx, http.MethodPost, "/v1/tokens/"+id.String()+"/actions/revoke", nil, "", http.StatusNoContent, nil)
}
