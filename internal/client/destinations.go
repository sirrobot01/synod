package client

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/destination"
)

func (c *Client) AddDestination(ctx context.Context, entry destination.Entry) (destination.Destination, error) {
	var result destination.Destination
	err := c.request(ctx, http.MethodPost, "/v1/destinations", entry, entry.Destination.ID.String(), http.StatusOK, &result)
	if err == nil && result.ID != entry.Destination.ID {
		err = &Error{Code: "invalid_response", Message: "The API returned an unexpected destination identity."}
	}
	return result, err
}

func (c *Client) ListDestinations(ctx context.Context) (destination.Listing, error) {
	var result destination.Listing
	err := c.request(ctx, http.MethodGet, "/v1/destinations", nil, "", http.StatusOK, &result)
	return result, err
}

// RemoveDestination leaves the backup repository intact.
func (c *Client) RemoveDestination(ctx context.Context, id uuid.UUID) error {
	return c.request(ctx, http.MethodDelete, "/v1/destinations/"+id.String(), nil, id.String(), http.StatusNoContent, nil)
}
