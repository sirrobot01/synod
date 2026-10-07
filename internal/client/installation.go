package client

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

// Installation identifies the installation and its running controller build.
type Installation struct {
	InstallationID uuid.UUID `json:"installation_id"`
	Version        string    `json:"version"`
	Revision       string    `json:"revision"`
}

// GetInstallation reads the installation identity.
func (c *Client) GetInstallation(ctx context.Context) (Installation, error) {
	var value Installation
	err := c.request(ctx, http.MethodGet, "/v1/installation", nil, "", http.StatusOK, &value)
	if err == nil && value.InstallationID == uuid.Nil {
		err = &Error{Code: "invalid_response", Message: "The API returned an incomplete installation identity."}
	}
	return value, err
}
