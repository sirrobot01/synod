package client

import (
	"context"
	"net/http"

	"github.com/sirrobot01/synod/internal/inventory"
)

func (c *Client) ListApps(ctx context.Context) (inventory.Catalog, error) {
	var catalog inventory.Catalog
	err := c.request(ctx, http.MethodGet, "/v1/apps", nil, "", http.StatusOK, &catalog)
	if err == nil {
		for _, app := range catalog.Apps {
			if app.Definition == nil {
				return inventory.Catalog{}, &Error{Code: "invalid_response", Message: "The app inventory has no definition."}
			}
		}
	}
	return catalog, err
}
