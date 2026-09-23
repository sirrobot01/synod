package client

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

// ListClusters reads one bounded page. The caller chooses whether to read another page.
func (c *Client) ListClusters(ctx context.Context, options cluster.ListOptions) (cluster.Page, error) {
	if options.Limit < 1 || options.Limit > 200 || len(options.Cursor) > 1024 {
		return cluster.Page{}, &Error{Code: "invalid_request", Message: "Use a page limit from 1 to 200 and a cursor of at most 1024 bytes."}
	}
	query := url.Values{
		"limit":           {strconv.Itoa(options.Limit)},
		"include_deleted": {strconv.FormatBool(options.IncludeDeleted)},
	}
	if options.Cursor != "" {
		query.Set("cursor", options.Cursor)
	}
	var wire struct {
		Items      []cluster.Cluster `json:"items"`
		NextCursor *string           `json:"next_cursor"`
	}
	if err := c.request(ctx, http.MethodGet, "/v1/clusters?"+query.Encode(), nil, "", http.StatusOK, &wire); err != nil {
		return cluster.Page{}, err
	}
	invalid := &Error{Code: "invalid_response", Message: "The API returned an invalid cluster page."}
	if wire.Items == nil || len(wire.Items) > options.Limit || wire.NextCursor == nil || len(*wire.NextCursor) > 1024 || (*wire.NextCursor != "" && (len(wire.Items) != options.Limit || *wire.NextCursor == options.Cursor)) {
		return cluster.Page{}, invalid
	}
	seen := make(map[uuid.UUID]bool, len(wire.Items))
	for i, item := range wire.Items {
		if item.ID == uuid.Nil || item.InstallationID == uuid.Nil || seen[item.ID] || item.Spec.Validate() != nil ||
			item.DesiredRevision < 1 || item.ObservedRevision < 0 || item.ExecutionEpoch < 0 ||
			item.CreatedAt.IsZero() || item.UpdatedAt.IsZero() {
			return cluster.Page{}, invalid
		}
		seen[item.ID] = true
		switch item.DesiredLifecycle {
		case cluster.DesiredRunning, cluster.DesiredStopped, cluster.DesiredDeleted:
		default:
			return cluster.Page{}, invalid
		}
		switch item.ObservedLifecycle {
		case cluster.ObservedProvisioning, cluster.ObservedRunning, cluster.ObservedStopped, cluster.ObservedDeleting, cluster.ObservedDeleted, cluster.ObservedUnknown:
		default:
			return cluster.Page{}, invalid
		}
		if item.DeletedAt != nil && (!options.IncludeDeleted || item.DeletedAt.IsZero()) {
			return cluster.Page{}, invalid
		}
		if i > 0 {
			previous := wire.Items[i-1]
			if item.InstallationID != previous.InstallationID || item.CreatedAt.After(previous.CreatedAt) ||
				(item.CreatedAt.Equal(previous.CreatedAt) && bytes.Compare(item.ID[:], previous.ID[:]) >= 0) {
				return cluster.Page{}, invalid
			}
		}
	}
	return cluster.Page{Items: wire.Items, NextCursor: *wire.NextCursor}, nil
}
