package client

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/restore"
)

func fileURL(input restore.FileRequest) string {
	query := url.Values{}
	for key, id := range map[string]uuid.UUID{"snapshot": input.SnapshotID, "destination": input.DestinationID, "via": input.NodeID} {
		if id != uuid.Nil {
			query.Set(key, id.String())
		}
	}
	if input.Path != "" {
		query.Set("path", input.Path)
	}
	endpoint := "/v1/apps/" + input.AppID.String()
	switch input.Action {
	case restore.FileSnapshots:
		endpoint += "/snapshots"
	case restore.FileRead:
		endpoint += "/files/content"
	default:
		endpoint += "/files"
	}
	return endpoint + "?" + query.Encode()
}

func (c *Client) ListFiles(ctx context.Context, input restore.FileRequest) (restore.FileHeader, error) {
	var header restore.FileHeader
	err := c.request(ctx, http.MethodGet, fileURL(input), nil, "", http.StatusOK, &header)
	return header, err
}

// DownloadFile streams bytes once. It requires the final completion trailer.
// The caller discards partial output on any error.
func (c *Client) DownloadFile(ctx context.Context, input restore.FileRequest, output io.Writer) (restore.FileHeader, error) {
	input.Action = restore.FileRead
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+fileURL(input), nil)
	if err != nil {
		return restore.FileHeader{}, &Error{Code: "invalid_request", Message: "The file request is invalid."}
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	client := *c.http
	client.Timeout = 12 * time.Hour
	response, err := client.Do(request)
	if err != nil {
		return restore.FileHeader{}, &Error{Code: "transport_error", Message: "The download could not start. Check the API connection and retry.", cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return restore.FileHeader{}, &Error{Code: "file_read_failed", Message: "The download was refused. Check the saved path, snapshot, and destination."}
	}
	header := restore.FileHeader{Format: response.Header.Get("X-Synod-Format"), Warnings: response.Header.Values("X-Synod-Warning")}
	header.SnapshotID, err = uuid.Parse(response.Header.Get("X-Synod-Snapshot-ID"))
	if err != nil || header.SnapshotID == uuid.Nil || (input.SnapshotID != uuid.Nil && input.SnapshotID != header.SnapshotID) {
		return header, &Error{Code: "invalid_response", Message: "The download has no matching snapshot ID."}
	}
	header.DestinationID, err = uuid.Parse(response.Header.Get("X-Synod-Destination-ID"))
	if err != nil || header.DestinationID == uuid.Nil || (input.DestinationID != uuid.Nil && input.DestinationID != header.DestinationID) || (header.Format != "file" && header.Format != "tar") {
		return header, &Error{Code: "invalid_response", Message: "The download has invalid destination or format details."}
	}
	n, err := io.Copy(output, response.Body)
	if err != nil || response.Trailer.Get("X-Synod-Transfer") != "complete" {
		return header, &Error{Code: "file_transfer_incomplete", Message: "The download did not finish. Discard partial output and retry.", cause: err}
	}
	header.Size = n
	return header, nil
}
