package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/auth"
	"github.com/sirrobot01/synod/internal/restore"
)

func TestFilesRequireAdminAndMarkOnlyCompleteStreams(t *testing.T) {
	server, metadata, _ := identityServer(t)
	id, snapshot, destination := uuid.New(), uuid.New(), uuid.New()
	endpoint := "/v1/apps/" + id.String() + "/files/content?path=%2Fsrv%2Fsecret"
	called := 0
	server.ReadFile = func(ctx context.Context, request restore.FileRequest) (restore.FileHeader, io.ReadCloser, error) {
		called++
		if request.AppID != id || request.InstallationID != server.installationID || request.Action != restore.FileRead || request.Path != "/srv/secret" {
			t.Error("wrong request selection")
		}
		return restore.FileHeader{SnapshotID: snapshot, DestinationID: destination, Format: "file", Name: "secret", Size: 7}, io.NopCloser(strings.NewReader("private")), nil
	}
	if response := call(t, server, "GET", endpoint, ""); response.Code != 401 || called != 0 {
		t.Fatal("anonymous file read accepted")
	}
	token, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := metadata.CreateAPIToken(t.Context(), token.ID, "reader", "reader", "admin", token.Verifier, nil); err != nil {
		t.Fatal(err)
	}
	if err := metadata.RevokeAPIToken(t.Context(), token.ID); err != nil {
		t.Fatal(err)
	}
	if response := call(t, server, "GET", endpoint, "", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token.Plaintext) }); response.Code != 401 || called != 0 {
		t.Fatal("revoked token file read accepted", response.Code)
	}
	response := call(t, server, "GET", endpoint, "", withBootstrap)
	if response.Code != 200 || response.Body.String() != "private" || response.Result().Trailer.Get("X-Synod-Transfer") != "complete" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("file response changed", response.Code)
	}
	for _, invalid := range []string{"snapshot=bad", "via=bad", "destination=bad"} {
		if response := call(t, server, "GET", endpoint+"&"+invalid, "", withBootstrap); response.Code != 400 {
			t.Fatal("invalid selection accepted")
		}
	}
	// A real HTTP server must abort, rather than terminate a partial body normally.
	server.ReadFile = func(context.Context, restore.FileRequest) (restore.FileHeader, io.ReadCloser, error) {
		reader, writer := io.Pipe()
		go func() { writer.Write([]byte("partial")); writer.CloseWithError(io.ErrUnexpectedEOF) }()
		return restore.FileHeader{SnapshotID: snapshot, DestinationID: destination, Format: "tar", Name: "saved"}, reader, nil
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	request, err := http.NewRequestWithContext(t.Context(), "GET", httpServer.URL+endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	withBootstrap(request)
	broken, err := httpServer.Client().Do(request)
	if err == nil {
		defer broken.Body.Close()
		_, err = io.ReadAll(broken.Body)
		if err == nil || broken.Trailer.Get("X-Synod-Transfer") == "complete" {
			t.Fatal("partial body was marked complete")
		}
	}
}
