package api

import (
	"bytes"
	json "encoding/json/v2"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/notification"
	"github.com/sirrobot01/synod/internal/secret"
	"testing"
)

func TestNotificationAPIKeepsURLPrivateAndRequiresAuthentication(t *testing.T) {
	server, _, _ := identityServer(t)
	key := uuid.New()
	ring, err := secret.NewKeyring(key, map[uuid.UUID][]byte{key: bytes.Repeat([]byte{21}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	server.CredentialKeyring = ring
	settings := notification.DefaultSettings()
	settings.URL = "generic+http://127.0.0.1:1/private-notification-token"
	update := notification.Update{ID: uuid.New(), ExpectedRevision: 1, Settings: settings}
	raw, _ := json.Marshal(update)
	for _, route := range []struct{ method, path, body string }{{"GET", "/v1/notifications", ""}, {"PUT", "/v1/notifications", string(raw)}, {"POST", "/v1/notifications/test", `{}`}} {
		if response := call(t, server, route.method, route.path, route.body); response.Code != 401 {
			t.Fatal("anonymous notification route", response.Code)
		}
	}
	for range 2 {
		response := call(t, server, "PUT", "/v1/notifications", string(raw), withBootstrap)
		if response.Code != 200 || bytes.Contains(response.Body.Bytes(), []byte("private-notification-token")) {
			t.Fatal("configuration response", response.Code)
		}
	}
	update.ID = uuid.New()
	raw, _ = json.Marshal(update)
	if response := call(t, server, "PUT", "/v1/notifications", string(raw), withBootstrap); response.Code != 409 || !bytes.Contains(response.Body.Bytes(), []byte("notification_settings_changed")) {
		t.Fatal("stale update accepted", response.Code)
	}
	request := notification.TestRequest{ID: uuid.New(), ExpectedRevision: 2}
	raw, _ = json.Marshal(request)
	for range 2 {
		if response := call(t, server, "POST", "/v1/notifications/test", string(raw), withBootstrap); response.Code != 200 {
			t.Fatal("test request", response.Code)
		}
	}
	response := call(t, server, "GET", "/v1/notifications", "", withBootstrap)
	var status notification.Status
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &status) != nil || !status.Settings.Configured || len(status.Alerts) != 1 || status.Alerts[0].ID != request.ID {
		t.Fatal("notification status", response.Code)
	}
	if bytes.Contains(response.Body.Bytes(), []byte("private-notification-token")) {
		t.Fatal("URL leaked")
	}
}
