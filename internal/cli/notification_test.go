package cli_test

import (
	"bytes"
	json "encoding/json/v2"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/notification"
)

func TestNotificationCLIReadsPrivateSettingsAndNeverPrintsURL(t *testing.T) {
	secretURL := "generic+http://127.0.0.1:1/private-notification-token"
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/notifications":
			json.MarshalWrite(w, notification.Status{Revision: 1, Settings: notification.DefaultSettings().Public()})
		case "PUT /v1/notifications":
			var update notification.Update
			if json.UnmarshalRead(r.Body, &update) != nil || update.ID == uuid.Nil || update.ExpectedRevision != 1 || update.Settings.URL != secretURL || update.Settings.MissingAfterHours != 168 || r.Header.Get("Idempotency-Key") != update.ID.String() {
				t.Error("CLI changed configuration")
			}
			json.MarshalWrite(w, notification.Confirmation{ID: update.ID, Revision: 2})
		default:
			t.Error("unexpected route")
			http.NotFound(w, r)
		}
	}))
	path := filepath.Join(t.TempDir(), "notification.json")
	raw, _ := json.Marshal(struct {
		URL string `json:"url"`
	}{secretURL})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, permissions := range []os.FileMode{0600, 0644} {
		if err := os.Chmod(path, permissions); err != nil {
			t.Fatal(err)
		}
		var out, diagnostics bytes.Buffer
		args := append(append([]string{}, flags...), "notification", "configure", "--file", path)
		result := cli.Run(t.Context(), args, &out, &diagnostics)
		if (result == 0) != (permissions == 0600) {
			t.Fatalf("configuration exit=%d mode=%o", result, permissions)
		}
		if bytes.Contains(out.Bytes(), []byte(secretURL)) || bytes.Contains(diagnostics.Bytes(), []byte(secretURL)) {
			t.Fatal("CLI exposed the URL")
		}
	}
}
