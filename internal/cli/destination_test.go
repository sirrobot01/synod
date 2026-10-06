package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
	"github.com/sirrobot01/synod/internal/destination"
)

func TestDestinationCLIPrivateInputWarningAndRemoval(t *testing.T) {
	node := uuid.New()
	password := "private-repository-cli-passphrase"
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(`{"password":"`+password+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var saved destination.Destination
	removed := false
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/nodes/by-name/host":
			json.NewEncoder(w).Encode(client.Node{ID: node, Name: "host"})
		case r.Method == "POST" && r.URL.Path == "/v1/destinations":
			var entry destination.Entry
			if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if entry.Credentials.Password != password || entry.Destination.NodeID != node || entry.Destination.Location.Path != "/backup" || r.Header.Get("Idempotency-Key") != entry.Destination.ID.String() {
				t.Error("wrong destination input")
			}
			saved = entry.Destination
			json.NewEncoder(w).Encode(saved)
		case r.Method == "GET" && r.URL.Path == "/v1/destinations":
			json.NewEncoder(w).Encode(destination.Listing{Revision: 2, Destinations: []destination.Destination{saved}})
		case r.Method == "DELETE" && r.URL.Path == "/v1/destinations/"+saved.ID.String():
			removed = true
			w.WriteHeader(204)
		default:
			t.Error("unexpected destination request")
			w.WriteHeader(404)
		}
	}))
	var stdout, stderr bytes.Buffer
	args := append([]string{}, flags...)
	args = append(args, "destination", "add", "Home", "--device", "host", "--path", "/backup", "--credentials-file", path)
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 0 {
		t.Fatalf("add failed: %s", stderr.String())
	}
	var result cli.Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Destination == nil || len(result.Warnings) != 1 || !strings.Contains(result.Message, "No backup has run") {
		t.Fatal("missing configuration limit or own-device warning")
	}
	if strings.Contains(stdout.String()+stderr.String(), password) {
		t.Fatal("CLI leaked password")
	}
	stdout.Reset()
	stderr.Reset()
	if code := cli.Run(t.Context(), append(flags, "destination", "remove", "Home"), &stdout, &stderr); code != 0 || !removed {
		t.Fatalf("remove failed: %s", stderr.String())
	}
	if !strings.Contains(stdout.String(), "Repository data was kept") {
		t.Fatal("missing repository retention result")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := cli.Run(t.Context(), args, &stdout, &stderr); code != 1 || strings.Contains(stdout.String()+stderr.String(), password) {
		t.Fatal("public credential file accepted or printed")
	}
}
