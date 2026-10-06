package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cli"
	"github.com/sirrobot01/synod/internal/client"
)

func TestInstallationGetPrintsTheIdentity(t *testing.T) {
	value := client.Installation{InstallationID: uuid.New()}
	flags := fixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/installation" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(value)
	}))
	// The fixture flags select JSON output.
	var stdout, stderr bytes.Buffer
	var result cli.Result
	if code := cli.Run(t.Context(), append(flags, "installation", "get"), &stdout, &stderr); code != 0 || json.Unmarshal(stdout.Bytes(), &result) != nil || result.Installation == nil || *result.Installation != value {
		t.Fatalf("installation get: code=%d out=%s err=%s", code, stdout.String(), stderr.String())
	}
}
