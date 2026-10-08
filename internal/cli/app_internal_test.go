package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirrobot01/synod/internal/client"
)

// The CLI on the controller reads the API address, CA, and token that init saved.
func TestLocalControllerReadsInitSettings(t *testing.T) {
	dir := t.TempDir()
	write := func(listen string) string {
		path := filepath.Join(dir, "controller.json")
		data := `{"listen":"` + listen + `","admin_token_file":"/c/admin.token","api_ca_cert_file":"/c/api-ca.crt"}`
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for listen, endpoint := range map[string]string{"192.168.0.20:8443": "https://192.168.0.20:8443", "0.0.0.0:8443": "https://127.0.0.1:8443", ":9443": "https://127.0.0.1:9443", "[::]:8443": "https://127.0.0.1:8443"} {
		got := localController(write(listen))
		if got != (client.Config{Endpoint: endpoint, TokenFile: "/c/admin.token", CAFile: "/c/api-ca.crt"}) {
			t.Fatalf("listen %s: got %+v", listen, got)
		}
	}
	if got := localController(filepath.Join(dir, "missing.json")); got != (client.Config{}) {
		t.Fatalf("a missing file gave settings: %+v", got)
	}
	if got := localController(write("no-port")); got != (client.Config{}) {
		t.Fatalf("an invalid listen address gave settings: %+v", got)
	}
}
