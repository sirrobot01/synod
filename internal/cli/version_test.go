package cli

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/buildinfo"
)

func TestVersionWorksWithoutControllerSettings(t *testing.T) {
	t.Setenv("SYNOD_API_URL", "invalid")
	t.Setenv("SYNOD_ADMIN_TOKEN_FILE", "/missing")
	var output, diagnostics bytes.Buffer
	if code := Run(t.Context(), []string{"--version"}, &output, &diagnostics); code != 0 {
		t.Fatalf("version failed: %d %s", code, diagnostics.String())
	}
	if !strings.Contains(output.String(), buildinfo.Version) || !strings.Contains(output.String(), buildinfo.Revision) || !strings.Contains(output.String(), runtime.GOOS+"/"+runtime.GOARCH) || diagnostics.Len() != 0 {
		t.Fatal("version did not report this build", output.String(), diagnostics.String())
	}
}
