package cli

import (
	"strings"
	"testing"

	"github.com/sirrobot01/synod/internal/proof"
)

func TestChecksPrintStatusKindResourceAndDetail(t *testing.T) {
	var output strings.Builder
	writeChecks(&output, []proof.Check{
		{Kind: proof.CheckHTTP, Resource: "app:80", Status: proof.CheckFailed, Detail: "status=500"},
		{Kind: proof.CheckAppFunction, Resource: "worker", Status: proof.CheckUnchecked},
	})
	for _, line := range []string{"Check: failed    http_response    app:80 (status=500)", "Check: unchecked app_function     worker"} {
		if !strings.Contains(output.String(), line+"\n") {
			t.Fatalf("missing %q in:\n%s", line, output.String())
		}
	}
}
