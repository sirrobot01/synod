package jointoken

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRoundTrip(t *testing.T) {
	want := Token{Address: "192.0.2.10:8443", InstallationID: uuid.New(), Fingerprint: Fingerprint([]byte("certificate")), Secret: NewSecret()}
	encoded := Encode(want)
	if !strings.HasPrefix(encoded, prefix) {
		t.Fatalf("encoded=%q, want prefix %s", encoded, prefix)
	}
	got, err := Decode(encoded + "\n")
	if err != nil || got != want {
		t.Fatalf("Decode=%+v, %v; want %+v", got, err, want)
	}
}

func TestDecodeRejectsIncompleteTokens(t *testing.T) {
	valid := Token{Address: "192.0.2.10:8443", InstallationID: uuid.New(), Fingerprint: Fingerprint([]byte("certificate")), Secret: NewSecret()}
	for name, token := range map[string]Token{
		"no port":         {Address: "192.0.2.10", InstallationID: valid.InstallationID, Fingerprint: valid.Fingerprint, Secret: valid.Secret},
		"no installation": {Address: valid.Address, Fingerprint: valid.Fingerprint, Secret: valid.Secret},
		"short pin":       {Address: valid.Address, InstallationID: valid.InstallationID, Fingerprint: "sha256:00", Secret: valid.Secret},
		"other pin":       {Address: valid.Address, InstallationID: valid.InstallationID, Fingerprint: "md5:" + strings.Repeat("0", 64), Secret: valid.Secret},
		"short secret":    {Address: valid.Address, InstallationID: valid.InstallationID, Fingerprint: valid.Fingerprint, Secret: "c2VjcmV0"},
	} {
		if _, err := Decode(Encode(token)); err == nil {
			t.Errorf("%s: Decode accepted %+v", name, token)
		}
	}
	for _, bad := range []string{"", "synod_join_", "other_" + Encode(valid)[len(prefix):], prefix + "!!!", prefix + "e30"} {
		if _, err := Decode(bad); err == nil {
			t.Errorf("Decode accepted %q", bad)
		}
	}
}

// The web UI builds join tokens in TypeScript. This token comes from its encoder.
func TestDecodeAcceptsTheWebEncoding(t *testing.T) {
	token, err := Decode("synod_join_eyJhZGRyZXNzIjoiMTkyLjAuMi4yMDo4NDQzIiwiaW5zdGFsbGF0aW9uX2lkIjoiN2QwZjBmNWUtOWY1ZS00YTM5LThkM2EtMmIzYTRjNWQ2ZTdmIiwiZmluZ2VycHJpbnQiOiJzaGEyNTY6YWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYmFiYWJhYiIsInNlY3JldCI6IkFBRUNBd1FGQmdjSUNRb0xEQTBPRHhBUkVoTVVGUllYR0JrYUd4d2RIaDgifQ")
	if err != nil || token.Address != "192.0.2.20:8443" || token.InstallationID.String() != "7d0f0f5e-9f5e-4a39-8d3a-2b3a4c5d6e7f" || token.NodeID != uuid.Nil {
		t.Fatalf("decode the web token: %+v %v", token, err)
	}
}
