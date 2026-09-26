package postgres

import (
	"encoding/json"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPatroniConfigurationBoundaries(t *testing.T) {
	settings := PatroniSettings{RequiredFreeBytes: 1 << 30, ApplicationCredentialID: uuid.New(), ApplicationCredentialVersion: 1, InstallationID: uuid.New(), ClusterID: uuid.New(), ResourceID: uuid.New(), VolumeID: uuid.New(), ProvisionID: uuid.New(), Address: netip.MustParseAddr("192.0.2.10"), DatabasePort: 5432, RESTPort: 8008, DCSEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}, ClientNetworks: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}
	secrets := PatroniSecrets{ApplicationVerifier: "SCRAM-SHA-256$4096:AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", Superuser: uuid.NewString(), Replication: uuid.NewString(), Rewind: uuid.NewString(), REST: uuid.NewString()}
	for _, synchronous := range []bool{false, true} {
		settings.Synchronous = synchronous
		encoded, err := RenderPatroni(settings, secrets)
		if err != nil {
			t.Fatal(err)
		}
		var config patroniConfiguration
		if err := json.Unmarshal(encoded, &config); err != nil {
			t.Fatal(err)
		}
		if config.Namespace != "/synod-patroni/"+settings.InstallationID.String()+"/" || config.Scope != settings.ClusterID.String() || config.Name != settings.ResourceID.String() {
			t.Fatal("lost member identity")
		}
		if config.RESTAPI.VerifyClient != "required" || config.Etcd3.Protocol != "https" || config.CTL.Insecure || config.PostgreSQL.Authentication.Replication.SSLMode != "verify-full" || config.PostgreSQL.Authentication.Rewind.SSLMode != "verify-full" || config.PostgreSQL.Parameters.SSL != "on" {
			t.Fatal("insecure native connection")
		}
		if config.Bootstrap.DCS.FailsafeMode || !config.Bootstrap.DCS.CheckTimeline || config.Bootstrap.DCS.SynchronousModeStrict != synchronous || config.Bootstrap.DCS.SynchronousNodeCount != 1 || (config.Bootstrap.DCS.SynchronousMode == "on") != synchronous {
			t.Fatal("native failure policy changed")
		}
		if config.PostgreSQL.RemoveDataDirectoryOnRewindFailure || config.PostgreSQL.RemoveDataDirectoryOnDivergedTimelines || config.PostgreSQL.Parameters.MaxSlotWALKeepSize != "1GB" {
			t.Fatal("unsafe data or WAL retention policy")
		}
		loopback := "hostssl all synod_admin,synod_app 127.0.0.0/8 scram-sha-256"
		for index, line := range config.PostgreSQL.PGHBA {
			if line == loopback {
				// First match wins, so the loopback rule must precede the administrative reject.
				if !slices.Contains(config.PostgreSQL.PGHBA[index:], "host all synod_admin,synod_repl,synod_rewind 0.0.0.0/0 reject") {
					t.Fatal("loopback readiness rule follows the administrative reject")
				}
				continue
			}
			if strings.Contains(line, "hostssl") && !strings.Contains(line, "192.0.2.0/24") && !strings.Contains(line, "198.51.100.0/24") {
				t.Fatal("undeclared client network")
			}
		}
		if !slices.Contains(config.PostgreSQL.PGHBA, loopback) {
			t.Fatal("missing loopback readiness rule")
		}
		if config.PostgreSQL.Listen != "192.0.2.10,127.0.2.10:5432" || config.PostgreSQL.ConnectAddress != "192.0.2.10:5432" {
			t.Fatalf("listen %q and connect %q", config.PostgreSQL.Listen, config.PostgreSQL.ConnectAddress)
		}
	}
	for name, change := range map[string]func(*PatroniSettings){
		"missing copy space":          func(s *PatroniSettings) { s.RequiredFreeBytes = 0 },
		"negative copy space":         func(s *PatroniSettings) { s.RequiredFreeBytes = -1 },
		"missing identity":            func(s *PatroniSettings) { s.ClusterID = uuid.Nil },
		"missing application":         func(s *PatroniSettings) { s.ApplicationCredentialID = uuid.Nil },
		"invalid application version": func(s *PatroniSettings) { s.ApplicationCredentialVersion = 0 },
		"missing volume":              func(s *PatroniSettings) { s.VolumeID = uuid.Nil },
		"missing provision":           func(s *PatroniSettings) { s.ProvisionID = uuid.Nil },
		"loopback":                    func(s *PatroniSettings) { s.Address = netip.MustParseAddr("127.0.0.1") },
		"unspecified":                 func(s *PatroniSettings) { s.Address = netip.IPv6Unspecified() },
		"broadcast":                   func(s *PatroniSettings) { s.Address = netip.MustParseAddr("255.255.255.255") },
		"port collision":              func(s *PatroniSettings) { s.RESTPort = s.DatabasePort },
		"no DCS":                      func(s *PatroniSettings) { s.DCSEndpoints = nil },
		"URL credential":              func(s *PatroniSettings) { s.DCSEndpoints = []string{"https://user:private@example:2379"} },
		"DCS duplicate":               func(s *PatroniSettings) { s.DCSEndpoints = []string{"example:2379", "example:2379"} },
		"DCS port":                    func(s *PatroniSettings) { s.DCSEndpoints = []string{"example:0"} },
		"DCS loopback":                func(s *PatroniSettings) { s.DCSEndpoints = []string{"127.0.0.1:2379"} },
		"DCS DNS":                     func(s *PatroniSettings) { s.DCSEndpoints = []string{"host..example:2379"} },
		"DCS control":                 func(s *PatroniSettings) { s.DCSEndpoints = []string{"host\x01:2379"} },
		"DCS alias":                   func(s *PatroniSettings) { s.DCSEndpoints = []string{"HOST.example:2379", "host.example.:02379"} },
		"no clients":                  func(s *PatroniSettings) { s.ClientNetworks = nil },
		"network host bits":           func(s *PatroniSettings) { s.PeerNetworks = []netip.Prefix{netip.MustParsePrefix("192.0.2.1/24")} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := settings
			change(&changed)
			if value, err := RenderPatroni(changed, secrets); err == nil || value != nil {
				t.Fatal("invalid settings rendered")
			}
		})
	}
	for _, password := range []string{"", "short", strings.Repeat("x", 257), strings.Repeat("x", 32) + "\n", secrets.Superuser} {
		changed := secrets
		changed.REST = password
		if value, err := RenderPatroni(settings, changed); err == nil || value != nil || strings.Contains(err.Error(), password) && password != "" && password != "short" {
			t.Fatal("invalid credential accepted or disclosed")
		}
	}

	for _, verifier := range []string{"", "private-password", "SCRAM-SHA-256$1:AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", strings.Replace(secrets.ApplicationVerifier, "==", "=", 1)} {
		changed := secrets
		changed.ApplicationVerifier = verifier
		if value, err := RenderPatroni(settings, changed); err == nil || value != nil || verifier != "" && strings.Contains(err.Error(), verifier) {
			t.Fatal("invalid application verifier accepted or disclosed")
		}
	}
}

// Members on one host with different addresses listen on different loopback addresses.
func TestReadinessLoopbackFollowsTheMember(t *testing.T) {
	for member, want := range map[string]string{
		"192.168.0.219":  "127.168.0.219/32",
		"100.87.229.38":  "127.87.229.38/32",
		"10.0.0.1":       "127.0.0.2/32",
		"10.255.255.255": "127.0.0.2/32",
		"fd00::10":       "::1/128",
	} {
		if got := ReadinessLoopback(netip.MustParseAddr(member)).String(); got != want {
			t.Errorf("%s: loopback %s, want %s", member, got, want)
		}
	}
}
