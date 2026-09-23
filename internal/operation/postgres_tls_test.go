package operation

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgresReadinessBindsTLSIdentityAndTrust(t *testing.T) {
	root, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	command := commandFixture()
	command.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: "postgres", PostgresRole: "primary", Address: "127.0.0.1", Port: 5432, Database: "app", Role: "synod_app", PostgresTls: &agentv1.PostgresTLS{RootCaPem: root, ServerName: "192.0.2.10"}}}
	hash, err := CommandHash(command)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.CheckReadiness){
		"missing root":     func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = nil },
		"malformed root":   func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = []byte("not a CA") },
		"missing identity": func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "" },
		"DNS identity":     func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "db.example" },
		"nonlocal target":  func(p *agentv1.CheckReadiness) { p.Address = "192.0.2.10" },
		"redis":            func(p *agentv1.CheckReadiness) { p.Engine = "redis"; p.PostgresRole = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(command).(*agentv1.Command)
			change(bad.Input.GetCheckReadiness())
			if _, err := CommandHash(bad); err == nil {
				t.Fatal("invalid TLS readiness admitted")
			}
		})
	}
	for name, change := range map[string]func(*agentv1.CheckReadiness){
		"identity": func(p *agentv1.CheckReadiness) { p.PostgresTls.ServerName = "192.0.2.11" },
		"trust":    func(p *agentv1.CheckReadiness) { p.PostgresTls.RootCaPem = append(p.PostgresTls.RootCaPem, '\n') },
		"mode":     func(p *agentv1.CheckReadiness) { p.PostgresTls = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(command).(*agentv1.Command)
			change(changed.Input.GetCheckReadiness())
			other, err := CommandHash(changed)
			if err != nil || other == hash {
				t.Fatal("TLS contract not bound", err)
			}
		})
	}
}
