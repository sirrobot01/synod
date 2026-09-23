package operation

import (
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"testing"
)

func TestReadinessRequiresTheDeclaredPostgresRole(t *testing.T) {
	for _, tc := range []struct {
		engine, role string
		valid        bool
	}{
		{"postgres", "primary", true}, {"postgres", "replica", true}, {"postgres", "", false},
		{"postgres", "writer", false}, {"postgres", "PRIMARY", false}, {"redis", "", true}, {"redis", "primary", false},
	} {
		t.Run(tc.engine+"/"+tc.role, func(t *testing.T) {
			command := commandFixture()
			command.Input.Payload = &agentv1.CommandInput_CheckReadiness{CheckReadiness: &agentv1.CheckReadiness{Engine: tc.engine, PostgresRole: tc.role, Address: "127.0.0.1", Port: 5432, Database: "app", Role: "synod_app"}}
			hash, err := CommandHash(command)
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid role accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			command.InputHash = hash[:]
			if err := ValidateCommand(command); err != nil {
				t.Fatal(err)
			}
			if tc.engine == "postgres" {
				other := "primary"
				if tc.role == "primary" {
					other = "replica"
				}
				command.Input.GetCheckReadiness().PostgresRole = other
				if err := ValidateCommand(command); err == nil {
					t.Fatal("changed native role kept the old command hash")
				}
			}
		})
	}
}
