package inventory

import (
	"strings"
	"testing"

	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRefuseUnsafeAndAmbiguousInventory(t *testing.T) {
	valid := &agentv1.AppInventory{ObservedAt: timestamppb.Now(), Apps: []*agentv1.DiscoveredApp{{Key: "compose:notes", Name: "notes", Kind: "compose", Containers: []*agentv1.DiscoveredContainer{{Id: "abc", Name: "web", Image: "nginx:stable"}}, Paths: []*agentv1.DiscoveredPath{{Kind: "bind", Status: "Included", Source: "/srv/notes", Reason: "App state."}}}}}
	if err := Validate(valid); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*agentv1.AppInventory){
		"duplicate app":     func(v *agentv1.AppInventory) { v.Apps = append(v.Apps, v.Apps[0]) },
		"partial failure":   func(v *agentv1.AppInventory) { v.FailureCode = "docker_scan_failed" },
		"control path":      func(v *agentv1.AppInventory) { v.Apps[0].Paths[0].Source = "/srv/\x1b[0m" },
		"unknown status":    func(v *agentv1.AppInventory) { v.Apps[0].Paths[0].Status = "Safe" },
		"negative size":     func(v *agentv1.AppInventory) { v.Apps[0].Paths[0].SizeBytes = -1 },
		"unknown container": func(v *agentv1.AppInventory) { v.Apps[0].Paths[0].ContainerId = "other" },
		"excessive payload": func(v *agentv1.AppInventory) { v.Apps[0].Name = strings.Repeat("x", 1<<20) },
	} {
		t.Run(name, func(t *testing.T) {
			v := proto.Clone(valid).(*agentv1.AppInventory)
			change(v)
			if Validate(v) == nil {
				t.Fatal("invalid report accepted")
			}
		})
	}
}
