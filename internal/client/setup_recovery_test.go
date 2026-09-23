package client

import (
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPlanSetupRecoveryRejectsInvalidPlan(t *testing.T) {
	for _, mismatch := range []string{"target", "disposition", "revision", "token", "null volumes", "missing volumes", "zero volume", "member", "resource"} {
		t.Run(mismatch, func(t *testing.T) {
			id := uuid.New()
			plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: id, ClusterName: "photos", Action: "recover_setup", MemberID: uuid.New(), ResourceID: uuid.New(), ExpectedRevision: 4, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "The cluster stops.", ExpiresAt: time.Now().Add(5 * time.Minute)}
			switch mismatch {
			case "member":
				plan.MemberID = uuid.Nil
			case "resource":
				plan.ResourceID = uuid.Nil
			case "target":
				plan.ClusterID = uuid.New()
			case "disposition":
				plan.DataDisposition = "purge"
			case "revision":
				plan.ExpectedRevision = 0
			case "token":
				plan.PlanToken = uuid.Nil
			case "null volumes":
				plan.RetainedVolumes = nil
			case "zero volume":
				plan.RetainedVolumes = []uuid.UUID{uuid.New(), uuid.Nil}
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mismatch == "missing volumes" {
					json.NewEncoder(w).Encode(struct {
						cluster.ActionPlan
						RetainedVolumes []uuid.UUID `json:"retained_volumes,omitempty"`
					}{ActionPlan: plan})
					return
				}
				json.NewEncoder(w).Encode(plan)
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
			_, err := api.PlanSetupRecovery(t.Context(), id)
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != "invalid_response" {
				t.Fatalf("invalid plan reached confirmation: %v", err)
			}
		})
	}
}
