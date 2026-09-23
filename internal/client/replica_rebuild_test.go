package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestPlanReplicaRebuildRejectsInvalidPlan(t *testing.T) {
	for _, mismatch := range []string{"target", "disposition", "revision", "token", "null volumes", "missing volumes", "zero volume", "member", "resource", "empty volumes", "empty name"} {
		t.Run(mismatch, func(t *testing.T) {
			id := uuid.New()
			plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: id, ClusterName: "photos", Action: "rebuild_replica", MemberID: uuid.New(), ResourceID: uuid.New(), ExpectedRevision: 4, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Replace the failed replica and retain its old volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
			switch mismatch {
			case "empty volumes":
				plan.RetainedVolumes = []uuid.UUID{}
			case "empty name":
				plan.ClusterName = ""
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
			_, err := api.PlanReplicaRebuild(t.Context(), id)
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != "invalid_response" {
				t.Fatalf("invalid plan reached confirmation: %v", err)
			}
		})
	}
}
