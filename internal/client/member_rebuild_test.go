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

func TestPlanMemberRebuildSendsMemberAndChecksPlan(t *testing.T) {
	for _, mismatch := range []string{"", "member", "action", "resource", "volumes", "zero volume", "name"} {
		t.Run(mismatch, func(t *testing.T) {
			id, member := uuid.New(), uuid.New()
			plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: id, ClusterName: "ha", Action: "rebuild_member", MemberID: member, ResourceID: uuid.New(), ExpectedRevision: 4, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{uuid.New()}, Downtime: "Replace the failed member and retain its old volume.", ExpiresAt: time.Now().Add(5 * time.Minute)}
			switch mismatch {
			case "member":
				plan.MemberID = uuid.New()
			case "action":
				plan.Action = "rebuild_replica"
			case "resource":
				plan.ResourceID = uuid.Nil
			case "volumes":
				plan.RetainedVolumes = []uuid.UUID{uuid.New(), uuid.New()}
			case "zero volume":
				plan.RetainedVolumes = []uuid.UUID{uuid.Nil}
			case "name":
				plan.ClusterName = ""
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Action   string    `json:"action"`
					MemberID uuid.UUID `json:"member_id"`
				}
				if r.URL.Path != "/v1/clusters/"+id.String()+"/action-plans" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Action != "rebuild_member" || body.MemberID != member {
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				json.NewEncoder(w).Encode(plan)
			}))
			defer server.Close()
			api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
			_, err := api.PlanMemberRebuild(t.Context(), id, member)
			if mismatch == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != "invalid_response" {
				t.Fatalf("invalid plan reached confirmation: %v", err)
			}
		})
	}
}

func TestActRebuildMemberRequiresConfirmation(t *testing.T) {
	api := &Client{endpoint: "https://127.0.0.1:1", token: "fixture", http: http.DefaultClient, attempts: 1}
	_, err := api.Act(t.Context(), uuid.New(), "rebuild-member", 3, "key", nil)
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != "invalid_request" {
		t.Fatalf("unconfirmed member rebuild: %v", err)
	}
}

func TestActRenewCertificatesTakesNoConfirmation(t *testing.T) {
	api := &Client{endpoint: "https://127.0.0.1:1", token: "fixture", http: http.DefaultClient, attempts: 1}
	_, err := api.Act(t.Context(), uuid.New(), "renew-certificates", 3, "key", &cluster.ActionConfirmation{PlanToken: uuid.New(), Confirm: true, DataDisposition: "retain"})
	if failure, ok := errors.AsType[*Error](err); !ok || failure.Code != "invalid_request" {
		t.Fatalf("confirmed renewal: %v", err)
	}
}
