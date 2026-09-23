package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

func TestActRefusesUnboundMutations(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
	for _, test := range []struct {
		action       string
		revision     int64
		confirmation *cluster.ActionConfirmation
	}{
		{action: "start", revision: 0},
		{action: "stop", revision: -1},
		{action: "recover-setup", revision: 3},
		{action: "recover-setup", revision: 3, confirmation: &cluster.ActionConfirmation{PlanToken: uuid.New(), Confirm: true, DataDisposition: "purge"}},
		{action: "delete", revision: 3},
		{action: "delete", revision: 3, confirmation: &cluster.ActionConfirmation{PlanToken: uuid.New(), DataDisposition: "retain"}},
		{action: "delete", revision: 3, confirmation: &cluster.ActionConfirmation{Confirm: true, DataDisposition: "retain"}},
		{action: "delete", revision: 3, confirmation: &cluster.ActionConfirmation{PlanToken: uuid.New(), Confirm: true, DataDisposition: "purge"}},
		{action: "start", revision: 3, confirmation: &cluster.ActionConfirmation{PlanToken: uuid.New(), Confirm: true, DataDisposition: "retain"}},
	} {
		_, err := api.Act(t.Context(), uuid.New(), test.action, test.revision, "retry-key", test.confirmation)
		failure, ok := errors.AsType[*Error](err)
		if !ok || failure.Code != "invalid_request" {
			t.Fatalf("unbound action was accepted: %+v %v", test, err)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("unbound actions sent %d requests", requests.Load())
	}
}

func TestPlanDeleteRejectsInvalidPlan(t *testing.T) {
	for _, mismatch := range []string{"target", "disposition", "revision", "token", "null volumes", "missing volumes", "zero volume"} {
		t.Run(mismatch, func(t *testing.T) {
			id := uuid.New()
			plan := cluster.ActionPlan{PlanToken: uuid.New(), ClusterID: id, ClusterName: "photos", Action: "delete", ExpectedRevision: 4, DataDisposition: "retain", RetainedVolumes: []uuid.UUID{}, Downtime: "The cluster stops.", ExpiresAt: time.Now().Add(5 * time.Minute)}
			switch mismatch {
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
			_, err := api.PlanDelete(t.Context(), id)
			failure, ok := errors.AsType[*Error](err)
			if !ok || failure.Code != "invalid_response" {
				t.Fatalf("invalid plan reached confirmation: %v", err)
			}
		})
	}
}
