package client

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/cluster"
)

// PlanSetupRecovery reviews the existing member and data covered by setup recovery.
func (c *Client) PlanSetupRecovery(ctx context.Context, id uuid.UUID) (cluster.ActionPlan, error) {
	var value cluster.ActionPlan
	if id == uuid.Nil {
		return value, &Error{Code: "invalid_request", Message: "Use a nonzero cluster UUID."}
	}
	err := c.request(ctx, http.MethodPost, "/v1/clusters/"+id.String()+"/action-plans", struct {
		Action string `json:"action"`
	}{Action: "recover_setup"}, "", http.StatusOK, &value)
	if err == nil && (value.ClusterID != id || value.PlanToken == uuid.Nil || value.ClusterName == "" || value.ExpectedRevision < 1 || value.Action != "recover_setup" || value.MemberID == uuid.Nil || value.ResourceID == uuid.Nil || value.DataDisposition != "retain" || len(value.RetainedVolumes) != 1 || value.RetainedVolumes[0] == uuid.Nil || value.Downtime == "" || value.ExpiresAt.IsZero()) {
		err = &Error{Code: "invalid_response", Message: "The API returned an invalid setup recovery plan. No recovery request was sent."}
	}
	return value, err
}
