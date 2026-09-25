package client

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestSwitchoverSendsMemberAndRevision(t *testing.T) {
	id, member, operation := uuid.New(), uuid.New(), uuid.New()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			MemberID uuid.UUID `json:"member_id"`
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/clusters/"+id.String()+"/actions/switchover" || r.Header.Get("If-Match") != `"4"` || r.Header.Get("Idempotency-Key") != "retry-key" ||
			json.NewDecoder(r.Body).Decode(&body) != nil || body.MemberID != member {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(Admission{ClusterID: id, OperationID: operation, DesiredRevision: 5})
	}))
	defer server.Close()
	api := &Client{endpoint: server.URL, token: "fixture", http: server.Client(), attempts: 1}
	admission, err := api.Switchover(t.Context(), id, member, 4, "retry-key")
	if err != nil || admission.OperationID != operation {
		t.Fatal(admission, err)
	}
	for name, call := range map[string]func() error{
		"no member":   func() error { _, err := api.Switchover(t.Context(), id, uuid.Nil, 4, "retry-key"); return err },
		"no revision": func() error { _, err := api.Switchover(t.Context(), id, member, 0, "retry-key"); return err },
		"bad key":     func() error { _, err := api.Switchover(t.Context(), id, member, 4, "has space"); return err },
	} {
		if failure, ok := errors.AsType[*Error](call()); !ok || failure.Code != "invalid_request" {
			t.Errorf("%s: %v", name, failure)
		}
	}
}
