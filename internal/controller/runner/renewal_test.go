package runner

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/store/postgres"
)

type renewalFake struct {
	expiries []postgres.CertificateExpiry
	admitted []postgres.ClusterActionRequest
}

func (f *renewalFake) ListCertificateExpiries(context.Context, uuid.UUID) ([]postgres.CertificateExpiry, error) {
	return f.expiries, nil
}

func (f *renewalFake) AdmitClusterAction(_ context.Context, request postgres.ClusterActionRequest) (postgres.Admission, error) {
	f.admitted = append(f.admitted, request)
	return postgres.Admission{ClusterID: request.ClusterID, OperationID: uuid.New()}, nil
}

// The sweep admits renewals only inside the window, keys each by cluster revision, and warns
// when a certificate is close to expiry.
func TestRenewalSweepAdmitsDueClusters(t *testing.T) {
	now := time.Now()
	installation := uuid.New()
	late, due, urgent := uuid.New(), uuid.New(), uuid.New()
	store := &renewalFake{expiries: []postgres.CertificateExpiry{
		{ClusterID: late, DesiredRevision: 4, ExpiresAt: now.Add(20 * 24 * time.Hour)},
		{ClusterID: due, DesiredRevision: 7, ExpiresAt: now.Add(9 * 24 * time.Hour)},
		{ClusterID: urgent, DesiredRevision: 2, ExpiresAt: now.Add(time.Hour)},
	}}
	var logs bytes.Buffer
	renewCertificates(t.Context(), store, installation, slog.New(slog.NewTextHandler(&logs, nil)), now)
	if len(store.admitted) != 2 {
		t.Fatalf("admitted %d renewals, want two", len(store.admitted))
	}
	for index, want := range []struct {
		cluster  uuid.UUID
		revision int64
	}{{due, 7}, {urgent, 2}} {
		request := store.admitted[index]
		key := "renew-" + want.cluster.String() + "-" + map[int64]string{7: "7", 2: "2"}[want.revision]
		if request.ClusterID != want.cluster || request.ExpectedRevision != want.revision || request.Identity.IdempotencyKey != key ||
			request.Identity.APIOperation != postgres.RenewCertificatesAPIOperation || request.Identity.ActorID != renewalActor || request.Identity.InstallationID != installation {
			t.Fatalf("renewal request %d: %+v", index, request)
		}
	}
	if strings.Count(logs.String(), "Patroni certificate expires soon") != 1 || !strings.Contains(logs.String(), urgent.String()) {
		t.Fatalf("expiry warning: %s", logs.String())
	}
}
