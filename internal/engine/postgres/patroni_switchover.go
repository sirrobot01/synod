package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/secret"
)

// Switchover failures. Each is a stable failure code.
var (
	// ErrSwitchoverIneligible reports a candidate that is not a healthy replica, or a cluster
	// with no leader. No switchover request was sent.
	ErrSwitchoverIneligible = errors.New("patroni_switchover_ineligible")
	// ErrSwitchoverRefused reports a switchover request that Patroni answered with a refusal.
	ErrSwitchoverRefused = errors.New("patroni_switchover_refused")
	// ErrSwitchoverUnconfirmed reports a switchover whose result was not seen before the deadline.
	ErrSwitchoverUnconfirmed = errors.New("patroni_switchover_unconfirmed")
)

// switchoverInterval is how often the cluster view is read again after a request.
const switchoverInterval = time.Second

// DecodePatroniTLSMaterial reads the JSON form of a member or DCS credential.
func DecodePatroniTLSMaterial(value []byte) (PatroniTLSMaterial, error) {
	var material PatroniTLSMaterial
	if len(value) == 0 || len(value) > secret.MaxPlaintextBytes {
		return material, errors.New("Patroni TLS material has an invalid size")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&material); err != nil {
		return material, errors.New("Patroni TLS material is invalid")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return material, errors.New("Patroni TLS material has trailing data")
	}
	return material, nil
}

// PatroniRESTClient returns a client for the REST API of the member at address. It verifies the
// server against the native CA for that address and presents the member certificate.
func PatroniRESTClient(material PatroniTLSMaterial, address netip.Addr) (*http.Client, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(material.CA) {
		return nil, errors.New("Patroni REST trust is invalid")
	}
	pair, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		return nil, errors.New("Patroni REST client certificate is invalid")
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: address.String(), Certificates: []tls.Certificate{pair}},
	}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, nil
}

type patroniClusterView struct {
	Members []struct {
		Name  string `json:"name"`
		Role  string `json:"role"`
		State string `json:"state"`
	} `json:"members"`
}

// PatroniSwitchover makes candidate the Patroni leader through the REST API at base, such as
// https://192.0.2.10:8008. It returns false when the candidate already leads and no request
// was sent. It waits until the cluster view shows the candidate as the running leader.
func PatroniSwitchover(ctx context.Context, client *http.Client, base string, candidate uuid.UUID, password string) (bool, error) {
	name := candidate.String()
	leader, role, err := patroniRoles(ctx, client, base, name)
	if err != nil {
		return false, err
	}
	if leader == name {
		return false, nil
	}
	if leader == "" || (role != "replica" && role != "sync_standby") {
		return false, ErrSwitchoverIneligible
	}
	body, err := json.Marshal(struct {
		Leader    string `json:"leader"`
		Candidate string `json:"candidate"`
	}{leader, name})
	if err != nil {
		return false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/switchover", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("synod_patroni", password)
	response, err := client.Do(request)
	// A lost reply does not tell whether Patroni acted, so the cluster view decides.
	if err == nil {
		response.Body.Close()
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
			return false, fmt.Errorf("%w: HTTP %d", ErrSwitchoverRefused, response.StatusCode)
		}
	}
	for {
		if leader, _, err := patroniRoles(ctx, client, base, name); err == nil && leader == name {
			return true, nil
		}
		select {
		case <-ctx.Done():
			return false, ErrSwitchoverUnconfirmed
		case <-time.After(switchoverInterval):
		}
	}
}

// patroniRoles reads the cluster view. It returns the running leader, if one exists, and the
// role of the named member when that member is healthy.
func patroniRoles(ctx context.Context, client *http.Client, base, name string) (string, string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/cluster", nil)
	if err != nil {
		return "", "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrSwitchoverUnconfirmed, err)
	}
	defer response.Body.Close()
	var view patroniClusterView
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&view) != nil {
		return "", "", fmt.Errorf("%w: cluster view HTTP %d", ErrSwitchoverUnconfirmed, response.StatusCode)
	}
	var leader, role string
	for _, member := range view.Members {
		if member.Role == "leader" && member.State == "running" {
			leader = member.Name
		}
		if member.Name == name && (member.State == "running" || member.State == "streaming") {
			role = member.Role
		}
	}
	return leader, role, nil
}
