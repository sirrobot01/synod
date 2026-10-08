package controller

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

func TestControllerRenewsItsCertificatesBeforeExpiry(t *testing.T) {
	dir := t.TempDir()
	if err := runInit(t.Context(), InitOptions{Directory: dir, Address: "192.168.0.20", ServiceDir: filepath.Join(dir, "none")}, io.Discard); err != nil {
		t.Fatal(err)
	}
	var cfg controllerJSON
	if err := json.Unmarshal([]byte(readFile(t, dir, "controller.json")), &cfg); err != nil {
		t.Fatal(err)
	}
	load := func(certificate, key string) *liveCertificate {
		t.Helper()
		pair, err := tls.LoadX509KeyPair(certificate, key)
		if err != nil {
			t.Fatal(err)
		}
		live := new(liveCertificate)
		live.current.Store(&pair)
		return live
	}
	agent := load(cfg.AgentTLSCertFile, cfg.AgentTLSKeyFile)
	api := load(cfg.TLSCertFile, cfg.TLSKeyFile)
	if agent.due(time.Now()) || api.due(time.Now()) {
		t.Fatal("new certificates are due for renewal")
	}
	oldAgent, oldAPI := agent.current.Load().Leaf, api.current.Load().Leaf
	authority, err := pki.LoadCA(uuid.MustParse(cfg.InstallationID), []byte(readFile(t, dir, "ca.crt")), []byte(readFile(t, dir, "ca.key")))
	if err != nil {
		t.Fatal(err)
	}
	soon := oldAgent.NotAfter.Add(-pki.RenewalWindow / 2)
	if !agent.due(soon) {
		t.Fatal("an agent-transport certificate near expiry is not due")
	}
	if err := renewAgentTransport(authority, cfg.AgentTLSCertFile, cfg.AgentTLSKeyFile, agent, soon); err != nil {
		t.Fatal(err)
	}
	renewed := agent.current.Load().Leaf
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(readFile(t, dir, "ca.crt")))
	if _, err := renewed.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: soon.Add(pki.RenewalWindow), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("agents cannot verify the renewed certificate after the old one expires: %v", err)
	}
	if !renewed.NotAfter.After(oldAgent.NotAfter) || renewed.URIs[0].String() != oldAgent.URIs[0].String() || !slices.Equal(renewed.DNSNames, oldAgent.DNSNames) || len(renewed.IPAddresses) != len(oldAgent.IPAddresses) || !bytes.Equal(renewed.RawSubjectPublicKeyInfo, oldAgent.RawSubjectPublicKeyInfo) {
		t.Fatal("the renewed agent-transport certificate changed identity, names, or key")
	}
	if onDisk := load(cfg.AgentTLSCertFile, cfg.AgentTLSKeyFile).current.Load().Leaf; !onDisk.Equal(renewed) {
		t.Fatal("the renewed agent-transport certificate was not saved for the next start")
	}

	later := oldAPI.NotAfter.Add(-time.Hour)
	if err := renewAPI(cfg.APICACertFile, cfg.APICAKeyFile, cfg.TLSCertFile, cfg.TLSKeyFile, api, later); err != nil {
		t.Fatal(err)
	}
	renewedAPI := api.current.Load().Leaf
	apiRoots := x509.NewCertPool()
	apiRoots.AppendCertsFromPEM([]byte(readFile(t, dir, "api-ca.crt")))
	if _, err := renewedAPI.Verify(x509.VerifyOptions{Roots: apiRoots, CurrentTime: later.Add(24 * time.Hour), DNSName: "localhost"}); err != nil {
		t.Fatalf("clients cannot verify the renewed API certificate: %v", err)
	}
	if !renewedAPI.NotAfter.After(oldAPI.NotAfter) || !slices.Equal(renewedAPI.DNSNames, oldAPI.DNSNames) {
		t.Fatal("the renewed API certificate changed its names or did not extend its life")
	}
	info, err := os.Stat(cfg.TLSKeyFile)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("the renewed API key is not private: %v", err)
	}
	if onDisk := load(cfg.TLSCertFile, cfg.TLSKeyFile).current.Load(); !onDisk.Leaf.Equal(renewedAPI) || len(onDisk.Certificate) != 2 {
		t.Fatal("the renewed API certificate and its CA chain were not saved")
	}
}
