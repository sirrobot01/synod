package executor

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/runtime"
	agentv1 "github.com/sirrobot01/synod/proto/agent/v1"
)

// prepareEngine creates the Synod-owned directories of a tier 1 copy. When the copy
// mounts "tls", it also writes the node certificate, its key, and the installation CA
// there. The container copies them to a private path before the engine starts.
func (e *Executor) prepareEngine(appID string, mounts []*agentv1.Mount) error {
	id, err := uuid.Parse(appID)
	if err != nil || id == uuid.Nil || !filepath.IsAbs(e.options.AppsDir) {
		return runtime.ErrUnsupportedConfiguration
	}
	root := filepath.Join(e.options.AppsDir, id.String(), "engine")
	for _, m := range mounts {
		if m.Type != "engine" {
			continue
		}
		if err := os.MkdirAll(filepath.Join(root, m.Source), 0o700); err != nil {
			return err
		}
		if m.Source != "tls" {
			continue
		}
		certificate := e.options.EngineTLS
		if certificate == nil || len(certificate.Certificate) == 0 || len(e.options.EngineCA) == 0 {
			return runtime.ErrUnsupportedConfiguration
		}
		key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
		if err != nil {
			return err
		}
		var chain []byte
		for _, der := range certificate.Certificate {
			chain = append(chain, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
		}
		files := map[string][]byte{"server.crt": chain, "server.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), "ca.crt": e.options.EngineCA}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(root, "tls", name), content, 0o600); err != nil {
				return err
			}
		}
		clear(key)
	}
	return nil
}
