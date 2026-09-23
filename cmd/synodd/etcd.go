package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/sirrobot01/synod/internal/config"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// connectEtcd refuses the client's implicit fallback when server authentication is disabled.
func connectEtcd(ctx context.Context, settings config.Controller) (*clientv3.Client, error) {
	connection, err := etcdClientConfig(ctx, settings)
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(connection)
	if err != nil {
		return nil, fmt.Errorf("connect to coordination service: %w", err)
	}
	if !settings.EtcdAllowPlaintext {
		authCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := client.Authenticate(authCtx, connection.Username, connection.Password)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("authenticate coordination account: %w", errors.Join(err, client.Close()))
		}
	}
	return client, nil
}

// etcdClientConfig loads protected credentials before the controller serves requests.
func etcdClientConfig(ctx context.Context, settings config.Controller) (clientv3.Config, error) {
	connection := clientv3.Config{DialTimeout: 5 * time.Second, Context: ctx, Logger: zap.NewNop()}
	scheme := "https://"
	if settings.EtcdAllowPlaintext {
		scheme = "http://"
	}
	for endpoint := range strings.SplitSeq(settings.EtcdEndpoints, ",") {
		connection.Endpoints = append(connection.Endpoints, scheme+strings.TrimSpace(endpoint))
	}
	if settings.EtcdAllowPlaintext {
		return connection, nil
	}
	trust, err := readStartupFile(settings.EtcdCAFile, 1<<20, false)
	if err != nil {
		return clientv3.Config{}, fmt.Errorf("read coordination CA bundle: %w", err)
	}
	roots := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(trust)) > 0 {
		trust = bytes.TrimSpace(trust)
		if !bytes.HasPrefix(trust, []byte("-----BEGIN CERTIFICATE-----")) {
			return clientv3.Config{}, errors.New("coordination trust must contain only CA certificates")
		}
		block, rest := pem.Decode(trust)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return clientv3.Config{}, errors.New("coordination trust contains an invalid certificate block")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA || !certificate.BasicConstraintsValid || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return clientv3.Config{}, errors.New("coordination trust contains an invalid CA certificate")
		}
		roots.AddCert(certificate)
		count++
		trust = rest
	}
	if count == 0 {
		return clientv3.Config{}, errors.New("coordination requires a nonempty CA bundle")
	}
	connection.TLS = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	if settings.EtcdTLSCertFile != "" {
		certificate, err := readStartupFile(settings.EtcdTLSCertFile, 1<<20, false)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("read coordination client certificate: %w", err)
		}
		key, err := readStartupFile(settings.EtcdTLSKeyFile, 64<<10, true)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("read protected coordination client key: %w", err)
		}
		pair, err := tls.X509KeyPair(certificate, key)
		if err != nil {
			return clientv3.Config{}, errors.New("coordination client certificate and key do not match")
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil || leaf.IsCA || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
			return clientv3.Config{}, errors.New("coordination requires a current client-auth certificate")
		}
		connection.TLS.Certificates = []tls.Certificate{pair}
	}
	password, err := readStartupFile(settings.EtcdPasswordFile, 258, true)
	if err != nil {
		return clientv3.Config{}, fmt.Errorf("read protected coordination password: %w", err)
	}
	if bytes.HasSuffix(password, []byte("\n")) {
		password = bytes.TrimSuffix(password, []byte("\n"))
		password = bytes.TrimSuffix(password, []byte("\r"))
	}
	if len(password) < 32 || len(password) > 256 {
		return clientv3.Config{}, errors.New("coordination password requires 32 to 256 visible ASCII bytes")
	}
	for _, value := range password {
		if value < 33 || value > 126 {
			return clientv3.Config{}, errors.New("coordination password requires visible ASCII bytes")
		}
	}
	connection.Username, connection.Password = settings.EtcdUsername, string(password)
	return connection, nil
}
