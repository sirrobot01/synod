package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirrobot01/synod/internal/engine"
	"github.com/sirrobot01/synod/internal/pki"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPatroniNativeTLSReplicationAndFailover(t *testing.T) {
	host, image := os.Getenv("SYNOD_TEST_DOCKER_HOST"), os.Getenv("SYNOD_TEST_PATRONI_IMAGE")
	if host == "" || image == "" {
		t.Skip("set Docker host and an immutable Patroni fixture image")
	}
	if !strings.Contains(image, "sha256:") {
		t.Fatal("Patroni fixture image must use a digest")
	}
	for _, policy := range []struct {
		name                 string
		synchronous          bool
		interruptClone       bool
		interruptApplication bool
		pauseApplication     bool
	}{{"false", false, false, false, false}, {"true", true, false, false, false}, {"interrupted_clone", false, true, false, false}, {"interrupted_application", false, false, true, false}, {"paused_application", false, false, false, true}} {
		t.Run(policy.name, func(t *testing.T) {
			synchronous := policy.synchronous
			testCtx, cancelTest := context.WithTimeout(t.Context(), 4*time.Minute)
			defer cancelTest()
			f := &bootstrapFixture{host: host}
			prefix := "synod-patroni-" + uuid.NewString()
			network := prefix + "-net"
			f.docker(t, "network", "create", "--label", "io.synod.fixture=patroni", network)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if err := exec.CommandContext(ctx, "docker", "--host", host, "network", "rm", network).Run(); err != nil {
					t.Error(err)
				}
			})
			subnet := netip.MustParsePrefix(strings.TrimSpace(f.docker(t, "network", "inspect", "--format", "{{(index .IPAM.Config 0).Subnet}}", network)))
			names := []string{prefix + "-dcs", prefix + "-one", prefix + "-two"}
			// The router takes a fixed address that member leaves also name.
			routerAddress := subnet.Addr()
			for range 10 {
				routerAddress = routerAddress.Next()
			}
			memberCommands := make([][]string, 3)
			for index, name := range names {
				address := subnet.Addr().Next().Next()
				for range index {
					address = address.Next()
				}
				program := "while [ ! -f /run/synod-patroni/ready ]; do sleep 0.1; done; "
				if index == 0 {
					program += "exec etcd --data-dir /tmp/etcd --listen-client-urls https://0.0.0.0:2379 --advertise-client-urls https://$(hostname -i):2379 --cert-file /run/synod-patroni/member.crt --key-file /run/synod-patroni/member.key --trusted-ca-file /run/synod-patroni/ca.crt --client-cert-auth=true --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 --initial-cluster default=http://127.0.0.1:2380"
				} else {
					program = `if [ ! -f /tmp/synod-autostart ]; then while [ ! -f /run/synod-patroni/ready ]; do sleep 0.1; done; fi; rm -f /run/synod-patroni/*; chown root:root /run/synod-patroni; exec "$@"`
					memberCommands[index] = PatroniCommand()
					if policy.interruptClone && index == 2 {
						boundary := `        native(["/usr/local/bin/pg_verifybackup", str(staged_data)], 120)`
						if strings.Count(memberCommands[index][4], boundary) != 1 {
							t.Fatal("clone interruption boundary changed")
						}
						memberCommands[index][4] = strings.Replace(memberCommands[index][4], boundary, "        os.kill(os.getpid(), signal.SIGKILL)\n"+boundary, 1)
					}
					if policy.interruptApplication && index == 1 {
						boundary := `            cursor.execute("CREATE DATABASE app OWNER synod_app TEMPLATE template0")`
						if strings.Count(memberCommands[index][5], boundary) != 1 {
							t.Fatal("application interruption boundary changed")
						}
						memberCommands[index][5] = strings.Replace(memberCommands[index][5], boundary, "            os.kill(os.getpid(), signal.SIGKILL)\n"+boundary, 1)
					}
					if policy.pauseApplication && index == 1 {
						boundary := "        verify(connection, config, expected)\n    finally:"
						if strings.Count(memberCommands[index][5], boundary) != 1 {
							t.Fatal("application completion boundary changed")
						}
						memberCommands[index][5] = strings.Replace(memberCommands[index][5], boundary, "        verify(connection, config, expected)\n        os.kill(os.getpid(), signal.SIGSTOP)\n    finally:", 1)
					}
				}
				args := []string{"run", "--detach", "--name", name, "--label", "io.synod.fixture=patroni", "--network", network, "--ip", address.String(), "--memory", "512m", "--cpus", "1", "--pids-limit", "128", "--tmpfs", PatroniDirectory + ":rw,noexec,nosuid,nodev,size=4m", "--entrypoint", "/bin/sh"}
				if index == 0 {
					args = append(args, "--publish", "127.0.0.1::2379")
				} else {
					args = append(args, "--publish", "127.0.0.1::5432", "--publish", "127.0.0.1::8008")
				}
				args = append(args, image, "-ec", program, "fixture")
				args = append(args, memberCommands[index]...)
				f.docker(t, args...)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					if err := exec.CommandContext(ctx, "docker", "--host", host, "rm", "--force", "--volumes", name).Run(); err != nil {
						t.Error(err)
					}
				})
			}
			installation, clusterID := uuid.New(), uuid.New()
			caPEM, caKeyPEM, err := pki.CreatePatroniCA(installation, time.Now(), 2*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			issuer, err := pki.LoadPatroniCA(installation, caPEM, caKeyPEM)
			if err != nil {
				t.Fatal(err)
			}
			caBlock, _ := pem.Decode(caPEM)
			ca, err := x509.ParseCertificate(caBlock.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			keyBlock, _ := pem.Decode(caKeyPEM)
			parsedCAKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			caKey := parsedCAKey.(*ecdsa.PrivateKey)
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(caPEM)
			_, dcsKey, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			dcsCSR, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, dcsKey)
			if err != nil {
				t.Fatal(err)
			}
			dcsCert, err := issuer.IssuePatroni(dcsCSR, pki.PatroniIdentity{InstallationID: installation, ClusterID: clusterID}, netip.Addr{}, time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			dcsKeyDER, err := x509.MarshalPKCS8PrivateKey(dcsKey)
			if err != nil {
				t.Fatal(err)
			}
			dcsKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: dcsKeyDER})
			dcsMaterial := PatroniTLSMaterial{CA: caPEM, Certificate: dcsCert, PrivateKey: dcsKeyPEM}
			secrets := PatroniSecrets{Superuser: uuid.NewString(), Replication: uuid.NewString(), Rewind: uuid.NewString(), DCS: uuid.NewString(), REST: uuid.NewString()}

			applicationID, applicationPassword := uuid.New(), uuid.NewString()
			salt := []byte("native-test-salt")
			salted, err := pbkdf2.Key(sha256.New, applicationPassword, salt, 4096, 32)
			if err != nil {
				t.Fatal(err)
			}
			clientKey := hmac.New(sha256.New, salted)
			clientKey.Write([]byte("Client Key"))
			storedKey := sha256.Sum256(clientKey.Sum(nil))
			serverKey := hmac.New(sha256.New, salted)
			serverKey.Write([]byte("Server Key"))
			secrets.ApplicationVerifier = "SCRAM-SHA-256$4096:" + base64.StdEncoding.EncodeToString(salt) + "$" + base64.StdEncoding.EncodeToString(storedKey[:]) + ":" + base64.StdEncoding.EncodeToString(serverKey.Sum(nil))
			dirs, addresses := make([]string, 3), make([]string, 3)
			materials := make([]PatroniTLSMaterial, 3)
			settings := make([]PatroniSettings, 3)
			var clientPair tls.Certificate
			install := func(index int) {
				t.Helper()
				var archive bytes.Buffer
				writer := tar.NewWriter(&archive)
				entries, err := os.ReadDir(dirs[index])
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					data, err := os.ReadFile(filepath.Join(dirs[index], entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
					if err := writer.WriteHeader(&tar.Header{Name: entry.Name(), Mode: 0o600, Size: int64(len(data))}); err != nil {
						t.Fatal(err)
					}
					if _, err := writer.Write(data); err != nil {
						t.Fatal(err)
					}
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "-i", names[index], "tar", "-x", "-C", PatroniDirectory)
				command.Stdin = bytes.NewReader(archive.Bytes())
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("install native private files: %v (%d output bytes)", err, len(output))
				}
				if index == 1 || index == 2 {
					f.docker(t, "exec", names[index], "mkdir", "-p", "-m", "700", PatroniSourceDirectory)
					command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "-i", names[index], "tar", "-x", "-C", PatroniSourceDirectory, "config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key")
					command.Stdin = bytes.NewReader(archive.Bytes())
					if output, err := command.CombinedOutput(); err != nil {
						t.Fatalf("install protected source: %v (%d bytes)", err, len(output))
					}
				}
			}
			for index, name := range names {
				addresses[index] = strings.TrimSpace(f.docker(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name))
				dirs[index] = t.TempDir()
				resourceID := uuid.New()
				var certPEM, keyPEM []byte
				if index == 0 {
					key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
					if err != nil {
						t.Fatal(err)
					}
					leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP(addresses[index]), net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
					der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
					if err != nil {
						t.Fatal(err)
					}
					keyDER, err := x509.MarshalPKCS8PrivateKey(key)
					if err != nil {
						t.Fatal(err)
					}
					certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
					keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
				} else {
					_, key, err := ed25519.GenerateKey(rand.Reader)
					if err != nil {
						t.Fatal(err)
					}
					csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
					if err != nil {
						t.Fatal(err)
					}
					certPEM, err = issuer.IssuePatroni(csr, pki.PatroniIdentity{InstallationID: installation, ClusterID: clusterID, ResourceID: resourceID}, netip.MustParseAddr(addresses[index]), time.Now(), time.Hour, routerAddress)
					if err != nil {
						t.Fatal(err)
					}
					keyDER, err := x509.MarshalPKCS8PrivateKey(key)
					if err != nil {
						t.Fatal(err)
					}
					keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
				}
				if index == 0 {
					clientPair, err = tls.X509KeyPair(certPEM, keyPEM)
					if err != nil {
						t.Fatal(err)
					}
				}
				program, applicationProgram := PatroniGuardProgram, PatroniApplicationProgram
				if index > 0 {
					program, applicationProgram = memberCommands[index][4], memberCommands[index][5]
				}
				for filename, data := range map[string][]byte{"application.py": []byte(applicationProgram), "guard.py": []byte(program), "ca.crt": caPEM, "member.crt": certPEM, "member.key": keyPEM, "dcs-ca.crt": caPEM, "dcs-client.crt": dcsCert, "dcs-client.key": dcsKeyPEM} {
					if err := os.WriteFile(filepath.Join(dirs[index], filename), data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if index > 0 {
					settings[index] = PatroniSettings{RequiredFreeBytes: 1 << 30, ApplicationCredentialID: applicationID, ApplicationCredentialVersion: 1, InstallationID: installation, ClusterID: clusterID, ResourceID: resourceID, VolumeID: uuid.New(), ProvisionID: uuid.New(), AllowInitialization: true, Address: netip.MustParseAddr(addresses[index]), DatabasePort: 5432, RESTPort: 8008, DCSEndpoints: []string{net.JoinHostPort(addresses[0], "2379")}, PeerNetworks: []netip.Prefix{subnet}, ClientNetworks: []netip.Prefix{subnet}, Synchronous: synchronous}
					material := PatroniTLSMaterial{CA: caPEM, Certificate: certPEM, PrivateKey: keyPEM}
					materials[index] = material
					files, err := PatroniFiles(settings[index], secrets, material, dcsMaterial, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					for filename, data := range files {
						if err := os.WriteFile(filepath.Join(dirs[index], filename), data, 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				install(index)
			}
			f.docker(t, "exec", names[0], "touch", PatroniDirectory+"/ready")
			dcsAddress := strings.TrimSpace(f.docker(t, "port", names[0], "2379/tcp"))
			dcs, err := clientv3.New(clientv3.Config{Endpoints: []string{"https://" + dcsAddress}, DialTimeout: 5 * time.Second, TLS: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{clientPair}, ServerName: addresses[0]}})
			if err != nil {
				t.Fatal(err)
			}
			defer dcs.Close()
			ctx, cancel := context.WithTimeout(testCtx, 30*time.Second)
			defer cancel()
			if _, err := dcs.Auth.UserAdd(ctx, "root", uuid.NewString()); err != nil {
				t.Fatalf("DCS root setup: %v; %s", err, f.docker(t, "logs", "--tail", "30", names[0]))
			}
			if _, err := dcs.Auth.UserGrantRole(ctx, "root", "root"); err != nil {
				t.Fatal(err)
			}
			dcsUser := "synod_patroni_" + clusterID.String()
			if _, err := dcs.Auth.UserAdd(ctx, dcsUser, secrets.DCS); err != nil {
				t.Fatal(err)
			}
			if _, err := dcs.Auth.RoleAdd(ctx, dcsUser); err != nil {
				t.Fatal(err)
			}
			keyPrefix := "/synod-patroni/" + installation.String() + "/" + clusterID.String() + "/"
			if _, err := dcs.Auth.RoleGrantPermission(ctx, dcsUser, keyPrefix, clientv3.GetPrefixRangeEnd(keyPrefix), clientv3.PermissionType(clientv3.PermReadWrite)); err != nil {
				t.Fatal(err)
			}
			if _, err := dcs.Auth.UserGrantRole(ctx, dcsUser, dcsUser); err != nil {
				t.Fatal(err)
			}
			if _, err := dcs.Auth.AuthEnable(ctx); err != nil {
				t.Fatal(err)
			}
			rest := make([]string, 3)
			database := make([]*pgx.ConnConfig, 3)
			publishedREST := func(index int) string {
				t.Helper()
				endpoint := strings.TrimSpace(f.docker(t, "port", names[index], "8008/tcp"))
				_, port, err := net.SplitHostPort(endpoint)
				if err != nil {
					t.Fatal(err)
				}
				return "https://" + net.JoinHostPort(addresses[index], port)
			}
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				if host == addresses[1] || host == addresses[2] {
					address = net.JoinHostPort("127.0.0.1", port)
				}
				return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, address)
			}
			transport := &http.Transport{DialContext: dial, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{clientPair}}}
			defer transport.CloseIdleConnections()
			httpClient := &http.Client{Transport: transport, Timeout: 2 * time.Second}
			authBody, err := json.Marshal(map[string]string{"name": dcsUser, "password": secrets.DCS})
			if err != nil {
				t.Fatal(err)
			}
			authResponse, err := httpClient.Post("https://"+dcsAddress+"/v3/auth/authenticate", "application/json", bytes.NewReader(authBody))
			if err != nil {
				t.Fatal("DCS gateway authentication", err)
			}
			authResult, err := io.ReadAll(io.LimitReader(authResponse.Body, 4096))
			authResponse.Body.Close()
			if err != nil || authResponse.StatusCode != http.StatusOK {
				t.Fatalf("DCS gateway authentication HTTP %d: %s; %v", authResponse.StatusCode, strings.ReplaceAll(string(authResult), secrets.DCS, "[redacted]"), err)
			}
			for index := 1; index < 3; index++ {
				rest[index] = publishedREST(index)
				endpoint := strings.TrimSpace(f.docker(t, "port", names[index], "5432/tcp"))
				dbHost, dbPort, err := net.SplitHostPort(endpoint)
				if err != nil {
					t.Fatal(err)
				}
				port, err := strconv.ParseUint(dbPort, 10, 16)
				if err != nil {
					t.Fatal(err)
				}
				config, err := pgx.ParseConfig("user=synod_admin dbname=postgres sslmode=verify-full")
				if err != nil {
					t.Fatal(err)
				}
				config.Host, config.Port, config.Password = dbHost, uint16(port), secrets.Superuser
				config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: addresses[index]}
				config.Fallbacks = nil
				config.ConnectTimeout = 2 * time.Second
				config.RuntimeParams["statement_timeout"] = "20000"
				database[index] = config
			}
			// Start the initial writer before the follower, so the fixture's initial role is deterministic.
			waitRole := func(index int, role string, status int) {
				t.Helper()
				deadline := time.Now().Add(60 * time.Second)
				lastStatus := 0
				for {
					response, err := httpClient.Get(rest[index] + "/" + role)
					if err == nil {
						response.Body.Close()
						lastStatus = response.StatusCode
						if response.StatusCode == status {
							if status != http.StatusOK {
								return
							}
							// Native promotion can finish after the REST role changes.
							nativeCtx, cancel := context.WithTimeout(testCtx, 2*time.Second)
							connection, nativeErr := pgx.ConnectConfig(nativeCtx, database[index])
							var inRecovery bool
							if nativeErr == nil {
								nativeErr = connection.QueryRow(nativeCtx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
								connection.Close(nativeCtx)
							}
							cancel()
							if nativeErr == nil && inRecovery == (role != "primary") {
								return
							}
						}
					}
					if time.Now().After(deadline) || err != nil && strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.State.Status}}", names[index])) == "exited" {
						output := f.docker(t, "logs", "--tail", "120", names[index])
						for _, secret := range []string{secrets.Superuser, secrets.Replication, secrets.Rewind, secrets.DCS, secrets.REST, secrets.ApplicationVerifier, applicationPassword} {
							output = strings.ReplaceAll(output, secret, "[redacted]")
						}
						t.Fatalf("member %d did not reach %s HTTP %d (last %d, %v): %s", index, role, status, lastStatus, err, output)
					}
					select {
					case <-testCtx.Done():
						t.Fatal(testCtx.Err())
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
			f.docker(t, "exec", names[1], "touch", PatroniDirectory+"/ready")
			if policy.pauseApplication {
				deadline := time.Now().Add(30 * time.Second)
				for {
					command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "--user", "postgres", names[1], "/usr/bin/python3", PatroniDirectory+"/application.py", "verify")
					if err := command.Run(); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("application receipt did not complete")
					}
					time.Sleep(100 * time.Millisecond)
				}
				arguments := append([]string{"--host", host, "exec", names[1]}, PatroniHealthCommand()...)
				if output, err := exec.CommandContext(testCtx, "docker", arguments...).CombinedOutput(); err == nil || string(output) != "patroni_startup_unknown" {
					t.Fatal("native health accepted unfinished Patroni bootstrap")
				}
				f.docker(t, "exec", names[1], "/usr/bin/python3", "-c", `import os, signal
for name in os.listdir('/proc'):
    if name.isdecimal():
        try:
            with open('/proc/'+name+'/cmdline','rb') as source: args=source.read().split(b'\x00')
            if b'/run/synod-patroni/application.py' in args and b'bootstrap' in args:
                os.kill(int(name),signal.SIGCONT)
        except FileNotFoundError:
            pass
`)
				deadline = time.Now().Add(30 * time.Second)
				for {
					output, err := exec.CommandContext(testCtx, "docker", arguments...).CombinedOutput()
					if err == nil && string(output) == "patroni_ready" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("native health did not accept completed bootstrap")
					}
					time.Sleep(100 * time.Millisecond)
				}
				return
			}
			if policy.interruptApplication {
				deadline := time.Now().Add(30 * time.Second)
				for strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.State.Status}}", names[1])) != "exited" {
					if time.Now().After(deadline) {
						t.Fatal("interrupted application bootstrap did not stop")
					}
					time.Sleep(100 * time.Millisecond)
				}
				f.docker(t, "start", names[1])
				if err := os.WriteFile(filepath.Join(dirs[1], "application.py"), []byte(PatroniApplicationProgram), 0o600); err != nil {
					t.Fatal(err)
				}
				install(1)
				f.docker(t, "exec", names[1], "chown", "-R", "postgres:postgres", PatroniDirectory)
				f.docker(t, "exec", names[1], "mv", PatroniDataDirectory+".failed", PatroniDataDirectory)
				f.docker(t, "exec", names[1], "gosu", "postgres", "/usr/local/bin/pg_ctl", "-D", PatroniDataDirectory, "-o", "-c listen_addresses= -c unix_socket_directories=/run/synod-patroni -c unix_socket_permissions=0700 -c default_transaction_read_only=on", "-l", "/dev/null", "-w", "start")
				arguments := append([]string{"--host", host, "exec", names[1]}, PatroniHealthCommand()...)
				if output, err := exec.CommandContext(testCtx, "docker", arguments...).CombinedOutput(); err == nil || string(output) != "patroni_startup_unknown" {
					t.Fatal("native health accepted interrupted application setup")
				}
				output := f.docker(t, "exec", names[1], "gosu", "postgres", "/usr/local/bin/psql", "-h", PatroniDirectory, "-U", "synod_admin", "-d", "postgres", "-Atc", "SELECT NOT rolcanlogin AND NOT EXISTS (SELECT FROM pg_catalog.pg_database WHERE datname='app') AND NOT (SELECT completed FROM synod_control.application) FROM pg_catalog.pg_authid WHERE rolname='synod_app'")
				if strings.TrimSpace(output) != "t" {
					t.Fatal("interrupted setup enabled incomplete application access")
				}
				f.docker(t, "exec", names[1], "gosu", "postgres", "/usr/local/bin/pg_ctl", "-D", PatroniDataDirectory, "-m", "fast", "-w", "stop")
				f.docker(t, "exec", names[1], "mv", PatroniDataDirectory, PatroniDataDirectory+".failed")
				f.docker(t, "exec", names[1], "test", "-s", "/var/lib/synod-patroni/application-attempt.json")
				f.docker(t, "exec", names[1], "touch", PatroniDirectory+"/ready")
				deadline = time.Now().Add(10 * time.Second)
				for strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.State.Status}}", names[1])) != "exited" {
					if time.Now().After(deadline) {
						t.Fatal("failed application setup restarted")
					}
					time.Sleep(100 * time.Millisecond)
				}
				if output := f.docker(t, "logs", "--tail", "5", names[1]); !strings.Contains(output, "Synod Patroni storage guard:") {
					t.Fatal("missing storage refusal after failed application setup")
				}
				return
			}
			waitRole(1, "primary", 200)
			for _, index := range []int{1, 2} {
				metadata := f.docker(t, "inspect", "--format", "{{json .Config}}", names[index])
				for _, secret := range []string{secrets.Superuser, secrets.Replication, secrets.Rewind, secrets.DCS, secrets.REST, secrets.ApplicationVerifier, applicationPassword} {
					if strings.Contains(metadata, secret) {
						t.Fatal("native secret entered Docker configuration metadata")
					}
				}
			}
			writer, err := pgx.ConnectConfig(testCtx, database[1])
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close(context.WithoutCancel(testCtx))
			if policy.interruptClone {
				if _, err := writer.Exec(testCtx, "CREATE TABLE native_ha(value int); INSERT INTO native_ha VALUES(144)"); err != nil {
					t.Fatal(err)
				}
			}
			f.docker(t, "exec", names[2], "touch", PatroniDirectory+"/ready")
			if policy.interruptClone {
				deadline := time.Now().Add(30 * time.Second)
				for {
					output := f.docker(t, "logs", "--tail", "30", names[2])
					if strings.Contains(output, "native initialization already attempted") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("interrupted clone did not retain its attempt")
					}
					time.Sleep(100 * time.Millisecond)
				}
				stage := DataDirectory + "/.synod-patroni-stage/data"
				f.docker(t, "exec", "--user", "postgres", names[2], "/usr/local/bin/pg_verifybackup", stage)
				before := f.docker(t, "exec", names[2], "sha256sum", stage+"/backup_manifest")
				command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "-i", "--user", "postgres", names[2], "/usr/bin/python3", "-c", "import sys; open('/run/synod-patroni/guard.py','w').write(sys.stdin.read())")
				command.Stdin = strings.NewReader(PatroniGuardProgram)
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("restore fixed guard: %v (%d bytes)", err, len(output))
				}
				command = exec.CommandContext(testCtx, "docker", "--host", host, "exec", "--user", "postgres", names[2], "/usr/bin/python3", PatroniDirectory+"/guard.py", "clone")
				if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "native initialization already attempted") {
					t.Fatal("interrupted copy could restart")
				}
				after := f.docker(t, "exec", names[2], "sha256sum", stage+"/backup_manifest")
				if before != after {
					t.Fatal("interrupted copy data changed")
				}
				f.docker(t, "exec", names[2], "/bin/sh", "-ec", "test ! -e /var/lib/postgresql/data/patroni/PG_VERSION; test ! -e /var/lib/synod-patroni/complete.json")
				arguments := append([]string{"--host", host, "exec", names[2]}, PatroniHealthCommand()...)
				if output, err := exec.CommandContext(testCtx, "docker", arguments...).CombinedOutput(); err == nil || string(output) != "patroni_startup_unknown" {
					t.Fatal("native health accepted interrupted cloning")
				}
				return
			}
			waitRole(2, "replica", 200)
			if _, err := writer.Exec(testCtx, "CREATE TABLE native_ha(value int); INSERT INTO native_ha VALUES(144)"); err != nil {
				t.Fatal(err)
			}
			reader, err := pgx.ConnectConfig(testCtx, database[2])
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close(context.WithoutCancel(testCtx))

			applicationConfig := database[1].Copy()
			applicationConfig.User, applicationConfig.Password, applicationConfig.Database = "synod_app", applicationPassword, "app"
			application, err := pgx.ConnectConfig(testCtx, applicationConfig)
			if err != nil {
				t.Fatal("application login failed", err)
			}
			if _, err := application.Exec(testCtx, "CREATE TABLE application_ha(value int); INSERT INTO application_ha VALUES(147)"); err != nil {
				t.Fatal(err)
			}
			for _, query := range []string{"CREATE ROLE forbidden_role", "SELECT rolpassword FROM pg_catalog.pg_authid"} {
				if _, err := application.Exec(testCtx, query); err == nil {
					t.Fatal("application obtained administrative access")
				}
			}
			application.Close(testCtx)
			for _, databaseName := range []string{"postgres", "template1"} {
				denied := applicationConfig.Copy()
				denied.Database = databaseName
				if connection, err := pgx.ConnectConfig(testCtx, denied); err == nil {
					connection.Close(testCtx)
					t.Fatal("application accessed administrative database")
				}
			}
			verifyApplication := func(index, expected int) {
				t.Helper()
				config := database[index].Copy()
				config.User, config.Password, config.Database = "synod_app", applicationPassword, "app"
				connection, err := pgx.ConnectConfig(testCtx, config)
				if err != nil {
					t.Fatal("application reconnect", err)
				}
				defer connection.Close(context.WithoutCancel(testCtx))
				deadline := time.Now().Add(20 * time.Second)
				for {
					var actual int
					err := connection.QueryRow(testCtx, "SELECT sum(value) FROM application_ha").Scan(&actual)
					if err == nil && actual == expected {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("application data mismatch", actual, expected, err)
					}
					time.Sleep(100 * time.Millisecond)
				}
				var recovery bool
				if err := connection.QueryRow(testCtx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
					t.Fatal(err)
				}
				role := "primary"
				if recovery {
					role = "replica"
				}
				endpoint := engine.Endpoint{Address: config.Host, Port: config.Port, Database: "app", Role: "synod_app", PostgresRole: role, PostgresTLS: &engine.PostgresTLS{RootCAPEM: caPEM, ServerName: addresses[index]}}
				if observed := New().Probe(testCtx, endpoint, []byte(applicationPassword), 155); !observed.Healthy() {
					t.Fatalf("native TLS application readiness failed: %+v", observed)
				}
				// Role any accepts the elected primary and a replica that streams from it.
				endpoint.PostgresRole = "any"
				if observed := New().Probe(testCtx, endpoint, []byte(applicationPassword), 169); !observed.Healthy() || observed.Postgres == nil || observed.Postgres.Role != role {
					t.Fatalf("role-any readiness on the %s: %+v", role, observed)
				}
				f.docker(t, "exec", "--user", "postgres", names[index], "/usr/bin/python3", PatroniDirectory+"/application.py", "verify")
				arguments := append([]string{"exec", names[index]}, PatroniHealthCommand()...)
				if output := f.docker(t, arguments...); output != "patroni_ready" {
					t.Fatal("native startup health proof failed", output)
				}
			}
			verifyApplication(1, 147)
			verifyApplication(2, 147)
			if policy.name == "false" {
				foreign, _, err := pki.CreateCA(uuid.New(), time.Now(), time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				expired := *ca
				expired.NotBefore, expired.NotAfter = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
				expiredDER, err := x509.CreateCertificate(rand.Reader, &expired, &expired, caKey.Public(), caKey)
				if err != nil {
					t.Fatal(err)
				}
				expiredPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: expiredDER})
				for _, test := range []struct {
					name                       string
					trust                      []byte
					identity, password, reason string
					plaintext                  bool
				}{
					{"wrong_trust", foreign, addresses[1], applicationPassword, reasonTLSVerification, false},
					{"expired_trust", expiredPEM, addresses[1], applicationPassword, reasonTLSVerification, false},
					{"wrong_identity", caPEM, "192.0.2.250", applicationPassword, reasonTLSVerification, false},
					{"wrong_password", caPEM, addresses[1], "incorrect-password", reasonAuthRejected, false},
					{"plaintext", caPEM, addresses[1], applicationPassword, reasonAuthRejected, true},
				} {
					t.Run("readiness_"+test.name, func(t *testing.T) {
						endpoint := engine.Endpoint{Address: database[1].Host, Port: database[1].Port, Database: "app", Role: "synod_app", PostgresRole: "primary", PostgresTLS: &engine.PostgresTLS{RootCAPEM: test.trust, ServerName: test.identity}}
						if test.plaintext {
							endpoint.PostgresTLS = nil
						}
						observed := New().Probe(testCtx, endpoint, []byte(test.password), 155)
						if observed.Healthy() || observed.Ready.Reason != test.reason {
							t.Fatalf("native refusal mismatch: %+v", observed)
						}
					})
				}
				t.Run("readiness_ignores_environment", func(t *testing.T) {
					t.Setenv("PGSSLMODE", "disable")
					t.Setenv("PGTARGETSESSIONATTRS", "read-only")
					t.Setenv("PGOPTIONS", "-c default_transaction_read_only=on")
					endpoint := engine.Endpoint{Address: database[1].Host, Port: database[1].Port, Database: "app", Role: "synod_app", PostgresRole: "primary", PostgresTLS: &engine.PostgresTLS{RootCAPEM: caPEM, ServerName: addresses[1]}}
					if observed := New().Probe(testCtx, endpoint, []byte(applicationPassword), 155); !observed.Healthy() {
						t.Fatalf("ambient settings replaced TLS: %+v", observed)
					}
				})
			}
			f.docker(t, "exec", "--user", "postgres", names[1], "/usr/bin/python3", PatroniDirectory+"/application.py", "bootstrap")
			if !synchronous {
				for _, drift := range []struct{ change, restore, reason string }{
					{"ALTER ROLE synod_app CREATEDB", "ALTER ROLE synod_app NOCREATEDB", "application role or database mismatch"},
					{`UPDATE synod_control.application SET identity=jsonb_set(identity,'{application_credential_version}','2')`, `UPDATE synod_control.application SET identity=jsonb_set(identity,'{application_credential_version}','1')`, "application receipt identity mismatch"},
					{"ALTER DATABASE app RENAME TO retained_app", "ALTER DATABASE retained_app RENAME TO app", "application role or database mismatch"},
					{"CREATE TEMP TABLE retained_receipt AS SELECT * FROM synod_control.application; DELETE FROM synod_control.application", "INSERT INTO synod_control.application SELECT * FROM retained_receipt; DROP TABLE retained_receipt", "application receipt identity mismatch"},
				} {
					if _, err := writer.Exec(testCtx, drift.change); err != nil {
						t.Fatal(err)
					}
					for _, action := range []string{"verify", "bootstrap"} {
						command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "--user", "postgres", names[1], "/usr/bin/python3", PatroniDirectory+"/application.py", action)
						if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), drift.reason) {
							t.Fatal("application drift accepted", action, drift.reason)
						}
					}
					arguments := append([]string{"--host", host, "exec", names[1]}, PatroniHealthCommand()...)
					if output, err := exec.CommandContext(testCtx, "docker", arguments...).CombinedOutput(); err == nil || string(output) != "patroni_startup_unknown" {
						t.Fatal("native health accepted application drift", drift.reason)
					}
					if _, err := writer.Exec(testCtx, drift.restore); err != nil {
						t.Fatal(err)
					}
					verifyApplication(1, 147)
				}
			}
			var sum int
			initialDeadline := time.Now().Add(20 * time.Second)
			for {
				err := reader.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum)
				if err == nil && sum == 144 {
					break
				}
				if time.Now().After(initialDeadline) {
					t.Fatal("replica data", sum, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			if _, err := reader.Exec(testCtx, "INSERT INTO native_ha VALUES(0)"); err == nil {
				t.Fatal("replica accepted write")
			}
			var encrypted bool
			if err := writer.QueryRow(testCtx, "SELECT bool_and(s.ssl) FROM pg_stat_replication r JOIN pg_stat_ssl s USING(pid)").Scan(&encrypted); err != nil || !encrypted {
				t.Fatal("replication is not encrypted", err)
			}
			insecure := database[1].Copy()
			insecure.TLSConfig = nil
			insecure.Fallbacks = nil
			if conn, err := pgx.ConnectConfig(testCtx, insecure); err == nil {
				conn.Close(testCtx)
				t.Fatal("plaintext database connection accepted")
			}
			wrongTrust := database[1].Copy()
			wrongTrust.TLSConfig = wrongTrust.TLSConfig.Clone()
			wrongTrust.TLSConfig.RootCAs = x509.NewCertPool()
			if conn, err := pgx.ConnectConfig(testCtx, wrongTrust); err == nil {
				conn.Close(testCtx)
				t.Fatal("untrusted database connection accepted")
			}
			// Route: a pooler in each member's network namespace, and one router on the bridge.
			// Clients verify the router address, which each member leaf names.
			routed := policy.name == "false"
			var routeConfig func(route string) *pgx.ConnConfig
			var routeServer func(route string, wantAddress string, recovery bool)
			var held *pgx.Conn
			if routed {
				installRoute := func(container, directory string, files map[string][]byte) {
					t.Helper()
					var archive bytes.Buffer
					writer := tar.NewWriter(&archive)
					if err := writer.WriteHeader(&tar.Header{Name: filepath.Base(directory) + "/", Typeflag: tar.TypeDir, Mode: 0o700}); err != nil {
						t.Fatal(err)
					}
					for name, data := range files {
						if err := writer.WriteHeader(&tar.Header{Name: filepath.Base(directory) + "/" + name, Mode: 0o600, Size: int64(len(data))}); err != nil {
							t.Fatal(err)
						}
						if _, err := writer.Write(data); err != nil {
							t.Fatal(err)
						}
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					command := exec.CommandContext(testCtx, "docker", "--host", host, "cp", "-", container+":/etc")
					command.Stdin = &archive
					if output, err := command.CombinedOutput(); err != nil {
						t.Fatalf("install route files: %v %s", err, output)
					}
				}
				removeLater := func(name string) {
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						exec.CommandContext(ctx, "docker", "--host", host, "rm", "--force", name).Run()
					})
				}
				var backends []RouterBackend
				for _, index := range []int{1, 2} {
					member := netip.MustParseAddr(addresses[index])
					files, err := PoolerFiles(PoolerSettings{Address: member, Port: 6432, DatabasePort: 5432, RESTPort: 8008}, applicationPassword, materials[index])
					if err != nil {
						t.Fatal(err)
					}
					pooler := names[index] + "-pooler"
					f.docker(t, append([]string{"create", "--name", pooler, "--label", "io.synod.fixture=patroni", "--network", "container:" + names[index], "--entrypoint", PoolerCommand()[0], image}, PoolerCommand()[1:]...)...)
					removeLater(pooler)
					installRoute(pooler, PoolerSourceDirectory, files)
					f.docker(t, "start", pooler)
					backends = append(backends, RouterBackend{Address: member, PoolerPort: 6432, RESTPort: 8008})
				}
				files, err := RouterFiles(RouterSettings{Address: routerAddress, WriterPort: 5000, ReaderPort: 5001, Backends: backends}, materials[1])
				if err != nil {
					t.Fatal(err)
				}
				router := prefix + "-router"
				f.docker(t, append([]string{"create", "--name", router, "--label", "io.synod.fixture=patroni", "--network", network, "--ip", routerAddress.String(), "--publish", "127.0.0.1::5000", "--publish", "127.0.0.1::5001", "--entrypoint", RouterCommand()[0], image}, RouterCommand()[1:]...)...)
				removeLater(router)
				installRoute(router, RouterSourceDirectory, files)
				f.docker(t, "start", router)
				routeConfig = func(route string) *pgx.ConnConfig {
					t.Helper()
					container := map[string]string{"writer": "5000/tcp", "reader": "5001/tcp"}[route]
					endpoint := strings.TrimSpace(f.docker(t, "port", router, container))
					routeHost, routePort, err := net.SplitHostPort(endpoint)
					if err != nil {
						t.Fatal(err)
					}
					port, err := strconv.ParseUint(routePort, 10, 16)
					if err != nil {
						t.Fatal(err)
					}
					config, err := pgx.ParseConfig("user=synod_app dbname=app sslmode=verify-full")
					if err != nil {
						t.Fatal(err)
					}
					config.Host, config.Port, config.Password = routeHost, uint16(port), applicationPassword
					config.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: routerAddress.String()}
					config.Fallbacks = nil
					config.ConnectTimeout = 2 * time.Second
					return config
				}
				// routeServer waits until the route reaches the member at wantAddress in the expected role.
				// The in_hot_standby value that the pooler reports at startup must agree, because libpq
				// target_session_attrs trusts it. A pool that logged in before a role change reports the old value.
				routeServer = func(route string, wantAddress string, recovery bool) {
					t.Helper()
					deadline := time.Now().Add(45 * time.Second)
					var last error
					for {
						connection, err := pgx.ConnectConfig(testCtx, routeConfig(route))
						if err == nil {
							var server string
							var inRecovery bool
							err = connection.QueryRow(testCtx, "SELECT host(inet_server_addr()), pg_catalog.pg_is_in_recovery()").Scan(&server, &inRecovery)
							reported := connection.PgConn().ParameterStatus("in_hot_standby")
							connection.Close(testCtx)
							want := map[bool]string{true: "on", false: "off"}[recovery]
							if err == nil && server == wantAddress && inRecovery == recovery && reported == want {
								return
							}
							if err == nil {
								err = fmt.Errorf("route reached %s with recovery %t and reported in_hot_standby %q", server, inRecovery, reported)
							}
						}
						last = err
						if time.Now().After(deadline) {
							t.Fatalf("%s route did not reach %s: %v; router: %s", route, wantAddress, last, f.docker(t, "logs", "--tail", "20", router))
						}
						time.Sleep(250 * time.Millisecond)
					}
				}
				// A failed login in a new pool must not stop later logins. With SCRAM pass-through,
				// PgBouncer 1.25 then refuses every server login until it restarts.
				// The attempt repeats until PgBouncer itself refuses the password, because the router
				// can refuse a connection before its first health checks pass.
				wrong := routeConfig("writer")
				wrong.Password = "wrong-" + applicationPassword
				deadline := time.Now().Add(45 * time.Second)
				for {
					connection, err := pgx.ConnectConfig(testCtx, wrong)
					if err == nil {
						connection.Close(testCtx)
						t.Fatal("the pooler accepted a wrong password")
					}
					if strings.Contains(err.Error(), "SASL authentication failed") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("the wrong password never reached the pooler", err)
					}
					time.Sleep(250 * time.Millisecond)
				}
				routeServer("writer", addresses[1], false)
				routeServer("reader", addresses[2], true)
				through, err := pgx.ConnectConfig(testCtx, routeConfig("writer"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := through.Exec(testCtx, "INSERT INTO application_ha VALUES(0)"); err != nil {
					t.Fatal("write through the writer route", err)
				}
				through.Close(testCtx)
				// Plaintext and a wrong router name are refused.
				plain := routeConfig("writer")
				plain.TLSConfig = nil
				if connection, err := pgx.ConnectConfig(testCtx, plain); err == nil {
					connection.Close(testCtx)
					t.Fatal("the pooler accepted plaintext")
				}
				misnamed := routeConfig("writer")
				misnamed.TLSConfig.ServerName = "192.0.2.250"
				if connection, err := pgx.ConnectConfig(testCtx, misnamed); err == nil {
					connection.Close(testCtx)
					t.Fatal("the pooler certificate matched a wrong name")
				}
				// A writer session held across the switchover must be cut.
				held, err = pgx.ConnectConfig(testCtx, routeConfig("writer"))
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close(context.WithoutCancel(testCtx))
			}
			noCertificate := &http.Transport{DialContext: dial, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
			defer noCertificate.CloseIdleConnections()
			unauthenticated := &http.Client{Transport: noCertificate, Timeout: 2 * time.Second}
			if response, err := unauthenticated.Get(rest[1] + "/primary"); err == nil {
				response.Body.Close()
				t.Fatal("REST accepted a connection without a client certificate")
			}
			body, _ := json.Marshal(struct {
				Leader    string `json:"leader"`
				Candidate string `json:"candidate"`
			}{settings[1].ResourceID.String(), settings[2].ResourceID.String()})
			if synchronous {
				waitRole(2, "sync", 200)
			}
			req, err := http.NewRequestWithContext(testCtx, http.MethodPost, rest[1]+"/switchover", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatal("REST mutation did not require password", response.StatusCode)
			}
			// The agent's client verifies the member it names and presents that member's certificate.
			// Only the dial differs, because the fixture publishes REST on loopback ports.
			control, err := PatroniRESTClient(materials[2], netip.MustParseAddr(addresses[1]))
			if err != nil {
				t.Fatal(err)
			}
			control.Transport.(*http.Transport).DialContext = dial
			defer control.CloseIdleConnections()
			switchCtx, cancelSwitch := context.WithTimeout(testCtx, 60*time.Second)
			moved, err := PatroniSwitchover(switchCtx, control, rest[1], settings[2].ResourceID, secrets.REST)
			cancelSwitch()
			if err != nil || !moved {
				t.Fatal("switchover to member 2 failed", moved, err)
			}
			// A repeat finds member 2 leading and sends no request.
			if moved, err := PatroniSwitchover(testCtx, control, rest[1], settings[2].ResourceID, secrets.REST); err != nil || moved {
				t.Fatal("a repeated switchover acted again", moved, err)
			}
			waitRole(2, "primary", 200)
			waitRole(1, "replica", 200)
			if synchronous {
				waitRole(1, "sync", 200)
			}
			verifyApplication(2, 147)
			verifyApplication(1, 147)
			if routed {
				routeServer("writer", addresses[2], false)
				routeServer("reader", addresses[1], true)
				if _, err := held.Exec(testCtx, "SELECT 1"); err == nil {
					t.Fatal("a writer session to the demoted member survived the switchover")
				}
			}
			writer.Close(testCtx)
			reader.Close(testCtx)
			switched, err := pgx.ConnectConfig(testCtx, database[2])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := switched.Exec(testCtx, "INSERT INTO native_ha VALUES(145)"); err != nil {
				t.Fatal(err)
			}
			switched.Close(testCtx)
			// Wait for the acknowledged fixture row before inducing asynchronous primary loss.
			deadline := time.Now().Add(20 * time.Second)
			for {
				caught, err := pgx.ConnectConfig(testCtx, database[1])
				if err != nil {
					t.Fatal(err)
				}
				err = caught.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum)
				caught.Close(testCtx)
				if err == nil && sum == 289 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("replica did not catch up")
				}
				time.Sleep(100 * time.Millisecond)
			}
			f.docker(t, "kill", names[2])
			waitRole(1, "primary", 200)
			if routed {
				routeServer("writer", addresses[1], false)
			}
			recovered, err := pgx.ConnectConfig(testCtx, database[1])
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close(context.WithoutCancel(testCtx))
			verifyApplication(1, 147)
			if err := recovered.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum); err != nil || sum != 289 {
				t.Fatal("failover lost fixture rows", sum, err)
			}
			if synchronous {
				blocked, err := pgx.ConnectConfig(testCtx, database[1])
				if err != nil {
					t.Fatal(err)
				}
				defer blocked.Close(context.WithoutCancel(testCtx))
				writeCtx, cancelWrite := context.WithTimeout(testCtx, 10*time.Second)
				defer cancelWrite()
				done := make(chan error, 1)
				go func() {
					_, err := blocked.Exec(writeCtx, "INSERT INTO native_ha VALUES(0)")
					done <- err
				}()
				for {
					var waiting bool
					if err := recovered.QueryRow(writeCtx, "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid=$1 AND wait_event='SyncRep')", blocked.PgConn().PID()).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					select {
					case err := <-done:
						t.Fatal("write did not wait for the missing synchronous standby", err)
					case <-writeCtx.Done():
						t.Fatal("write did not enter synchronous replication wait")
					case <-time.After(100 * time.Millisecond):
					}
				}
				// Cancellation cannot prove rollback after the local commit.
				cancelWrite()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("client cancellation did not finish")
				}

			} else if _, err := recovered.Exec(testCtx, "INSERT INTO native_ha VALUES(146)"); err != nil {
				t.Fatal(err)
			}
			f.docker(t, "start", names[2])
			rest[2] = publishedREST(2)
			_, restartedPort, err := net.SplitHostPort(strings.TrimSpace(f.docker(t, "port", names[2], "5432/tcp")))
			if err != nil {
				t.Fatal(err)
			}
			number, err := strconv.ParseUint(restartedPort, 10, 16)
			if err != nil {
				t.Fatal(err)
			}
			database[2].Port = uint16(number)
			install(2)
			f.docker(t, "exec", names[2], "touch", PatroniDirectory+"/ready")
			waitRole(2, "replica", 200)
			if synchronous {
				if _, err := recovered.Exec(testCtx, "INSERT INTO native_ha VALUES(146)"); err != nil {
					t.Fatal(err)
				}
			}
			returned, err := pgx.ConnectConfig(testCtx, database[2])
			if err != nil {
				t.Fatal(err)
			}
			defer returned.Close(context.WithoutCancel(testCtx))
			deadline = time.Now().Add(20 * time.Second)
			for {
				err := returned.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum)
				if err == nil && sum == 435 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("old primary did not recover data", sum, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
			verifyApplication(2, 147)
			f.docker(t, "kill", names[0])
			waitRole(1, "primary", 503)
			writeCtx, stop := context.WithTimeout(testCtx, 5*time.Second)
			defer stop()
			if _, err := recovered.Exec(writeCtx, "INSERT INTO native_ha VALUES(999)"); err == nil {
				t.Fatal("writer accepted traffic after DCS loss")
			}

			if !synchronous {
				returned.Close(testCtx)
				f.docker(t, "stop", "--time", "10", names[2])
				settings[2].AllowInitialization = false
				prepareRestart := func() {
					t.Helper()
					f.docker(t, "start", names[2])
					data, err := RenderPatroni(settings[2], secrets)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dirs[2], "config.json"), data, 0o600); err != nil {
						t.Fatal(err)
					}
					install(2)
					rest[2] = publishedREST(2)
					_, port, err := net.SplitHostPort(strings.TrimSpace(f.docker(t, "port", names[2], "5432/tcp")))
					if err != nil {
						t.Fatal(err)
					}
					number, err := strconv.ParseUint(port, 10, 16)
					if err != nil {
						t.Fatal(err)
					}
					database[2].Port = uint16(number)
				}
				expectRefusal := func(name, reason string) {
					t.Helper()
					f.docker(t, "exec", name, "touch", PatroniDirectory+"/ready")
					deadline := time.Now().Add(10 * time.Second)
					for strings.TrimSpace(f.docker(t, "inspect", "--format", "{{.State.Status}}", name)) != "exited" {
						if time.Now().After(deadline) {
							t.Fatal("storage guard did not stop", reason)
						}
						time.Sleep(100 * time.Millisecond)
					}
					output := f.docker(t, "logs", "--tail", "8", name)
					for _, secret := range []string{secrets.Superuser, secrets.Replication, secrets.Rewind, secrets.DCS, secrets.REST, secrets.ApplicationVerifier, applicationPassword} {
						output = strings.ReplaceAll(output, secret, "[redacted]")
					}
					if !strings.Contains(output, "Synod Patroni storage guard: "+reason) {
						t.Fatalf("wrong storage refusal: %s", output)
					}
				}
				volumeID := settings[2].VolumeID
				settings[2].VolumeID = uuid.New()
				prepareRestart()
				expectRefusal(names[2], "storage identity mismatch")
				settings[2].VolumeID = volumeID
				// A completed member must restart without reserving its initial copy space again.
				settings[2].RequiredFreeBytes = 1<<63 - 1
				prepareRestart()
				f.docker(t, "exec", names[2], "mv", PatroniDataDirectory, DataDirectory+"/.retained-patroni")
				expectRefusal(names[2], "native initialization or storage check failed")
				prepareRestart()
				f.docker(t, "exec", names[2], "ln", "-s", DataDirectory+"/.retained-patroni", PatroniDataDirectory)
				expectRefusal(names[2], "directory identity refused")
				prepareRestart()
				f.docker(t, "exec", names[2], "rm", PatroniDataDirectory)
				f.docker(t, "exec", names[2], "gosu", "postgres", "/usr/local/bin/initdb", "-D", PatroniDataDirectory, "-U", "synod_admin", "--auth-host=scram-sha-256", "--auth-local=trust")
				expectRefusal(names[2], "native system identity mismatch")
				prepareRestart()
				f.docker(t, "exec", names[2], "mv", PatroniDataDirectory, DataDirectory+"/.foreign-patroni")
				f.docker(t, "exec", names[2], "mv", DataDirectory+"/.retained-patroni", PatroniDataDirectory)
				f.docker(t, "exec", names[2], "mv", DataDirectory+"/.synod-patroni-identity.json", DataDirectory+"/.retained-identity.json")
				f.docker(t, "exec", names[2], "gosu", "postgres", "mkfifo", "-m", "600", DataDirectory+"/.synod-patroni-identity.json")
				expectRefusal(names[2], "private record refused")
				prepareRestart()
				f.docker(t, "exec", names[2], "rm", DataDirectory+"/.synod-patroni-identity.json")
				f.docker(t, "exec", names[2], "mv", DataDirectory+"/.retained-identity.json", DataDirectory+"/.synod-patroni-identity.json")
				// A completed member must refuse both native creation hooks, even with intact data.
				f.docker(t, "exec", names[2], "chown", "-R", "postgres:postgres", PatroniDirectory)
				for _, action := range []string{"init", "clone"} {
					command := exec.CommandContext(testCtx, "docker", "--host", host, "exec", "--user", "postgres", names[2], "/usr/bin/python3", PatroniDirectory+"/guard.py", action)
					output, err := command.CombinedOutput()
					if err == nil || !strings.Contains(string(output), "native initialization already attempted") {
						t.Fatal("completed member allowed another native copy", action)
					}
				}
				f.docker(t, "start", names[0])
				install(0)
				f.docker(t, "exec", names[0], "touch", PatroniDirectory+"/ready")
				waitRole(1, "primary", 200)
				f.docker(t, "exec", names[2], "touch", PatroniDirectory+"/ready")
				waitRole(2, "replica", 200)
				verified, err := pgx.ConnectConfig(testCtx, database[2])
				if err != nil {
					t.Fatal(err)
				}
				defer verified.Close(context.WithoutCancel(testCtx))
				if err := verified.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum); err != nil || sum != 435 {
					t.Fatal("guard refusal changed retained data", sum, err)
				}

				// Patroni must not recopy a live member after its data directory disappears.
				since := time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
				f.docker(t, "exec", names[2], "mv", PatroniDataDirectory, DataDirectory+"/.live-retained-patroni")
				deadline = time.Now().Add(30 * time.Second)
				for {
					output := f.docker(t, "logs", "--since", since, names[2])
					if strings.Contains(output, "native initialization already attempted") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("live storage loss did not reach the guarded clone refusal")
					}
					time.Sleep(100 * time.Millisecond)
				}
				f.docker(t, "exec", names[2], "/bin/sh", "-ec", "test ! -e /var/lib/postgresql/data/patroni/PG_VERSION")
				if processes := f.docker(t, "top", names[2], "-eo", "pid,comm"); strings.Contains(processes, "pg_basebackup\n") {
					t.Fatal("live data loss started another physical copy")
				}
				f.docker(t, "stop", "--time", "10", names[2])
				prepareRestart()
				f.docker(t, "exec", names[2], "mv", DataDirectory+"/.live-retained-patroni", PatroniDataDirectory)
				f.docker(t, "exec", names[2], "touch", PatroniDirectory+"/ready")
				waitRole(2, "replica", 200)
				restored, err := pgx.ConnectConfig(testCtx, database[2])
				if err != nil {
					t.Fatal(err)
				}
				defer restored.Close(context.WithoutCancel(testCtx))
				if err := restored.QueryRow(testCtx, "SELECT sum(value) FROM native_ha").Scan(&sum); err != nil || sum != 435 {
					t.Fatal("live-loss guard changed retained rows", sum, err)
				}

				verifyApplication(2, 147)

				// No install call is allowed after these member restarts.
				for _, index := range []int{2, 1} {
					f.docker(t, "exec", names[index], "touch", "/tmp/synod-autostart")
					before := f.docker(t, "exec", names[index], "sha256sum", PatroniSourceDirectory+"/config.json")
					f.docker(t, "stop", "--time", "10", names[index])
					if index == 1 {
						waitRole(2, "primary", 200)
					}
					f.docker(t, "start", names[index])
					rest[index] = publishedREST(index)
					_, port, err := net.SplitHostPort(strings.TrimSpace(f.docker(t, "port", names[index], "5432/tcp")))
					if err != nil {
						t.Fatal(err)
					}
					number, err := strconv.ParseUint(port, 10, 16)
					if err != nil {
						t.Fatal(err)
					}
					database[index].Port = uint16(number)
					waitRole(index, "replica", 200)
					verifyApplication(index, 147)
					after := f.docker(t, "exec", names[index], "sha256sum", PatroniSourceDirectory+"/config.json")
					if before != after {
						t.Fatal("autonomous restart changed protected configuration")
					}
				}
				verifyApplication(2, 147)

				// A fresh member with initialization disabled must stop before contacting DCS.
				emptyName := prefix + "-empty"
				f.docker(t, "run", "--detach", "--name", emptyName, "--label", "io.synod.fixture=patroni", "--memory", "256m", "--tmpfs", PatroniDirectory+":rw,noexec,nosuid,nodev,size=4m", "--entrypoint", "/bin/sh", image, "-ec", "while [ ! -f /run/synod-patroni/ready ]; do sleep 0.1; done; exec /usr/bin/python3 /run/synod-patroni/guard.py run")
				t.Cleanup(func() {
					ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					if err := exec.CommandContext(ctx, "docker", "--host", host, "rm", "--force", "--volumes", emptyName).Run(); err != nil {
						t.Error(err)
					}
				})
				names = append(names, emptyName)
				dirs = append(dirs, t.TempDir())
				empty := settings[2]
				empty.ResourceID, empty.VolumeID, empty.ProvisionID = uuid.New(), uuid.New(), uuid.New()
				data, err := RenderPatroni(empty, secrets)
				if err != nil {
					t.Fatal(err)
				}
				for file, content := range map[string][]byte{"config.json": data, "guard.py": []byte(PatroniGuardProgram)} {
					if err := os.WriteFile(filepath.Join(dirs[3], file), content, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				install(3)
				expectRefusal(emptyName, "initialization not authorized for empty storage")
			}
		})
	}
}

func TestPatroniProtectedConfigurationRefusals(t *testing.T) {
	host, image := os.Getenv("SYNOD_TEST_DOCKER_HOST"), os.Getenv("SYNOD_TEST_PATRONI_IMAGE")
	if host == "" || image == "" {
		t.Skip("set Docker host and an immutable Patroni fixture image")
	}
	if !strings.Contains(image, "sha256:") {
		t.Fatal("Patroni fixture image must use a digest")
	}
	f := &bootstrapFixture{host: host}
	name := "synod-patroni-source-" + uuid.NewString()
	f.docker(t, "run", "--detach", "--name", name, "--label", "io.synod.fixture=patroni", "--memory", "128m", "--pids-limit", "32", "--network", "none", "--tmpfs", PatroniDirectory+":rw,noexec,nosuid,nodev,size=4m", "--entrypoint", "/bin/sh", image, "-ec", "sleep 600")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := exec.CommandContext(ctx, "docker", "--host", host, "rm", "--force", "--volumes", name).Run(); err != nil {
			t.Error(err)
		}
	})
	settings := PatroniSettings{RequiredFreeBytes: 1 << 30, InstallationID: uuid.New(), ClusterID: uuid.New(), ResourceID: uuid.New(), VolumeID: uuid.New(), ProvisionID: uuid.New(), ApplicationCredentialID: uuid.New(), ApplicationCredentialVersion: 1, Address: netip.MustParseAddr("192.0.2.10"), DatabasePort: 5432, RESTPort: 8008, DCSEndpoints: []string{"192.0.2.20:2379"}, PeerNetworks: []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}, ClientNetworks: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}
	secrets := PatroniSecrets{Superuser: uuid.NewString(), Replication: uuid.NewString(), Rewind: uuid.NewString(), DCS: uuid.NewString(), REST: uuid.NewString(), ApplicationVerifier: "SCRAM-SHA-256$4096:AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
	config, err := RenderPatroni(settings, secrets)
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, file := range []string{"config.json", "ca.crt", "member.crt", "member.key", "dcs-ca.crt", "dcs-client.crt", "dcs-client.key"} {
		data := []byte("fixture")
		if file == "config.json" {
			data = config
		}
		if err := writer.WriteHeader(&tar.Header{Name: file, Mode: 0o600, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	f.docker(t, "exec", name, "mkdir", "-m", "700", PatroniSourceDirectory)
	command := exec.CommandContext(t.Context(), "docker", "--host", host, "exec", "-i", name, "tar", "-x", "-C", PatroniSourceDirectory)
	command.Stdin = &archive
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("install refusal fixture: %v (%d bytes)", err, len(output))
	}
	// These scripts affect only fixed files in this isolated fixture container.
	for _, fault := range []struct{ name, change, restore string }{
		{"directory_mode", "chmod 755 /etc/synod-patroni", "chmod 700 /etc/synod-patroni"},
		{"directory_owner", "chown postgres /etc/synod-patroni", "chown root /etc/synod-patroni"},
		{"directory_link", "mv /etc/synod-patroni /tmp/saved-source; ln -s /tmp/saved-source /etc/synod-patroni", "rm /etc/synod-patroni; mv /tmp/saved-source /etc/synod-patroni"},
		{"file_mode", "chmod 644 /etc/synod-patroni/config.json", "chmod 600 /etc/synod-patroni/config.json"},
		{"file_owner", "chown postgres /etc/synod-patroni/config.json", "chown root /etc/synod-patroni/config.json"},
		{"file_directory", "mv /etc/synod-patroni/config.json /tmp/saved-config; mkdir /etc/synod-patroni/config.json", "rmdir /etc/synod-patroni/config.json; mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"file_fifo", "mv /etc/synod-patroni/config.json /tmp/saved-config; mkfifo -m 600 /etc/synod-patroni/config.json", "rm /etc/synod-patroni/config.json; mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"file_link", "mv /etc/synod-patroni/config.json /tmp/saved-config; ln -s /tmp/saved-config /etc/synod-patroni/config.json", "rm /etc/synod-patroni/config.json; mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"file_hardlink", "ln /etc/synod-patroni/config.json /tmp/aliased-config", "rm /tmp/aliased-config"},
		{"file_missing", "mv /etc/synod-patroni/config.json /tmp/saved-config", "mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"file_extra", "touch /etc/synod-patroni/unexpected", "rm /etc/synod-patroni/unexpected"},
		{"file_empty", "cp -p /etc/synod-patroni/config.json /tmp/saved-config; truncate -s 0 /etc/synod-patroni/config.json", "mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"file_large", "cp -p /etc/synod-patroni/config.json /tmp/saved-config; truncate -s 131073 /etc/synod-patroni/config.json", "mv /tmp/saved-config /etc/synod-patroni/config.json"},
		{"private_owner", "chown postgres /run/synod-patroni", "chown root /run/synod-patroni"},
		{"private_nonempty", "touch /run/synod-patroni/unexpected", "rm /run/synod-patroni/unexpected"},
	} {
		t.Run(fault.name, func(t *testing.T) {
			f.docker(t, "exec", name, "/bin/sh", "-ec", fault.change)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			args := append([]string{"--host", host, "exec", name}, PatroniCommand()...)
			output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
			f.docker(t, "exec", name, "/bin/sh", "-ec", fault.restore)
			if err == nil || ctx.Err() != nil || strings.TrimSpace(string(output)) != "Synod Patroni startup: protected configuration refused" {
				t.Fatal("protected configuration did not refuse promptly")
			}
			f.docker(t, "exec", name, "/bin/sh", "-ec", "test -z \"$(ls -A /var/lib/postgresql/data)\"; test ! -e /var/lib/synod-patroni/attempt.json")
		})
	}
	args := append([]string{"--host", host, "exec", name}, PatroniCommand()...)
	if output, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput(); err == nil || strings.TrimSpace(string(output)) != "Synod Patroni storage guard: initialization not authorized for empty storage" {
		t.Fatal("protected source did not reach the fixed storage guard")
	}
	f.docker(t, "exec", name, "/bin/sh", "-ec", "test -z \"$(ls -A /var/lib/postgresql/data)\"; test ! -e /var/lib/synod-patroni/attempt.json")
	f.docker(t, "exec", name, "/usr/bin/python3", "-c", `import json
path='/run/synod-patroni/config.json'
with open(path) as source: config=json.load(source)
config['synod']['allow_initialization']=True
config['synod']['required_free_bytes']=2**63-1
with open(path,'w') as target: json.dump(config,target)
`)
	for _, action := range []string{"run", "init", "clone"} {
		command := exec.CommandContext(t.Context(), "docker", "--host", host, "exec", "--user", "postgres", name, "/usr/bin/python3", PatroniDirectory+"/guard.py", action)
		if output, err := command.CombinedOutput(); err == nil || strings.TrimSpace(string(output)) != "Synod Patroni storage guard: insufficient space for initial native work" {
			t.Fatal("initial work did not refuse insufficient capacity", action)
		}
		f.docker(t, "exec", name, "/bin/sh", "-ec", "test ! -e /var/lib/synod-patroni/attempt.json; test ! -e /var/lib/postgresql/data/.synod-patroni-stage; test ! -e /var/lib/postgresql/data/patroni")
	}
	args = append([]string{"--host", host, "exec", name}, PatroniHealthCommand()...)
	if output, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput(); err == nil || string(output) != "patroni_startup_unknown" {
		t.Fatal("native health accepted missing completion")
	}
}
