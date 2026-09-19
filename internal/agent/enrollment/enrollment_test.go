package enrollment

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/pki"
)

type fixture struct {
	options         Options
	directory       string
	config          Config
	ca              *pki.CA
	rootPEM, keyPEM []byte
	token           string
	apiCAPath       string
}

func newFixture(t *testing.T, handler http.Handler) *fixture {
	t.Helper()
	f := &fixture{options: Options{Identity: Identity{InstallationID: uuid.New(), NodeID: uuid.New()}, AllowCreate: true}, directory: filepath.Join(t.TempDir(), "state"), token: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x39}, 32))}
	var err error
	f.rootPEM, f.keyPEM, err = pki.CreateCA(f.options.Identity.InstallationID, time.Now(), 48*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.ca, err = pki.LoadCA(f.options.Identity.InstallationID, f.rootPEM, f.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(f.token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.apiCAPath = filepath.Join(t.TempDir(), "api-ca.pem")
	if err := os.WriteFile(f.apiCAPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	f.config = Config{Endpoint: server.URL, RootCAs: roots, TokenFile: tokenFile, Attempts: 2, RequestTimeout: time.Second}
	return f
}

func (f *fixture) issue(t *testing.T, request Request) Response {
	t.Helper()
	certificate, err := f.ca.Issue(request.CSRDER, pki.Identity{InstallationID: f.options.Identity.InstallationID, Role: pki.Agent, SubjectID: request.NodeID}, nil, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return Response{NodeID: request.NodeID, CertificateID: uuid.New(), CertificatePEM: string(certificate), CACertificatePEM: string(f.rootPEM), AcceptedAt: time.Now().UTC()}
}

func TestLostReplyAndDurableRestart(t *testing.T) {
	var f *fixture
	var mutex sync.Mutex
	var bodies [][]byte
	var issued Response
	f = newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || r.Method != "POST" || r.URL.Path != "/v1/enrollments" || r.Header.Get("Authorization") != "Bearer "+f.token {
			t.Error("invalid enrollment transport")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var request Request
		if err := json.Unmarshal(body, &request); err != nil {
			t.Error(err)
			return
		}
		disk, err := os.ReadFile(filepath.Join(f.directory, "state.json"))
		if err != nil {
			t.Error(err)
			return
		}
		var persisted diskState
		if err := json.Unmarshal(disk, &persisted); err != nil || !bytes.Equal(persisted.Body, body) || len(persisted.TokenHash) != 32 {
			t.Error("network preceded durable request/token binding")
		}
		mutex.Lock()
		defer mutex.Unlock()
		bodies = append(bodies, body)
		if len(bodies) == 1 {
			issued = f.issue(t, request)
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		issued.Replayed = true
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(issued)
	}))
	state, err := Open(f.directory, f.options)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := state.Request()
	credentials, err := state.Submit(t.Context(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.RequestID != request.RequestID || credentials.CertificateID != issued.CertificateID {
		t.Fatal("completion changed identity")
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	f.options.AllowCreate = false
	reopened, err := Open(f.directory, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.Submit(t.Context(), Config{})
	if err != nil || again.CertificateID != credentials.CertificateID {
		t.Fatal("completed enrollment did not survive reopen")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("lost reply regenerated request")
	}
	for _, path := range []string{f.directory, filepath.Join(f.directory, "state.json"), filepath.Join(f.directory, "lock")} {
		info, err := os.Stat(path)
		if err != nil || !private(info) {
			t.Fatal("unsafe state permissions")
		}
	}
	encoded, _ := json.Marshal(credentials)
	if strings.Contains(string(encoded), "PrivateKey") || strings.Contains(fmt.Sprintf("%v %+v %#v", credentials, credentials, state), f.token) {
		t.Fatal("credential formatting exposed private data")
	}
	if bytes.Contains([]byte(fmt.Sprintf("%#v", state)), []byte("KeyDER")) {
		t.Fatal("state debug formatting exposed key")
	}
}

func TestUnsafeOrInconsistentState(t *testing.T) {
	for _, kind := range []string{"missing", "missing lock", "corrupt", "public file", "public directory", "FIFO", "symlink", "wrong identity", "wrong key", "changed body"} {
		t.Run(kind, func(t *testing.T) {
			options := Options{Identity: Identity{InstallationID: uuid.New(), NodeID: uuid.New()}, AllowCreate: true}
			directory := filepath.Join(t.TempDir(), "state")
			state, err := Open(directory, options)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := state.Request()
			request.CSRDER[0] ^= 1
			fresh, _ := state.Request()
			if bytes.Equal(request.CSRDER, fresh.CSRDER) {
				t.Fatal("request snapshot aliases state")
			}
			if _, err := Open(directory, options); !errors.Is(err, ErrLocked) {
				t.Fatalf("second writer accepted: %v", err)
			}
			state.Close()
			path := filepath.Join(directory, "state.json")
			switch kind {
			case "missing":
				os.Remove(path)
			case "missing lock":
				os.Remove(filepath.Join(directory, "lock"))
			case "corrupt":
				os.WriteFile(path, []byte("incomplete private state"), 0o600)
			case "public file":
				os.Chmod(path, 0o644)
			case "public directory":
				os.Chmod(directory, 0o755)
			case "FIFO":
				os.Remove(path)
				if err := syscall.Mkfifo(path, 0o600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				outside := filepath.Join(t.TempDir(), "private")
				os.Rename(path, outside)
				os.Symlink(outside, path)
			case "wrong identity":
				options.Identity.NodeID = uuid.New()
			case "wrong key", "changed body":
				data, _ := os.ReadFile(path)
				var disk diskState
				json.Unmarshal(data, &disk)
				if kind == "wrong key" {
					_, key, _ := ed25519.GenerateKey(rand.Reader)
					disk.KeyDER, _ = x509.MarshalPKCS8PrivateKey(key)
				} else {
					disk.Body = []byte(`{"node_id":"wrong"}`)
				}
				data, _ = json.Marshal(disk)
				os.WriteFile(path, data, 0o600)
			}
			if opened, err := Open(directory, options); opened != nil || !errors.Is(err, ErrState) {
				if opened != nil {
					opened.Close()
				}
				t.Fatalf("unsafe state accepted: %v", err)
			}
		})
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := Open(missing, Options{Identity: Identity{InstallationID: uuid.New(), NodeID: uuid.New()}}); !errors.Is(err, ErrState) {
		t.Fatal("normal open created missing state")
	}
}

func TestRejectCertificates(t *testing.T) {
	for _, kind := range []string{"wrong key", "wrong node", "wrong CA", "expired", "future", "server usage", "extra PEM", "bad identity"} {
		t.Run(kind, func(t *testing.T) {
			var f *fixture
			f = newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request Request
				json.NewDecoder(r.Body).Decode(&request)
				if kind == "wrong key" {
					_, key, _ := ed25519.GenerateKey(rand.Reader)
					request.CSRDER, _ = x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
				}
				if kind == "wrong node" {
					request.NodeID = uuid.New()
				}
				response := f.issue(t, request)
				if kind == "wrong CA" {
					other, _, _ := pki.CreateCA(uuid.New(), time.Now(), 24*time.Hour)
					response.CACertificatePEM = string(other)
				}
				if kind == "bad identity" {
					response.CertificateID = uuid.Nil
				}
				if kind == "extra PEM" {
					response.CertificatePEM += response.CertificatePEM
				}
				if kind == "expired" || kind == "future" || kind == "server usage" {
					block, _ := pem.Decode([]byte(response.CertificatePEM))
					leaf, _ := x509.ParseCertificate(block.Bytes)
					switch kind {
					case "expired":
						leaf.NotAfter = time.Now().Add(-time.Minute)
					case "future":
						leaf.NotBefore = time.Now().Add(time.Minute)
					case "server usage":
						leaf.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
					}
					rootBlock, _ := pem.Decode(f.rootPEM)
					root, _ := x509.ParseCertificate(rootBlock.Bytes)
					keyBlock, _ := pem.Decode(f.keyPEM)
					key, _ := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
					der, err := x509.CreateCertificate(rand.Reader, leaf, root, leaf.PublicKey, key)
					if err != nil {
						t.Error(err)
						return
					}
					response.CertificatePEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
				}
				w.WriteHeader(201)
				json.NewEncoder(w).Encode(response)
			}))
			state, err := Open(f.directory, f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			before, _ := state.Request()
			if _, err := state.Submit(t.Context(), f.config); !errors.Is(err, ErrCertificate) {
				t.Fatalf("invalid certificate accepted: %v", err)
			}
			after, _ := state.Request()
			if !bytes.Equal(before.CSRDER, after.CSRDER) || before.RequestID != after.RequestID || state.state.Response != nil {
				t.Fatal("certificate rejection changed durable request")
			}
		})
	}
}

func TestHTTPSCredentialsAndRetryBinding(t *testing.T) {
	var requests atomic.Int32
	f := newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(503)
		io.WriteString(w, `{"message":"do-not-echo-private-server-data"}`)
	}))
	state, err := Open(f.directory, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	config := f.config
	config.Attempts = 1
	if _, err := state.Submit(t.Context(), config); !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "do-not-echo") {
		t.Fatal("server error was not bounded and redacted")
	}
	other := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x55}, 32))
	os.WriteFile(config.TokenFile, []byte(other), 0o600)
	if _, err := state.Submit(t.Context(), config); !errors.Is(err, ErrConflict) || requests.Load() != 1 {
		t.Fatal("different token changed replay authority")
	}
	os.WriteFile(config.TokenFile, []byte(f.token), 0o600)
	for _, kind := range []string{"HTTP", "URL credentials", "wrong trust", "public token", "token symlink"} {
		t.Run(kind, func(t *testing.T) {
			changed := config
			switch kind {
			case "HTTP":
				changed.Endpoint = "http://localhost"
			case "URL credentials":
				changed.Endpoint = "https://user:private-token@localhost"
			case "wrong trust":
				changed.RootCAs = x509.NewCertPool()
			case "public token":
				path := filepath.Join(t.TempDir(), "token")
				os.WriteFile(path, []byte(f.token), 0o644)
				changed.TokenFile = path
			case "token symlink":
				path := filepath.Join(t.TempDir(), "token")
				os.Symlink(config.TokenFile, path)
				changed.TokenFile = path
			}
			if _, err := state.Submit(t.Context(), changed); err == nil || strings.Contains(err.Error(), "private-token") {
				t.Fatal("unsafe request accepted or disclosed")
			}
		})
	}
	var redirected atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer target.Close()
	redirect := newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	pending, err := Open(redirect.directory, redirect.options)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	if _, err := pending.Submit(t.Context(), redirect.config); err == nil || redirected.Load() != 0 {
		t.Fatal("followed credential redirect")
	}
}

func TestProcessInterruption(t *testing.T) {
	if os.Getenv("SYNOD_ENROLL_CHILD") == "1" {
		var options Options
		json.Unmarshal([]byte(os.Getenv("SYNOD_ENROLL_OPTIONS")), &options)
		state, err := Open(os.Getenv("SYNOD_ENROLL_STATE"), options)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(os.Getenv("SYNOD_ENROLL_CA"))
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(data)
		_, err = state.Submit(t.Context(), Config{Endpoint: os.Getenv("SYNOD_ENROLL_URL"), TokenFile: os.Getenv("SYNOD_ENROLL_TOKEN"), RootCAs: roots, RequestTimeout: 10 * time.Second, Attempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("DURABLE")
		select {}
	}
	for _, boundary := range []string{"uncertain response", "durable completion"} {
		t.Run(boundary, func(t *testing.T) {
			var f *fixture
			observed := make(chan struct{}, 1)
			var mutex sync.Mutex
			var issued Response
			var bodies [][]byte
			f = newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var request Request
				json.Unmarshal(body, &request)
				mutex.Lock()
				bodies = append(bodies, body)
				count := len(bodies)
				if count == 1 {
					issued = f.issue(t, request)
				}
				response := issued
				mutex.Unlock()
				if count == 1 && boundary == "uncertain response" {
					observed <- struct{}{}
					<-r.Context().Done()
					return
				}
				w.WriteHeader(201)
				json.NewEncoder(w).Encode(response)
			}))
			state, err := Open(f.directory, f.options)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := state.Request()
			state.Close()
			options := f.options
			options.AllowCreate = false
			encoded, _ := json.Marshal(options)
			child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessInterruption$")
			child.Env = append(os.Environ(), "SYNOD_ENROLL_CHILD=1", "SYNOD_ENROLL_OPTIONS="+string(encoded), "SYNOD_ENROLL_STATE="+f.directory, "SYNOD_ENROLL_CA="+f.apiCAPath, "SYNOD_ENROLL_URL="+f.config.Endpoint, "SYNOD_ENROLL_TOKEN="+f.config.TokenFile)
			stdout, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics bytes.Buffer
			child.Stderr = &diagnostics
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if child.ProcessState == nil {
					child.Process.Kill()
					child.Wait()
				}
			})
			if boundary == "uncertain response" {
				select {
				case <-observed:
				case <-time.After(4 * time.Second):
					t.Fatal("child did not submit")
				}
			} else {
				scanner := bufio.NewScanner(stdout)
				if !scanner.Scan() || scanner.Text() != "DURABLE" {
					t.Fatalf("child did not persist completion: %s", diagnostics.String())
				}
			}
			child.Process.Kill()
			if err := child.Wait(); err == nil {
				t.Fatal("child was not interrupted")
			}
			recovered, err := Open(f.directory, options)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			after, _ := recovered.Request()
			if before.RequestID != after.RequestID || !bytes.Equal(before.CSRDER, after.CSRDER) {
				t.Fatal("process death changed request")
			}
			credentials, err := recovered.Submit(t.Context(), f.config)
			if err != nil || credentials.CertificateID != issued.CertificateID {
				t.Fatalf("recovery did not retain accepted identity: %v", err)
			}
			mutex.Lock()
			defer mutex.Unlock()
			want := 1
			if boundary == "uncertain response" {
				want = 2
			}
			if len(bodies) != want || (want == 2 && !bytes.Equal(bodies[0], bodies[1])) {
				t.Fatal("crash recovery did not replay exact request")
			}
		})
	}
}

func TestFailedLocalCompletionRequiresReopen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission failure requires an unprivileged process")
	}
	var f *fixture
	var mutex sync.Mutex
	var response Response
	var bodies [][]byte
	f = newFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var request Request
		json.Unmarshal(body, &request)
		mutex.Lock()
		defer mutex.Unlock()
		bodies = append(bodies, body)
		if len(bodies) == 1 {
			response = f.issue(t, request)
			if err := os.Chmod(f.directory, 0o500); err != nil {
				t.Error(err)
			}
		}
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(response)
	}))
	state, err := Open(f.directory, f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(f.directory, 0o700); state.Close() })
	if _, err := state.Submit(t.Context(), f.config); !errors.Is(err, ErrState) {
		t.Fatalf("completion succeeded without local checkpoint: %v", err)
	}
	if err := os.Chmod(f.directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Submit(t.Context(), f.config); !errors.Is(err, ErrState) {
		t.Fatal("failed handle admitted another attempt")
	}
	state.Close()
	f.options.AllowCreate = false
	reopened, err := Open(f.directory, f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	credentials, err := reopened.Submit(t.Context(), f.config)
	if err != nil || credentials.CertificateID != response.CertificateID {
		t.Fatalf("reopen could not resolve remote completion: %v", err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("local failure changed replay input")
	}
}

func TestProcessLockExclusion(t *testing.T) {
	if os.Getenv("SYNOD_ENROLL_LOCK_CHILD") == "1" {
		var options Options
		if err := json.Unmarshal([]byte(os.Getenv("SYNOD_ENROLL_OPTIONS")), &options); err != nil {
			t.Fatal(err)
		}
		opened, err := Open(os.Getenv("SYNOD_ENROLL_STATE"), options)
		if opened != nil {
			opened.Close()
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("concurrent process did not reject held lock: %v", err)
		}
		return
	}
	options := Options{Identity: Identity{InstallationID: uuid.New(), NodeID: uuid.New()}, AllowCreate: true}
	directory := filepath.Join(t.TempDir(), "state")
	state, err := Open(directory, options)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	options.AllowCreate = false
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessLockExclusion$")
	child.Env = append(os.Environ(), "SYNOD_ENROLL_LOCK_CHILD=1", "SYNOD_ENROLL_OPTIONS="+string(encoded), "SYNOD_ENROLL_STATE="+directory)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("lock child: %v %s", err, output)
	}
}
