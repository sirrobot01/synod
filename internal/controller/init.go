package controller

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/store/postgres"
	"github.com/spf13/cobra"
)

// caLifetime is how long an installation CA is valid.
const caLifetime = 10 * 365 * 24 * time.Hour

// controllerCertLifetime is how long the controller transport certificate lasts.
const controllerCertLifetime = 30 * 24 * time.Hour

// metadataPostgresImage is the pinned image used for the compact metadata container.
const metadataPostgresImage = "postgres:17"

const systemdServiceTemplate = `[Unit]
Description=Synod Database Controller
After=network.target docker.service
Wants=docker.service

[Service]
Type=simple
ExecStart=%s server --config %s
Restart=always
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
`

// InitOptions specifies settings for initializing a Synod controller.
type InitOptions struct {
	Address                 string
	Directory               string
	DatabaseURL             string
	InstallationID          string
	PlacementArchitecture   string
	PlacementRuntimeVersion string
	PlacementStorageID      string
	SystemdDir              string
	NoContainer             bool
}

type materialResult struct {
	id               uuid.UUID
	wrappingID       uuid.UUID
	caCertPEM        []byte
	apiCACertPEM     []byte
	adminToken       string
	metadataPassword string
}

type controllerJSON struct {
	InstallationID          string `json:"installation_id"`
	Listen                  string `json:"listen"`
	AgentListen             string `json:"agent_listen"`
	DatabaseURL             string `json:"database_url"`
	AdminTokenFile          string `json:"admin_token_file"`
	TLSCertFile             string `json:"tls_cert_file"`
	TLSKeyFile              string `json:"tls_key_file"`
	CACertFile              string `json:"ca_cert_file"`
	CAKeyFile               string `json:"ca_key_file"`
	AgentTLSCertFile        string `json:"agent_tls_cert_file"`
	AgentTLSKeyFile         string `json:"agent_tls_key_file"`
	AgentCAFile             string `json:"agent_ca_file"`
	SecretKeyFile           string `json:"secret_key_file"`
	SecretKeyID             string `json:"secret_key_id"`
	PlacementArchitecture   string `json:"placement_architecture,omitempty"`
	PlacementRuntimeVersion string `json:"placement_runtime_version,omitempty"`
	PlacementStorageID      string `json:"placement_storage_id,omitempty"`
}

func initCommand() *cobra.Command {
	var options InitOptions
	command := &cobra.Command{
		Use:   "init",
		Short: "Initialize a controller installation with material, configuration, and dependencies",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInit(cmd.Context(), options, cmd.OutOrStdout())
		},
	}
	command.Flags().StringVar(&options.Address, "address", "127.0.0.1", "Routable address or hostname for the controller")
	command.Flags().StringVar(&options.Directory, "dir", "", "Directory to write the installation material and configuration into (required)")
	command.Flags().StringVar(&options.DatabaseURL, "database-url", "", "PostgreSQL URL for an existing metadata database")
	command.Flags().StringVar(&options.InstallationID, "installation-id", "", "Installation UUID; one is generated when absent")
	command.Flags().StringVar(&options.PlacementArchitecture, "placement-architecture", "", "Node CPU architectures to place onto, in preference order (default: any)")
	command.Flags().StringVar(&options.PlacementRuntimeVersion, "placement-runtime-version", "", "Container runtime version placement requires (default: any)")
	command.Flags().StringVar(&options.PlacementStorageID, "placement-storage-id", "", "Storage pool identifier placement requires (default: any)")
	command.Flags().StringVar(&options.SystemdDir, "systemd-dir", "/etc/systemd/system", "Systemd units directory to install the service into")
	command.Flags().BoolVar(&options.NoContainer, "no-container", false, "Do not run the metadata PostgreSQL container")
	return command
}

func runInit(ctx context.Context, options InitOptions, output io.Writer) error {
	dir := options.Directory
	if dir == "" {
		return errors.New("init requires --dir")
	}
	address := options.Address
	if address == "" {
		address = "127.0.0.1"
	}
	res, err := writeMaterialWithAddress(dir, options.InstallationID, address, output)
	if err != nil {
		return err
	}

	databaseURL := options.DatabaseURL
	if databaseURL == "" && !options.NoContainer {
		var containerErr error
		databaseURL, containerErr = bootstrapMetadataContainer(ctx, dir, res.metadataPassword, output)
		if containerErr != nil {
			return fmt.Errorf("bootstrap metadata container: %w", containerErr)
		}
	} else if databaseURL != "" {
		if err := waitForPostgres(ctx, databaseURL, 10*time.Second); err != nil {
			return fmt.Errorf("connect to metadata database: %w", err)
		}
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			return fmt.Errorf("connect to metadata database: %w", err)
		}
		defer pool.Close()
		store := postgres.New(pool)
		if err := store.Migrate(ctx); err != nil {
			return fmt.Errorf("migrate metadata database: %w", err)
		}
	} else {
		databaseURL = fmt.Sprintf("postgres://synod:%s@127.0.0.1:5432/synod?sslmode=disable", res.metadataPassword)
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		absDir = dir
	}

	cfg := controllerJSON{
		InstallationID:          res.id.String(),
		Listen:                  net.JoinHostPort(address, "8443"),
		AgentListen:             net.JoinHostPort(address, "8444"),
		DatabaseURL:             databaseURL,
		AdminTokenFile:          filepath.Join(absDir, "admin.token"),
		TLSCertFile:             filepath.Join(absDir, "api.crt"),
		TLSKeyFile:              filepath.Join(absDir, "api.key"),
		CACertFile:              filepath.Join(absDir, "ca.crt"),
		CAKeyFile:               filepath.Join(absDir, "ca.key"),
		AgentTLSCertFile:        filepath.Join(absDir, "agent-tls.crt"),
		AgentTLSKeyFile:         filepath.Join(absDir, "agent-tls.key"),
		AgentCAFile:             filepath.Join(absDir, "ca.crt"),
		SecretKeyFile:           filepath.Join(absDir, "secret.key"),
		SecretKeyID:             res.wrappingID.String(),
		PlacementArchitecture:   options.PlacementArchitecture,
		PlacementRuntimeVersion: options.PlacementRuntimeVersion,
		PlacementStorageID:      options.PlacementStorageID,
	}

	encodedConfig, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode controller configuration: %w", err)
	}
	configPath := filepath.Join(absDir, "controller.json")
	if err := os.WriteFile(configPath, append(encodedConfig, '\n'), 0o600); err != nil {
		return fmt.Errorf("write controller configuration: %w", err)
	}

	executable, err := os.Executable()
	if err != nil {
		executable = "synod"
	}
	serviceContent := fmt.Sprintf(systemdServiceTemplate, executable, configPath)
	serviceLocalPath := filepath.Join(absDir, "synod-server.service")
	if err := os.WriteFile(serviceLocalPath, []byte(serviceContent), 0o644); err != nil {
		return fmt.Errorf("write service file: %w", err)
	}

	installedService := false
	systemdDir := options.SystemdDir
	if systemdDir == "" {
		systemdDir = "/etc/systemd/system"
	}
	if info, err := os.Stat(systemdDir); err == nil && info.IsDir() {
		installedPath := filepath.Join(systemdDir, "synod-server.service")
		if err := os.WriteFile(installedPath, []byte(serviceContent), 0o644); err == nil {
			installedService = true
		}
	}

	joinNote := "No metadata database is reachable yet. Start the controller, then run synod node join-token to create a join token."
	if options.DatabaseURL != "" || !options.NoContainer {
		encoded, err := registerJoinToken(ctx, databaseURL, res.id, address, res.apiCACertPEM)
		if err != nil {
			return err
		}
		joinNote = fmt.Sprintf("Join token for one agent node (valid for 7 days):\n  %s\n\nTo join an agent node, run on that node:\n  synod agent join %s\n\nTo join more nodes, run synod node join-token for each node.", encoded, encoded)
	}

	serviceNote := fmt.Sprintf("Wrote systemd service to %s.", serviceLocalPath)
	if installedService {
		serviceNote = fmt.Sprintf("Installed systemd service to %s/synod-server.service.", systemdDir)
	}

	report := fmt.Sprintf(`Synod installation %s initialized at %s.

Metadata database:
  %s

Controller configuration written to:
  %s

%s

%s

To administer this installation:
  export SYNOD_API_URL=https://%s:8443
  export SYNOD_CA_FILE=%s
  export SYNOD_ADMIN_TOKEN_FILE=%s
`,
		res.id, absDir,
		databaseURL,
		configPath,
		serviceNote,
		joinNote,
		address,
		filepath.Join(absDir, "api-ca.crt"),
		filepath.Join(absDir, "admin.token"),
	)

	_, err = fmt.Fprint(output, report)
	return err
}

// registerJoinToken stores the verifier of a one-use join token that binds no node yet.
func registerJoinToken(ctx context.Context, databaseURL string, installationID uuid.UUID, address string, apiCACertPEM []byte) (string, error) {
	block, _ := pem.Decode(apiCACertPEM)
	if block == nil {
		return "", errors.New("decode the API CA certificate for the join token")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return "", fmt.Errorf("connect to metadata database: %w", err)
	}
	defer pool.Close()
	token := jointoken.Token{
		Address:        net.JoinHostPort(address, "8443"),
		InstallationID: installationID,
		Fingerprint:    jointoken.Fingerprint(block.Bytes),
		Secret:         jointoken.NewSecret(),
	}
	if _, err := postgres.New(pool).RegisterEnrollmentToken(ctx, postgres.TokenRegistration{
		InstallationID: installationID,
		RequestID:      uuid.NewString(),
		ActorID:        "synod-server-init",
		IdempotencyKey: uuid.NewString(),
		TokenVerifier:  sha256.Sum256([]byte(token.Secret)),
		TTLSeconds:     7 * 24 * 3600,
	}); err != nil {
		return "", fmt.Errorf("register the join token: %w", err)
	}
	return jointoken.Encode(token), nil
}

// generateSecret returns bytes random bytes as hex.
func generateSecret(bytes int) (string, error) {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func writeMaterial(directory, installation string, output io.Writer) error {
	_, err := writeMaterialWithAddress(directory, installation, "127.0.0.1", output)
	return err
}

func writeMaterialWithAddress(directory, installation, address string, output io.Writer) (materialResult, error) {
	id, err := installationID(installation)
	if err != nil {
		return materialResult{}, err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return materialResult{}, fmt.Errorf("create the material directory %s: %w", directory, err)
	}
	certificatePEM, keyPEM, err := pki.CreateCA(id, time.Now(), caLifetime)
	if err != nil {
		return materialResult{}, fmt.Errorf("create the installation CA for %s: %w", id, err)
	}
	authority, err := pki.LoadCA(id, certificatePEM, keyPEM)
	if err != nil {
		return materialResult{}, fmt.Errorf("load the new installation CA for %s: %w", id, err)
	}
	controllerCert, controllerKey, err := controllerIdentity(authority, id, address)
	if err != nil {
		return materialResult{}, err
	}
	apiCACertPEM, apiCAKeyPEM, err := createAPICA(id, time.Now(), caLifetime)
	if err != nil {
		return materialResult{}, fmt.Errorf("create API CA: %w", err)
	}
	names, err := controllerNames(address)
	if err != nil {
		return materialResult{}, err
	}
	apiCertPEM, apiKeyPEM, err := issueAPICertificate(apiCACertPEM, apiCAKeyPEM, names, time.Now(), 365*24*time.Hour)
	if err != nil {
		return materialResult{}, fmt.Errorf("issue API certificate: %w", err)
	}
	wrapping := make([]byte, 32)
	if _, err := rand.Read(wrapping); err != nil {
		return materialResult{}, fmt.Errorf("generate the credential wrapping key: %w", err)
	}
	wrappingID, err := uuid.NewRandom()
	if err != nil {
		return materialResult{}, fmt.Errorf("generate the wrapping key identity: %w", err)
	}
	adminToken, err := generateSecret(20)
	if err != nil {
		return materialResult{}, fmt.Errorf("generate admin token: %w", err)
	}
	metadataPassword, err := generateSecret(16)
	if err != nil {
		return materialResult{}, fmt.Errorf("generate metadata password: %w", err)
	}

	written := []struct {
		name string
		mode os.FileMode
		data []byte
	}{
		{"ca.crt", 0o644, certificatePEM},
		{"ca.key", 0o600, keyPEM},
		{"agent-tls.crt", 0o644, controllerCert},
		{"agent-tls.key", 0o600, controllerKey},
		{"api-ca.crt", 0o644, apiCACertPEM},
		{"api-ca.key", 0o600, apiCAKeyPEM},
		// The chain lets a joining agent pin the API CA from its join token fingerprint.
		{"api.crt", 0o644, append(apiCertPEM, apiCACertPEM...)},
		{"api.key", 0o600, apiKeyPEM},
		{"secret.key", 0o600, []byte(base64.StdEncoding.EncodeToString(wrapping) + "\n")},
		{"installation-id", 0o644, []byte(id.String() + "\n")},
		{"secret-key-id", 0o644, []byte(wrappingID.String() + "\n")},
		{"admin.token", 0o600, []byte(adminToken + "\n")},
		{"metadata.password", 0o600, []byte(metadataPassword + "\n")},
	}
	for _, file := range written {
		if _, err := os.Stat(filepath.Join(directory, file.name)); err == nil {
			return materialResult{}, fmt.Errorf("%s already exists; init refuses to replace installation material", file.name)
		}
	}
	for _, file := range written {
		if err := os.WriteFile(filepath.Join(directory, file.name), file.data, file.mode); err != nil {
			return materialResult{}, fmt.Errorf("write %s: %w", filepath.Join(directory, file.name), err)
		}
	}
	return materialResult{
		id:               id,
		wrappingID:       wrappingID,
		caCertPEM:        certificatePEM,
		apiCACertPEM:     apiCACertPEM,
		adminToken:       adminToken,
		metadataPassword: metadataPassword,
	}, nil
}

func createAPICA(installationID uuid.UUID, now time.Time, lifetime time.Duration) ([]byte, []byte, error) {
	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate API CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate API CA serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Synod API CA", Organization: []string{"Synod"}},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(lifetime),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, private.Public(), private)
	if err != nil {
		return nil, nil, fmt.Errorf("sign API CA cert: %w", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("encode API CA key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return certPEM, keyPEM, nil
}

func issueAPICertificate(caCertPEM, caKeyPEM []byte, names []string, now time.Time, lifetime time.Duration) ([]byte, []byte, error) {
	caBlock, _ := pem.Decode(caCertPEM)
	if caBlock == nil {
		return nil, nil, errors.New("invalid API CA certificate PEM")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse API CA certificate: %w", err)
	}
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return nil, nil, errors.New("invalid API CA key PEM")
	}
	caKeyParsed, err := x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse API CA key: %w", err)
	}
	caKey, ok := caKeyParsed.(crypto.Signer)
	if !ok {
		return nil, nil, errors.New("API CA key is not a signer")
	}

	private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate API key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate API cert serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "synod-api"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(lifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	for _, name := range names {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, name)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, private.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("sign API certificate: %w", err)
	}
	keyBytes, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("encode API key: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return certPEM, keyPEM, nil
}

func controllerIdentity(authority *pki.CA, installation uuid.UUID, address string) (certificatePEM, keyPEM []byte, err error) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate the controller key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, private)
	if err != nil {
		return nil, nil, fmt.Errorf("create the controller certificate request: %w", err)
	}
	subject, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, fmt.Errorf("generate the controller identity: %w", err)
	}
	names, err := controllerNames(address)
	if err != nil {
		return nil, nil, err
	}
	certificate, err := authority.Issue(csr, pki.Identity{
		InstallationID: installation, SubjectID: subject, Role: pki.Controller,
	}, names, time.Now(), controllerCertLifetime)
	if err != nil {
		return nil, nil, fmt.Errorf("issue the controller certificate for %s: %w", subject, err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, fmt.Errorf("encode the controller key: %w", err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func controllerNames(address string) ([]string, error) {
	declared := os.Getenv("SYNOD_CONTROLLER_NAMES")
	if declared != "" {
		var names []string
		for name := range strings.SplitSeq(declared, ",") {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				names = append(names, trimmed)
			}
		}
		if len(names) == 0 {
			return nil, errors.New("SYNOD_CONTROLLER_NAMES is set but names nothing")
		}
		return names, nil
	}
	names := []string{"localhost", "127.0.0.1"}
	if address != "" && address != "127.0.0.1" && address != "localhost" {
		names = append(names, address)
	}
	return names, nil
}

func installationID(declared string) (uuid.UUID, error) {
	if declared == "" {
		return uuid.NewRandom()
	}
	id, err := uuid.Parse(declared)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse installation-id %q: %w", declared, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, errors.New("installation-id must be a nonzero UUID")
	}
	return id, nil
}

func bootstrapMetadataContainer(ctx context.Context, dir, password string, output io.Writer) (string, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", fmt.Errorf("connect to Docker: %w", err)
	}
	defer cli.Close()

	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		return "", fmt.Errorf("Docker daemon is unreachable: %w; start Docker or supply --database-url", err)
	}

	containerName := "synod-metadata"
	inspect, err := cli.ContainerInspect(ctx, containerName, client.ContainerInspectOptions{})
	if err == nil {
		if inspect.Container.State != nil && !inspect.Container.State.Running {
			if _, err := cli.ContainerStart(ctx, inspect.Container.ID, client.ContainerStartOptions{}); err != nil {
				return "", fmt.Errorf("start existing metadata container: %w", err)
			}
		}
	} else {
		port, err := findAvailablePort(5432)
		if err != nil {
			return "", fmt.Errorf("find port for metadata database: %w", err)
		}
		if _, err := cli.ImageInspect(ctx, metadataPostgresImage); err != nil {
			reader, pullErr := cli.ImagePull(ctx, metadataPostgresImage, client.ImagePullOptions{})
			if pullErr == nil {
				io.Copy(io.Discard, reader)
				reader.Close()
			}
		}
		tcpPort, err := network.ParsePort("5432/tcp")
		if err != nil {
			return "", fmt.Errorf("parse port: %w", err)
		}
		cfg := &container.Config{
			Image: metadataPostgresImage,
			Env: []string{
				"POSTGRES_USER=synod",
				"POSTGRES_PASSWORD=" + password,
				"POSTGRES_DB=synod",
			},
			ExposedPorts: network.PortSet{tcpPort: struct{}{}},
		}
		hostCfg := &container.HostConfig{
			PortBindings: network.PortMap{
				tcpPort: []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
			},
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: "synod-metadata-data", Target: "/var/lib/postgresql/data"},
			},
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		}
		created, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
			Name:       containerName,
			Config:     cfg,
			HostConfig: hostCfg,
		})
		if err != nil {
			return "", fmt.Errorf("create metadata container: %w", err)
		}
		if _, err := cli.ContainerStart(ctx, created.ID, client.ContainerStartOptions{}); err != nil {
			return "", fmt.Errorf("start metadata container: %w", err)
		}
	}

	inspect, err = cli.ContainerInspect(ctx, containerName, client.ContainerInspectOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect metadata container: %w", err)
	}
	port := 5432
	if inspect.Container.HostConfig != nil {
		tcpPort, _ := network.ParsePort("5432/tcp")
		bindings := inspect.Container.HostConfig.PortBindings[tcpPort]
		if len(bindings) > 0 && bindings[0].HostPort != "" {
			p, err := strconv.Atoi(bindings[0].HostPort)
			if err == nil && p > 0 {
				port = p
			}
		}
	}

	databaseURL := fmt.Sprintf("postgres://synod:%s@127.0.0.1:%d/synod?sslmode=disable", password, port)
	if err := waitForPostgres(ctx, databaseURL, 30*time.Second); err != nil {
		return "", fmt.Errorf("wait for metadata database: %w", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return "", fmt.Errorf("connect to metadata database for migrations: %w", err)
	}
	defer pool.Close()

	store := postgres.New(pool)
	if err := store.Migrate(ctx); err != nil {
		return "", fmt.Errorf("migrate metadata database: %w", err)
	}

	return databaseURL, nil
}

func waitForPostgres(ctx context.Context, dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			pingErr := pool.Ping(ctx)
			pool.Close()
			if pingErr == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("timed out waiting for PostgreSQL to be ready")
}

func findAvailablePort(preferred int) (int, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", preferred))
	if err == nil {
		ln.Close()
		return preferred, nil
	}
	ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
