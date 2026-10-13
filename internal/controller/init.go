package controller

import (
	"cmp"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/synod/internal/config"
	"github.com/sirrobot01/synod/internal/jointoken"
	"github.com/sirrobot01/synod/internal/pki"
	"github.com/sirrobot01/synod/internal/repository"
	"github.com/sirrobot01/synod/internal/store"
	"github.com/spf13/cobra"
)

// caLifetime is how long an installation CA is valid.
const caLifetime = 10 * 365 * 24 * time.Hour

// controllerCertLifetime is how long the controller transport certificate lasts.
const controllerCertLifetime = 30 * 24 * time.Hour

// InitOptions specifies settings for initializing a Synod controller.
type InitOptions struct {
	Address               string
	Directory             string
	InstallationID        string
	ServiceDir            string
	FromRepository        string
	PasswordFile          string
	RepositoryDestination string
}

type materialResult struct {
	id           uuid.UUID
	wrappingID   uuid.UUID
	caCertPEM    []byte
	apiCACertPEM []byte
	adminToken   string
}

type controllerJSON struct {
	InstallationID   string `json:"installation_id"`
	Listen           string `json:"listen"`
	AgentListen      string `json:"agent_listen"`
	DatabasePath     string `json:"database_path"`
	AdminTokenFile   string `json:"admin_token_file"`
	TLSCertFile      string `json:"tls_cert_file"`
	TLSKeyFile       string `json:"tls_key_file"`
	CACertFile       string `json:"ca_cert_file"`
	CAKeyFile        string `json:"ca_key_file"`
	APICACertFile    string `json:"api_ca_cert_file"`
	APICAKeyFile     string `json:"api_ca_key_file"`
	AgentTLSCertFile string `json:"agent_tls_cert_file"`
	AgentTLSKeyFile  string `json:"agent_tls_key_file"`
	AgentCAFile      string `json:"agent_ca_file"`
	SecretKeyFile    string `json:"secret_key_file"`
	SecretKeyID      string `json:"secret_key_id"`
}

func initCommand() *cobra.Command {
	var options InitOptions
	command := &cobra.Command{
		Use:   "init",
		Short: "Initialize a controller installation with its material and configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("address") {
				options.Address = hostAddress()
			}
			return runInit(cmd.Context(), options, cmd.OutOrStdout())
		},
	}
	command.Flags().StringVar(&options.Address, "address", "", "Routable address or hostname for the controller (default: this host's address on its default route)")
	command.Flags().StringVar(&options.Directory, "dir", config.DefaultControllerDir, "Directory to write the installation material and configuration into")
	command.Flags().StringVar(&options.InstallationID, "installation-id", "", "Installation UUID; one is generated when absent")
	command.Flags().StringVar(&options.ServiceDir, serviceDirFlag, defaultServiceDir, "Service directory (a different directory writes the file without starting it)")
	command.Flags().StringVar(&options.FromRepository, "from-repository", "", "Recover from a repository directory or private connection JSON file")
	command.Flags().StringVar(&options.PasswordFile, "password-file", "", "Private file with the repository password")
	command.Flags().StringVar(&options.RepositoryDestination, "repository-destination", "", "Saved destination name or UUID of the surviving repository")
	return command
}

// hostAddress returns the address that this host uses for its default route.
// A UDP dial selects the route and sends no packet. It returns 127.0.0.1 without a route.
func hostAddress() string {
	connection, err := net.Dial("udp", "192.0.2.1:9")
	if err != nil {
		return "127.0.0.1"
	}
	defer connection.Close()
	return connection.LocalAddr().(*net.UDPAddr).IP.String()
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
	var catalog repository.Catalog
	var joinNode uuid.UUID
	var password string
	complete := false
	if options.FromRepository != "" {
		var err error
		catalog, joinNode, password, err = readRecovery(ctx, options)
		if err != nil {
			return err
		}
		options.InstallationID = catalog.InstallationID.String()
		if err := os.Mkdir(dir, 0o700); err != nil {
			return fmt.Errorf("create recovery directory: %w; use a new --dir", err)
		}
		defer func() {
			if !complete {
				os.RemoveAll(dir)
			}
		}()
	} else if options.PasswordFile != "" || options.RepositoryDestination != "" {
		return errors.New("--password-file and --repository-destination require --from-repository")
	}
	res, err := writeMaterialWithAddress(dir, options.InstallationID, address, output)
	if err != nil {
		return err
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		absDir = dir
	}

	cfg := controllerJSON{
		InstallationID:   res.id.String(),
		Listen:           net.JoinHostPort(address, "8443"),
		AgentListen:      net.JoinHostPort(address, "8444"),
		DatabasePath:     filepath.Join(absDir, "synod.db"),
		AdminTokenFile:   filepath.Join(absDir, "admin.token"),
		TLSCertFile:      filepath.Join(absDir, "api.crt"),
		TLSKeyFile:       filepath.Join(absDir, "api.key"),
		CACertFile:       filepath.Join(absDir, "ca.crt"),
		CAKeyFile:        filepath.Join(absDir, "ca.key"),
		APICACertFile:    filepath.Join(absDir, "api-ca.crt"),
		APICAKeyFile:     filepath.Join(absDir, "api-ca.key"),
		AgentTLSCertFile: filepath.Join(absDir, "agent-tls.crt"),
		AgentTLSKeyFile:  filepath.Join(absDir, "agent-tls.key"),
		AgentCAFile:      filepath.Join(absDir, "ca.crt"),
		SecretKeyFile:    filepath.Join(absDir, "secret.key"),
		SecretKeyID:      res.wrappingID.String(),
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
	serviceContent, err := controllerService(executable, configPath)
	if err != nil {
		return err
	}
	serviceLocalPath := filepath.Join(absDir, serviceFileName)
	if err := os.WriteFile(serviceLocalPath, []byte(serviceContent), 0o644); err != nil {
		return fmt.Errorf("write service file: %w", err)
	}

	metadata, err := store.Open(ctx, cfg.DatabasePath)
	if err != nil {
		return err
	}
	defer metadata.Close()
	ring, err := loadKeyring(cfg.SecretKeyFile, cfg.SecretKeyID)
	if err != nil {
		return err
	}
	if options.FromRepository != "" {
		if err := metadata.ImportCatalog(ctx, catalog, ring); err != nil {
			return fmt.Errorf("import recovery catalog: %w", err)
		}
	} else if password, err = newRepositoryPassword(); err != nil {
		return err
	}
	// A recovered repository keeps its password. Shorter custom passwords stay explicit per destination.
	kit := ""
	if len(password) >= 16 {
		if err := metadata.SetRepositoryPassword(ctx, res.id, ring, password); err != nil {
			return fmt.Errorf("save the recovery kit password: %w", err)
		}
		kit = recoveryKit(res.id, password, time.Now().UTC())
		if err := os.WriteFile(filepath.Join(absDir, "recovery-kit.txt"), []byte(kit), 0o600); err != nil {
			return fmt.Errorf("write the recovery kit: %w", err)
		}
	}
	encoded, err := registerJoinToken(ctx, metadata, res.id, joinNode, address, res.apiCACertPEM)
	if err != nil {
		return err
	}
	joinNote := fmt.Sprintf("Join token for one device (valid for 7 days):\n  %s\n\nTo join a device, run on that device:\n  synod agent join %s\n\nTo join more devices, run synod node join-token for each one.", encoded, encoded)

	if err := metadata.Close(); err != nil {
		return err
	}
	complete = true

	if options.FromRepository != "" {
		joinNote += fmt.Sprintf("\n\nRecovered %d devices, %d apps, and %d destinations.\nNew controller keys were created. Stop old agents before recovery.\nUse a new agent --dir, then approve each device.\nRestored apps live outside the agent directory and keep running.\nFor each other saved device, run synod node join-token --device DEVICE.\nOld activity records and approval are not restored.", len(catalog.Devices), len(catalog.Apps), len(catalog.Destinations))
		if joinNode != uuid.Nil {
			joinNote += fmt.Sprintf("\nThis join token reclaims device %s. Join it on the host with the repository directory.", joinNode)
		}
	}
	serviceNote := installControllerService(ctx, serviceContent, serviceLocalPath, configPath, cmp.Or(options.ServiceDir, defaultServiceDir))

	kitNote := "No recovery kit was written. The recovered repository password is shorter than 16 characters.\nEach new destination needs its password in its credentials file."
	if kit != "" {
		kitNote = fmt.Sprintf("%s\nThe recovery kit is also in %s.\nCopy it to a password manager or to paper, away from your Synod devices. Then delete the file.", kit, filepath.Join(absDir, "recovery-kit.txt"))
	}
	// A recovery already used the password, so the output does not repeat it.
	if kit != "" && options.FromRepository != "" {
		kitNote = fmt.Sprintf("The recovery kit in %s holds the password that opened the repository.\nNew destinations use it unless you give another password.", filepath.Join(absDir, "recovery-kit.txt"))
	}
	report := fmt.Sprintf(`Synod installation %s initialized at %s.

Metadata database:
  %s

Controller configuration written to:
  %s

%s

%s

%s

%s
`,
		res.id, absDir,
		cfg.DatabasePath,
		configPath,
		serviceNote,
		kitNote,
		joinNote,
		adminNote(absDir, address),
	)

	_, err = fmt.Fprint(output, report)
	return err
}

// registerJoinToken stores the verifier of a one-use join token.
func registerJoinToken(ctx context.Context, metadata *store.Store, installationID, nodeID uuid.UUID, address string, apiCACertPEM []byte) (string, error) {
	block, _ := pem.Decode(apiCACertPEM)
	if block == nil {
		return "", errors.New("decode the API CA certificate for the join token")
	}
	token := jointoken.Token{
		Address:        net.JoinHostPort(address, "8443"),
		InstallationID: installationID,
		NodeID:         nodeID,
		Fingerprint:    jointoken.Fingerprint(block.Bytes),
		Secret:         jointoken.NewSecret(),
	}
	if _, err := metadata.RegisterEnrollmentToken(ctx, store.TokenRegistration{
		NodeID:         nodeID,
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
		id:           id,
		wrappingID:   wrappingID,
		caCertPEM:    certificatePEM,
		apiCACertPEM: apiCACertPEM,
		adminToken:   adminToken,
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

// newRepositoryPassword returns 160 random bits in groups of four base32 characters.
func newRepositoryPassword() (string, error) {
	raw := make([]byte, 20)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate the repository password: %w", err)
	}
	encoded := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw))
	var groups []string
	for len(encoded) > 0 {
		groups = append(groups, encoded[:4])
		encoded = encoded[4:]
	}
	return strings.Join(groups, "-"), nil
}

// recoveryKit is the text that the user keeps away from Synod devices.
func recoveryKit(installationID uuid.UUID, password string, created time.Time) string {
	return fmt.Sprintf(`SYNOD RECOVERY KIT
Installation: %s
Created: %s

Repository password:
  %s

Without this password, nobody can read your backups, including Synod.
Each destination that you add without its own password uses this password.

To recover all apps after you lose the controller:
1. Install Synod on a new device.
2. Write the password into a private file:
     umask 077; printf '%%s' 'PASSWORD' > "$HOME/repository-password"
3. Rebuild the installation from a surviving repository:
     synod server init --address ADDRESS \
       --from-repository REPOSITORY --password-file "$HOME/repository-password"
   REPOSITORY is the repository directory, or a private connection JSON file for S3 or SFTP.
4. Join the devices again and restore each app with synod restore APP --to DEVICE.
`, installationID, created.Format(time.RFC3339), password)
}

// adminNote tells the operator how to run synod commands. The CLI reads the default
// directory by itself, so only another directory needs the settings.
func adminNote(dir, address string) string {
	if dir == config.DefaultControllerDir {
		return fmt.Sprintf("Run synod commands as root on this device. Open the web UI at https://%s after synod user create NAME.", net.JoinHostPort(address, "8443"))
	}
	return fmt.Sprintf(`To administer this installation:
  export SYNOD_API_URL='%s'
  export SYNOD_CA_FILE='%s'
  export SYNOD_ADMIN_TOKEN_FILE='%s'`, strings.ReplaceAll("https://"+net.JoinHostPort(address, "8443"), "'", "'\"'\"'"), strings.ReplaceAll(filepath.Join(dir, "api-ca.crt"), "'", "'\"'\"'"), strings.ReplaceAll(filepath.Join(dir, "admin.token"), "'", "'\"'\"'"))
}
